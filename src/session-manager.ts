import makeWASocket, {
  DisconnectReason,
  useMultiFileAuthState,
  WASocket,
  WAMessage,
  fetchLatestBaileysVersion,
  makeCacheableSignalKeyStore,
  proto,
} from '@whiskeysockets/baileys'
import { Boom } from '@hapi/boom'
import pino from 'pino'
import * as fs from 'fs'
import * as path from 'path'
import * as QRCode from 'qrcode'

const logger = pino({ level: process.env.LOG_LEVEL ?? 'silent' })

const BACKEND_URL  = process.env.BACKEND_URL  ?? 'http://localhost:8080'
const BRIDGE_SECRET = process.env.BRIDGE_SECRET ?? 'change-me'
const SESSIONS_DIR  = process.env.SESSIONS_DIR  ?? './sessions'

// ── Estado en memoria de cada sesión ─────────────────────────────────────────

type SessionStatus = 'disconnected' | 'connecting' | 'qr' | 'connected'

interface SessionState {
  socket:    WASocket | null
  status:    SessionStatus
  qr:        string | null   // QR string crudo (antes de convertir a PNG)
  phone:     string | null   // Número vinculado (ej: "573001234567")
  adminId:   number
}

const sessions = new Map<number, SessionState>()

/**
 * Mapa por sesión: LID (sin @lid) → número de teléfono real (sin @s.whatsapp.net).
 * WhatsApp con privacidad avanzada oculta el número real en el JID del remitente
 * y usa un LID (Linked Identity). Baileys notifica el mapeo via contacts.upsert.
 * El mapa se persiste a disco (lid-map.json) para sobrevivir reinicios.
 */
const lidToPhone = new Map<number, Map<string, string>>()

// ── Helpers ───────────────────────────────────────────────────────────────────

function sessionDir(adminId: number): string {
  return path.join(SESSIONS_DIR, String(adminId))
}

function lidMapFile(adminId: number): string {
  return path.join(sessionDir(adminId), 'lid-map.json')
}

/** Carga el mapa LID→teléfono persistido en disco al iniciar la sesión. */
function loadLidMap(adminId: number): Map<string, string> {
  const file = lidMapFile(adminId)
  if (fs.existsSync(file)) {
    try {
      const raw = JSON.parse(fs.readFileSync(file, 'utf-8')) as Record<string, string>
      return new Map(Object.entries(raw))
    } catch { /* ignorar JSON corrupto */ }
  }
  return new Map()
}

/** Persiste el mapa LID→teléfono a disco de forma asíncrona. */
function saveLidMap(adminId: number, map: Map<string, string>): void {
  try {
    const obj = Object.fromEntries(map)
    fs.writeFileSync(lidMapFile(adminId), JSON.stringify(obj), 'utf-8')
  } catch { /* ignorar errores de escritura */ }
}

function getOrCreate(adminId: number): SessionState {
  if (!sessions.has(adminId)) {
    sessions.set(adminId, {
      socket:  null,
      status:  'disconnected',
      qr:      null,
      phone:   null,
      adminId,
    })
  }
  return sessions.get(adminId)!
}

/** Extrae solo los dígitos del número desde el JID de baileys.
 *  Ej: "573001234567:1@s.whatsapp.net" → "573001234567"
 */
function jidToPhone(jid: string): string {
  return jid.replace(/@.+/, '').replace(/:.*/, '')
}

/** Notifica a call-monitor sobre un mensaje recibido o enviado. */
async function forwardMessage(
  adminId:   number,
  fromPhone: string,
  toPhone:   string,
  body:      string,
  direction: 'INBOUND' | 'OUTBOUND',
  wamid:     string,
  timestamp: number,
): Promise<void> {
  try {
    const res = await fetch(`${BACKEND_URL}/api/internal/wa-bridge/message`, {
      method:  'POST',
      headers: {
        'Content-Type':  'application/json',
        'Authorization': `Bearer ${BRIDGE_SECRET}`,
      },
      body: JSON.stringify({ adminId, fromPhone, toPhone, body, direction, wamid, timestamp }),
    })
    if (!res.ok) {
      logger.error({ adminId, status: res.status }, 'bridge forward failed')
    }
  } catch (err) {
    logger.error({ adminId, err }, 'bridge forward error')
  }
}

/** Notifica el cambio de estado de la sesión a call-monitor. */
async function notifyStatus(
  adminId: number,
  status:  SessionStatus,
  phone:   string | null,
): Promise<void> {
  try {
    await fetch(`${BACKEND_URL}/api/internal/wa-bridge/status`, {
      method:  'POST',
      headers: {
        'Content-Type':  'application/json',
        'Authorization': `Bearer ${BRIDGE_SECRET}`,
      },
      body: JSON.stringify({ adminId, status, phone }),
    })
  } catch {
    /* silencioso — call-monitor puede estar arrancando */
  }
}

// ── Ciclo de vida de la sesión ────────────────────────────────────────────────

export async function startSession(adminId: number): Promise<void> {
  const state = getOrCreate(adminId)

  if (state.status === 'connected' || state.status === 'connecting') {
    return // ya hay una sesión activa o en proceso
  }

  state.status = 'connecting'
  state.qr     = null

  const dir = sessionDir(adminId)
  fs.mkdirSync(dir, { recursive: true })

  const { state: authState, saveCreds } = await useMultiFileAuthState(dir)
  const { version } = await fetchLatestBaileysVersion()

  const sock = makeWASocket({
    version,
    logger,
    auth:          {
      creds: authState.creds,
      keys:  makeCacheableSignalKeyStore(authState.keys, logger),
    },
    browser:       ['ZentCall', 'Chrome', '120.0.0'],
    generateHighQualityLinkPreview: false,
    syncFullHistory: false,
  })

  state.socket = sock

  // ── Mapa LID → teléfono para esta sesión (cargado desde disco) ───────────
  if (!lidToPhone.has(adminId)) {
    lidToPhone.set(adminId, loadLidMap(adminId))
  }
  const contacts = lidToPhone.get(adminId)!

  // ── Actualización de credenciales ──────────────────────────────────────────
  sock.ev.on('creds.update', saveCreds)

  /**
   * contacts.upsert — Baileys envía actualizaciones de contactos cuando recibe
   * mensajes de usuarios con privacidad avanzada (LID). Cada contacto puede
   * tener `id` como JID real y `lid` como el identificador opaco, o viceversa.
   * Construimos el mapa en ambas direcciones para máxima cobertura.
   */
  sock.ev.on('contacts.upsert', (contactList) => {
    for (const c of contactList) {
      if (!c.id) continue
      const rawId  = c.id.replace(/@.+/, '').replace(/:.*/, '')
      if ((c as any).lid) {
        const rawLid = String((c as any).lid).replace(/@.+/, '').replace(/:.*/, '')
        if (rawId && rawLid && rawId !== rawLid) {
          contacts.set(rawLid, rawId)   // LID → teléfono real
          contacts.set(rawId,  rawLid)  // inverso (por si acaso)
          logger.info({ adminId, lid: rawLid, phone: rawId }, 'Contact LID mapped')
          saveLidMap(adminId, contacts) // persistir a disco
        }
      }
    }
  })

  // ── Estado de la conexión ──────────────────────────────────────────────────
  sock.ev.on('connection.update', async (update) => {
    const { connection, lastDisconnect, qr } = update

    if (qr) {
      state.qr     = qr
      state.status = 'qr'
      logger.info({ adminId }, 'QR generated — scan to connect')
    }

    if (connection === 'open') {
      state.status = 'connected'
      state.qr     = null
      const phone  = sock.user ? jidToPhone(sock.user.id) : null
      state.phone  = phone
      logger.info({ adminId, phone }, 'WhatsApp session connected')
      await notifyStatus(adminId, 'connected', phone)
    }

    if (connection === 'close') {
      const reason = (lastDisconnect?.error as Boom)?.output?.statusCode
      logger.warn({ adminId, reason }, 'WhatsApp session closed')

      const shouldReconnect = reason !== DisconnectReason.loggedOut

      state.status = 'disconnected'
      state.socket = null
      state.phone  = null
      await notifyStatus(adminId, 'disconnected', null)

      if (shouldReconnect) {
        // Reconexión automática tras errores de red (ej: 408 timeout)
        logger.info({ adminId }, 'Reconnecting in 5s…')
        setTimeout(() => startSession(adminId), 5_000)
      } else {
        // Sesión cerrada por logout — borrar sesión guardada
        logger.info({ adminId }, 'Logged out — removing session files')
        fs.rmSync(sessionDir(adminId), { recursive: true, force: true })
      }
    }
  })

  // ── Mensajes entrantes y salientes ─────────────────────────────────────────
  sock.ev.on('messages.upsert', async ({ messages, type }) => {
    if (type !== 'notify') return // 'append' = histórico, ignorar

    const myPhone = state.phone ?? ''

    for (const msg of messages) {
      try {
        await processMessage(adminId, msg, myPhone)
      } catch (err) {
        logger.error({ adminId, err }, 'Error processing message')
      }
    }
  })
}

async function processMessage(
  adminId: number,
  msg: WAMessage,
  myPhone: string,
): Promise<void> {
  const { key, message, messageTimestamp } = msg
  if (!key || !message) return

  // Extraer texto del mensaje (solo texto plano por ahora)
  const body =
    message.conversation ??
    message.extendedTextMessage?.text ??
    null

  if (!body || body.trim() === '') return // ignorar stickers, imágenes, audio, etc.

  const wamid     = key.id ?? ''
  const remoteJid = key.remoteJid ?? ''
  const fromMe    = key.fromMe ?? false
  const timestamp = typeof messageTimestamp === 'number'
    ? messageTimestamp
    : Number(messageTimestamp ?? Math.floor(Date.now() / 1000))

  // Ignorar mensajes de grupos y broadcast
  if (remoteJid.endsWith('@g.us') || remoteJid === 'status@broadcast') return

  let remote = jidToPhone(remoteJid)

  // Resolver LID → teléfono real si el JID es un Linked Identity (privacidad avanzada).
  // contacts.upsert puede llegar ligeramente DESPUÉS de messages.upsert (race condition),
  // así que reintentamos hasta 5 veces con 200 ms de espera entre intentos (1 s total).
  if (remoteJid.endsWith('@lid') || remoteJid.endsWith('@newsletter')) {
    const MAX_RETRIES = 5
    const DELAY_MS    = 200
    let resolved: string | undefined

    for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
      resolved = lidToPhone.get(adminId)?.get(remote)
      if (resolved) break
      if (attempt < MAX_RETRIES - 1) {
        await new Promise(r => setTimeout(r, DELAY_MS))
      }
    }

    if (resolved) {
      logger.info({ adminId, lid: remote, phone: resolved, attempt: 'ok' }, 'Resolved LID to real phone')
      remote = resolved
    } else {
      logger.warn({ adminId, jid: remoteJid, lid: remote }, 'LID not resolved after retries — forwarding LID as-is')
    }
  }

  const fromPhone = fromMe ? myPhone : remote
  const toPhone   = fromMe ? remote  : myPhone
  const direction = fromMe ? 'OUTBOUND' : 'INBOUND'

  await forwardMessage(adminId, fromPhone, toPhone, body, direction, wamid, timestamp)
}

// ── API pública del manager ───────────────────────────────────────────────────

export async function stopSession(adminId: number): Promise<void> {
  const state = sessions.get(adminId)
  if (!state) return

  try {
    await state.socket?.logout()
  } catch { /* puede fallar si ya está desconectado */ }

  state.socket = null
  state.status = 'disconnected'
  state.qr     = null
  state.phone  = null

  // Borrar archivos de sesión
  fs.rmSync(sessionDir(adminId), { recursive: true, force: true })
  sessions.delete(adminId)
  lidToPhone.delete(adminId)
  await notifyStatus(adminId, 'disconnected', null)
}

export function getStatus(adminId: number): { status: SessionStatus; phone: string | null } {
  const state = sessions.get(adminId)
  return {
    status: state?.status ?? 'disconnected',
    phone:  state?.phone  ?? null,
  }
}

export async function getQrBase64(adminId: number): Promise<string | null> {
  const state = sessions.get(adminId)
  if (!state?.qr) return null

  try {
    return await QRCode.toDataURL(state.qr, { type: 'image/png', width: 256, margin: 2 })
  } catch {
    return null
  }
}

export async function sendTextMessage(
  adminId: number,
  toPhone: string,
  text:    string,
): Promise<{ wamid: string }> {
  const state = sessions.get(adminId)
  if (!state?.socket || state.status !== 'connected') {
    throw new Error(`No hay sesión activa para adminId=${adminId}`)
  }

  // Formatear JID: "573001234567" → "573001234567@s.whatsapp.net"
  const jid     = toPhone.replace(/\D/g, '') + '@s.whatsapp.net'
  const result  = await state.socket.sendMessage(jid, { text })
  const wamid   = result?.key?.id ?? ''

  return { wamid }
}

/** Reconectar sesiones persistidas al arrancar el servicio. */
export async function restorePersistedSessions(): Promise<void> {
  if (!fs.existsSync(SESSIONS_DIR)) return

  const dirs = fs.readdirSync(SESSIONS_DIR)
  for (const d of dirs) {
    const adminId = parseInt(d, 10)
    if (isNaN(adminId)) continue

    const credsFile = path.join(SESSIONS_DIR, d, 'creds.json')
    if (!fs.existsSync(credsFile)) continue

    logger.info({ adminId }, 'Restoring persisted session…')
    // No await — iniciar en paralelo para no bloquear el arranque
    startSession(adminId).catch(err =>
      logger.error({ adminId, err }, 'Failed to restore session'),
    )
  }
}
