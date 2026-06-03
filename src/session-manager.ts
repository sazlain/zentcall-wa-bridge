import makeWASocket, {
  DisconnectReason,
  useMultiFileAuthState,
  WASocket,
  WAMessage,
  fetchLatestBaileysVersion,
  makeCacheableSignalKeyStore,
  downloadMediaMessage,
  proto,
} from '@whiskeysockets/baileys'
import { Boom } from '@hapi/boom'
import pino from 'pino'
import * as fs from 'fs'
import * as path from 'path'
import * as QRCode from 'qrcode'

const logger = pino({ level: process.env.LOG_LEVEL ?? 'info' })

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

/**
 * Mapa temporal: wamid → toPhone (sin símbolos).
 * Permite resolver el LID cuando llega el echo de un mensaje saliente:
 * el echo puede tener remoteJid=LID@lid pero el wamid coincide con el envío.
 */
const pendingOutbound = new Map<number, Map<string, string>>() // adminId → (wamid → toPhone)

/**
 * Todos los contactos recibidos desde Baileys (cualquier evento) por sesión.
 * Clave: JID completo (ej: "573157665297@s.whatsapp.net").
 * Permite buscar el teléfono real cuando un LID no está en lidToPhone.
 */
const allContacts = new Map<number, Map<string, any>>() // adminId → (jid → contact)

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

interface MediaPayload {
  mediaType:   string   // "image" | "video" | "document" | "audio"
  mediaBase64: string
  mimetype:    string
  fileName?:   string
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
  media?:    MediaPayload,
): Promise<void> {
  try {
    const payload: Record<string, unknown> = {
      adminId, fromPhone, toPhone, body, direction, wamid, timestamp,
    }
    if (media) {
      payload.mediaType   = media.mediaType
      payload.mediaBase64 = media.mediaBase64
      payload.mimetype    = media.mimetype
      if (media.fileName) payload.fileName = media.fileName
    }
    const res = await fetch(`${BACKEND_URL}/api/internal/wa-bridge/message`, {
      method:  'POST',
      headers: {
        'Content-Type':  'application/json',
        'Authorization': `Bearer ${BRIDGE_SECRET}`,
      },
      body: JSON.stringify(payload),
    })
    if (!res.ok) {
      logger.error({ adminId, status: res.status }, 'bridge forward failed')
    }
  } catch (err) {
    logger.error({ adminId, err }, 'bridge forward error')
  }
}

/**
 * Detecta si el mensaje contiene media y devuelve la info necesaria para descargarlo.
 * Devuelve null para mensajes sin media (texto, reacciones, stickers ignorados).
 */
function detectMediaInfo(message: proto.IMessage): { mediaType: string; mimetype: string; fileName?: string } | null {
  if (message.imageMessage)    return { mediaType: 'image',    mimetype: message.imageMessage.mimetype    ?? 'image/jpeg' }
  if (message.videoMessage)    return { mediaType: 'video',    mimetype: message.videoMessage.mimetype    ?? 'video/mp4'  }
  if (message.audioMessage)    return { mediaType: 'audio',    mimetype: message.audioMessage.mimetype    ?? 'audio/ogg'  }
  if (message.documentMessage) return {
    mediaType: 'document',
    mimetype:  message.documentMessage.mimetype  ?? 'application/octet-stream',
    fileName:  message.documentMessage.fileName  ?? undefined,
  }
  return null
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

  // ── Inicializar colección de contactos para esta sesión ──────────────────
  if (!allContacts.has(adminId)) {
    allContacts.set(adminId, new Map())
  }

  // ── Mapa LID → teléfono para esta sesión (cargado desde disco) ───────────
  if (!lidToPhone.has(adminId)) {
    lidToPhone.set(adminId, loadLidMap(adminId))
  }
  const contacts = lidToPhone.get(adminId)!

  // ── Actualización de credenciales ──────────────────────────────────────────
  sock.ev.on('creds.update', saveCreds)

  /** Procesa un array de contactos y actualiza el mapa LID↔teléfono. */
  function indexContacts(contactList: any[]): void {
    let changed = false
    const sessionAllContacts = allContacts.get(adminId)!
    for (const c of contactList) {
      if (!c.id) continue

      // Guardar contacto completo para fallback de búsqueda por LID
      sessionAllContacts.set(c.id, c)

      const rawId  = c.id.replace(/@.+/, '').replace(/:.*/, '')
      // LID puede venir en c.lid (campo propio) o cuando c.id termina en @lid
      const lidRaw  = (c as any).lid
        ? String((c as any).lid).replace(/@.+/, '').replace(/:.*/, '')
        : c.id.endsWith('@lid') ? rawId : null

      const phoneRaw = (c as any).lid
        ? rawId
        : (c as any).phone ?? null

      if (lidRaw && phoneRaw && lidRaw !== phoneRaw) {
        if (!contacts.has(lidRaw) || contacts.get(lidRaw) !== phoneRaw) {
          contacts.set(lidRaw,   phoneRaw)  // LID → teléfono real
          contacts.set(phoneRaw, lidRaw)    // inverso
          logger.info({ adminId, lid: lidRaw, phone: phoneRaw }, 'LID mapped')
          changed = true
        }
      }
    }
    if (changed) saveLidMap(adminId, contacts)
  }

  /**
   * contacts.upsert — contactos nuevos/actualizados individualmente.
   * contacts.update — cambios en contactos existentes.
   * messaging-history.set — sync inicial; trae la lista COMPLETA de contactos
   *   con sus LIDs (fuente más fiable en Baileys 6.7.x).
   */
  sock.ev.on('contacts.upsert', indexContacts)
  sock.ev.on('contacts.update', indexContacts)
  sock.ev.on('messaging-history.set', ({ contacts: histContacts }) => {
    if (histContacts?.length) {
      logger.info({ adminId, count: histContacts.length }, 'Indexing contacts from history sync')
      indexContacts(histContacts)
      // Leer también sock.contacts porque Baileys los habrá mergeado ya
      const cached = Object.values((sock as any).contacts ?? {}) as any[]
      if (cached.length) indexContacts(cached)
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

      // Leer el caché interno de Baileys; puede incluir .lid de sesiones previas
      const cached = Object.values((sock as any).contacts ?? {}) as any[]
      if (cached.length > 0) {
        logger.info({ adminId, count: cached.length }, 'Indexing contacts from internal cache on connect')
        indexContacts(cached)
      }
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
        await processMessage(adminId, msg, myPhone, sock)
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
  sock: WASocket,
): Promise<void> {
  const { key, message, messageTimestamp } = msg
  if (!key || !message) return

  // Extraer texto / caption del mensaje
  const body =
    message.conversation              ??
    message.extendedTextMessage?.text ??
    message.imageMessage?.caption     ??
    message.videoMessage?.caption     ??
    message.documentMessage?.caption  ??
    null

  // Detectar media adjunta
  const mediaInfo = detectMediaInfo(message)

  // Sin texto ni media → ignorar (stickers, reacciones, ephemeral notices, etc.)
  if (!body && !mediaInfo) return

  const wamid     = key.id ?? ''
  const remoteJid = key.remoteJid ?? ''
  const fromMe    = key.fromMe ?? false
  const timestamp = typeof messageTimestamp === 'number'
    ? messageTimestamp
    : Number(messageTimestamp ?? Math.floor(Date.now() / 1000))

  // Ignorar mensajes de grupos y broadcast
  if (remoteJid.endsWith('@g.us') || remoteJid === 'status@broadcast') return

  let remote = jidToPhone(remoteJid)

  // ── Captura de LID desde echo saliente + deduplicación ───────────────────
  // Cuando enviamos a "573157665297@s.whatsapp.net", el echo llega con
  // remoteJid="187106055983173@lid" (o el JID real). El wamid coincide con
  // el envío en pendingOutbound: capturamos el LID y NO reenviamos a
  // call-monitor (ya guardado por sendViaBridge → evita duplicados en chat).
  if (fromMe) {
    const sentPhone = pendingOutbound.get(adminId)?.get(wamid)
    if (sentPhone) {
      // Capturar LID si el echo vino como @lid
      if (remoteJid.endsWith('@lid') || remoteJid.endsWith('@newsletter')) {
        const lid = remote
        const map = lidToPhone.get(adminId) ?? new Map()
        if (!map.has(lid)) {
          map.set(lid, sentPhone)
          map.set(sentPhone, lid)
          lidToPhone.set(adminId, map)
          saveLidMap(adminId, map)
          logger.info({ adminId, lid, phone: sentPhone }, 'LID captured from outbound echo')
        }
      }
      // El mensaje ya fue guardado por call-monitor al enviar. No reenviar.
      logger.debug({ adminId, wamid }, 'Echo for bridge-API message — skipping forward (already saved)')
      return
    }
    // fromMe=true pero NO está en pendingOutbound → enviado directamente desde
    // el teléfono (fuera de Zentcall) → sí reenviar para registrar.
  }

  // ── Resolver LID → teléfono real (mensajes entrantes) ────────────────────
  // contacts.upsert puede llegar ligeramente DESPUÉS de messages.upsert (race condition),
  // así que reintentamos hasta 5 veces con 200 ms de espera entre intentos (1 s total).
  if (!fromMe && (remoteJid.endsWith('@lid') || remoteJid.endsWith('@newsletter'))) {
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
      logger.info({ adminId, lid: remote, phone: resolved }, 'Resolved LID to real phone')
      remote = resolved
    } else {
      // Fallback: buscar en allContacts un contacto cuyo .lid coincida
      const sessionAllContacts = allContacts.get(adminId)
      if (sessionAllContacts) {
        for (const [jid, contact] of sessionAllContacts) {
          if (jid.endsWith('@lid') || jid.endsWith('@g.us')) continue
          const cLid = contact.lid as string | undefined
          if (cLid && (cLid === remoteJid || cLid.replace(/@.+/, '') === remote)) {
            resolved = jid.replace(/@.+/, '').replace(/:.*/, '')
            const cMap = lidToPhone.get(adminId)!
            cMap.set(remote, resolved)
            cMap.set(resolved, remote)
            saveLidMap(adminId, cMap)
            logger.info({ adminId, lid: remote, phone: resolved }, 'Resolved LID from allContacts fallback')
            break
          }
        }
      }

      if (resolved) {
        remote = resolved
      } else {
        // Último recurso: escanear sock.contacts directamente (cache interno de Baileys)
        const sockContacts = Object.values((sock as any).contacts ?? {}) as any[]
        for (const c of sockContacts) {
          if (!c.id || c.id.endsWith('@lid') || c.id.endsWith('@g.us')) continue
          const cLid = c.lid as string | undefined
          if (cLid && (cLid === remoteJid || cLid.replace(/@.+/, '') === remote)) {
            const resolvedPhone: string = c.id.replace(/@.+/, '').replace(/:.*/, '')
            const cMap = lidToPhone.get(adminId) ?? new Map<string, string>()
            cMap.set(remote, resolvedPhone)
            cMap.set(resolvedPhone, remote)
            lidToPhone.set(adminId, cMap)
            saveLidMap(adminId, cMap)
            // Poblar allContacts también para futuras búsquedas
            allContacts.get(adminId)?.set(c.id, c)
            logger.info({ adminId, lid: remote, phone: resolvedPhone }, 'Resolved LID from sock.contacts (last resort)')
            resolved = resolvedPhone
            break
          }
        }
        if (resolved) {
          remote = resolved
        } else {
          logger.warn({ adminId, jid: remoteJid, lid: remote }, 'LID not resolved — forwarding LID as-is')
        }
      }
    }
  }

  const fromPhone = fromMe ? myPhone : remote
  const toPhone   = fromMe ? remote  : myPhone
  const direction = fromMe ? 'OUTBOUND' : 'INBOUND'

  // Descargar media si existe
  let media: MediaPayload | undefined
  if (mediaInfo) {
    try {
      const buffer = await downloadMediaMessage(
        msg, 'buffer', {},
        { logger, reuploadRequest: sock.updateMediaMessage },
      ) as Buffer
      media = {
        mediaType:   mediaInfo.mediaType,
        mediaBase64: buffer.toString('base64'),
        mimetype:    mediaInfo.mimetype,
        fileName:    mediaInfo.fileName,
      }
      logger.info({ adminId, mediaType: mediaInfo.mediaType, size: buffer.length }, 'Media downloaded')
    } catch (err) {
      logger.error({ adminId, mediaType: mediaInfo.mediaType, err }, 'Failed to download media — forwarding text only')
    }
  }

  await forwardMessage(adminId, fromPhone, toPhone, body ?? '', direction, wamid, timestamp, media)
}

// ── Helpers de envío ─────────────────────────────────────────────────────────

/**
 * Resuelve el JID correcto para enviar un mensaje a `cleanPhone`.
 *
 * Prioridad:
 * 1. LID ya mapeado en memoria (lidToPhone).
 * 2. Consulta `sock.onWhatsApp(phone)` → WA devuelve el JID canónico del
 *    contacto. Para usuarios con privacidad avanzada devuelve `<lid>@lid`;
 *    esto nos da el mapeo y garantiza entrega E2E correcta.
 * 3. Fallback a `<phone>@s.whatsapp.net`.
 *
 * El resultado se persiste en lid-map.json para sesiones futuras.
 */
async function resolveJid(
  adminId:    number,
  cleanPhone: string,
  sock:       WASocket,
): Promise<string> {
  const map = lidToPhone.get(adminId)

  // 1. LID ya en caché (exacto o por sufijo)
  const cachedLid = map?.get(cleanPhone)
    ?? (cleanPhone.length > 10 ? map?.get(cleanPhone.slice(-10)) : undefined)
    ?? (cleanPhone.length > 11 ? map?.get(cleanPhone.slice(-11)) : undefined)

  if (cachedLid && cachedLid !== cleanPhone && cachedLid.length >= 13) {
    return `${cachedLid}@lid`
  }

  // 2. Consultar WA para obtener JID canónico (puede ser @lid)
  try {
    const results = await sock.onWhatsApp(cleanPhone)
    const found   = results?.[0]
    if (found?.exists && found.jid) {
      const canonical = found.jid
      const lidNum    = canonical.replace(/@.+/, '').replace(/:.*/, '')

      if (canonical.endsWith('@lid') && lidNum !== cleanPhone) {
        // Nuevo mapeo: persistir bidireccional
        const cMap = lidToPhone.get(adminId) ?? new Map<string, string>()
        cMap.set(lidNum,    cleanPhone)
        cMap.set(cleanPhone, lidNum)
        lidToPhone.set(adminId, cMap)
        saveLidMap(adminId, cMap)
        logger.info({ adminId, phone: cleanPhone, lid: lidNum }, 'LID discovered via onWhatsApp — cached')
        return `${lidNum}@lid`
      }

      // WA devolvió JID de teléfono normal
      return canonical
    }
  } catch (err) {
    logger.warn({ adminId, phone: cleanPhone, err }, 'onWhatsApp lookup failed — using phone JID')
  }

  // 3. Fallback
  return `${cleanPhone}@s.whatsapp.net`
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

  const cleanPhone = toPhone.replace(/\D/g, '')
  const jid        = await resolveJid(adminId, cleanPhone, state.socket)

  logger.info({ adminId, toPhone: cleanPhone, jid }, 'Sending WA message')

  const result = await state.socket.sendMessage(jid, { text })
  const wamid  = result?.key?.id ?? ''

  // Registrar wamid → toPhone para deduplicar el echo saliente
  // y capturar el LID→phone si el echo llega como @lid.
  if (wamid) {
    if (!pendingOutbound.has(adminId)) pendingOutbound.set(adminId, new Map())
    pendingOutbound.get(adminId)!.set(wamid, cleanPhone)
    setTimeout(() => pendingOutbound.get(adminId)?.delete(wamid), 60_000)
  }

  return { wamid }
}

export async function sendMediaMessage(
  adminId:      number,
  toPhone:      string,
  mediaType:    string,
  mediaBase64:  string,
  mimetype:     string,
  caption?:     string,
): Promise<{ wamid: string }> {
  const state = sessions.get(adminId)
  if (!state?.socket || state.status !== 'connected') {
    throw new Error(`No hay sesión activa para adminId=${adminId}`)
  }

  const cleanPhone = toPhone.replace(/\D/g, '')
  const jid        = await resolveJid(adminId, cleanPhone, state.socket)

  logger.info({ adminId, toPhone: cleanPhone, jid, mediaType }, 'Sending WA media')

  const buf = Buffer.from(mediaBase64, 'base64')
  let content: any
  if (mediaType.startsWith('image/') || mediaType === 'image') {
    content = { image: buf, mimetype, caption: caption ?? '' }
  } else if (mediaType.startsWith('video/') || mediaType === 'video') {
    content = { video: buf, mimetype, caption: caption ?? '' }
  } else {
    content = { document: buf, mimetype, caption: caption ?? '', fileName: 'archivo' }
  }

  const result = await state.socket.sendMessage(jid, content)
  const wamid  = result?.key?.id ?? ''

  if (wamid) {
    if (!pendingOutbound.has(adminId)) pendingOutbound.set(adminId, new Map())
    pendingOutbound.get(adminId)!.set(wamid, cleanPhone)
    setTimeout(() => pendingOutbound.get(adminId)?.delete(wamid), 60_000)
  }

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
