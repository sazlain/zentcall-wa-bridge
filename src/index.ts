import express, { Request, Response, NextFunction } from 'express'
import pino from 'pino'
import {
  startSession,
  stopSession,
  getStatus,
  getQrBase64,
  sendTextMessage,
  restorePersistedSessions,
} from './session-manager'

const log  = pino({ level: process.env.LOG_LEVEL ?? 'info' })
const PORT = parseInt(process.env.PORT ?? '3002', 10)
const app  = express()

app.use(express.json())

// ── Health check ──────────────────────────────────────────────────────────────

app.get('/health', (_req, res) => {
  res.json({ status: 'ok', service: 'zentcall-wa-bridge' })
})

// ── Sessions API ──────────────────────────────────────────────────────────────

/** POST /sessions/:adminId/start — Inicia (o retoma) la sesión */
app.post('/sessions/:adminId/start', async (req: Request, res: Response) => {
  const adminId = parseInt(req.params.adminId, 10)
  if (isNaN(adminId)) return res.status(400).json({ error: 'adminId inválido' })

  try {
    await startSession(adminId)
    return res.json(getStatus(adminId))
  } catch (err) {
    log.error({ adminId, err }, 'start session failed')
    return res.status(500).json({ error: String(err) })
  }
})

/** DELETE /sessions/:adminId — Desconecta y borra la sesión */
app.delete('/sessions/:adminId', async (req: Request, res: Response) => {
  const adminId = parseInt(req.params.adminId, 10)
  if (isNaN(adminId)) return res.status(400).json({ error: 'adminId inválido' })

  try {
    await stopSession(adminId)
    return res.status(204).send()
  } catch (err) {
    log.error({ adminId, err }, 'stop session failed')
    return res.status(500).json({ error: String(err) })
  }
})

/** GET /sessions/:adminId/status — Estado actual */
app.get('/sessions/:adminId/status', (req: Request, res: Response) => {
  const adminId = parseInt(req.params.adminId, 10)
  if (isNaN(adminId)) return res.status(400).json({ error: 'adminId inválido' })
  return res.json(getStatus(adminId))
})

/** GET /sessions/:adminId/qr — QR como data URL base64 */
app.get('/sessions/:adminId/qr', async (req: Request, res: Response) => {
  const adminId = parseInt(req.params.adminId, 10)
  if (isNaN(adminId)) return res.status(400).json({ error: 'adminId inválido' })

  const qr = await getQrBase64(adminId)
  if (!qr) {
    const { status } = getStatus(adminId)
    return res.status(404).json({
      error: 'QR no disponible',
      hint:  status === 'connected'
        ? 'La sesión ya está conectada'
        : 'Llama POST /sessions/:adminId/start primero',
    })
  }

  return res.json({ qr })
})

/** POST /sessions/:adminId/send — Envía un mensaje de texto */
app.post('/sessions/:adminId/send', async (req: Request, res: Response) => {
  const adminId = parseInt(req.params.adminId, 10)
  if (isNaN(adminId)) return res.status(400).json({ error: 'adminId inválido' })

  const { toPhone, body } = req.body as { toPhone?: string; body?: string }
  if (!toPhone || !body) {
    return res.status(400).json({ error: 'Se requieren toPhone y body' })
  }

  try {
    const result = await sendTextMessage(adminId, toPhone, body)
    return res.json(result)
  } catch (err) {
    const msg = String(err)
    const code = msg.includes('No hay sesión') ? 503 : 500
    return res.status(code).json({ error: msg })
  }
})

// ── Error handler ─────────────────────────────────────────────────────────────

app.use((err: Error, _req: Request, res: Response, _next: NextFunction) => {
  log.error(err)
  res.status(500).json({ error: err.message })
})

// ── Arranque ──────────────────────────────────────────────────────────────────

app.listen(PORT, async () => {
  log.info({ port: PORT }, '🟢 zentcall-wa-bridge started')

  // Reconectar sesiones que estaban activas antes del reinicio
  await restorePersistedSessions()
})
