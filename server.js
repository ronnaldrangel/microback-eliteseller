import 'dotenv/config'
import express from 'express'
import cors from 'cors'
import { OpenAI } from 'openai'
import { createClient } from 'redis'
import crypto from 'node:crypto'
 

import sharp from 'sharp'

const app = express()
app.disable('x-powered-by')
app.use(cors())
app.use(express.json({ limit: '25mb' }))
app.use(express.urlencoded({ extended: true, limit: '25mb' }))

const redisUrl = process.env.REDIS_URL || ''
const redis = createClient({ url: redisUrl })
redis.on('error', (e) => { console.error('Redis error:', e.message) })
try {
  const infoUrl = redisUrl
  let host = ''
  let port = ''
  let ssl = false
  try {
    const u = new URL(infoUrl)
    host = u.hostname || host
    port = u.port || port
    ssl = infoUrl.startsWith('rediss://')
  } catch (_) {}
  console.log(`Redis connecting host=${host} port=${port} ssl=${ssl}`)
  await redis.connect()
  console.log('Redis connected')
} catch (e) {
  console.error('Redis connect failed:', e.message)
}

const PATH = '/webhook'
const openaiClient = process.env.OPENAI_API_KEY ? new OpenAI({ apiKey: process.env.OPENAI_API_KEY }) : null
const AI_IMAGE_CACHE_TTL = Number(process.env.AI_IMAGE_CACHE_TTL || '86400')
const AI_AUDIO_CACHE_TTL = Number(process.env.AI_AUDIO_CACHE_TTL || '86400')
const AI_CONCURRENCY = Math.max(1, Number(process.env.AI_CONCURRENCY || '3'))

function get(obj, path, def) {
  try {
    return path.split('.').reduce((o, k) => (o && k in o ? o[k] : undefined), obj) ?? def
  } catch (_) {
    return def
  }
}

function isEmptyNumber(v) {
  return v === undefined || v === null || v === ''
}

function trimOrEmpty(s) {
  if (typeof s !== 'string') return ''
  const t = s.trim()
  return t
}

function collapseSpaces(s) {
  return s.replace(/\s+/g, ' ').trim()
}

function isDataUrl(u) {
  return typeof u === 'string' && u.startsWith('data:')
}


function guessMimeFromUrl(u) {
  if (typeof u !== 'string') return 'image/jpeg'
  const m = u.toLowerCase()
  if (m.endsWith('.jpg') || m.endsWith('.jpeg')) return 'image/jpeg'
  if (m.endsWith('.png')) return 'image/png'
  if (m.endsWith('.webp')) return 'image/webp'
  if (m.endsWith('.gif')) return 'image/gif'
  return 'image/jpeg'
}


 



async function bufferFromDataUrl(u) {
  const [, meta, data] = u.match(/^data:(.*?);base64,(.*)$/) || []
  const mime = meta || 'application/octet-stream'
  const buf = Buffer.from(data || '', 'base64')
  return { buf, mime }
}

async function fetchBuffer(u) {
  if (isDataUrl(u)) return bufferFromDataUrl(u)
  let res = await fetch(u, { redirect: 'follow' })
  let ct = res.headers.get('content-type') || 'application/octet-stream'
  let buf = Buffer.from(await res.arrayBuffer())
  const small = buf.length < 1024
  const looksXmlHtml = ct.includes('xml') || ct.includes('html')
  const isActive = typeof u === 'string' && u.includes('/rails/active_storage/blobs/redirect/')
  if ((small || looksXmlHtml) && isActive) {
    try {
      const urlObj = new URL(u)
      urlObj.pathname = urlObj.pathname.replace('/blobs/redirect/', '/blobs/')
      const url2 = urlObj.toString()
      const res2 = await fetch(url2, { redirect: 'follow' })
      const ct2 = res2.headers.get('content-type') || ct
      const buf2 = Buffer.from(await res2.arrayBuffer())
      if (buf2.length > buf.length && !(ct2.includes('xml') || ct2.includes('html'))) {
        res = res2
        ct = ct2
        buf = buf2
      }
    } catch (_) {}
  }
  return { buf, mime: ct }
}

async function logUsage(model, usage, mastertext) {
  if (!usage) return { cost: 0, tokens: 0 }
  const { prompt_tokens, completion_tokens, total_tokens } = usage
  const p_tokens_raw = Number(prompt_tokens) || 0
  const c_tokens_raw = Number(completion_tokens) || 0
  const t_tokens_raw = Number(total_tokens) || (p_tokens_raw + c_tokens_raw)
  const p_tokens = Math.ceil(p_tokens_raw)
  const c_tokens = Math.ceil(c_tokens_raw)
  const t_tokens = Math.ceil(t_tokens_raw)
  let cost = 0
  if (model === 'gpt-4o-mini') {
    cost = (p_tokens * 0.15 / 1000000) + (c_tokens * 0.60 / 1000000)
  } else if (model.includes('llava') || model.includes('llama')) {
    cost = (p_tokens * 0.11 / 1000000) + (c_tokens * 0.34 / 1000000)
  } else if (model === 'whisper-large-v3-turbo') {
    cost = (t_tokens_raw / 3600) * 0.111
  }
  return { cost, tokens: t_tokens }
}

function hashKey(s) {
  try {
    return crypto.createHash('sha1').update(String(s || ''), 'utf8').digest('hex')
  } catch (_) {
    return String(s || '')
  }
}

async function cacheGetJSON(key) {
  try {
    const s = await redis.get(key)
    return s ? JSON.parse(s) : null
  } catch (_) {
    return null
  }
}

async function cacheSetJSON(key, obj, ttlSec) {
  try {
    await redis.set(key, JSON.stringify(obj || {}), { EX: Math.max(1, Number(ttlSec || 60)) })
  } catch (_) {}
}

async function analyzeImage(dataUrl, prompt) {
  let finalImageUrl = dataUrl

  const cacheKey = `cache:image:${hashKey(dataUrl)}:${hashKey(prompt).slice(0, 8)}`
  const cached = await cacheGetJSON(cacheKey)
  if (cached && typeof cached === 'object') {
    return { content: trimOrEmpty(cached.content || ''), cost: Number(cached.cost) || 0, tokens: Number(cached.tokens) || 0 }
  }

  if (!isDataUrl(finalImageUrl)) {
    try {
      const { buf } = await fetchBuffer(finalImageUrl)
      const mime = guessMimeFromUrl(finalImageUrl)
      const b64 = buf.toString('base64')
      finalImageUrl = `data:${mime};base64,${b64}`
    } catch (_) {}
  }

  if (!process.env.OPENAI_API_KEY) return { content: '', cost: 0, tokens: 0 }
  try {
    const openai = openaiClient || new OpenAI({ apiKey: process.env.OPENAI_API_KEY })
    const r = await openai.chat.completions.create({
      model: 'gpt-4o-mini',
      messages: [
        {
          role: 'user',
          content: [
            { type: 'text', text: prompt },
            { type: 'image_url', image_url: { url: finalImageUrl, detail: 'low' } }
          ]
        }
      ],
      max_tokens: 300
    })
    const c = r.choices?.[0]?.message?.content || ''
    let usageData = { cost: 0, tokens: 0 }
    if (r.usage) {
      console.log('OpenAI Usage:', JSON.stringify(r.usage))
      usageData = await logUsage('gpt-4o-mini', r.usage, prompt) || { cost: 0, tokens: 0 }
    }
    const out = { content: trimOrEmpty(c), ...usageData }
    await cacheSetJSON(cacheKey, out, AI_IMAGE_CACHE_TTL)
    return out
  } catch (err) {
    console.error('\x1b[31m%s\x1b[0m', `OpenAI Image analysis failed: ${err.message}`)
    return { content: '', cost: 0, tokens: 0 }
  }
}

function getExtensionFromMime(mime) {
  if (typeof mime !== 'string') return 'mp3'
  if (mime.includes('opus')) return 'ogg'
  if (mime.includes('ogg')) return 'ogg'
  if (mime.includes('webm')) return 'webm'
  if (mime.includes('wav')) return 'wav'
  if (mime.includes('mp4') || mime.includes('m4a')) return 'm4a'
  if (mime.includes('mpeg') || mime.includes('mp3')) return 'mp3'
  return 'mp3'
}

async function transcribeAudio(dataUrl) {
  let lastMime = null
  let lastSize = null
  try {
    const { buf, mime } = await fetchBuffer(dataUrl)
    lastMime = mime
    lastSize = buf?.length || 0
    console.log(`Audio buffer fetched url=${dataUrl} mime=${mime} size=${buf.length}`)
    const ext = getExtensionFromMime(mime)
    const filename = `audio.${ext}`
    const uploadFile = await OpenAI.toFile(buf, filename)
    const cacheKey = `cache:audio:${hashKey(dataUrl)}`
    const cached = await cacheGetJSON(cacheKey)
    if (cached && typeof cached === 'object') {
      return { content: trimOrEmpty(cached.content || ''), cost: Number(cached.cost) || 0, tokens: Number(cached.tokens) || 0 }
    }
    if (!process.env.OPENAI_API_KEY) {
      console.error('\x1b[31m%s\x1b[0m', 'OpenAI API key not found for audio transcription')
      return { content: '', cost: 0, tokens: 0 }
    }
    const openai = openaiClient || new OpenAI({ apiKey: process.env.OPENAI_API_KEY })
    const r = await openai.audio.transcriptions.create({ model: 'whisper-1', file: uploadFile })
    const text = r.text || ''
    const usageData = await logUsage('whisper-1', { prompt_tokens: 0, completion_tokens: 0, total_tokens: 0 }, 'AUDIO TRANSCRIPTION') || { cost: 0, tokens: 0 }
    const out = { content: trimOrEmpty(text), ...usageData }
    await cacheSetJSON(cacheKey, out, AI_AUDIO_CACHE_TTL)
    return out
  } catch (err) {
    const status = err?.status || err?.response?.status
    const detail = (() => { try { return JSON.stringify(err?.error ?? err?.response ?? {}, null, 2) } catch (_) { return '' } })()
    console.error('\x1b[31m%s\x1b[0m', `Audio transcription failed url=${dataUrl} mime=${lastMime} size=${lastSize} status=${status} error=${err?.message}`)
    if (detail) console.error(detail)
    if (err && err.stack) console.error(err.stack)
    return { content: '', cost: 0, tokens: 0 }
  }
}

async function runWithConcurrency(tasks, limit) {
  const results = new Array(tasks.length)
  let idx = 0
  const workers = []
  const n = Math.min(limit, tasks.length)
  for (let w = 0; w < n; w++) {
    workers.push((async () => {
      while (true) {
        const i = idx
        if (i >= tasks.length) break
        idx = i + 1
        try {
          results[i] = await tasks[i]()
        } catch (_) {
          results[i] = null
        }
      }
    })())
  }
  await Promise.all(workers)
  return results
}

function buildReplyContext(body) {
  const m0 = get(body, 'conversation.messages.0', {})
  const v =
    get(body, 'content_attributes.in_reply_to') ||
    get(m0, 'content_attributes.in_reply_to') ||
    get(body, 'content_attributes.in_reply_to_external_id') ||
    get(m0, 'content_attributes.in_reply_to_external_id') ||
    get(m0, 'additional_attributes.context.message_id') ||
    get(m0, 'additional_attributes.quoted_message_id') ||
    ''
  return v
}

function getQuotedContent(body) {
  const m0 = get(body, 'conversation.messages.0', {})
  
  // 1. Chatwoot / Estructuras planas
  const qBody = get(body, 'content_attributes.quoted_content_body') || get(m0, 'content_attributes.quoted_content_body')
  if (qBody) return qBody

  // 2. Waha / Baileys (contextInfo)
  // A veces viene en 'additional_attributes.context' o directamente en el mensaje
  const context = get(m0, 'additional_attributes.context') || get(m0, 'context_info')
  if (context && context.quotedMessage) {
    const qm = context.quotedMessage
    return (
      qm.conversation ||
      qm.extendedTextMessage?.text ||
      qm.imageMessage?.caption ||
      qm.videoMessage?.caption ||
      ''
    )
  }
  
  return ''
}

function classify(body) {
  const attachRaw = get(body, 'conversation.messages.0.attachments', null)
  const attachments = Array.isArray(attachRaw) ? attachRaw : []
  const a0 = attachments[0]
  const fileType = a0?.file_type
  const content = trimOrEmpty(get(body, 'content', '')) || trimOrEmpty(get(body, 'conversation.messages.0.content', ''))
  const imageText = fileType === 'image' && content !== ''
  const imageOnly = fileType === 'image'
  const audioOnly = fileType === 'audio'
  const textOnly = attachments.length === 0
  if (imageText) return 'IMAGE-TEXT'
  if (imageOnly) return 'IMAGEN'
  if (audioOnly) return 'AUDIO'
  if (textOnly) return 'TEXT'
  return 'TEXT'
}

function ensureJid(body) {
  const direct = trimOrEmpty(get(body, 'sender.custom_attributes.waha_whatsapp_jid', ''))
  if (direct) return direct
  const phone = trimOrEmpty(get(body, 'sender.phone_number', ''))
  const digits = phone.replace(/\D+/g, '')
  return digits ? `${digits}@c.us` : ''
}

function buildMasterText(text, audio, image, imagenText, replyText) {
  // Mantener formato del flujo original: espacios dobles si faltan partes
  const parts = [text || '', audio || '', image || '', imagenText || '']
  let joined = parts.join(' ')
  if (replyText) {
    joined = `[REPLY_TO: ${replyText}] ${joined}`
  }
  return joined
}

function nowIsoWithOffset() {
  const d = new Date()
  const pad = (n, l = 2) => String(n).padStart(l, '0')
  const y = d.getFullYear()
  const mo = pad(d.getMonth() + 1)
  const da = pad(d.getDate())
  const h = pad(d.getHours())
  const mi = pad(d.getMinutes())
  const s = pad(d.getSeconds())
  const ms = pad(d.getMilliseconds(), 3)
  const offMin = -d.getTimezoneOffset()
  const sign = offMin >= 0 ? '+' : '-'
  const abs = Math.abs(offMin)
  const oh = pad(Math.floor(abs / 60))
  const om = pad(abs % 60)
  return `${y}-${mo}-${da}T${h}:${mi}:${s}.${ms}${sign}${oh}:${om}`
}

async function cleanRedisBuffer(key, maxKeep) {
  const list = (await safeLRange(key, 0, -1)) || []
  const cleaned = []
  for (const s of list) {
    let o = null
    try {
      o = JSON.parse(s || '{}')
    } catch (_) {
      o = null
    }
    if (!o || typeof o !== 'object') continue
    const mt = collapseSpaces(trimOrEmpty(o.mastertext || ''))
    if (!mt) continue
    const obj = { ...o, mastertext: mt }
    cleaned.push(JSON.stringify(obj))
  }
  const start = cleaned.length > maxKeep ? cleaned.length - maxKeep : 0
  const sliced = cleaned.slice(start)
  let changed = sliced.length !== list.length
  if (!changed) {
    for (let i = 0; i < list.length; i++) {
      if (list[i] !== sliced[i]) { changed = true; break }
    }
  }
  if (changed) {
    await safeDel(key)
    for (const s of sliced) {
      await safeRPush(key, s)
    }
  }
  const final = await safeLRange(key, 0, -1)
  return final || []
}

const pendingTimers = {} // { key: timer }

function buildMessageData(mastertext, messageId, replyContextId, replyContextText, body, queryQ, cost, tokens) {
  return {
    mastertext,
    cost: cost || 0,
    tokens: tokens || 0,
    timestamp: nowIsoWithOffset(),
    id: messageId,
    reply_to_id: replyContextId,
    reply_to_text: replyContextText,
    waha_whatsapp_jid: get(body, 'sender.custom_attributes.waha_whatsapp_jid'),
    conversation_id: get(body, 'conversation.messages.0.conversation_id'),
    _raw_conversation: get(body, 'conversation.messages.0', {}),
    _query: queryQ
  }
}

app.post(PATH, async (req, res) => {
  // console.log('--- INCOMING REQUEST ---')
  // console.log('Headers:', JSON.stringify(req.headers, null, 2))
  
  if (parseInt(req.headers['content-length'] || '0') === 0) {
    console.error('\x1b[31m%s\x1b[0m', '⚠️ WARNING: Request body is EMPTY (Content-Length: 0). Check n8n/sender configuration.')
  }

  try {
    // console.log('Body Preview:', JSON.stringify(req.body).substring(0, 500))
  } catch (_) {}

  const payload = req.body || {}
  let body
  if (Array.isArray(payload)) {
    const item = payload[0] || {}
    body = item.body && typeof item.body === 'object' ? item.body : item
  } else {
    body = payload.body && typeof payload.body === 'object' ? payload.body : payload
  }

  const messageType = get(body, 'message_type')
  const assigneeRaw = get(body, 'conversation.meta.assignee')
  const assigneeIsNullOrUndefined = assigneeRaw == null
  if (String(messageType).toLowerCase() !== 'incoming' || !assigneeIsNullOrUndefined) {
    console.log(`IGNORADO reason messageType=${messageType} assignee=${assigneeRaw}`)
    return res.status(200).send('IGNORADO')
  }

  // console.log('Extracted Body keys:', body ? Object.keys(body) : 'body is null/undefined')
  res.status(200).send('OK')
  
  const messageId = get(body, 'id', null)
  const replyContextId = buildReplyContext(body)
  const replyContextText = getQuotedContent(body)
  let type = classify(body)
  // console.log('Classified Type:', type)
  if (replyContextId) {} // console.log('Reply Context ID:', replyContextId)
  if (replyContextText) {} // console.log('Reply Context Text:', replyContextText)

  // --- ITERATE ALL ATTACHMENTS ---
  const attachRaw = get(body, 'conversation.messages.0.attachments', null)
  const attachments = Array.isArray(attachRaw) ? attachRaw : []

  let image = ''
  let text = ''
  const rawContent = get(body, 'content')
  if (typeof rawContent === 'string') {
    text = rawContent.trim()
  } else if (rawContent !== undefined && rawContent !== null) {
    try { text = String(rawContent).trim() } catch (_) { text = '' }
  }
  if (!text) {
    const raw2 = get(body, 'conversation.messages.0.content')
    if (typeof raw2 === 'string') text = raw2.trim()
  }
  let audio = ''
  let imagenText = ''

  let combinedImages = []
  let combinedAudios = []
  let totalCost = 0
  let totalTokens = 0

  if (attachments.length > 0) {
      const tasks = attachments.map(att => async () => {
          const fType = att.file_type
          const dUrl = att.data_url
          if (!dUrl) return null
          if (fType === 'image') {
              const result = await analyzeImage(dUrl, '¿Analiza esta imagen profundamente, se breve?')
              return { kind: 'image', result }
          } else if (fType === 'audio') {
              const result = await transcribeAudio(dUrl)
              return { kind: 'audio', result }
          }
          return null
      })
      const results = await runWithConcurrency(tasks, AI_CONCURRENCY)
      for (const r of results) {
        if (r && r.result && r.result.content) {
          if (r.kind === 'image') {
            combinedImages.push(r.result.content)
          } else if (r.kind === 'audio') {
            combinedAudios.push(r.result.content)
          }
          totalCost += (r.result.cost || 0)
          totalTokens += (r.result.tokens || 0)
        }
      }
  }

  // Construct final strings
  if (combinedImages.length > 0) {
      // If text exists, treat as IMAGE-TEXT, else IMAGEN
      // But actually we just append all analyses
      const joinedImages = combinedImages.join('\n---\n')
      if (text) {
          // IMAGE-TEXT scenario
          imagenText = `IMAGE: ${joinedImages} IMAGEN-FOOTER:${text}`
          type = 'IMAGE-TEXT' // force update type for logging if needed
      } else {
          image = `IMAGE: ${joinedImages}`
          type = 'IMAGEN'
      }
  }
  
  if (combinedAudios.length > 0) {
      audio = `AUDIO: ${combinedAudios.join('\n---\n')}`
      if (!image && !imagenText) type = 'AUDIO'
  }

  if (!image && !imagenText && !audio && text) {
      type = 'TEXT'
  }
  
  // Fallback if type was classified but no attachments found (weird edge case)
  // or if we need to preserve original single-attachment logic for compatibility?
  // The loop above handles single attachment too (length=1).
  
  const mastertextRaw = buildMasterText(text, audio, image, imagenText, replyContextText)
  const mastertext = collapseSpaces(mastertextRaw)

  

  const jid = ensureJid(body)
  const convId = get(body, 'conversation.messages.0.conversation_id')
  const key = convId ? `conv_${convId}_buffer` : `${jid}_buffer`

  const qFromPayload = Array.isArray(payload) ? get(payload[0], 'query.q') : get(payload, 'query.q')
  const queryQ = req.query.q || ''

  // Check if query parameter 'flush' is present to clear the buffer
  const flush = req.query.flush || get(body, 'query.flush') || get(body, 'flush')
  if (flush === 'true' || flush === true) {
    // console.log(`Flushing buffer for key: ${key}`)
    await safeDel(key)
  }

  if (mastertext) {
    // console.log('Mastertext generated:', mastertext)
    
    // --- GLOBAL DEDUPLICATION ---
    // Verificar si el ID ya fue procesado recientemente (fuera del buffer actual)
    if (messageId) {
      const processedKey = `processed:${messageId}`
      const alreadyProcessed = await redis.get(processedKey)
      if (alreadyProcessed) {
        // console.log(`♻️ Skipping globally processed message ID: ${messageId}`)
        // Respondemos "éxito" falso al debounce para que no se quede colgado esperando
        // Aunque en realidad, si es duplicado, simplemente no lo añadimos al buffer.
        // El debounce se encargará de devolver lo que haya (o nada).
        
        // Si NO añadimos nada al buffer, y era el único mensaje, el timeout devolverá lista vacía.
        // Esto es correcto para un duplicado.
      } else {
        // Marcar como procesado con TTL de 1 hora
        await redis.set(processedKey, '1', { EX: 3600 })
        
        // --- BUFFER ADDITION ---
        // Verificar duplicados EN EL BUFFER ACTUAL (por si llegan varios iguales en la misma ráfaga)
        const currentList = await safeLRange(key, 0, -1)
        let isDuplicate = false
        
        if (messageId) {
          for (const item of currentList) {
            try {
              const parsed = JSON.parse(item)
              if (String(parsed.id) === String(messageId)) {
                isDuplicate = true
                break
              }
            } catch (_) {}
          }
        }

        if (!isDuplicate) {
          const messageData = buildMessageData(mastertext, messageId, replyContextId, replyContextText, body, queryQ, totalCost, totalTokens)
          // console.log('Adding new message to Redis:', JSON.stringify(messageData))
          await safeRPush(key, JSON.stringify(messageData))
          const maxKeepRaw = Number(process.env.REDIS_MAX_BUFFER || '20')
          const maxKeep = Number.isFinite(maxKeepRaw) && maxKeepRaw >= 1 ? maxKeepRaw : 20
          const curLen = await safeLLen(key)
          console.log(`buffer push id=${messageId} key=${key} size=${curLen}`)
          if (curLen > maxKeep) {
            await safeLTrim(key, curLen - maxKeep, -1)
          }
        } else {
          // console.log('⚠️ Skipping duplicate message in buffer with ID:', messageId)
        }
      }
    } else {
      // Si no tiene ID (es null), verificamos por contenido exacto en el buffer
      const currentList = await safeLRange(key, 0, -1)
      let isDuplicate = false
      if (currentList.length > 0) {
        try {
          const lastItem = JSON.parse(currentList[currentList.length - 1])
          if (lastItem.mastertext === mastertext) {
              isDuplicate = true
           }
        } catch (_) {}
      }
      
      if (!isDuplicate) {
          const messageData = buildMessageData(mastertext, messageId, replyContextId, replyContextText, body, queryQ, totalCost, totalTokens)
          // console.log('Adding new message to Redis (no-ID):', JSON.stringify(messageData))
          await safeRPush(key, JSON.stringify(messageData))
          const afterLen = await safeLLen(key)
          console.log(`buffer push no-id key=${key} size=${afterLen}`)
      }
    }
  } else {
  // console.log('⚠️ Mastertext is empty. Nothing to add to Redis. Type:', type)
}

  // --- DEBOUNCE / WAIT LOGIC ---
  // Cancelar temporizador anterior para esta conversación/JID
  if (pendingTimers[key]) {
    clearTimeout(pendingTimers[key])
    delete pendingTimers[key]
  }

  let debounceSeconds = Number(process.env.DEBOUNCE_SECONDS || '5')
  // Rich media takes longer to produce/send multiple. Extend wait time for audio/image.
  if (type === 'AUDIO' || type === 'IMAGEN' || type === 'IMAGE-TEXT') {
     debounceSeconds = Math.max(debounceSeconds, 10)
  }
  const debounceMs = debounceSeconds * 1000
  console.log(`Waiting ${debounceSeconds}s for more messages from ${jid}...`)
  
  pendingTimers[key] = setTimeout(async () => {
      console.log(`debounce timeout jid=${jid} key=${key}`)
      // Limpieza y obtencion del buffer final
      const maxKeep2Raw = Number(process.env.REDIS_MAX_BUFFER || '20')
      const maxKeep2 = Number.isFinite(maxKeep2Raw) && maxKeep2Raw >= 1 ? maxKeep2Raw : 20
      
      let list = await cleanRedisBuffer(key, maxKeep2)
      
      // Filtro final en memoria para eliminar duplicados por ID
      const uniqueMap = new Map()
      const uniqueList = []
      for (const itemStr of list) {
        try {
          const item = JSON.parse(itemStr)
          if (item.id) {
            if (!uniqueMap.has(String(item.id))) {
              uniqueMap.set(String(item.id), true)
              uniqueList.push(itemStr)
            }
          } else {
            uniqueList.push(itemStr)
          }
        } catch (_) {
          uniqueList.push(itemStr)
        }
      }
      list = uniqueList
      
      // Consumir (borrar) el buffer SIEMPRE para evitar duplicados en la siguiente llamada
      await safeDel(key)
      console.log(`buffer consumed key=${key}`)
      delete pendingTimers[key]

      // console.log(`Processing ${list.length} messages for forwarding:`, JSON.stringify(list))
      if (list.length === 0) {
        console.log(`no messages to forward jid=${jid} key=${key}`)
        return
      }
        
        // Extraemos los metadatos de conversación del último mensaje (el más reciente)
        // o de cualquiera, asumiendo que es la misma conversación.
        let conversationMeta = {}
        let rootQuery = null
        let rootWahaJid = null
        let rootConvId = null

        if (list.length > 0) {
            try {
                const lastItem = JSON.parse(list[list.length - 1])
                if (lastItem._raw_conversation) {
                    conversationMeta = lastItem._raw_conversation
                }
                rootQuery = lastItem._query
                rootWahaJid = lastItem.waha_whatsapp_jid
                rootConvId = lastItem.conversation_id
            } catch (_) {}
        }

        const cleanedList = list
          .map(itemStr => {
            try {
              const item = JSON.parse(itemStr)
              return collapseSpaces(trimOrEmpty(item.mastertext || ''))
            } catch (_) {
              return ''
            }
          })
          .filter(mt => !!mt)

        const webhookBaseUrl = process.env.WEBHOOK_URL
        const finalQ = rootQuery || queryQ
        const targetUrl = webhookBaseUrl && finalQ ? `${webhookBaseUrl}?q=${encodeURIComponent(finalQ)}` : (webhookBaseUrl || '')

        let responsePayload = {}
        if (body && typeof body === 'object') {
          responsePayload = { ...body }
        }
        responsePayload.mastertext = cleanedList || []
        console.log(`forwarding grouped count=${responsePayload.mastertext.length} url=${targetUrl}`)

        try {
          console.log(`sending to webhook url=${targetUrl} count=${responsePayload.mastertext.length}`)
          const r = await fetch(targetUrl, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(responsePayload)
          })
          console.log(`webhook response status=${r.status} ok=${r.ok}`)
          console.log(`forwarded grouped count=${responsePayload.mastertext.length} url=${targetUrl}`)
        } catch (err) {
          console.error('\x1b[31m%s\x1b[0m', `Error forwarding to EliteSeller: ${err.message}`)
        }
    }, debounceMs)

  return
})

 


const port = process.env.PORT || 3000
app.listen(port, () => {
  console.log(`Mini-back listening on port ${port}`)
})
async function safeRPush(key, value) {
  try {
    return await redis.rPush(key, value)
  } catch (_) {
    return 0
  }
}

async function safeLRange(key, start, stop) {
  try {
    return await redis.lRange(key, start, stop)
  } catch (_) {
    return []
  }
}

async function safeLLen(key) {
  try {
    return await redis.lLen(key)
  } catch (_) {
    return 0
  }
}

async function safeLTrim(key, start, stop) {
  try {
    return await redis.lTrim(key, start, stop)
  } catch (_) {
    return 0
  }
}

async function safeDel(key) {
  try {
    return await redis.del(key)
  } catch (_) {
    return 0
  }
}
