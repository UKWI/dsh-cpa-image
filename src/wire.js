/**
 * OpenAI `chat/completions` wire format for the CPA adapter.
 *
 * Three pure-ish concerns live here so the adapter itself stays about block
 * lifecycle:
 *   - request assembly (DSH `GenerateOptions` -> OpenAI request body)
 *   - SSE line reading (`data:` frames -> parsed JSON objects)
 *   - image payload normalization (the CPA-specific part: images ride in
 *     `delta.images` / `message.images` instead of `delta.content`)
 *
 * Nothing here imports from `@deepseek-ai/*`: the plugin is loaded from an
 * external directory and only talks to services through `ctx`.
 *
 * @module dsh-cpa-image/wire
 */

/** Sentinel yielded by {@link readSse} for the terminal `data: [DONE]` frame. */
export const SSE_DONE = Symbol('sse-done')

/** Media types accepted as image output. */
const IMAGE_MEDIA_TYPES = new Set(['image/png', 'image/jpeg', 'image/webp', 'image/gif'])

/**
 * Split one `data:` line into its payload.
 * @param line - one SSE line, already stripped of its newline.
 * @returns the parsed JSON body, {@link SSE_DONE}, or undefined for lines that
 *   carry no data (`event:`, `id:`, comments, blank keep-alives, bad JSON).
 */
export function dataLine(line) {
  if (line === '' || line.startsWith(':')) return undefined
  if (!line.startsWith('data:')) return undefined
  const raw = line.slice('data:'.length)
  const text = (raw.startsWith(' ') ? raw.slice(1) : raw).trim()
  if (text === '') return undefined
  if (text === '[DONE]') return SSE_DONE
  try {
    return JSON.parse(text)
  } catch {
    // A provider may emit non-JSON keep-alive payloads; ignoring them is the
    // documented behaviour of every long-lived SSE client.
    return undefined
  }
}


/**
 * Incremental SSE line scanner for a stream that is being read by someone
 * else: bytes go in as they arrive, complete `data:` frames come out.
 *
 * A pull-based reader (`readSse`) cannot be used when the harness adapter owns
 * the body — this scanner lets the middleware watch the same bytes without
 * taking the stream away.
 * @param onFrame - receives each parsed frame, in order.
 * @returns an object with `push(bytes)` and `end()`.
 */
export function createSseScanner(onFrame) {
  const decoder = new TextDecoder()
  let buffer = ''
  const emit = (line) => {
    const frame = dataLine(line.replace(/\r$/, ''))
    if (frame !== undefined && frame !== SSE_DONE) onFrame(frame)
  }
  return {
    /** Feed one chunk of bytes. */
    push(chunk) {
      buffer += decoder.decode(chunk, { stream: true })
      let newline
      while ((newline = buffer.indexOf('\n')) !== -1) {
        emit(buffer.slice(0, newline))
        buffer = buffer.slice(newline + 1)
      }
    },
    /** Flush the decoder and any trailing line. */
    end() {
      buffer += decoder.decode()
      if (buffer !== '') emit(buffer)
      buffer = ''
    },
  }
}

/**
 * Read an SSE response body as decoded `data:` frames.
 * @param body - the fetch response body stream.
 * @param signal - cancellation; aborting throws the signal's reason.
 * @returns every parsed frame in order, ending with {@link SSE_DONE} when the
 *   server sent one.
 */
export async function* readSse(body, signal) {
  const reader = body.getReader()
  const decoder = new TextDecoder()
  let buffer = ''
  try {
    for (;;) {
      if (signal?.aborted) throw signal.reason ?? new Error('aborted')
      const { done, value } = await reader.read()
      if (done) break
      buffer += decoder.decode(value, { stream: true })
      let newline
      while ((newline = buffer.indexOf('\n')) !== -1) {
        const line = buffer.slice(0, newline).replace(/\r$/, '')
        buffer = buffer.slice(newline + 1)
        const frame = dataLine(line)
        if (frame !== undefined) yield frame
      }
    }
    buffer += decoder.decode()
    const tail = dataLine(buffer.replace(/\r$/, ''))
    if (tail !== undefined) yield tail
  } finally {
    reader.releaseLock()
  }
}

/**
 * Flatten the text blocks of one content array.
 * @param content - DSH content blocks.
 * @returns the concatenated visible text.
 */
export function textOf(content) {
  let text = ''
  for (const block of content) if (block.type === 'text') text += block.text
  return text
}

/**
 * Render one DSH content array as user-role parts.
 *
 * The common all-text case returns a plain string: an array of text parts is
 * legal but needlessly re-tokenized by some upstreams, and CPA forwards this
 * body verbatim.
 * @param content - DSH content blocks.
 * @param resolveImage - projects one attachment reference to a data URL.
 * @returns OpenAI `content` (string or part array).
 */
async function userContent(content, resolveImage) {
  const parts = []
  let hasImage = false
  for (const block of content) {
    switch (block.type) {
      case 'text':
        if (block.text !== '') parts.push({ type: 'text', text: block.text })
        break
      case 'image': {
        const projected = await resolveImage(block.attachment)
        if (projected?.url !== undefined) {
          hasImage = true
          parts.push({ type: 'image_url', image_url: { url: projected.url } })
        } else {
          parts.push({ type: 'text', text: projected?.text ?? `[image ${block.attachment?.name ?? 'unavailable'}]` })
        }
        break
      }
      case 'file':
        // Files are normally projected to handle text before dispatch; keep a
        // placeholder so a direct caller never loses the reference silently.
        parts.push({ type: 'text', text: `[file ${block.attachment?.name ?? 'unavailable'}]` })
        break
      default:
        break
    }
  }
  if (!hasImage) {
    return parts
      .filter(part => part.type === 'text')
      .map(part => part.text)
      .join('\n\n')
  }
  return parts
}

/**
 * Render one assistant content array as an OpenAI assistant message.
 *
 * History images are reduced to placeholder text: re-uploading a generated
 * megabyte on every later turn is worse than losing the pixels, and the
 * durable session still holds the real block.
 * @param content - DSH assistant content blocks.
 * @returns the assistant message, or undefined when it carries nothing.
 */
function assistantMessage(content) {
  const text = []
  const toolCalls = []
  for (const block of content) {
    switch (block.type) {
      case 'text':
        if (block.text !== '') text.push(block.text)
        break
      case 'image':
        text.push(`[image ${block.attachment?.name ?? 'assistant image'}]`)
        break
      case 'tool-call':
        toolCalls.push({
          id: block.id,
          type: 'function',
          function: { name: block.name, arguments: block.arguments === '' ? '{}' : block.arguments },
        })
        break
      default:
        break
    }
  }
  const joined = text.join('\n\n')
  if (joined === '' && toolCalls.length === 0) return undefined
  return {
    role: 'assistant',
    content: joined === '' ? null : joined,
    ...toolCalls.length === 0 ? {} : { tool_calls: toolCalls },
  }
}

/**
 * Build one OpenAI chat-completions request body.
 * @param options - the harness request.
 * @param helpers - `resolveImage` projects one attachment for the wire;
 *   `includeUsage` asks for the trailing usage frame.
 * @returns the request body to POST.
 */
export async function buildChatRequest(options, helpers) {
  const { resolveImage, includeUsage = true, maxImageBytes } = helpers
  void maxImageBytes
  const messages = []

  // A one-shot caller passes the system prompt beside the messages; the agent
  // loop instead puts it in a leading system-role message.
  if (typeof options.system === 'string' && options.system.trim() !== '') {
    messages.push({ role: 'system', content: options.system })
  }

  for (const message of options.messages) {
    switch (message.role) {
      case 'system': {
        const text = textOf(message.content)
        if (text.trim() !== '') messages.push({ role: 'system', content: text })
        break
      }
      case 'developer':
        // Tool additions/removals are already folded into `options.tools` by
        // the runtime projection; the OpenAI protocol has no developer slot.
        break
      case 'user': {
        const content = await userContent(message.content, resolveImage)
        if (typeof content === 'string' && content.trim() === '') break
        if (Array.isArray(content) && content.length === 0) break
        messages.push({ role: 'user', content })
        break
      }
      case 'assistant': {
        const assistant = assistantMessage(message.content)
        if (assistant !== undefined) messages.push(assistant)
        break
      }
      case 'tool': {
        const text = textOf(message.content)
        messages.push({
          role: 'tool',
          tool_call_id: message.toolCallId,
          content: text === '' ? '(no output)' : text,
        })
        break
      }
      default:
        break
    }
  }

  const body = {
    model: options.model,
    messages,
    stream: true,
    ...includeUsage ? { stream_options: { include_usage: true } } : {},
    ...options.tools === undefined || options.tools.length === 0
      ? {}
      : {
          tools: options.tools.map(tool => ({
            type: 'function',
            function: {
              name: tool.name,
              description: tool.description,
              parameters: tool.parameters,
            },
          })),
        },
    ...options.maxTokens === undefined ? {} : { max_tokens: options.maxTokens },
    ...options.temperature === undefined ? {} : { temperature: options.temperature },
    ...options.stop === undefined || options.stop.length === 0 ? {} : { stop: options.stop },
  }
  return body
}

/**
 * Decode one `data:` URL into bytes.
 * @param url - the URL to decode.
 * @returns media type plus payload bytes, or undefined when it is not a data URL.
 */
export function decodeDataUrl(url) {
  const match = /^data:([^;,]*)((?:;[^;,]*)*),(.*)$/s.exec(url)
  if (match === null) return undefined
  const mediaType = match[1] === '' ? 'image/png' : match[1]
  const isBase64 = match[2].split(';').includes('base64')
  const payload = match[3]
  try {
    if (isBase64) return { mediaType, data: Buffer.from(payload, 'base64') }
    return { mediaType, data: Buffer.from(decodeURIComponent(payload), 'utf8') }
  } catch {
    return undefined
  }
}

/**
 * Normalize one image object from a CPA response into bytes.
 *
 * CPA (and the OpenAI-compatible servers it wraps) emit at least four shapes
 * depending on the upstream: a `data:` URL, a remote URL, bare `b64_json`, or
 * a `{ data, media_type }` pair. All of them mean the same thing here.
 * @param image - one entry of `delta.images` / `message.images`.
 * @param signal - cancellation for remote fetches.
 * @param maxBytes - refuse payloads above this size.
 * @returns normalized image bytes, or a reason string when unusable.
 */
export async function normalizeImage(image, signal, maxBytes = 32 * 1024 * 1024) {
  if (image === null || typeof image !== 'object') return { skipped: 'not an object' }

  const nested = image.image_url ?? image.imageUrl
  const url = typeof nested === 'string'
    ? nested
    : typeof nested?.url === 'string' ? nested.url : typeof image.url === 'string' ? image.url : undefined

  if (url !== undefined) {
    const decoded = decodeDataUrl(url)
    if (decoded !== undefined) {
      if (decoded.data.byteLength > maxBytes) return { skipped: `payload exceeds ${String(maxBytes)} bytes` }
      return { mediaType: normalizeMediaType(decoded.mediaType), data: decoded.data }
    }
    if (/^https?:\/\//i.test(url)) {
      const response = await fetch(url, { signal })
      if (!response.ok) return { skipped: `image download failed with HTTP ${String(response.status)}` }
      const data = Buffer.from(await response.arrayBuffer())
      if (data.byteLength > maxBytes) return { skipped: `payload exceeds ${String(maxBytes)} bytes` }
      const mediaType = (response.headers.get('content-type') ?? 'image/png').split(';')[0].trim()
      return { mediaType: normalizeMediaType(mediaType), data }
    }
    return { skipped: 'unsupported image URL' }
  }

  const base64 = typeof image.b64_json === 'string'
    ? image.b64_json
    : typeof image.data === 'string' && image.data !== '' ? image.data : undefined
  if (base64 === undefined) return { skipped: 'no image payload' }
  const data = Buffer.from(base64, 'base64')
  if (data.byteLength === 0) return { skipped: 'empty image payload' }
  if (data.byteLength > maxBytes) return { skipped: `payload exceeds ${String(maxBytes)} bytes` }
  const declared = typeof image.media_type === 'string'
    ? image.media_type
    : typeof image.mediaType === 'string' ? image.mediaType : 'image/png'
  return { mediaType: normalizeMediaType(declared), data }
}

/**
 * Keep only media types the attachment service can persist.
 * @param mediaType - a declared or sniffed media type.
 * @returns a supported image media type, defaulting to `image/png`.
 */
function normalizeMediaType(mediaType) {
  const base = mediaType.toLowerCase().split(';')[0].trim()
  if (base === 'image/jpg') return 'image/jpeg'
  return IMAGE_MEDIA_TYPES.has(base) ? base : 'image/png'
}

/**
 * Map an HTTP status and body to one harness failure.
 * @param status - HTTP status of the failed response.
 * @param detail - response body text, already truncated by the caller.
 * @param retryAfterMs - provider-requested delay, when the header parsed.
 * @returns the failure to report on the terminal chunk.
 */
export function httpFailure(status, detail, retryAfterMs) {
  const code = status === 401 || status === 403
    ? 'AUTH'
    : status === 429
      ? 'RATE_LIMIT'
      : status === 413 || status === 400 || status === 422
        ? 'INVALID_REQUEST'
        : status >= 500 ? 'SERVER' : 'HTTP_ERROR'
  const message = detail === '' ? `HTTP ${String(status)}` : `HTTP ${String(status)}: ${detail}`
  return {
    message,
    code,
    status,
    ...retryAfterMs === undefined ? {} : { providerRetryAfterMs: retryAfterMs },
  }
}

/**
 * Parse a `Retry-After` header as milliseconds.
 * @param value - the raw header value.
 * @returns the delay in milliseconds, or undefined when absent or unparseable.
 */
export function retryAfterMs(value) {
  if (value === null || value === '') return undefined
  const seconds = Number(value)
  if (Number.isFinite(seconds) && seconds >= 0) return Math.round(seconds * 1000)
  const at = Date.parse(value)
  return Number.isNaN(at) ? undefined : Math.max(0, at - Date.now())
}

/**
 * Map one OpenAI `usage` object onto harness counts.
 *
 * Harness counts are disjoint: cached prompt tokens are reported separately
 * and subtracted from the input count.
 * @param usage - the provider's usage object.
 * @returns harness token usage, or undefined when the object is unusable.
 */
export function mapUsage(usage) {
  if (usage === null || typeof usage !== 'object') return undefined
  const input = numberOr(usage.prompt_tokens, 0)
  const cached = numberOr(usage.prompt_tokens_details?.cached_tokens, 0)
  const output = numberOr(usage.completion_tokens, 0)
  const total = numberOr(usage.total_tokens, input + output)
  return {
    inputTokens: Math.max(0, input - cached),
    outputTokens: output,
    totalTokens: total,
    ...cached > 0 ? { cacheReadTokens: cached } : {},
  }
}

/**
 * Coerce one numeric field with a fallback.
 * @param value - candidate value.
 * @param fallback - value used when the candidate is not a finite number.
 * @returns the number to use.
 */
function numberOr(value, fallback) {
  return typeof value === 'number' && Number.isFinite(value) ? value : fallback
}

/**
 * Map one OpenAI `finish_reason` onto a harness reason.
 * @param reason - the provider's finish reason.
 * @returns the harness finish reason.
 */
export function mapFinishReason(reason) {
  switch (reason) {
    case 'tool_calls':
    case 'function_call':
      return { kind: 'tool-calls' }
    case 'length':
      return { kind: 'max-tokens' }
    default:
      return { kind: 'stop' }
  }
}
