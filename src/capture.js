/**
 * Wire-level image capture for CPA-style upstreams.
 *
 * The harness asks exactly one adapter per provider route, so a fix that lives
 * *inside* an adapter can only ever cover the routes that adapter owns. This
 * module takes the other road: every adapter, every provider, every protocol
 * and every Models-page setting stays exactly as it is, and the HTTP responses
 * they receive are inspected on the way past.
 *
 * `globalThis.fetch` is wrapped once. For the duration of one `llm/stream` call
 * an {@link AsyncLocalStorage} token is active, so a response can be attributed
 * to the call that caused it without guessing from headers or timing.
 *
 * The response body is NOT cloned. Cloning a live network stream and reading
 * only the copy stalls the original reader on Node's undici — observed as
 * pi-ai's 300-second idle timeout. The tap therefore owns the single reader and
 * re-exposes the same bytes as a fresh stream: the adapter reads at its own
 * pace, and a line scanner watches the bytes go by.
 *
 * The fields it looks for are the non-standard ones CPA emits:
 *
 *   streaming  choices[0].delta.images[]
 *   one-shot   choices[0].message.images[]
 *   images API data[] with b64_json / url
 *
 * @module dsh-cpa-image/capture
 */

import { AsyncLocalStorage } from 'node:async_hooks'

import { createSseScanner, normalizeImage } from './wire.js'

/** Upper bound on how long a finished call waits for its tap to drain. */
const TAP_SETTLE_MS = 15_000

/** Statuses whose responses may not carry a body. */
const NULL_BODY_STATUS = new Set([101, 204, 205, 304])

/**
 * Diagnostic trace, enabled with `DSH_CPA_DEBUG=1`.
 * @param args - values to print.
 */
function debug(...args) {
  if (process.env.DSH_CPA_DEBUG) console.error('[cpa-image]', ...args)
}

/**
 * Resolve the URL of a fetch input.
 * @param input - string, URL, or Request.
 * @returns the absolute URL, or an empty string when unknown.
 */
function urlOf(input) {
  if (typeof input === 'string') return input
  if (input instanceof URL) return input.href
  if (typeof input?.url === 'string') return input.url
  return ''
}

/**
 * Persist one batch of raw image objects.
 * @param images - `delta.images` / `message.images` / `data[]`, when present.
 * @param options - attachment service, size cap, capture state, and logger.
 */
async function takeImages(images, options) {
  if (!Array.isArray(images) || images.length === 0) return
  const { capture, attachments, maxImageBytes, logger, signal } = options
  for (const image of images) {
    let normalized
    try {
      normalized = await normalizeImage(image, signal, maxImageBytes)
    } catch (error) {
      logger?.warn?.('cpa-image: image normalization failed', error)
      continue
    }
    if (normalized.skipped !== undefined) {
      logger?.warn?.(`cpa-image: image skipped (${normalized.skipped})`)
      continue
    }
    // A stream may repeat the same picture in `delta` and in the closing
    // `message`; media type plus byte size is a cheap, stable identity.
    const id = `${normalized.mediaType}:${String(normalized.data.byteLength)}`
    if (capture.seen.has(id)) continue
    capture.seen.add(id)
    try {
      const refs = await attachments.saveImages([{
        data: normalized.data,
        mediaType: normalized.mediaType,
        name: `cpa-image-${String(Date.now())}`,
      }])
      const ref = refs?.[0]
      if (ref !== undefined) {
        debug('captured image', ref.mediaType, ref.bytes)
        capture.images.push(ref)
      }
    } catch (error) {
      logger?.warn?.('cpa-image: attachment save failed', error)
    }
  }
}

/**
 * Queue the images carried by one parsed frame or document.
 * @param payload - a chat chunk, a chat completion, or an images response.
 * @param options - capture state and services.
 */
function queueFromPayload(payload, options) {
  if (payload === null || typeof payload !== 'object') return
  const batches = []
  for (const choice of payload.choices ?? []) {
    if (choice?.delta?.images !== undefined) batches.push(choice.delta.images)
    if (choice?.message?.images !== undefined) batches.push(choice.message.images)
  }
  if (payload.images !== undefined) batches.push(payload.images)
  if (Array.isArray(payload.data)) batches.push(payload.data)
  for (const batch of batches) options.capture.pending.push(takeImages(batch, options))
}

/**
 * Re-expose a response body while scanning its bytes.
 *
 * The caller keeps full control of reading: `pull` forwards exactly the chunk
 * the consumer asked for. Scanning is synchronous and only *queues* the
 * (asynchronous) attachment work, so it cannot apply backpressure.
 * @param response - the upstream response.
 * @param contentType - its content type.
 * @param options - capture state, services, and limits.
 * @returns a response over the same bytes, safe to hand to the adapter.
 */
function tapResponse(response, contentType, options) {
  if (response.body === null || NULL_BODY_STATUS.has(response.status)) return response

  const reader = response.body.getReader()
  const isSse = contentType.includes('text/event-stream')
  const scanner = isSse ? createSseScanner(frame => queueFromPayload(frame, options)) : undefined
  const decoder = new TextDecoder()
  let buffered = ''
  let finished = false

  const finish = () => {
    if (finished) return
    finished = true
    if (scanner !== undefined) {
      scanner.end()
    } else if (buffered !== '') {
      try {
        queueFromPayload(JSON.parse(buffered), options)
      } catch (error) {
        debug('body was not JSON', error?.message)
      }
    }
    buffered = ''
    debug('tap end')
  }

  const stream = new ReadableStream({
    async pull(controller) {
      try {
        const { done, value } = await reader.read()
        if (done === true) {
          finish()
          controller.close()
          return
        }
        if (scanner !== undefined) scanner.push(value)
        else buffered += decoder.decode(value, { stream: true })
        controller.enqueue(value)
      } catch (error) {
        finish()
        controller.error(error)
      }
    },
    cancel(reason) {
      debug('consumer cancelled the tapped body')
      finish()
      return reader.cancel(reason)
    },
  })

  const headers = new Headers(response.headers)
  // These describe the transport encoding we already decoded, not these bytes.
  headers.delete('content-encoding')
  headers.delete('content-length')
  headers.delete('transfer-encoding')

  debug('tap start', contentType)
  return new Response(stream, { status: response.status, statusText: response.statusText, headers })
}

/**
 * Create the per-plugin tap.
 * @param options - attachment service, size cap, and logger.
 * @returns the async-local store, install/dispose, and capture construction.
 */
export function createTee(options) {
  const als = new AsyncLocalStorage()
  let patched
  let previous

  /**
   * Wrap `globalThis.fetch` so tapped responses are observed in passing.
   * Idempotent; {@link createTee}'s `dispose` restores the previous one.
   */
  function install() {
    if (patched !== undefined) return
    previous = globalThis.fetch
    patched = async function tappedFetch(input, init) {
      const capture = als.getStore()
      const response = await previous.call(this, input, init)
      if (capture === undefined || capture === null) return response
      try {
        const url = urlOf(input)
        const contentType = response.headers.get('content-type') ?? ''
        const accepted = capture.accepts(url, contentType)
        debug('fetch', response.status, contentType, accepted ? 'tapped' : 'skipped', url)
        if (!accepted) return response
        return tapResponse(response, contentType, { ...options, capture, signal: capture.signal })
      } catch (error) {
        options.logger?.warn?.('cpa-image: could not tap a response', error)
        return response
      }
    }
    globalThis.fetch = patched
  }

  /** Restore the fetch implementation this plugin replaced. */
  function dispose() {
    if (patched !== undefined && globalThis.fetch === patched) globalThis.fetch = previous
    patched = undefined
    previous = undefined
  }

  /**
   * Build the capture one `llm/stream` call carries.
   * @param accepts - whether a URL/content-type pair is in scope.
   * @param signal - cancellation inherited from the call.
   * @returns the capture record.
   */
  function createCapture(accepts, signal) {
    return { images: [], pending: [], seen: new Set(), accepts, signal }
  }

  return { als, install, dispose, createCapture }
}

/**
 * Wait for a capture's background work, bounded so a stuck upstream cannot
 * hold a finished turn open.
 * @param capture - the capture record.
 */
export async function settleCapture(capture) {
  if (capture.pending.length === 0) return
  let timer
  const timeout = new Promise((resolve) => {
    timer = setTimeout(resolve, TAP_SETTLE_MS)
    timer.unref?.()
  })
  try {
    await Promise.race([Promise.allSettled(capture.pending), timeout])
  } finally {
    if (timer !== undefined) clearTimeout(timer)
  }
}
