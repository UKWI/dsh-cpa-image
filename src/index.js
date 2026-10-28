/**
 * `dsh-cpa-image` — image-aware model calls for CPA (CLIProxyAPI) and any other
 * OpenAI-compatible upstream, **without replacing the provider adapter**.
 *
 * Why this is a middleware and not an adapter: the harness asks exactly one
 * adapter per provider route, and that adapter is where every provider's
 * config, credential, retry, replay, and tool plumbing lives. Replacing it to
 * fix one field means owning a route forever, per provider. The `llm/stream`
 * waterfall runs *above* adapter selection, so a listener here wraps every
 * model call — whatever provider and protocol produced it.
 *
 * What it does on each call:
 *   1. image models (`gpt-image-*`) are answered straight from
 *      `/v1/images/generations`, because CPA refuses them on `chat/completions`
 *      with a 503;
 *   2. everything else runs through the original adapter untouched, while the
 *      fetch tap (see `capture.js`) collects any `delta.images` /
 *      `message.images` the adapter is about to drop, and those pictures are
 *      appended as durable image blocks;
 *   3. when the adapter reported `EMPTY_RESPONSE` because the content was empty
 *      apart from those images, the error is replaced with a normal completion.
 *
 * @module dsh-cpa-image
 */

import { createTee, settleCapture } from './capture.js'
import { httpFailure, mapUsage, normalizeImage, retryAfterMs } from './wire.js'

/** Plugin name used by the loader row. */
export const name = 'dsh-cpa-image'

/** Services this plugin needs before `apply` runs. */
export const inject = ['llm', 'attachments']

/** Default `User-Agent`: the harness's own attribution token. */
const DEFAULT_USER_AGENT = 'deepseek-harness (+https://github.com/deepseek-ai/deepseek-harness)'

/** Models CPA serves only through `/v1/images/generations`. */
const DEFAULT_IMAGE_MODELS = Object.freeze([
  'gpt-image-1.5',
  'gpt-image-2',
  'gpt-image-2.5',
  'gpt-image-2.5-flare',
  'gpt-image-2.5-sunburst',
])

/**
 * Merge the loader row's config over the defaults.
 * @param raw - the `config:` object of the loader row, if any.
 * @returns the normalized configuration.
 */
export function normalizeConfig(raw) {
  const source = raw ?? {}
  return {
    baseURL: typeof source.baseURL === 'string' && source.baseURL !== ''
      ? source.baseURL.replace(/\/+$/, '')
      : 'http://127.0.0.1:8317/v1',
    apiKey: typeof source.apiKey === 'string' ? source.apiKey : undefined,
    apiKeyEnv: typeof source.apiKeyEnv === 'string' ? source.apiKeyEnv : 'CPA_API_KEY',
    userAgent: typeof source.userAgent === 'string' && source.userAgent !== ''
      ? source.userAgent
      : DEFAULT_USER_AGENT,
    /** Hosts whose responses are tapped; empty means every host. */
    hosts: Array.isArray(source.hosts) ? source.hosts.map(String) : [],
    /** Models CPA only serves through the images endpoint. */
    imageModels: Array.isArray(source.imageModels) && source.imageModels.length > 0
      ? source.imageModels.map(String)
      : [...DEFAULT_IMAGE_MODELS],
    autoImageModels: source.autoImageModels !== false,
    /** Largest accepted generated image; anything bigger is skipped, not fatal. */
    maxImageBytes: Number.isFinite(source.maxImageBytes) ? source.maxImageBytes : 32 * 1024 * 1024,
    /** Forwarded to /images/generations (`size`, `quality`, `background`, `n`). */
    imagesOptions: source.imagesOptions !== null && typeof source.imagesOptions === 'object'
      ? { ...source.imagesOptions }
      : {},
  }
}

/**
 * Build the response-tap filter for one configuration.
 * @param hosts - allowed hostnames; empty taps every host.
 * @returns a predicate over (url, content-type).
 */
function tapFilter(hosts) {
  return (url, contentType) => {
    if (!/^https?:/i.test(url)) return false
    if (!contentType.includes('text/event-stream') && !contentType.includes('application/json')) return false
    if (hosts.length === 0) return true
    try {
      return hosts.includes(new URL(url).hostname)
    } catch {
      return false
    }
  }
}

/**
 * Whether one model id must go through the images endpoint.
 * @param cfg - normalized configuration.
 * @param model - the model id on the request.
 * @returns true when the model cannot be served by chat/completions.
 */
function isImageModel(cfg, model) {
  if (cfg.imageModels.includes(model)) return true
  return cfg.autoImageModels && /image/i.test(model)
}

/**
 * Extract the prompt an image model should draw.
 *
 * Only the newest message the human typed counts. A harness session also
 * carries runtime-context snapshots in the user role ("Current working
 * directory: …"), and handing those to an image model turns the picture into a
 * portrait of whatever path the session happens to run in.
 * @param options - the harness request.
 * @returns the trimmed prompt, or an empty string when there is none.
 */
export function lastUserText(options) {
  const typed = []
  const any = []
  for (const message of options.messages) {
    if (message.role !== 'user') continue
    const text = message.content
      .filter(block => block.type === 'text')
      .map(block => block.text)
      .join('\n')
      .trim()
    if (text === '') continue
    any.push(text)
    if (message.source?.kind === 'user') typed.push(text)
  }
  return typed.at(-1) ?? any.at(-1) ?? ''
}

/**
 * Read a response body as bounded diagnostic text.
 * @param response - the failed response.
 * @returns the body, truncated to 2 KiB.
 */
async function failureDetail(response) {
  try {
    const text = await response.text()
    return text.length > 2048 ? `${text.slice(0, 2048)}…` : text
  } catch {
    return ''
  }
}

/**
 * Build the terminal chunk for a request that never produced a response.
 * @param error - the thrown value.
 * @param signal - the call's cancellation signal.
 * @returns a `finish` chunk with `aborted` or transport classification.
 */
function escapeFailure(error, signal) {
  if (signal?.aborted === true) {
    return { type: 'finish', reason: { kind: 'aborted', failure: { message: 'request aborted', code: 'ABORTED' } } }
  }
  const message = error instanceof Error ? error.message : String(error)
  const code = /\b(?:fetch failed|ECONN|socket|terminated|other side close|premature close)\b/i.test(message)
    ? 'TRANSPORT'
    : 'NETWORK'
  return { type: 'finish', reason: { kind: 'error', failure: { message, code } } }
}

/**
 * Resolve the API key used only by the images endpoint.
 *
 * The credentials seam is asked first: `launchEnvironment` is only the process
 * environment plus project `.env` files, so a key stored in
 * `~/.dsh/.credentials.yaml` is invisible there.
 * @param ctx - plugin context.
 * @param cfg - normalized configuration.
 * @returns the bearer token.
 */
async function resolveApiKey(ctx, cfg) {
  const candidates = []
  if (cfg.apiKey !== undefined && cfg.apiKey !== '') candidates.push(cfg.apiKey)

  const credentials = ctx.get('credentials')
  if (credentials !== undefined && cfg.apiKeyEnv !== '') {
    try {
      const hit = await credentials.resolve(cfg.apiKeyEnv)
      if (typeof hit?.value === 'string') candidates.push(hit.value)
    } catch (error) {
      ctx.logger?.warn?.(`cpa-image: credential ref "${cfg.apiKeyEnv}" could not be resolved`)
      ctx.logger?.warn?.(error)
    }
  }
  if (cfg.apiKeyEnv !== '') {
    candidates.push(ctx.get('launchEnvironment')?.get(cfg.apiKeyEnv)?.value)
    candidates.push(process.env[cfg.apiKeyEnv])
  }

  const key = candidates.find(candidate => typeof candidate === 'string' && candidate !== '')
  if (key === undefined) {
    throw new Error(
      'cpa-image: no API key for the images endpoint'
      + ` (set apiKey, or store the ${cfg.apiKeyEnv} credential in ~/.dsh/.credentials.yaml)`,
    )
  }
  return key
}

/**
 * Normalize and persist one image object.
 * @param ctx - plugin context.
 * @param cfg - normalized configuration.
 * @param image - one raw image object.
 * @param signal - cancellation.
 * @returns the durable reference, or undefined when the image is unusable.
 */
async function saveImage(ctx, cfg, image, signal) {
  let normalized
  try {
    normalized = await normalizeImage(image, signal, cfg.maxImageBytes)
  } catch (error) {
    ctx.logger?.warn?.('cpa-image: image normalization failed')
    ctx.logger?.warn?.(error)
    return undefined
  }
  if (normalized.skipped !== undefined) {
    ctx.logger?.warn?.(`cpa-image: image skipped (${normalized.skipped})`)
    return undefined
  }
  try {
    const refs = await ctx.attachments.saveImages([{
      data: normalized.data,
      mediaType: normalized.mediaType,
      name: `cpa-image-${String(Date.now())}`,
    }])
    return refs?.[0]
  } catch (error) {
    ctx.logger?.warn?.('cpa-image: attachment save failed')
    ctx.logger?.warn?.(error)
    return undefined
  }
}

/**
 * Answer one image-model request through `/v1/images/generations`.
 * @param ctx - plugin context.
 * @param cfg - normalized configuration.
 * @param options - the harness request.
 * @returns the chunk stream.
 */
async function* streamImages(ctx, cfg, options) {
  const signal = options.signal
  const prompt = lastUserText(options)
  if (prompt === '') {
    yield {
      type: 'finish',
      reason: {
        kind: 'error',
        failure: { message: 'an image model needs a text prompt in the last user message', code: 'INVALID_REQUEST' },
      },
    }
    return
  }

  let key
  try {
    key = await resolveApiKey(ctx, cfg)
  } catch (error) {
    yield escapeFailure(error, signal)
    return
  }

  let response
  try {
    response = await fetch(`${cfg.baseURL}/images/generations`, {
      method: 'POST',
      headers: {
        authorization: `Bearer ${key}`,
        'content-type': 'application/json',
        'user-agent': cfg.userAgent,
      },
      body: JSON.stringify({
        model: options.model,
        prompt,
        n: 1,
        response_format: 'b64_json',
        ...cfg.imagesOptions,
      }),
      signal,
    })
  } catch (error) {
    yield escapeFailure(error, signal)
    return
  }

  if (!response.ok) {
    const detail = await failureDetail(response)
    yield {
      type: 'finish',
      reason: {
        kind: 'error',
        failure: httpFailure(response.status, detail, retryAfterMs(response.headers.get('retry-after'))),
      },
    }
    return
  }

  let payload
  try {
    payload = await response.json()
  } catch (error) {
    yield escapeFailure(error, signal)
    return
  }

  if (payload?.error !== undefined) {
    yield {
      type: 'finish',
      reason: {
        kind: 'error',
        failure: {
          message: typeof payload.error?.message === 'string' ? payload.error.message : JSON.stringify(payload.error),
          code: 'PROVIDER_ERROR',
        },
      },
    }
    return
  }

  const images = Array.isArray(payload?.data) ? payload.data : []
  let index = 0
  let saved = 0
  for (const image of images) {
    const ref = await saveImage(ctx, cfg, image, signal)
    if (ref === undefined) continue
    yield { type: 'block-start', index, blockType: 'image' }
    yield { type: 'block-end', index, block: { type: 'image', attachment: ref } }
    index += 1
    saved += 1
  }

  if (saved === 0) {
    yield {
      type: 'finish',
      reason: {
        kind: 'error',
        failure: { message: 'the image endpoint returned no usable image', code: 'EMPTY_RESPONSE' },
      },
    }
    return
  }

  const usage = mapUsage(payload?.usage)
  if (usage !== undefined) yield { type: 'usage', usage }
  yield { type: 'finish', reason: { kind: 'stop' } }
}

/**
 * Whether one terminal chunk is the "completed with no content" failure.
 * @param finish - the adapter's finish chunk, if any.
 * @returns true when the stream reported an empty response.
 */
function isEmptyResponse(finish) {
  return finish?.type === 'finish'
    && finish.reason?.kind === 'error'
    && (finish.reason.failure?.code === 'EMPTY_RESPONSE' || /no content/i.test(finish.reason.failure?.message ?? ''))
}

/**
 * Wrap one model call: delegate to the original adapter, then append whatever
 * images the upstream returned beside the text.
 * @param ctx - plugin context.
 * @param cfg - normalized configuration.
 * @param tee - the fetch tap.
 * @param options - the harness request.
 * @param next - the wrapped call, provided by the `llm/stream` waterfall.
 * @returns the chunk stream.
 */
async function* intercept(ctx, cfg, tee, options, next) {
  if (isImageModel(cfg, options.model)) {
    yield* streamImages(ctx, cfg, options)
    return
  }

  const capture = tee.createCapture(tapFilter(cfg.hosts), options.signal)
  const iterator = await tee.als.run(capture, () => next()[Symbol.asyncIterator]())
  let maxIndex = -1
  let finish
  let completed = false

  try {
    for (;;) {
      const step = await tee.als.run(capture, () => iterator.next())
      if (step.done === true) break
      const chunk = step.value
      if (typeof chunk.index === 'number' && chunk.index > maxIndex) maxIndex = chunk.index
      if (chunk.type === 'finish') {
        finish = chunk
        continue
      }
      yield chunk
    }
    completed = true
  } finally {
    // The consumer stopped early (cancellation or a thrown failure): release
    // the adapter stream instead of leaking it.
    if (!completed) await iterator.return?.()
  }

  await settleCapture(capture)

  if (capture.images.length > 0) {
    for (const ref of capture.images) {
      maxIndex += 1
      yield { type: 'block-start', index: maxIndex, blockType: 'image' }
      yield { type: 'block-end', index: maxIndex, block: { type: 'image', attachment: ref } }
    }
    // The adapter dropped the pictures and then reported "no content"; with
    // them restored the turn simply succeeded.
    if (isEmptyResponse(finish)) finish = { type: 'finish', reason: { kind: 'stop' } }
  }

  if (finish !== undefined) yield finish
}

/**
 * Mount the middleware.
 * @param ctx - the plugin context.
 * @param config - the loader row's `config:` object.
 */
export function apply(ctx, config) {
  const cfg = normalizeConfig(config)
  const tee = createTee({
    attachments: ctx.attachments,
    maxImageBytes: cfg.maxImageBytes,
    logger: ctx.logger,
  })
  tee.install()
  ctx.effect(() => () => tee.dispose(), 'cpa-image: restore global fetch')
  ctx.on('llm/stream', (options, next) => intercept(ctx, cfg, tee, options, next))
  ctx.logger?.info?.(
    `cpa-image: intercepting model calls (inline images + image models: ${cfg.imageModels.join(', ')})`,
  )
}

export { createTee }
