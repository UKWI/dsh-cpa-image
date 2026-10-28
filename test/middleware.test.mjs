/**
 * Offline tests for the `llm/stream` middleware: a real fetch is replaced by a
 * stub that answers CPA's wire format, the tap parses the cloned response, and
 * the emitted chunk stream is asserted.
 *
 * Run with: node --test test/
 */

import test from 'node:test'
import assert from 'node:assert/strict'

import { apply } from '../src/index.js'

/**
 * Build a plugin context good enough for `apply`.
 * @param options - optional attachment override.
 * @returns the context plus its recorded listeners.
 */
function fakeCtx(options = {}) {
  const listeners = []
  const disposers = []
  const saved = []
  return {
    listeners,
    disposers,
    saved,
    attachments: {
      async saveImages(inputs) {
        return inputs.map((input, index) => {
          saved.push(input)
          return {
            attachmentId: `sha256:${String(index).padStart(64, '0')}`,
            mediaType: input.mediaType,
            bytes: input.data.byteLength,
            width: 2,
            height: 2,
            name: input.name,
          }
        })
      },
      ...options.attachments,
    },
    get: name => (name === 'credentials' ? options.credentials : undefined),
    logger: { warn() {}, info() {} },
    effect(fn) { disposers.push(fn()) },
    on(event, handler) { listeners.push({ event, handler }) },
  }
}

/**
 * Install a stub `fetch`, mount the plugin, and hand back the interceptor.
 * @param fetchImpl - the stub replacing `globalThis.fetch`.
 * @param config - loader-row config.
 * @param options - credential hook override.
 * @returns the interceptor plus a teardown.
 */
function mount(fetchImpl, config = {}, options = {}) {
  const original = globalThis.fetch
  globalThis.fetch = fetchImpl
  const ctx = fakeCtx(options)
  apply(ctx, { apiKey: 'sk-test', baseURL: 'https://cpa.example/v1', hosts: [], ...config })
  const listener = ctx.listeners.find(entry => entry.event === 'llm/stream')
  assert.ok(listener, 'the plugin must register an llm/stream listener')
  return {
    intercept: (request, next) => listener.handler(request, next),
    ctx,
    teardown() {
      for (const dispose of ctx.disposers) dispose()
      globalThis.fetch = original
    },
  }
}

/** One SSE body carrying a single inline image. */
function imageSse() {
  const png = Buffer.from('89504e470d0a1a0a', 'hex')
  return `data: ${JSON.stringify({
    choices: [{
      index: 0,
      delta: {
        role: 'assistant',
        images: [{ type: 'image_url', image_url: { url: `data:image/png;base64,${png.toString('base64')}` } }],
      },
    }],
  })}\n\n`
    + `data: ${JSON.stringify({ choices: [{ index: 0, delta: {}, finish_reason: 'stop' }] })}\n\n`
    + 'data: [DONE]\n\n'
}

/** Collect a whole chunk stream. */
async function collect(stream) {
  const chunks = []
  for await (const chunk of stream) chunks.push(chunk)
  return chunks
}

/** Call the (tapped) endpoint the way an adapter would: fetch, then read. */
async function readBody() {
  const response = await fetch('https://cpa.example/v1/chat/completions', { method: 'POST' })
  return response.text()
}

/** One ordinary chat request. */
const OPTIONS = {
  provider: 'cpa',
  model: 'gpt-6.1-terra',
  messages: [{ role: 'user', source: { kind: 'user' }, content: [{ type: 'text', text: 'draw a snail' }] }],
}

test('appends the image the adapter dropped and turns the empty response into a completion', async () => {
  const mounted = mount(async () => new Response(imageSse(), {
    headers: { 'content-type': 'text/event-stream' },
  }))
  try {
    // What pi-ai produces today: no blocks at all, then EMPTY_RESPONSE.
    const next = async function* next() {
      await readBody()
      yield { type: 'finish', reason: { kind: 'error', failure: { message: 'returned a completed response with no content', code: 'EMPTY_RESPONSE' } } }
    }
    const chunks = await collect(mounted.intercept(OPTIONS, next))

    const starts = chunks.filter(chunk => chunk.type === 'block-start')
    assert.deepEqual(starts.map(chunk => [chunk.index, chunk.blockType]), [[0, 'image']])
    const end = chunks.find(chunk => chunk.type === 'block-end' && chunk.block.type === 'image')
    assert.equal(end.block.attachment.mediaType, 'image/png')
    assert.equal(end.block.attachment.bytes, 8)
    assert.deepEqual(chunks.at(-1), { type: 'finish', reason: { kind: 'stop' } })
  } finally {
    mounted.teardown()
  }
})

test('keeps the adapter stream intact and puts the image after the text', async () => {
  const mounted = mount(async () => new Response(imageSse(), {
    headers: { 'content-type': 'text/event-stream' },
  }))
  try {
    const next = async function* next() {
      await readBody()
      yield { type: 'block-start', index: 0, blockType: 'text' }
      yield { type: 'text-delta', index: 0, text: 'here you go' }
      yield { type: 'block-end', index: 0, block: { type: 'text', text: 'here you go' } }
      yield { type: 'finish', reason: { kind: 'stop' }, replayState: { response: { kind: 'pi-ai' } } }
    }
    const chunks = await collect(mounted.intercept(OPTIONS, next))

    assert.deepEqual(
      chunks.map(chunk => chunk.type),
      ['block-start', 'text-delta', 'block-end', 'block-start', 'block-end', 'finish'],
    )
    assert.equal(chunks[3].index, 1, 'the image must not collide with the text block')
    assert.deepEqual(chunks.at(-1).reason, { kind: 'stop' })
    assert.ok(chunks.at(-1).replayState !== undefined, 'the adapter finish must be preserved')
  } finally {
    mounted.teardown()
  }
})

test('leaves a text-only call byte-for-byte identical', async () => {
  const mounted = mount(async () => new Response('data: [DONE]\n\n', {
    headers: { 'content-type': 'text/event-stream' },
  }))
  try {
    const produced = [
      { type: 'block-start', index: 0, blockType: 'text' },
      { type: 'text-delta', index: 0, text: 'plain answer' },
      { type: 'block-end', index: 0, block: { type: 'text', text: 'plain answer' } },
      { type: 'usage', usage: { inputTokens: 1, outputTokens: 2, totalTokens: 3 } },
      { type: 'finish', reason: { kind: 'stop' } },
    ]
    const next = async function* next() {
      await readBody()
      for (const chunk of produced) yield chunk
    }
    const chunks = await collect(mounted.intercept(OPTIONS, next))
    assert.deepEqual(chunks, produced)
  } finally {
    mounted.teardown()
  }
})

test('keeps EMPTY_RESPONSE when the upstream really sent nothing', async () => {
  const mounted = mount(async () => new Response('data: [DONE]\n\n', {
    headers: { 'content-type': 'text/event-stream' },
  }))
  try {
    const finish = { type: 'finish', reason: { kind: 'error', failure: { message: 'no content', code: 'EMPTY_RESPONSE' } } }
    const next = async function* next() {
      await readBody()
      yield finish
    }
    const chunks = await collect(mounted.intercept(OPTIONS, next))
    assert.deepEqual(chunks.at(-1), finish)
  } finally {
    mounted.teardown()
  }
})

test('answers an image model from /images/generations without calling the adapter', async () => {
  const png = Buffer.from('89504e470d0a1a0a', 'hex')
  let seen
  const mounted = mount(async (url, init) => {
    seen = { url, body: JSON.parse(init.body) }
    return new Response(JSON.stringify({ data: [{ b64_json: png.toString('base64') }] }), {
      headers: { 'content-type': 'application/json' },
    })
  })
  try {
    let adaptorCalled = false
    const chunks = await collect(mounted.intercept(
      { ...OPTIONS, model: 'gpt-image-2' },
      async function* next() { adaptorCalled = true; yield { type: 'finish', reason: { kind: 'stop' } } },
    ))

    assert.equal(adaptorCalled, false, 'image models must not reach the chat adapter')
    assert.equal(seen.url, 'https://cpa.example/v1/images/generations')
    assert.equal(seen.body.prompt, 'draw a snail')
    assert.equal(seen.body.response_format, 'b64_json')
    assert.deepEqual(chunks.map(chunk => chunk.type), ['block-start', 'block-end', 'finish'])
  } finally {
    mounted.teardown()
  }
})

test('reads a one-shot JSON body with message.images', async () => {
  const png = Buffer.from('89504e470d0a1a0a', 'hex')
  const mounted = mount(async () => new Response(JSON.stringify({
    choices: [{
      index: 0,
      finish_reason: 'stop',
      message: {
        role: 'assistant',
        content: '',
        images: [{ type: 'image_url', image_url: { url: `data:image/png;base64,${png.toString('base64')}` } }],
      },
    }],
  }), { headers: { 'content-type': 'application/json' } }))
  try {
    const next = async function* next() {
      await readBody()
      yield { type: 'finish', reason: { kind: 'error', failure: { code: 'EMPTY_RESPONSE', message: 'no content' } } }
    }
    const chunks = await collect(mounted.intercept(OPTIONS, next))
    assert.equal(chunks.filter(chunk => chunk.type === 'block-end' && chunk.block.type === 'image').length, 1)
    assert.deepEqual(chunks.at(-1).reason, { kind: 'stop' })
  } finally {
    mounted.teardown()
  }
})

test('resolves the images-endpoint key through the credentials seam', async () => {
  const png = Buffer.from('89504e470d0a1a0a', 'hex')
  let auth
  const credentials = {
    async resolve(ref) { return ref === 'CPA_API_KEY' ? { value: 'sk-from-store' } : undefined },
  }
  const mounted = mount(
    async (_url, init) => {
      auth = init.headers.authorization
      return new Response(JSON.stringify({ data: [{ b64_json: png.toString('base64') }] }), {
        headers: { 'content-type': 'application/json' },
      })
    },
    { apiKey: undefined },
    { credentials },
  )
  try {
    await collect(mounted.intercept(
      { ...OPTIONS, model: 'gpt-image-2' },
      async function* next() { yield { type: 'finish', reason: { kind: 'stop' } } },
    ))
    assert.equal(auth, 'Bearer sk-from-store')
  } finally {
    mounted.teardown()
  }
})
