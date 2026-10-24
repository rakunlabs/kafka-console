import assert from 'node:assert/strict'
import { test } from 'node:test'
import { api, leavePage, isAbort, withSignal } from '../src/lib/api.ts'
import { HTTPError } from '../src/lib/http.ts'

test('leaving a page rejects its reads and runs caller cleanup', async (t) => {
  t.mock.method(globalThis, 'fetch', async (_url, { signal }) => new Promise((_resolve, reject) => {
    signal.addEventListener('abort', () => reject(signal.reason), { once: true })
  }))
  let cleaned = false
  const read = api.topic('c', 't').finally(() => { cleaned = true })
  leavePage()
  await assert.rejects(read, isAbort)
  assert.equal(cleaned, true)
})

test('a response arriving after navigation cannot become a successful stale read', async (t) => {
  const reply = Promise.withResolvers()
  t.mock.method(globalThis, 'fetch', () => reply.promise)
  const read = api.topic('c', 't')
  leavePage()
  reply.resolve(new Response('{}'))
  await assert.rejects(read, isAbort)
})

test('cancellation during response decoding also settles with AbortError', async (t) => {
  const body = Promise.withResolvers()
  const started = Promise.withResolvers()
  t.mock.method(globalThis, 'fetch', async () => ({ status: 200, ok: true, text: () => {
    started.resolve()
    return body.promise
  } }))
  const read = api.topic('c', 't')
  await started.promise
  leavePage()
  body.resolve('{}')
  await assert.rejects(read, isAbort)
})

test('writes and app-wide reads outlive page navigation', async (t) => {
  const reply = Promise.withResolvers()
  const signals = []
  t.mock.method(globalThis, 'fetch', (_url, opts) => { signals.push(opts.signal); return reply.promise })
  const write = api.produce('c', 't', { key: null, value: 'x', headers: [], partition: null })
  const app = api.names('c')
  leavePage()
  assert.deepEqual(signals, [undefined, undefined])
  reply.resolve({ status: 200, ok: true, text: async () => '{}' })
  await Promise.all([write, app])
})

test('explicit scoped cancellation rejects without aborting unrelated reads', async (t) => {
  t.mock.method(globalThis, 'fetch', async (_url, { signal }) => new Promise((resolve, reject) => {
    if (!signal.aborted) signal.addEventListener('abort', () => reject(signal.reason), { once: true })
    else reject(signal.reason)
  }))
  const ctl = new AbortController()
  const read = withSignal(ctl.signal, () => api.topic('c', 't'))
  ctl.abort()
  await assert.rejects(read, isAbort)
})

test('HTTP errors retain their status for safe import retry decisions', async (t) => {
  t.mock.method(globalThis, 'fetch', async () => new Response('{"message":"bad record"}', { status: 400 }))
  await assert.rejects(api.importRecords('c', 't', {}), (e) => e instanceof HTTPError && e.status === 400 && e.message === 'bad record')
})

test('a garbled live tail event ends the stream as an error', async (t) => {
  const { tail } = await import('../src/lib/api.ts')
  const listeners = {}
  let closed = false
  class FakeEventSource {
    constructor(url) { this.url = url }
    addEventListener(type, fn) { listeners[type] = fn }
    close() { closed = true }
  }
  globalThis.EventSource = FakeEventSource
  t.after(() => delete globalThis.EventSource)
  const events = []
  const errors = []
  tail('c', 't', {}, { event: (e) => events.push(e), error: (m) => errors.push(m), end: () => {} })
  listeners.message({ data: '{"scanned":1,"matched":0}' })
  listeners.message({ data: 'not json' })
  assert.equal(events.length, 1)
  assert.deepEqual(errors, ['The live tail sent an unreadable event'])
  assert.equal(closed, true)
})
