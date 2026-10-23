import assert from 'node:assert/strict'
import { test } from 'node:test'
import { createMessageFeed, initialFeed, tailQuery } from '../src/lib/messageFeed.ts'

const message = (offset) => ({ partition: 0, offset, timestamp: '2026-10-22T00:00:00Z', key: null, value: String(offset), headers: [] })
const done = (next = {}) => ({ type: 'done', scanned: 1, result: { next, stopped_by: 'limit', script_errors: 0 } })

test('live tail always starts at now and keeps only live-compatible filters', () => {
  for (const start of ['oldest', 'offset', 'last', 'newest']) {
    assert.deepEqual(tailQuery({ start, offset: 4, since: 100, until: 200, limit: 50, exact: true, q: 'orders', partitions: '1', filter: 'return true', raw: true }),
      { start: 'newest', q: 'orders', partitions: '1', filter: 'return true', raw: true })
  }
})

function setup(t) {
  let state = initialFeed()
  const reads = [], tails = [], trims = []
  let resets = 0
  const feed = createMessageFeed({
    read: (cluster, topic, query, event, signal) => new Promise((resolve, reject) => reads.push({ cluster, topic, query, event, signal, resolve, reject })),
    tail: (cluster, topic, query, on) => {
      const tail = { on, closed: false }
      tails.push(tail)
      return () => (tail.closed = true)
    },
  }, () => ({ cluster: 'test', topic: 'orders' }), (s) => (state = s), {
    reset: () => resets++, trim: (kept) => trims.push(kept),
  })
  t.after(feed.dispose)
  return { feed, reads, tails, trims, get state() { return state }, get resets() { return resets } }
}

test('a replaced read cannot overwrite newer results even if transport ignores abort', async (t) => {
  const s = setup(t)
  const first = s.feed.load({ start: 'newest', q: 'old' })
  const second = s.feed.load({ start: 'newest', q: 'new' })
  assert.equal(s.reads[0].signal.aborted, true)
  s.reads[1].event({ type: 'message', message: message(5), scanned: 1 })
  s.reads[1].event(done())
  s.reads[1].resolve()
  await second
  s.reads[0].event({ type: 'message', message: message(1), scanned: 1 })
  s.reads[0].event(done())
  s.reads[0].resolve()
  await first
  assert.deepEqual(s.state.result.messages.map((m) => m.offset), [5])
  assert.equal(s.state.searched, 'new')
  assert.equal(s.state.loading, false)
})

test('stop preserves received messages and load more keeps the original query/cursor', async (t) => {
  const s = setup(t)
  const first = s.feed.load({ start: 'oldest', q: 'original' })
  s.reads[0].event({ type: 'progress', next: { 0: 0, 1: 8 }, scanned: 0 })
  s.reads[0].event({ type: 'message', message: message(2), scanned: 3 })
  s.feed.stopReading()
  s.reads[0].reject(new DOMException('Stopped', 'AbortError'))
  await first
  assert.equal(s.state.result.stopped_by, 'cancelled')
  assert.deepEqual(s.state.result.next, { 0: 3, 1: 8 })
  const more = s.feed.load({ start: 'newest', q: 'edited' }, true)
  assert.equal(s.reads[1].query.q, 'original')
  assert.equal(s.reads[1].query.after, '0:3,1:8')
  s.reads[1].event({ type: 'message', message: message(3), scanned: 1 })
  s.reads[1].event(done())
  s.reads[1].resolve()
  await more
  assert.deepEqual(s.state.result.messages.map((m) => m.offset), [2, 3])
  assert.equal(s.state.result.scanned, 4)
})

test('tail replaces a read, pauses, caps rows and discards stale callbacks', async (t) => {
  const s = setup(t)
  const reading = s.feed.load({ start: 'newest' })
  s.feed.startLive({ start: 'newest' })
  s.reads[0].event(done())
  s.reads[0].resolve()
  await reading
  assert.equal(s.state.result.stopped_by, 'live')
  s.feed.pause(true)
  s.tails[0].on.event({ message: message(9999), scanned: 1, matched: 1 })
  s.feed.pause(false)
  for (let i = 0; i < 2001; i++) s.tails[0].on.event({ message: message(i), scanned: i + 2, matched: i + 2 })
  s.feed.stopLive()
  assert.equal(s.state.result.messages.length, 2000)
  assert.equal(s.state.result.messages[0].offset, 2000)
  assert.equal(s.trims[0].has('0:0'), false)
  assert.equal(s.feed.fresh.has('0:0'), false)
  s.tails[0].on.error('late error')
  assert.equal(s.state.error, '')
  assert.equal(s.tails[0].closed, true)
})

test('leaving the page prevents pending reads from publishing into the view', async (t) => {
  const s = setup(t)
  const reading = s.feed.load({ start: 'oldest' })
  const before = s.state
  s.feed.dispose()
  s.reads[0].event(done())
  s.reads[0].resolve()
  await reading
  assert.equal(s.reads[0].signal.aborted, true)
  assert.equal(s.state, before)
})
