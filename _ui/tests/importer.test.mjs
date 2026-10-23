import assert from 'node:assert/strict'
import { test } from 'node:test'
import { checkFile, sendFile } from '../src/lib/importer.ts'
import { HTTPError } from '../src/lib/http.ts'

const file = (n, bad = new Set()) =>
  new Blob([Array.from({ length: n }, (_, i) => (bad.has(i + 1) ? '{oops}' : JSON.stringify({ key: `k${i + 1}`, value: i + 1, partition: (i + 1) % 3 }))).join('\n')])

// A fake server: records the keys it wrote, can refuse a batch or fail a
// record by key.
function server({ refuse = new Set(), fail = new Set(), dryRefuse = new Set() } = {}) {
  const s = { written: [], calls: [] }
  s.send = async (records, dry) => {
    s.calls.push({ dry, keys: records.map((r) => r.key) })
    const i = records.findIndex((r) => (dry ? dryRefuse : refuse).has(r.key))
    if (i >= 0) throw new HTTPError(400, `record ${i + 1}: refused ${records[i].key}`)
    if (dry) return records.map(() => ({ partition: -1, offset: -1 }))
    return records.map((r) => {
      if (fail.has(r.key)) return { partition: -1, offset: -1, error: `broker said no to ${r.key}` }
      s.written.push(r.key)
      return { partition: r.partition ?? 0, offset: s.written.length - 1 }
    })
  }
  return s
}

const opts = (s, extra = {}) => ({
  format: 'jsonl', shape: 'record', batchSize: 4, unfit: () => '', prepare: (r) => r, send: s.send, stopped: () => false, ...extra,
})

const keys = (a, b) => Array.from({ length: b - a + 1 }, (_, i) => `k${a + i}`)

test('check counts every item and asks the server in batches', async () => {
  const s = server()
  const r = await checkFile(file(10), opts(s))
  assert.deepEqual(r, { total: 10, problemCount: 0, problems: [], fatal: '', stopped: false })
  assert.deepEqual(s.calls.map((c) => [c.dry, c.keys.length]), [[true, 4], [true, 4], [true, 2]])
})

test('check reports local and server problems with file item numbers and stops asking the server', async () => {
  const s = server({ dryRefuse: new Set(['k6']) })
  const r = await checkFile(file(12, new Set([10])), opts(s, { unfit: (rec) => (rec.key === 'k11' ? 'no partition' : '') }))
  assert.equal(r.total, 12)
  assert.deepEqual(r.problems, [{ item: 6, message: 'refused k6' }, { item: 10, message: r.problems[1].message }, { item: 11, message: 'no partition' }])
  assert.equal(r.problemCount, 3)
  assert.equal(s.calls.length, 2)
})

test('check keeps only the first problems', async () => {
  const bad = new Set(Array.from({ length: 30 }, (_, i) => i + 1))
  const r = await checkFile(file(30, bad), opts(server()), 5)
  assert.equal(r.problemCount, 30)
  assert.equal(r.problems.length, 5)
})

test('send writes everything in order', async () => {
  const s = server()
  const r = await sendFile(file(10), opts(s))
  assert.deepEqual(s.written, keys(1, 10))
  assert.equal(r.written, 10)
  assert.equal(r.resumeAt, 0)
  assert.equal(r.error, '')
  assert.deepEqual(r.byPartition, { 0: 3, 1: 4, 2: 3 })
})

test('a refused batch stops before it and resuming completes the file exactly once', async () => {
  const s = server({ refuse: new Set(['k7']) })
  const r = await sendFile(file(10), opts(s))
  assert.equal(r.error, 'item 7: refused k7')
  assert.equal(r.resumeAt, 5)
  assert.equal(r.uncertain, undefined)
  assert.deepEqual(s.written, keys(1, 4))
  s.send = server().send
  const s2 = server()
  const again = await sendFile(file(10), opts(s2), r.resumeAt - 1)
  assert.deepEqual([...s.written, ...s2.written], keys(1, 10))
  assert.equal(again.resumeAt, 0)
})

test('a failed record resumes from it and counts later records written twice', async () => {
  const s = server({ fail: new Set(['k6']) })
  const r = await sendFile(file(10), opts(s))
  assert.equal(r.resumeAt, 6)
  assert.equal(r.rewritten, 2)
  assert.equal(r.written, 7)
  assert.match(r.error, /^item 6: broker said no to k6/)
  assert.deepEqual(s.written, ['k1', 'k2', 'k3', 'k4', 'k5', 'k7', 'k8'])
})

test('stop leaves the batch being filled unsent and resumes from its first item', async () => {
  const s = server()
  let n = 0
  const r = await sendFile(file(10), opts(s, { send: async (recs, dry) => { n++; return s.send(recs, dry) }, stopped: () => n >= 1 }))
  assert.deepEqual(s.written, keys(1, 4))
  assert.equal(r.resumeAt, 5)
  assert.equal(r.error, '')
})

test('an item that turned bad after the check flushes what is before it', async () => {
  const s = server()
  const r = await sendFile(file(10, new Set([6])), opts(s))
  assert.deepEqual(s.written, keys(1, 5))
  assert.equal(r.resumeAt, 6)
  assert.match(r.error, /^item 6: .*changed after it was checked/)
})

test('a broken file reports the first unwritten item', async () => {
  const s = server()
  const blob = new Blob(['[' + Array.from({ length: 6 }, (_, i) => JSON.stringify({ key: `k${i + 1}`, value: 1 })).join(',')])
  const r = await sendFile(blob, opts(s, { format: 'json' }))
  assert.deepEqual(s.written, keys(1, 4))
  assert.equal(r.resumeAt, 5)
  assert.match(r.error, /not closed/)
})

test('a lost response marks the batch unknown even when the server wrote it', async () => {
  const s = server()
  const r = await sendFile(file(6), opts(s, { send: async (records, dry) => {
    await s.send(records, dry)
    throw new TypeError('Failed to fetch')
  } }))
  assert.deepEqual(s.written, keys(1, 4))
  assert.equal(r.written, 0)
  assert.equal(r.resumeAt, 1)
  assert.deepEqual(r.uncertain, { first: 1, last: 4 })
})

test('timeouts, gateway errors and malformed responses are not safe refusals', async () => {
  for (const reply of [new HTTPError(408, 'timeout'), new HTTPError(502, 'gateway'), [], 'not JSON']) {
    const r = await sendFile(file(6), opts(server(), { send: async () => {
      if (reply instanceof Error) throw reply
      return reply
    } }))
    assert.deepEqual(r.uncertain, { first: 1, last: 4 })
    assert.equal(r.resumeAt, 1)
  }
})
