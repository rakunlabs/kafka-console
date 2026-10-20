import assert from 'node:assert/strict'
import { test } from 'node:test'
import { parseFile, detectFormat, rawElements, csvRows } from '../src/lib/importFile.ts'

test('keeps payload text exactly, including large integers and spacing', () => {
  const text = '[{"key":"a","value":{"id": 9007199254740993, "v":1.50}}, {"value":"plain \\"q\\""}]'
  const { records, problems } = parseFile(text, 'json', 'record')
  assert.deepEqual(problems, [])
  assert.equal(records[0].value, '{"id": 9007199254740993, "v":1.50}')
  assert.equal(records[1].value, 'plain "q"')
  assert.equal(records[1].key, null)
})

test('reads a console JSON export back with encodings, schema IDs, partitions and timestamps', () => {
  const exported = JSON.stringify([
    { partition: 2, offset: 7, timestamp: '2026-10-18T10:00:00.000Z', key: '/wA=', key_encoding: 'base64', value: { n: 1 },
      value_format: 'avro', value_schema_id: 12, headers: [{ key: 'h', value: '/w==', encoding: 'base64' }, { key: 'n', value: '', encoding: 'utf8', null: true }],
      truncated: false, value_bytes: 3, key_bytes: 2 },
    { partition: 0, offset: 1, timestamp: '2026-10-18T10:00:01.000Z', key: null, value: null, headers: [], truncated: false },
  ])
  const p = parseFile(exported, 'json', 'record')
  assert.deepEqual(p.problems, [])
  assert.ok(p.partitions && p.timestamps && p.schemaIds)
  const [a, b] = p.records
  assert.equal(a.key_encoding, 'base64')
  assert.equal(a.value, '{"n":1}')
  assert.deepEqual(a.value_schema, { id: 12, message: undefined })
  assert.equal(a.partition, 2)
  assert.equal(a.timestamp, Date.parse('2026-10-18T10:00:00.000Z'))
  assert.deepEqual(a.headers, [{ key: 'h', value: '/w==', encoding: 'base64' }, { key: 'n', value: '', encoding: 'utf8', null: true }])
  assert.equal(b.value, null)
})

test('refuses truncated exports and protobuf without a message type', () => {
  const p = parseFile('[{"value":"x","truncated":true},{"value":{},"value_format":"protobuf","value_schema_id":3}]', 'json', 'record')
  assert.equal(p.records.length, 0)
  assert.match(p.problems[0].message, /truncated/)
  assert.match(p.problems[1].message, /message type/)
})

test('JSON lines: blank lines skipped, bad lines reported with their number', () => {
  const p = parseFile('{"value":1}\n\n{nope}\n{"key":"k","value":"v","headers":{"a":"b","c":null}}\n', 'jsonl', 'record')
  assert.equal(p.records.length, 2)
  assert.deepEqual(p.problems.map((x) => x.item), [2])
  assert.deepEqual(p.records[1].headers, [{ key: 'a', value: 'b', encoding: 'utf8' }, { key: 'c', value: '', encoding: 'utf8', null: true }])
})

test('value shape sends each item as the whole value', () => {
  const p = parseFile('{"status":"NEW", "id": 12345678901234567890}\n"text"\n', 'jsonl', 'value')
  assert.deepEqual(p.records.map((r) => r.value), ['{"status":"NEW", "id": 12345678901234567890}', 'text'])
  const q = parseFile('[{"a":1}, 2]', 'json', 'record')
  assert.match(q.problems[0].message, /Value/)
})

test('CSV with quoted fields, CRLF and the export column layout', () => {
  const csv = 'partition,offset,timestamp,key,value,headers\r\n1,5,2026-10-18T10:00:00Z,k1,"{""a"":""x,y""}","[{""key"":""h"",""value"":""v"",""encoding"":""utf8""}]"\r\n0,6,1760000000000,,"line1\nline2",\r\n'
  const p = parseFile(csv, 'csv', 'record')
  assert.deepEqual(p.problems, [])
  assert.equal(p.records[0].value, '{"a":"x,y"}')
  assert.equal(p.records[0].partition, 1)
  assert.deepEqual(p.records[0].headers, [{ key: 'h', value: 'v', encoding: 'utf8' }])
  assert.equal(p.records[1].key, null)
  assert.equal(p.records[1].value, 'line1\nline2')
  assert.equal(p.records[1].timestamp, 1760000000000)
  assert.match(parseFile('a,b\n1,2', 'csv', 'record').problems[0].message, /value/)
  assert.throws(() => csvRows('"open'), /unterminated/)
})

test('plain lines and format detection', () => {
  assert.deepEqual(parseFile('a\n\nb\r\n', 'lines', 'value').records.map((r) => r.value), ['a', 'b'])
  assert.equal(detectFormat('x.csv', ''), 'csv')
  assert.equal(detectFormat('x.ndjson', ''), 'jsonl')
  assert.equal(detectFormat('dump', '  [1]'), 'json')
  assert.equal(detectFormat('dump', '{"a":1}\n'), 'jsonl')
  assert.deepEqual(rawElements('[ ]'), [])
  assert.match(parseFile('{"a":1}', 'json', 'record').problems[0].message, /array/)
  assert.match(parseFile('[{"value":"x","partition":-1}]', 'json', 'record').problems[0].message, /partition/)
})

import { RecordReader, FatalError, readFile } from '../src/lib/importFile.ts'

const samples = [
  ['json', 'record', '[ {"key":"a","value":{"s":"x\\"]},{y","n": 9007199254740993}} , {"value":"\\\\"}, {"value":12}, {bad}, {"value":null}\n]'],
  ['json', 'value', '[1, -2.5e3 ,"t" ,true,null,[1,[2]]]'],
  ['jsonl', 'record', '{"value":"a"}\r\n\r\n{nope}\r\n{"key":"k","value":{"q":"\\n"}}'],
  ['csv', 'record', 'key,value,partition\r\nk1,"say ""hi"",\r\nok",0\r\n,plain,1\n"",""""\n'],
  ['lines', 'value', 'one\r\ntwo\n\nthree'],
]

function whole(format, shape, text) {
  const r = new RecordReader(format, shape)
  return [...r.feed(text), ...r.end()]
}

function chunked(format, shape, text, cuts) {
  const r = new RecordReader(format, shape)
  const out = []
  let at = 0
  for (const c of [...cuts, text.length]) {
    out.push(...r.feed(text.slice(at, c)))
    at = c
  }
  out.push(...r.end())
  return out
}

test('chunk boundaries anywhere give the same items as one read', () => {
  for (const [format, shape, text] of samples) {
    const want = whole(format, shape, text)
    assert.ok(want.length > 2, `${format} sample parses`)
    for (let i = 1; i < text.length; i++) {
      assert.deepEqual(chunked(format, shape, text, [i]), want, `${format} split at ${i}`)
      for (let j = i + 1; j < text.length; j += 3) assert.deepEqual(chunked(format, shape, text, [i, j]), want, `${format} split at ${i},${j}`)
    }
    assert.deepEqual(chunked(format, shape, text, [...Array(text.length).keys()].slice(1)), want, `${format} one char at a time`)
  }
})

test('items keep their numbers and bad ones are reported in place', () => {
  const items = whole('json', 'record', samples[0][2])
  assert.deepEqual(items.map((i) => [i.item, !!i.problem]), [[1, false], [2, false], [3, false], [4, true], [5, false]])
  assert.equal(items[0].record.value, '{"s":"x\\"]},{y","n": 9007199254740993}')
  const csv = whole('csv', 'record', samples[3][2])
  assert.deepEqual(csv.map((i) => i.record.value), ['say "hi",\r\nok', 'plain', '"'])
})

test('structural errors stop the file', () => {
  for (const [format, text, re] of [
    ['json', '{"a":1}', /array/], ['json', '[1, 2', /not closed/], ['json', '[1] 2', /after the array/],
    ['json', '[1 2]', /expected , or ]/], ['json', '', /empty/], ['csv', 'a,b\n1,2', /value/], ['csv', 'value\n"open', /unterminated/],
  ]) {
    assert.throws(() => whole(format, 'record', text), (e) => e instanceof FatalError && re.test(e.message), `${format}: ${text}`)
  }
})

test('readFile streams a Blob in chunks', async () => {
  const lines = Array.from({ length: 5000 }, (_, i) => JSON.stringify({ key: `k${i}`, value: { n: i, pad: 'é'.repeat(50) } }))
  const blob = new Blob([lines.join('\n')])
  let bytes = 0
  let n = 0
  for await (const it of readFile(blob, 'jsonl', 'record', (b) => (bytes = b))) {
    assert.equal(it.record.key, `k${n}`)
    n++
  }
  assert.equal(n, 5000)
  assert.equal(bytes, blob.size)
})

test('reads the encoding columns of a CSV export, so binary payloads stay binary', () => {
  const csv = 'partition,offset,timestamp,key,value,headers,key_encoding,value_encoding\n0,1,1760000000000,AAE=,/w==,[],base64,base64\n0,2,1760000000000,,text,[],,\n'
  const { records, problems } = parseFile(csv, 'csv', 'record')
  assert.deepEqual(problems, [])
  assert.equal(records[0].key_encoding, 'base64')
  assert.equal(records[0].value_encoding, 'base64')
  assert.equal(records[0].value, '/w==')
  assert.equal(records[1].key, null)
  assert.equal(records[1].key_encoding, undefined)
  assert.equal(records[1].value_encoding, undefined)
})
