import assert from 'node:assert/strict'
import { test } from 'node:test'
import { exportJSON, exportCSV } from '../src/lib/messageExport.ts'
import { parseFile, RecordReader } from '../src/lib/importFile.ts'

const message = (extra = {}) => ({ partition: 0, offset: 1, timestamp: '2026-10-22T00:00:00Z',
  key: 'k', value: 'v', headers: [], truncated: false, ...extra })

test('CSV round-trip distinguishes null, empty and literal null payloads', () => {
  const messages = [message({ key: null, value: null }), message({ key: '', value: '' }), message({ key: 'null', value: 'null' })]
  const csv = exportCSV(messages)
  const parsed = parseFile(csv, 'csv', 'record')
  assert.deepEqual(parsed.problems, [])
  assert.deepEqual(parsed.records.map(({ key, value }) => [key, value]), [[null, null], ['', ''], ['null', 'null']])
  // The extended metadata also survives arbitrary stream boundaries.
  const reader = new RecordReader('csv', 'record')
  const items = [...csv].flatMap((c) => reader.feed(c)).concat(reader.end())
  assert.deepEqual(items.map((it) => it.record), parsed.records)
})

test('CSV keeps binary, headers, CR, schema IDs and protobuf message types', () => {
  const headers = [{ key: 'h', value: '/w==', encoding: 'base64' }, { key: 'n', value: '', encoding: 'utf8', null: true }]
  const csv = exportCSV([message({ key: '/w==', key_encoding: 'base64', value: '{"id":9007199254740993}',
    value_format: 'protobuf', value_schema_id: 12, value_schema_message: 'example.Order', headers }), message({ value: 'a\rb' })])
  const { records, problems } = parseFile(csv, 'csv', 'record')
  assert.deepEqual(problems, [])
  assert.equal(records[0].key_encoding, 'base64')
  assert.equal(records[0].value, '{"id":9007199254740993}')
  assert.deepEqual(records[0].value_schema, { id: 12, message: 'example.Order' })
  assert.deepEqual(records[0].headers, headers)
  assert.equal(records[1].value, 'a\rb')
})

test('CSV and JSON refuse to re-import truncated or undecodable records', () => {
  for (const [format, encode] of [['csv', exportCSV], ['json', exportJSON]]) {
    const parsed = parseFile(encode([message({ truncated: true }), message({ decode_error: 'bad schema' })]), format, 'record')
    assert.equal(parsed.records.length, 0)
    assert.match(parsed.problems[0].message, /truncated/)
    assert.match(parsed.problems[1].message, /decoded/)
  }
})

test('JSON export preserves large integers without rounding', () => {
  const text = '{"id":9007199254740993,"n":1.50}'
  const exported = exportJSON([message({ value: text, value_format: 'json' })])
  assert.equal(parseFile(exported, 'json', 'record').records[0].value, text)
})

test('JSON export keeps JSON null/string documents distinct from tombstones and text', () => {
  const values = [null, '', 'null', '"hello"', '""', 'true', '123']
  const parsed = parseFile(exportJSON(values.map((value) => message({ value, value_format: 'json' }))), 'json', 'record')
  assert.deepEqual(parsed.problems, [])
  assert.deepEqual(parsed.records.map((r) => r.value), values)
})

test('CSV refuses invalid null flags and schema IDs instead of silently dropping metadata', () => {
  for (const csv of ['value,value_null\nx,yes', 'value,value_schema_id\nx,invalid', 'value,key_schema_id\nx,-1']) {
    const parsed = parseFile(csv, 'csv', 'record')
    assert.equal(parsed.records.length, 0)
    assert.equal(parsed.problems.length, 1)
  }
})

test('CSV re-saved by a spreadsheet keeps its TRUE/FALSE null flags', () => {
  const { records, problems } = parseFile('key,value,key_null,value_null\n,x,TRUE,FALSE\n', 'csv', 'record')
  assert.deepEqual(problems, [])
  assert.equal(records[0].key, null)
  assert.equal(records[0].value, 'x')
})

test('a null payload never carries an encoding', () => {
  const { records } = parseFile('key,value,key_encoding,key_null\n,x,base64,true\n', 'csv', 'record')
  assert.equal(records[0].key, null)
  assert.equal(records[0].key_encoding, undefined)
})
