import assert from 'node:assert/strict'
import { test } from 'node:test'
import { messageDraft, produceChanges, formatPayload } from '../src/lib/produce.ts'

const record = (extra = {}) => ({
  partition: 2, offset: 42, timestamp: '2026-10-18T10:00:00Z',
  key: '', value: '{"id":9007199254740993}', key_encoding: 'utf8', value_encoding: 'utf8',
  key_bytes: 0, value_bytes: 23, truncated: false,
  headers: [{ key: 'trace-id', value: '123', encoding: 'utf8' }], ...extra,
})

test('seeds a draft without parsing JSON or rounding large integers', () => {
  const source = record()
  const draft = messageDraft(source)
  assert.equal(draft.key, '')
  assert.equal(draft.value, source.value)
  assert.equal(draft.partition, 2)
  draft.headers[0].value = 'changed'
  assert.equal(source.headers[0].value, '123')
})

test('preserves null key, tombstone and empty strings as different values', () => {
  const draft = messageDraft(record({ key: null, value: null }))
  assert.equal(draft.key, null)
  assert.equal(draft.value, null)
  const changes = produceChanges(draft, { ...draft, key: '', value: '' }, 'orders', 'orders')
  assert.deepEqual(changes.map((c) => [c.field, c.before, c.after]), [['Key', '(null payload)', '(empty string)'], ['Value', '(null payload)', '(empty string)']])
})

test('preserves binary key, value, duplicate and null headers', () => {
  const source = record({ key: '/wA=', value: '/wE=', key_encoding: 'base64', value_encoding: 'base64',
    headers: [{ key: 'h', value: '/w==', encoding: 'base64' }, { key: 'h', value: '', encoding: 'utf8', null: true }] })
  const draft = messageDraft(source)
  assert.equal(draft.key_encoding, 'base64')
  assert.equal(draft.value_encoding, 'base64')
  assert.deepEqual(draft.headers, source.headers)
})

test('pins original schema IDs and full protobuf message types on both sides', () => {
  const draft = messageDraft(record({ key_schema_id: 7, key_format: 'protobuf', key_schema_message: 'shop.Envelope.Key',
    value_schema_id: 8, value_format: 'protobuf', value_schema_message: 'shop.FailedOrder' }))
  assert.deepEqual(draft.key_schema, { id: 7, message: 'shop.Envelope.Key' })
  assert.deepEqual(draft.value_schema, { id: 8, message: 'shop.FailedOrder' })
})

test('does not seed a truncated or undecodable message', () => {
  assert.throws(() => messageDraft(record({ truncated: true })), /full message/)
  assert.throws(() => messageDraft(record({ decode_error: 'schema unavailable' })), /schema unavailable/)
  assert.throws(() => messageDraft(record({ value_schema_id: 1, value_format: 'protobuf' })), /message type is missing/)
})

test('preview includes target, key, value, partition and header edits', () => {
  const before = messageDraft(record())
  const after = { ...before, key: 'fixed', value: '{"id":9007199254740994}', partition: null, headers: [] }
  const changes = produceChanges(before, after, 'orders.dlq', 'orders')
  assert.deepEqual(changes.map((c) => c.field), ['Topic', 'Partition', 'Key', 'Value', 'Headers'])
  assert.equal(changes.find((c) => c.field === 'Value').before, '(text)\n{"id":9007199254740993}')
  assert.equal(changes.find((c) => c.field === 'Value').after, '(text)\n' + after.value)
})

test('unchanged drafts have no spurious differences from default encodings', () => {
  const before = messageDraft(record({ key_encoding: undefined, value_encoding: undefined }))
  assert.deepEqual(produceChanges(before, { ...before, headers: before.headers.map((h) => ({ ...h, null: false })), key_encoding: 'utf8', value_encoding: 'utf8', compression: '' }, 'orders', 'orders'), [])
})

test('review distinguishes null payload, empty string and literal null text', () => {
  const before = messageDraft(record({ key: null, value: '' }))
  const changes = produceChanges(before, { ...before, key: 'null', value: 'null' }, 'orders', 'orders')
  assert.deepEqual(changes.map((c) => [c.before, c.after]), [['(null payload)', '(text)\nnull'], ['(empty string)', '(text)\nnull']])
})

test('formatting preserves large numbers, exponents, duplicate keys and string escapes', () => {
  const input = '{"id":9007199254740993,"decimal":0.12345678901234567890,"n":1e+999,"n":-0,"nested":[{},[],{"quoted":"\\u0061\\\"b"}]}'
  const formatted = formatPayload(input)
  const tokens = (s) => s.match(/"(?:\\.|[^"\\])*"|[^\s]/g)
  assert.deepEqual(tokens(formatted), tokens(input))
  assert.equal(formatPayload(' { "empty": [ ] } '), '{\n  "empty": []\n}')
  assert.equal(formatPayload('9007199254740993'), '9007199254740993')
  assert.throws(() => formatPayload('{invalid}'))
})

test('refuses to edit values decoded without the registry', () => {
  for (const format of ['protobuf', 'msgpack', 'cbor', 'uint', 'hex']) {
    assert.throws(() => messageDraft(record({ value_format: format })), /cannot encode it back/)
  }
  assert.doesNotThrow(() => messageDraft(record({ value_format: 'json' })))
  assert.doesNotThrow(() => messageDraft(record({ value_format: 'protobuf', value_schema_id: 3, value_schema_message: 'a.B' })))
})
