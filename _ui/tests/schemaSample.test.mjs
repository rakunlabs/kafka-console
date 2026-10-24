import assert from 'node:assert/strict'
import { test } from 'node:test'
import { avroSample, jsonSample, schemaSample } from '../src/lib/schemaSample.ts'
import { isBase64 } from '../src/lib/format.ts'

test('Avro samples follow the record, using defaults and logical types', () => {
  const now = new Date('2026-10-24T08:00:00Z')
  const schema = {
    type: 'record', name: 'Order', fields: [
      { name: 'id', type: 'string' },
      { name: 'qty', type: 'int' },
      { name: 'note', type: ['null', 'string'] },
      { name: 'status', type: { type: 'enum', name: 'S', symbols: ['NEW', 'DONE'] } },
      { name: 'at', type: { type: 'long', logicalType: 'timestamp-millis' } },
      { name: 'day', type: { type: 'int', logicalType: 'date' } },
      { name: 'tags', type: { type: 'array', items: 'string' } },
      { name: 'flag', type: 'boolean', default: true },
    ],
  }
  assert.deepEqual(avroSample(schema, 0, now), {
    id: '', qty: 0, note: '', status: 'NEW', at: '2026-10-24T08:00:00.000Z', day: '2026-10-24', tags: [], flag: true,
  })
})

test('Avro recursion stops at the depth limit', () => {
  const node = { type: 'record', name: 'Node', fields: [{ name: 'next', type: ['null', 'Node'] }] }
  assert.doesNotThrow(() => avroSample(node))
})

test('JSON Schema samples use defaults, enums and the first non-null type', () => {
  const schema = {
    type: 'object', properties: {
      id: { type: 'string' }, n: { type: ['null', 'integer'] }, kind: { enum: ['a', 'b'] },
      ok: { type: 'boolean', default: true }, list: { type: 'array' }, nested: { type: 'object', properties: { x: { type: 'number' } } },
    },
  }
  assert.deepEqual(jsonSample(schema), { id: '', n: 0, kind: 'a', ok: true, list: [], nested: { x: 0 } })
})

test('schemaSample pretty-prints by registry type', () => {
  assert.equal(schemaSample('JSON', '{"type":"object","properties":{"a":{"type":"string"}}}'), '{\n  "a": ""\n}')
  assert.equal(schemaSample('PROTOBUF', 'syntax = "proto3";'), '{}')
  assert.throws(() => schemaSample('AVRO', 'not json'))
})

test('base64 matches the server: padded, line breaks allowed', () => {
  assert.equal(isBase64('aGVsbG8='), true)
  assert.equal(isBase64('aGVs\r\nbG8='), true)
  assert.equal(isBase64(''), true)
  assert.equal(isBase64('aGVsbG8'), false)
  assert.equal(isBase64('a$=='), false)
})
