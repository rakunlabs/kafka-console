import assert from 'node:assert/strict'
import { test } from 'node:test'
import { readFileSync } from 'node:fs'
import { parseFile } from '../src/lib/importFile.ts'

// Files written by kafka-mcp search_messages output_file against a real broker.
// They pin the layout the two projects share: if either side changes it,
// this test fails.
const fixture = (name) => readFileSync(new URL(`./fixtures/${name}`, import.meta.url), 'utf8')

for (const [name, format] of [['kafka-mcp-export.jsonl', 'jsonl'], ['kafka-mcp-export.csv', 'csv']]) {
  test(`imports a kafka-mcp ${format} export exactly`, () => {
    const { records, problems } = parseFile(fixture(name), format, 'record')
    assert.deepEqual(problems, [])
    assert.equal(records.length, 6)
    const [tombstone, empty, binary, avro, long, doc] = records

    assert.equal(tombstone.key, null)
    assert.equal(tombstone.value, null)
    assert.deepEqual(tombstone.headers, [{ key: 'gone', value: '', encoding: 'utf8', null: true }])

    assert.equal(empty.key, '')
    assert.equal(empty.value, '')

    assert.equal(binary.key, '/wA=')
    assert.equal(binary.key_encoding, 'base64')
    assert.equal(binary.value, 'AP4=')
    assert.equal(binary.value_encoding, 'base64')
    assert.deepEqual(binary.headers, [{ key: 'raw', value: '/w==', encoding: 'base64' }, { key: 'plain', value: 'v', encoding: 'utf8' }])

    assert.equal(avro.value, '{"amount":900,"id":"o-1"}')
    assert.deepEqual(avro.value_schema, { id: 1, message: 'shop.Order' })
    assert.equal(avro.key_schema, undefined)

    assert.equal(long.value.length, 2000)
    assert.equal(doc.value, '{"id":9007199254740993}')
    for (const r of records) assert.equal(r.timestamp, Date.parse('2026-10-23T09:00:00Z'))
  })
}

test('imports a kafka-mcp transform_messages file, whose encoding covers key and value', () => {
  const { records, problems } = parseFile('{"topic":"orders","value":"/w==","key":"AAE=","headers":{"h":"v"},"partition":2,"encoding":"base64"}\n{"topic":"orders","value":"text","key":"k"}', 'jsonl', 'record')
  assert.deepEqual(problems, [])
  assert.equal(records[0].key_encoding, 'base64')
  assert.equal(records[0].value_encoding, 'base64')
  assert.equal(records[0].partition, 2)
  assert.deepEqual(records[0].headers, [{ key: 'h', value: 'v', encoding: 'utf8' }])
  assert.equal(records[1].key_encoding, undefined)
  assert.equal(records[1].value_encoding, undefined)
})

test('an older kafka-mcp search export keeps a binary value binary and its text key text', () => {
  const { records } = parseFile('{"partition":0,"offset":1,"timestamp":"2026-10-23T09:00:00Z","key":"k","value":"/w==","encoding":"base64","format":"binary","value_bytes":1,"truncated":false}', 'jsonl', 'record')
  assert.equal(records[0].value_encoding, 'base64')
  assert.equal(records[0].key_encoding, undefined)
})
