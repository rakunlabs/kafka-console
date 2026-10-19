import assert from 'node:assert/strict'
import { test } from 'node:test'
import { showTime, pick, splitPath, cell } from '../src/lib/viewer.ts'

const at = '2026-10-19T12:00:00.250Z'
const ms = Date.parse(at)

test('formats a record time in each format', () => {
  assert.equal(showTime(at, 'unix'), String(ms))
  assert.equal(showTime(at, 'utc'), '2026-10-19 12:00:00.250Z')
  assert.equal(showTime(at, 'relative', ms + 90_000), '1 minute ago')
  assert.equal(showTime(at, 'relative', ms + 2 * 3600_000), '2 hours ago')
  assert.equal(showTime(at, 'relative', ms - 30_000), 'in 30 seconds')
  assert.equal(showTime(at, 'relative', ms + 200), 'now')
  assert.match(showTime(at, 'local'), /^2026-10-\d\d \d\d:00:00\.250$/)
})

test('splits paths with dots, indexes and quoted keys', () => {
  assert.deepEqual(splitPath('order.items.0.sku'), ['order', 'items', '0', 'sku'])
  assert.deepEqual(splitPath('items[1].id'), ['items', '1', 'id'])
  assert.deepEqual(splitPath('user["first.name"]'), ['user', 'first.name'])
})

test('picks nested fields and renders them as cells', () => {
  const v = { order: { id: 7, items: [{ sku: 'a' }, { sku: 'b' }], tags: null }, 'a.b': true }
  assert.equal(cell(pick(v, 'order.id')), '7')
  assert.equal(cell(pick(v, 'order.items.1.sku')), 'b')
  assert.equal(cell(pick(v, 'order.tags')), 'null')
  assert.equal(cell(pick(v, 'order.missing')), '')
  assert.equal(cell(pick(v, 'order.id.deeper')), '')
  assert.equal(cell(pick(v, '["a.b"]')), 'true')
  assert.equal(cell(pick(v, 'order.items.0')), '{"sku":"a"}')
})
