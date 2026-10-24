import assert from 'node:assert/strict'
import { test } from 'node:test'
import { COLS, WIDE_COLS, colCount, fitWidth, hit, keyWidth, listMinWidth, preview, timeWidth } from '../src/lib/messageLayout.ts'
import { toggled } from '../src/lib/sets.ts'

test('the key column fits the longest key within its caps', () => {
  assert.equal(keyWidth([], false), 120)
  const long = [{ key: 'x'.repeat(100) }]
  assert.equal(keyWidth(long, false), 440)
  assert.equal(keyWidth(long, true), 240)
  assert.equal(keyWidth([{ key: 'x'.repeat(20) }], false), Math.ceil(20 * 7.6) + 36)
  assert.equal(keyWidth([{ key: null }], false), 120)
})

test('time column width follows the time format', () => {
  assert.equal(timeWidth('relative'), 130)
  assert.equal(timeWidth('unix'), 130)
  assert.equal(timeWidth('utc'), 210)
  assert.equal(timeWidth('local'), 200)
})

test('the list minimum is the sum of its columns and the value room', () => {
  const w = { readOnly: false, offset: 84, time: 200, key: 120, fields: [140, 60] }
  assert.equal(listMinWidth(w), COLS.pick + COLS.toggle + COLS.partition + 84 + 200 + 120 + 200 + COLS.value)
  assert.equal(listMinWidth({ ...w, readOnly: true }), listMinWidth(w) - COLS.pick)
})

test('column count matches the rendered columns', () => {
  assert.equal(colCount(false, false, 2), 1 + WIDE_COLS + 2)
  assert.equal(colCount(true, false, 0), WIDE_COLS)
  assert.equal(colCount(false, true, 3), 2)
})

test('a fitted width has slack and stays within bounds', () => {
  assert.equal(fitWidth(100.2), 105)
  assert.equal(fitWidth(1), 48)
  assert.equal(fitWidth(5000), 1200)
})

test('the value preview moves to a deep search match', () => {
  assert.equal(preview(null, ''), 'null')
  assert.equal(preview('short', ''), 'short')
  assert.equal(preview('a'.repeat(200), ''), 'a'.repeat(160) + '…')
  const v = 'a'.repeat(300) + 'NEEDLE' + 'b'.repeat(300)
  const p = preview(v, 'needle')
  assert.ok(p.startsWith('…') && p.endsWith('…'))
  assert.ok(p.includes('NEEDLE'))
})

test('hit splits around the first case-insensitive match', () => {
  assert.deepEqual(hit('Hello World', 'world'), ['Hello ', 'World', ''])
  assert.deepEqual(hit('abc', ''), ['abc', '', ''])
  assert.deepEqual(hit('abc', 'x'), ['abc', '', ''])
})

test('toggled adds or removes without changing the original', () => {
  const a = new Set([1])
  assert.deepEqual([...toggled(a, 2)], [1, 2])
  assert.deepEqual([...toggled(a, 1)], [])
  assert.deepEqual([...a], [1])
})
