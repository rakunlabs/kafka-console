import assert from 'node:assert/strict'
import { test } from 'node:test'
import { offsetDelta, inputMs, localInput } from '../src/lib/format.ts'

test('offsetDelta signs the move and dashes a missing commit', () => {
  assert.equal(offsetDelta({ from: -1, to: 5 }), '—')
  assert.equal(offsetDelta({ from: 10, to: 15 }), '+5')
  assert.equal(offsetDelta({ from: 15, to: 10 }), '-5')
  assert.equal(offsetDelta({ from: 3, to: 3 }), '0')
})

test('datetime-local text and milliseconds round-trip', () => {
  assert.equal(inputMs(''), undefined)
  const ms = Date.UTC(2026, 9, 24, 12, 30, 5)
  assert.equal(inputMs(localInput(ms)), ms)
  assert.equal(localInput(undefined), '')
})
