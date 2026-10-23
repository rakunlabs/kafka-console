import assert from 'node:assert/strict'
import { test } from 'node:test'
import { reviewedOffsets } from '../src/lib/offsetReset.ts'

test('Apply pins the exact preview targets and commits, without latest/shift recomputation', () => {
  const plan = [{ partition: 0, from: 1, to: 3, low: 0, high: 3 }]
  const request = reviewedOffsets('orders', plan)
  plan[0].to = 5
  assert.deepEqual(request, { topic: 'orders', plan: [{ partition: 0, from: 1, to: 3, low: 0, high: 3 }], dry_run: false })
  assert.equal(request.strategy, undefined)
})

test('Apply without a preview is refused', () => {
  assert.throws(() => reviewedOffsets('orders', undefined), /Preview/)
  assert.throws(() => reviewedOffsets('orders', []), /Preview/)
})
