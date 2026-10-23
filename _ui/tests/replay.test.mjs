import assert from 'node:assert/strict'
import { test } from 'node:test'
import { reviewedReplay } from '../src/lib/replay.ts'

test('write uses the reviewed offsets and options, not later form edits', () => {
  const body = { from: { type: 'oldest' }, to: { type: 'newest' }, target: 'dst', dry_run: true }
  const plan = [{ partition: 0, start: 0, end: 3 }]
  const write = reviewedReplay(body, plan, { type: 'done', stopped_by: 'end' })
  body.target = 'different'
  body.to.type = 'offset'
  plan[0].end = 5
  assert.equal(write.target, 'dst')
  assert.equal(write.to.type, 'newest')
  assert.equal(write.plan[0].end, 3)
  assert.equal(write.dry_run, false)
})

test('failed, interrupted and incomplete reviews cannot be written', () => {
  for (const stopped_by of ['error', 'timeout', 'cancelled', 'incomplete', undefined]) {
    assert.throws(() => reviewedReplay({}, [], { type: 'done', stopped_by }))
  }
  assert.throws(() => reviewedReplay({}, [], { type: 'progress', stopped_by: 'end' }))
})
