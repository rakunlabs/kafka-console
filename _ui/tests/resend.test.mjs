import assert from 'node:assert/strict'
import { test } from 'node:test'
import { resendOrder } from '../src/lib/resend.ts'

test('resend keeps partition offset order even when timestamps go backwards', () => {
  const messages = [
    { partition: 0, offset: 0, timestamp: '2026-10-23T00:01:00Z' },
    { partition: 0, offset: 1, timestamp: '2026-10-23T00:00:00Z' },
    { partition: 1, offset: 5, timestamp: '2026-10-23T00:00:00Z' },
    { partition: 1, offset: 4, timestamp: '2026-10-23T00:02:00Z' },
  ]
  assert.deepEqual(resendOrder(messages).map((m) => [m.partition, m.offset]), [[0, 0], [0, 1], [1, 4], [1, 5]])
  assert.equal(messages[2].offset, 5)
})
