import assert from 'node:assert/strict'
import { test } from 'node:test'
import { messageStart, startProblem, timeStarts } from '../src/lib/messageStart.ts'
import { tailQuery } from '../src/lib/messageFeed.ts'

const fields = { offset: 42, lastN: 25, since: '2026-10-23T10:00:00Z', until: '2026-10-23T11:00:00Z' }

test('position modes ignore hidden time fields and stale offsets', () => {
  assert.deepEqual(messageStart('newest', fields), { start: 'newest' })
  assert.deepEqual(messageStart('oldest', fields), { start: 'oldest' })
  assert.deepEqual(messageStart('offset', fields), { start: 'offset', offset: 42 })
  assert.deepEqual(messageStart('last', fields), { start: 'last', offset: 25 })
})

test('every time preset pins its range at Fetch and shows newest matches first', () => {
  const now = Date.parse('2026-10-23T12:00:00Z')
  for (const preset of timeStarts) {
    const query = messageStart(preset.value, fields, now)
    assert.deepEqual(query, { start: 'newest', since: now - preset.milliseconds, until: now })
    assert.deepEqual(tailQuery({ ...query, limit: 50, exact: true }), { start: 'newest' })
  }
})

test('custom time supports inclusive bounds and either open end', () => {
  assert.deepEqual(messageStart('custom', fields), { start: 'newest', since: Date.parse(fields.since), until: Date.parse(fields.until) })
  assert.deepEqual(messageStart('custom', { ...fields, since: '' }), { start: 'newest', since: undefined, until: Date.parse(fields.until) })
  assert.deepEqual(messageStart('custom', { ...fields, until: '' }), { start: 'newest', since: Date.parse(fields.since), until: undefined })
})

test('invalid or reversed custom time cannot start a read', () => {
  assert.match(startProblem('custom', fields.until, fields.since), /before/)
  assert.match(startProblem('custom', 'bad date', ''), /valid/)
  assert.throws(() => messageStart('custom', { ...fields, since: fields.until, until: fields.since }), /before/)
  assert.equal(startProblem('newest', 'bad date', 'also bad'), '')
})
