import assert from 'node:assert/strict'
import { test } from 'node:test'
import { quotaDraft, quotaChanges } from '../src/lib/quota.ts'

test('editing keeps every compound entity component, including default names', () => {
  for (const name of ['billing', null]) {
    const quota = { entity: [{ type: 'user', name: 'alice' }, { type: 'client-id', name }], values: { producer_byte_rate: 100 } }
    const draft = quotaDraft(quota)
    draft.values.producer_byte_rate = 200
    const changes = quotaChanges(draft)
    assert.deepEqual(changes.entity, quota.entity)
    assert.deepEqual(changes.set, { producer_byte_rate: 200 })
    changes.entity[0].name = 'different'
    assert.equal(quota.entity[0].name, 'alice')
    assert.equal(draft.entity[0].name, 'alice')
  }
})

test('new quotas use the chosen entity; only existing supported limits are removed', () => {
  const draft = quotaDraft()
  draft.type = 'user'
  draft.isDefault = true
  draft.values.consumer_byte_rate = '0'
  assert.deepEqual(quotaChanges(draft), { entity: [{ type: 'user', name: null }], set: { consumer_byte_rate: 0 }, remove: [] })
  const edit = quotaDraft({ entity: [{ type: 'client-id', name: 'c' }], values: { producer_byte_rate: 100, future_limit: 12 } })
  edit.values.producer_byte_rate = ''
  assert.deepEqual(quotaChanges(edit).remove, ['producer_byte_rate'])
})

test('invalid limits are rejected before a quota is changed', () => {
  for (const value of ['NaN', 'Infinity', '-1']) {
    const draft = quotaDraft()
    draft.values.producer_byte_rate = value
    assert.throws(() => quotaChanges(draft), /non-negative/)
  }
})
