import { test } from 'node:test'
import assert from 'node:assert/strict'
import {
  filterConsumers, groupIssues, nextConsumerSort, pushTrend, sortConsumers, sparkPoints, unreadRefs,
} from '../src/lib/consumers.ts'

const part = (partition, lag, extra = {}) => ({ topic: 't', partition, low: 0, high: 100, committed: 100 - lag, lag, ...extra })
const group = (name, lag, members = 1, partitions = [part(0, lag, { member_id: 'm1', client_id: 'svc-a' })]) =>
  ({ group: name, state: members ? 'Stable' : 'Empty', members, lag, partitions })

test('number columns start largest first, text columns A–Z, and the same header flips', () => {
  assert.deepEqual(nextConsumerSort({ key: 'lag', desc: true }, 'name'), { key: 'name', desc: false })
  assert.deepEqual(nextConsumerSort({ key: 'name', desc: false }, 'members'), { key: 'members', desc: true })
  assert.deepEqual(nextConsumerSort({ key: 'lag', desc: true }, 'lag'), { key: 'lag', desc: false })
})

test('sorting by lag breaks ties by name, and the name sort is natural', () => {
  const list = [group('b', 5), group('a', 5), group('c', 9)]
  assert.deepEqual(sortConsumers(list, { key: 'lag', desc: true }).map((g) => g.group), ['c', 'a', 'b'])
  const names = [group('g10', 0), group('g2', 0), group('g1', 0)]
  assert.deepEqual(sortConsumers(names, { key: 'name', desc: false }).map((g) => g.group), ['g1', 'g2', 'g10'])
  assert.deepEqual(sortConsumers(names, { key: 'name', desc: true }).map((g) => g.group), ['g10', 'g2', 'g1'])
})

test('the filter matches group or client names and can keep only lagging groups', () => {
  const list = [group('orders-svc', 0), group('billing', 3)]
  assert.deepEqual(filterConsumers(list, 'ORDERS', false).map((g) => g.group), ['orders-svc'])
  assert.deepEqual(filterConsumers(list, 'svc-a', false).length, 2)
  assert.deepEqual(filterConsumers(list, '', true).map((g) => g.group), ['billing'])
})

test('a group with lag but no members is stalled; an idle caught-up group is not', () => {
  assert.equal(groupIssues(group('g', 5, 0)).stalled, true)
  assert.equal(groupIssues(group('g', 0, 0)).stalled, false)
  assert.equal(groupIssues({ ...group('g', 5, 0), error: 'not authorized' }).stalled, false)
})

test('unassigned partitions are counted only while the group has members reading the topic', () => {
  const parts = [part(0, 0, { member_id: 'm' }), part(1, 0), part(2, 0, { no_access: true })]
  assert.equal(groupIssues(group('g', 0, 2, parts)).unassigned, 1)
  assert.equal(groupIssues(group('g', 0, 0, parts)).unassigned, 0)
  assert.equal(groupIssues(group('g', 0, 2, [part(0, 0), part(1, 0)])).unassigned, 0)
})

test('a partition holding most of the lag is pointed out, small lag is not', () => {
  const hot = groupIssues(group('g', 100, 1, [part(0, 80), part(1, 20)]))
  assert.deepEqual(hot.hot, { partition: 0, share: 0.8 })
  assert.equal(groupIssues(group('g', 4, 1, [part(0, 4), part(1, 0)])).hot, undefined)
  assert.equal(groupIssues(group('g', 100, 1, [part(0, 40), part(1, 30), part(2, 30)])).hot, undefined)
})

test('the trend keeps the last readings and forgets groups that left', () => {
  let h = new Map()
  for (let i = 0; i < 5; i++) h = pushTrend(h, [group('a', i), group('b', 1)], i, 3)
  assert.deepEqual(h.get('a').map((p) => p.lag), [2, 3, 4])
  h = pushTrend(h, [group('a', 9)], 6, 3)
  assert.equal(h.has('b'), false)
})

test('spark points span the box with the largest value at the top', () => {
  assert.equal(sparkPoints([5], 10, 10), '')
  assert.equal(sparkPoints([0, 10], 10, 10), '0,9 10,1')
})

test('unread refs are the committed offsets still inside the log, furthest behind first', () => {
  const rows = [part(0, 0), part(1, 5), part(2, 50), part(3, 5, { no_access: true }), part(4, 5, { committed: -1 })]
  assert.deepEqual(unreadRefs(rows), [{ partition: 2, offset: 50 }, { partition: 1, offset: 95 }])
})
