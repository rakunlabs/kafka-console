import assert from 'node:assert/strict'
import { test } from 'node:test'
import { compactOnly, keySettings, partitionCells, partitionSkew, replicationFactor, topicTotals } from '../src/lib/topicStats.ts'

const part = (id, low, high, extra = {}) => ({ id, leader: 1, replicas: [1, 2, 3], isr: [1, 2, 3], offline_replicas: [], low, high, size: 0, ...extra })
const cfg = (key, value, source = 'DEFAULT_CONFIG') => ({ key, value, sensitive: false, source })

test('totals count retained messages and under-replicated partitions', () => {
  const parts = [part(0, 10, 110), part(1, 0, 50, { isr: [1] })]
  assert.deepEqual(topicTotals(parts), { messages: 150, urp: 1 })
  assert.equal(replicationFactor(parts), 3)
  assert.equal(replicationFactor([]), 0)
})

test('key settings follow the cleanup policy and mark topic overrides', () => {
  const del = keySettings([cfg('retention.ms', '604800000', 'DYNAMIC_TOPIC_CONFIG'), cfg('retention.bytes', '-1'), cfg('max.message.bytes', '1048588')])
  assert.deepEqual(del.map((s) => s.key), ['retention.ms', 'retention.bytes', 'cleanup.policy', 'max.message.bytes'])
  assert.equal(del[0].value, '1 week')
  assert.equal(del[0].overridden, true)
  assert.equal(del[1].value, 'Unlimited')
  assert.equal(del[1].note, undefined)
  assert.equal(del[2].value, 'delete')

  const compact = keySettings([cfg('cleanup.policy', 'compact'), cfg('min.compaction.lag.ms', '0'), cfg('min.insync.replicas', '2'), cfg('compression.type', 'zstd')])
  assert.deepEqual(compact.map((s) => s.key), ['cleanup.policy', 'min.compaction.lag.ms', 'max.message.bytes', 'min.insync.replicas', 'compression.type'])
  assert.equal(compact.find((s) => s.key === 'max.message.bytes').value, '—')
})

test('compact-only topics are told apart from compact,delete', () => {
  assert.equal(compactOnly([cfg('cleanup.policy', 'compact')]), true)
  assert.equal(compactOnly([cfg('cleanup.policy', 'compact,delete')]), false)
  assert.equal(compactOnly([]), false)
})

test('partition cells show load, offline and under-replicated partitions', () => {
  const cells = partitionCells([part(0, 0, 100), part(1, 0, 50), part(2, 0, 0, { leader: -1 }), part(3, 0, 10, { isr: [1] })])
  assert.deepEqual(cells.map((c) => [c.tone, c.level]), [['ok', 1], ['ok', 0.5], ['danger', 1], ['warn', 1]])
  assert.match(cells[2].title, /offline/)
})

test('skew flags a partition with twice its fair share', () => {
  assert.equal(partitionSkew([part(0, 0, 10)]), undefined)
  assert.equal(partitionSkew([part(0, 0, 0), part(1, 0, 0)]), undefined)
  assert.equal(partitionSkew([part(0, 0, 50), part(1, 0, 50)]), undefined)
  assert.deepEqual(partitionSkew([part(0, 0, 10), part(1, 0, 10), part(2, 0, 80)]), { id: 2, pct: 80 })
})
