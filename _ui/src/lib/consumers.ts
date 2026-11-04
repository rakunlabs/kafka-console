// Pure helpers for a topic's Consumers tab: sorting, filtering, the problems
// worth flagging on a group, and its lag trend across refreshes.
import type { PartitionLag, PartitionOffset, TopicConsumer } from './api'

export type ConsumerSortKey = 'name' | 'state' | 'members' | 'lag'
export type ConsumerSort = { key: ConsumerSortKey; desc: boolean }

// Text columns start A–Z, number columns largest first; the same header flips.
export function nextConsumerSort(cur: ConsumerSort, key: ConsumerSortKey): ConsumerSort {
  if (cur.key === key) return { key, desc: !cur.desc }
  return { key, desc: key === 'members' || key === 'lag' }
}

const collator = new Intl.Collator(undefined, { numeric: true })

export function sortConsumers(list: TopicConsumer[], s: ConsumerSort): TopicConsumer[] {
  const by = (a: TopicConsumer, b: TopicConsumer) => {
    switch (s.key) {
      case 'state':
        return collator.compare(a.state, b.state)
      case 'members':
        return a.members - b.members
      case 'lag':
        return a.lag - b.lag
      default:
        return 0
    }
  }
  const dir = s.desc ? -1 : 1
  // Ties, and the name column itself, fall back to the group name.
  return [...list].sort((a, b) => dir * by(a, b) || (s.key === 'name' ? dir : 1) * collator.compare(a.group, b.group))
}

// Matches the group name or any client reading the topic, case-insensitive.
export function filterConsumers(list: TopicConsumer[], search: string, onlyLagging: boolean): TopicConsumer[] {
  const q = search.trim().toLowerCase()
  return list.filter((g) => {
    if (onlyLagging && g.lag <= 0) return false
    if (!q) return true
    return g.group.toLowerCase().includes(q) || g.partitions.some((p) => p.client_id?.toLowerCase().includes(q))
  })
}

export type GroupIssues = {
  // Messages are waiting but nothing is running to read them.
  stalled: boolean
  // Partitions no member owns while the group has members.
  unassigned: number
  // One partition holds most of the lag.
  hot?: { partition: number; share: number }
}

// Below this, a lopsided lag is noise between two commits.
const hotMinLag = 10

export function groupIssues(g: TopicConsumer): GroupIssues {
  // A group that could not be described reports no members; that says
  // nothing about whether it runs.
  const stalled = !g.error && g.members === 0 && g.lag > 0
  // Only while the group reads this topic at all: members busy with other
  // topics leave every partition here unowned, which is not a gap.
  const owned = g.partitions.some((p) => p.member_id)
  const unassigned = g.members > 0 && owned ? g.partitions.filter((p) => !p.member_id && !p.no_access).length : 0
  let hot: GroupIssues['hot']
  if (g.partitions.length > 1 && g.lag >= hotMinLag) {
    const top = g.partitions.reduce((m, p) => (p.lag > m.lag ? p : m), g.partitions[0])
    const share = top.lag / g.lag
    if (share >= 0.5) hot = { partition: top.partition, share }
  }
  return { stalled, unassigned, hot }
}

export type TrendPoint = { at: number; lag: number }

// Adds this reading of every group to its history, keeping the last max.
// Groups no longer listed are dropped.
export function pushTrend(history: Map<string, TrendPoint[]>, groups: TopicConsumer[], at: number, max = 20) {
  const next = new Map<string, TrendPoint[]>()
  for (const g of groups) next.set(g.group, [...(history.get(g.group) ?? []), { at, lag: g.lag }].slice(-max))
  return next
}

// Polyline points for values drawn in a w×h box, highest at the top.
export function sparkPoints(values: number[], w: number, h: number): string {
  if (values.length < 2) return ''
  const top = Math.max(...values, 1)
  return values.map((v, i) => `${+((i / (values.length - 1)) * w).toFixed(1)},${+(h - 1 - (v / top) * (h - 2)).toFixed(1)}`).join(' ')
}

// The next message the group reads on each lagging partition, furthest
// behind first: its age is how far behind the group is in time.
export function unreadRefs(rows: PartitionLag[]): PartitionOffset[] {
  return rows
    .filter((p) => !p.no_access && p.lag > 0 && p.committed >= 0 && p.committed >= p.low && p.committed < p.high)
    .sort((a, b) => b.lag - a.lag)
    .map((p) => ({ partition: p.partition, offset: p.committed }))
}
