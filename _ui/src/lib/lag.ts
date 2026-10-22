import type { PartitionLag } from './api'

// lagReading sums partitions' lag numbers into one reading for LagEta.
// Partitions the console may not read carry no numbers and are left out.
export function lagReading(partitions: PartitionLag[], keep: (p: PartitionLag) => boolean = () => true) {
  const rows = partitions.filter((p) => !p.no_access && keep(p))
  return {
    lag: rows.reduce((s, p) => s + Math.max(p.lag, 0), 0),
    committed: rows.reduce((s, p) => s + Math.max(p.committed, 0), 0),
    high: rows.reduce((s, p) => s + p.high, 0),
    parts: rows.map((p) => ({ topic: p.topic, partition: p.partition, committed: p.committed, lag: p.lag })),
  }
}
