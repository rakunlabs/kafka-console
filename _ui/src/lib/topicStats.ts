import type { ConfigEntry, Partition } from './api'
import { duration, fmt, sizeIEC } from './format.ts'

export type Setting = { key: string; label: string; value: string; note?: string; overridden: boolean }
export type PartitionCell = { label: string; tone: 'ok' | 'warn' | 'danger'; level: number; title: string }

const underReplicated = (p: Partition) => p.isr.length < p.replicas.length

/** Messages retained and partitions with fewer in-sync replicas than replicas. */
export function topicTotals(parts: Partition[]): { messages: number; urp: number } {
  let messages = 0
  let urp = 0
  for (const p of parts) {
    messages += p.high - p.low
    if (underReplicated(p)) urp++
  }
  return { messages, urp }
}

/** The largest replica count among the partitions. */
export const replicationFactor = (parts: Partition[]) => Math.max(0, ...parts.map((p) => p.replicas.length))

// The handful of settings people check first, read from the topic config.
export function keySettings(configs: ConfigEntry[]): Setting[] {
  const cfg = new Map(configs.map((c) => [c.key, c]))
  const num = (k: string) => {
    const v = cfg.get(k)?.value
    return v === null || v === undefined || v === '' ? undefined : Number(v)
  }
  const out: Setting[] = []
  const add = (key: string, label: string, value: string, note?: string) =>
    out.push({ key, label, value, note, overridden: !!cfg.get(key)?.source.startsWith('DYNAMIC_TOPIC') })
  const policy = cfg.get('cleanup.policy')?.value ?? 'delete'
  if (policy.includes('delete')) {
    const rms = num('retention.ms')
    const rb = num('retention.bytes')
    add('retention.ms', 'Retention', rms === undefined ? '—' : duration(rms))
    add('retention.bytes', 'Retention size', rb === undefined ? '—' : sizeIEC(rb), rb !== undefined && rb >= 0 ? 'per partition' : undefined)
  }
  add('cleanup.policy', 'Cleanup policy', policy)
  if (policy.includes('compact')) {
    const lag = num('min.compaction.lag.ms')
    add('min.compaction.lag.ms', 'Min compaction lag', lag === undefined ? '—' : duration(lag))
  }
  const mx = num('max.message.bytes')
  add('max.message.bytes', 'Max message', mx === undefined ? '—' : sizeIEC(mx))
  const isr = num('min.insync.replicas')
  if (isr !== undefined) add('min.insync.replicas', 'Min in-sync replicas', String(isr))
  const comp = cfg.get('compression.type')?.value
  if (comp) add('compression.type', 'Compression', comp)
  return out
}

/** Compact-only topics: records cannot be deleted from them. */
export const compactOnly = (configs: ConfigEntry[]) =>
  configs.some((c) => c.key === 'cleanup.policy' && !!c.value?.includes('compact') && !c.value.includes('delete'))

// Partition map: fill shows each partition's share of the topic's messages
// relative to the busiest one, so a hot or empty partition stands out.
export function partitionCells(parts: Partition[]): PartitionCell[] {
  const max = Math.max(1, ...parts.map((p) => p.high - p.low))
  return parts.map((p) => {
    const n = p.high - p.low
    const urp = underReplicated(p)
    const off = p.leader < 0
    return {
      label: String(p.id),
      tone: off ? 'danger' : urp ? 'warn' : 'ok',
      level: off || urp ? 1 : n / max,
      title: `partition ${p.id} · ${fmt(n)} messages${off ? ' · offline' : urp ? ' · under-replicated' : ''}`,
    }
  })
}

// A partition holding at least twice its fair share of messages.
export function partitionSkew(parts: Partition[]): { id: number; pct: number } | undefined {
  if (parts.length < 2) return undefined
  const counts = parts.map((p) => p.high - p.low)
  const total = counts.reduce((a, b) => a + b, 0)
  if (!total) return undefined
  const top = Math.max(...counts)
  const share = top / total
  const fair = 1 / parts.length
  return share >= fair * 2 ? { id: parts[counts.indexOf(top)].id, pct: Math.round(share * 100) } : undefined
}
