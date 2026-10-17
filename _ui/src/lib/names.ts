import { api, type Names } from './api'

// Names of topics, groups and subjects, shared by ⌘K and the topic pickers.
// Only names are fetched (no offsets, sizes or lag), and the result is kept
// for a short while so opening a picker repeatedly does not refetch.
const ttl = 30_000
const cache = new Map<string, { at: number; p: Promise<Names> }>()

export function names(cluster: string, fresh = false): Promise<Names> {
  const hit = cache.get(cluster)
  if (!fresh && hit && Date.now() - hit.at < ttl) return hit.p
  const p = api.names(cluster)
  cache.set(cluster, { at: Date.now(), p })
  p.catch(() => cache.delete(cluster))
  return p
}

// Drop cached names after a create or delete so the next picker is current.
export function forgetNames(cluster: string) {
  cache.delete(cluster)
}
