// Column sorting for lists sorted by the server.
//
// A list starts in name order. The name header flips A–Z and Z–A; any other
// header cycles largest first, smallest first, then back to name order, so
// a third click clears it.

export type Sort<K extends string> = { key: K | 'name'; desc: boolean }

export function nextSort<K extends string>(current: Sort<K>, key: K | 'name'): Sort<K> {
  if (key === 'name') return { key: 'name', desc: current.key === 'name' ? !current.desc : false }
  if (current.key !== key) return { key, desc: true }
  if (current.desc) return { key, desc: false }
  return { key: 'name', desc: false }
}
