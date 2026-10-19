// Pure helpers for the message viewer's preferences (see prefs.svelte.ts):
// formatting times and picking value fields for columns.

export type TimeFormat = 'local' | 'utc' | 'relative' | 'unix'

const pad = (n: number, l = 2) => String(n).padStart(l, '0')

/** A record time in the chosen format; now is passed in for relative times. */
export function showTime(iso: string, format: TimeFormat, now = Date.now()): string {
  const d = new Date(iso)
  switch (format) {
    case 'unix':
      return String(d.getTime())
    case 'utc':
      return `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())} ${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}:${pad(d.getUTCSeconds())}.${pad(d.getUTCMilliseconds(), 3)}Z`
    case 'relative':
      return relative(now - d.getTime())
    default:
      return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}.${pad(d.getMilliseconds(), 3)}`
  }
}

function relative(ms: number): string {
  const future = ms < 0
  const s = Math.abs(ms) / 1000
  const n = (v: number, unit: string) => {
    const r = Math.floor(v)
    const text = `${r} ${unit}${r === 1 ? '' : 's'}`
    return future ? `in ${text}` : `${text} ago`
  }
  if (s < 1) return 'now'
  if (s < 60) return n(s, 'second')
  if (s < 3600) return n(s / 60, 'minute')
  if (s < 86400) return n(s / 3600, 'hour')
  if (s < 86400 * 30) return n(s / 86400, 'day')
  if (s < 86400 * 365) return n(s / (86400 * 30), 'month')
  return n(s / (86400 * 365), 'year')
}

// ─── Value columns ───

/**
 * Reads a dotted path ("order.items.0.sku") from a JSON value. Keys with a
 * dot can be written in brackets: user["first.name"].
 */
export function pick(value: unknown, path: string): unknown {
  let cur = value
  for (const key of splitPath(path)) {
    if (cur === null || typeof cur !== 'object') return undefined
    cur = (cur as Record<string, unknown>)[key]
  }
  return cur
}

export function splitPath(path: string): string[] {
  const out: string[] = []
  const re = /\[\s*"((?:[^"\\]|\\.)*)"\s*\]|\[\s*(\d+)\s*\]|([^.[\]]+)/g
  for (const m of path.matchAll(re)) out.push(m[1] !== undefined ? JSON.parse(`"${m[1]}"`) : (m[2] ?? m[3]))
  return out
}

/** A picked value as cell text. */
export function cell(v: unknown): string {
  if (v === undefined) return ''
  if (v === null) return 'null'
  if (typeof v === 'object') return JSON.stringify(v)
  return String(v)
}
