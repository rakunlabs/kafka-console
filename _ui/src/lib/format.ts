const nf = new Intl.NumberFormat()

export const fmt = (n: number | null | undefined) => (n === null || n === undefined || n < 0 ? '—' : nf.format(n))

export function compact(n: number) {
  return new Intl.NumberFormat(undefined, { notation: 'compact', maximumFractionDigits: 1 }).format(n)
}

export function bytes(n: number) {
  if (n < 1024) return `${n} B`
  const units = ['KB', 'MB', 'GB']
  let v = n
  let i = -1
  do {
    v /= 1024
    i++
  } while (v >= 1024 && i < units.length - 1)
  return `${v.toFixed(1)} ${units[i]}`
}

export function time(iso: string) {
  const d = new Date(iso)
  const pad = (n: number, l = 2) => String(n).padStart(l, '0')
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}.${pad(d.getMilliseconds(), 3)}`
}

export function pretty(value: string | null, format?: string) {
  if (value === null) return ''
  if (format === 'json') {
    try {
      return JSON.stringify(JSON.parse(value), null, 2)
    } catch {
      return value
    }
  }
  return value
}

export function stateBadge(state: string) {
  switch (state) {
    case 'Stable':
      return 'ok'
    case 'PreparingRebalance':
    case 'CompletingRebalance':
      return 'warn'
    case 'Dead':
      return 'danger'
    default:
      return ''
  }
}

// Human form of a millisecond duration, e.g. 604800000 -> "7 days".
export function duration(ms: number) {
  if (ms < 0) return 'Forever'
  if (ms === 0) return '0 ms'
  const units: [number, string][] = [
    [86400000 * 365, 'year'],
    [86400000 * 7, 'week'],
    [86400000, 'day'],
    [3600000, 'hour'],
    [60000, 'minute'],
    [1000, 'second'],
  ]
  for (const [u, l] of units) {
    if (ms >= u && ms % u === 0) return `${ms / u} ${l}${ms / u === 1 ? '' : 's'}`
  }
  for (const [u, l] of units) {
    if (ms >= u) return `${+(ms / u).toFixed(1)} ${l}s`
  }
  return `${ms} ms`
}

// Binary size for config values (Kafka sizes are powers of two).
export function sizeIEC(n: number) {
  if (n < 0) return 'Unlimited'
  const units: [number, string][] = [[2 ** 40, 'TiB'], [2 ** 30, 'GiB'], [2 ** 20, 'MiB'], [2 ** 10, 'KiB']]
  for (const [u, l] of units) if (n >= u) return `${+(n / u).toFixed(1)} ${l}`
  return `${n} B`
}

// Parses JSON and, on failure, locates the error. Browsers word the message
// differently: V8 reports "position N" (and newer V8 "line L column C"),
// Firefox "line L column C"; Safari reports no location.
export function jsonProblem(text: string): { line?: number; column?: number; message: string } | undefined {
  try {
    JSON.parse(text)
    return undefined
  } catch (e: any) {
    const raw = String(e?.message ?? e)
    let line: number | undefined
    let column: number | undefined
    const lc = raw.match(/line (\d+) column (\d+)/i)
    const pos = raw.match(/position (\d+)/i)
    if (lc) {
      line = Number(lc[1])
      column = Number(lc[2])
    } else if (pos) {
      const before = text.slice(0, Number(pos[1]))
      line = before.split('\n').length
      column = before.length - before.lastIndexOf('\n')
    } else if (/end of (json )?(data|input)|unexpected EOF/i.test(raw)) {
      line = text.split('\n').length
    }
    const message = raw
      .replace(/^JSON\.parse: /, '')
      .replace(/^JSON Parse error: /, '')
      .replace(/ in JSON at position \d+.*$/, '')
      .replace(/ at line \d+ column \d+ of the JSON data$/, '')
      .replace(/^\w/, (c) => c.toUpperCase())
    return { line, column, message }
  }
}
