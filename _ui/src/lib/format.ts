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
