// Small viewer preferences kept in localStorage: how times are shown, and
// per topic which value fields get their own column. Nothing here is sent
// to the server.

import type { TimeFormat } from './viewer'

export type { TimeFormat }

const timeKey = 'kc.time'

function readTime(): TimeFormat {
  const v = localStorage.getItem(timeKey)
  return v === 'utc' || v === 'relative' || v === 'unix' ? v : 'local'
}

let timeFormat = $state<TimeFormat>(readTime())

export const prefs = {
  get time() {
    return timeFormat
  },
  set time(v: TimeFormat) {
    timeFormat = v
    if (v === 'local') localStorage.removeItem(timeKey)
    else localStorage.setItem(timeKey, v)
  },
}

// ─── Value columns ───

const columnsKey = (topic: string) => `kc.columns.${topic}`

/** The value field paths shown as columns for a topic, e.g. "order.id". */
export function loadColumns(topic: string): string[] {
  try {
    const v = JSON.parse(localStorage.getItem(columnsKey(topic)) ?? '[]')
    return Array.isArray(v) ? v.filter((x) => typeof x === 'string' && x) : []
  } catch {
    return []
  }
}

export function saveColumns(topic: string, paths: string[]) {
  if (paths.length) localStorage.setItem(columnsKey(topic), JSON.stringify(paths))
  else localStorage.removeItem(columnsKey(topic))
}

// ─── Message list column widths ───

const widthsKey = (topic: string) => `kc.widths.${topic}`

/** Column widths in px set by hand for a topic's message list, by column id. */
export function loadWidths(topic: string): Record<string, number> {
  try {
    const v = JSON.parse(localStorage.getItem(widthsKey(topic)) ?? '{}')
    if (!v || typeof v !== 'object' || Array.isArray(v)) return {}
    return Object.fromEntries(Object.entries(v).filter(([, w]) => typeof w === 'number' && Number.isFinite(w) && w > 0)) as Record<string, number>
  } catch {
    return {}
  }
}

export function saveWidths(topic: string, widths: Record<string, number>) {
  if (Object.keys(widths).length) localStorage.setItem(widthsKey(topic), JSON.stringify(widths))
  else localStorage.removeItem(widthsKey(topic))
}

// ─── Message detail width ───

const detailKey = 'kc.detailWidth'

/** The width in px the message detail was dragged to, or null for the default. */
export function loadDetailWidth(): number | null {
  const v = Number(localStorage.getItem(detailKey))
  return Number.isFinite(v) && v > 0 ? Math.round(v) : null
}

export function saveDetailWidth(px: number | null) {
  if (px === null) localStorage.removeItem(detailKey)
  else localStorage.setItem(detailKey, String(Math.round(px)))
}
