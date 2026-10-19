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
