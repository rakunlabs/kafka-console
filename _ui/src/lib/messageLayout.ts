import type { Message } from './api'
import type { TimeFormat } from './viewer'

// Fixed and default widths in px of the message list's columns. The <col>
// widths, the list's minimum width and the colspans all come from here.
export const COLS = {
  pick: 34,
  toggle: 40,
  partition: 44,
  offset: 84,
  // A value field shown as its own column, until resized.
  field: 140,
  // Room the value preview keeps before the detail may sit beside the list.
  value: 160,
} as const
// Columns of the wide layout after the pick box: toggle, partition, offset,
// time, key and value; value-field columns come on top.
export const WIDE_COLS = 6

// A column resized by hand stays within these.
export const COL_MIN = 48
export const COL_MAX = 1200
// Measured text gets this much slack so it is not cut by a rounding pixel.
const FIT_SLACK = 4

// The detail beside the list: its narrowest width and the gap between them.
export const DETAIL_MIN = 380
export const SPLIT_GAP = 12

// The key column fits the longest key shown, so keys read whole; the value
// preview takes what is left. Capped so the value never gets squeezed out.
const KEY_MIN_CHARS = 3
const KEY_CHAR_PX = 7.6
const KEY_PAD_PX = 36
const KEY_MIN_PX = 120
const KEY_MAX_PX = 440
const KEY_MAX_BESIDE_DETAIL_PX = 240

export function keyWidth(messages: Message[], detailOpen: boolean): number {
  let longest = KEY_MIN_CHARS
  for (const m of messages) longest = Math.max(longest, (m.key ?? '—').length)
  const px = Math.max(KEY_MIN_PX, Math.ceil(longest * KEY_CHAR_PX) + KEY_PAD_PX)
  return Math.min(px, detailOpen ? KEY_MAX_BESIDE_DETAIL_PX : KEY_MAX_PX)
}

export function timeWidth(format: TimeFormat): number {
  if (format === 'relative' || format === 'unix') return 130
  return format === 'utc' ? 210 : 200
}

// The list's columns have fixed widths, so it needs at least their sum (plus
// room for the value preview) before the detail can sit beside it.
export function listMinWidth(w: { readOnly: boolean; offset: number; time: number; key: number; fields: number[] }): number {
  return (w.readOnly ? 0 : COLS.pick) + COLS.toggle + COLS.partition + w.offset + w.time + w.key +
    w.fields.reduce((s, px) => s + px, 0) + COLS.value
}

/** How many columns a row spans: phones show the pick box and one card. */
export function colCount(readOnly: boolean, narrow: boolean, fields: number): number {
  return (readOnly ? 0 : 1) + (narrow ? 1 : WIDE_COLS + fields)
}

export const clamp = (px: number, min: number, max: number) => Math.round(Math.min(Math.max(px, min), max))

/** A column width that fits text measured at px. */
export const fitWidth = (px: number) => clamp(Math.ceil(px) + FIT_SLACK, COL_MIN, COL_MAX)

// Value preview. With a search, the window is moved so the first match is
// visible even when it sits deep in a long value.
const PREVIEW_CHARS = 160
const PREVIEW_MOVE_AFTER = 120
const PREVIEW_LEAD = 40

export function preview(value: string | null, searched: string): string {
  if (value === null) return 'null'
  const q = searched.toLowerCase()
  const at = q ? value.toLowerCase().indexOf(q) : -1
  if (at > PREVIEW_MOVE_AFTER) {
    const from = Math.max(0, at - PREVIEW_LEAD)
    const out = value.slice(from, from + PREVIEW_CHARS)
    return '…' + out + (from + PREVIEW_CHARS < value.length ? '…' : '')
  }
  return value.length > PREVIEW_CHARS ? value.slice(0, PREVIEW_CHARS) + '…' : value
}

/** Splits text around the first case-insensitive match of q. */
export function hit(text: string, q: string): [string, string, string] {
  const i = q ? text.toLowerCase().indexOf(q.toLowerCase()) : -1
  return i < 0 ? [text, '', ''] : [text.slice(0, i), text.slice(i, i + q.length), text.slice(i + q.length)]
}
