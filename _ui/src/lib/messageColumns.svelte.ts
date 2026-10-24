import type { Message } from './api'
import { narrow } from './media.svelte'
import { prefs, loadColumns, saveColumns, loadWidths, saveWidths } from './prefs.svelte'
import { COLS, colCount, fitWidth, keyWidth, listMinWidth, timeWidth } from './messageLayout'

// The message list's columns for one topic: the value fields shown as their
// own columns, and widths set by hand (drag or double-click on a header
// edge). A column without a width of its own keeps its automatic one.
export function messageColumns(
  topic: string,
  view: { readOnly: () => boolean; messages: () => Message[]; detailOpen: () => boolean },
) {
  let columns = $state<string[]>(loadColumns(topic))
  let widths = $state<Record<string, number>>(loadWidths(topic))

  const show = $derived(!narrow.matches && columns.length > 0)
  const colW = (id: string, auto: number) => widths[id] ?? auto
  const offset = $derived(colW('offset', COLS.offset))
  const time = $derived(colW('time', timeWidth(prefs.time)))
  const key = $derived(colW('key', keyWidth(view.messages(), view.detailOpen())))
  const field = (c: string) => colW(`v:${c}`, COLS.field)
  const shown = $derived(show ? columns : [])
  const listMin = $derived(listMinWidth({ readOnly: view.readOnly(), offset, time, key, fields: shown.map(field) }))
  const count = $derived(colCount(view.readOnly(), narrow.matches, shown.length))

  function setColumns(next: string[]) {
    columns = next
    saveColumns(topic, next)
  }
  function add(path: string) {
    if (!columns.includes(path)) setColumns([...columns, path])
  }
  function remove(path: string) {
    setColumns(columns.filter((c) => c !== path))
  }
  function move(path: string, by: number) {
    const i = columns.indexOf(path)
    const j = i + by
    if (i < 0 || j < 0 || j >= columns.length) return
    const next = [...columns]
    ;[next[i], next[j]] = [next[j], next[i]]
    setColumns(next)
  }

  function setWidth(id: string, px: number) {
    widths = { ...widths, [id]: px }
  }
  const keep = () => saveWidths(topic, widths)
  function reset() {
    widths = {}
    saveWidths(topic, {})
  }
  // Fit to the widest of the rows shown, header included, wider or narrower.
  // A range measures the text itself, even where the cell cuts it off.
  function fit(root: HTMLElement, id: string) {
    let px = 0
    const range = document.createRange()
    for (const el of root.querySelectorAll<HTMLElement>(`[data-col="${CSS.escape(id)}"]`)) {
      const parts = [...el.childNodes].filter((n) => !(n instanceof HTMLElement && n.classList.contains('rz')))
      if (!parts.length) continue
      range.setStartBefore(parts[0])
      range.setEndAfter(parts[parts.length - 1])
      const cs = getComputedStyle(el)
      px = Math.max(px, range.getBoundingClientRect().width + parseFloat(cs.paddingLeft) + parseFloat(cs.paddingRight))
    }
    if (!px) return
    setWidth(id, fitWidth(px))
    keep()
  }

  return {
    /** Every chosen value field, shown or not. */
    get columns() {
      return columns
    },
    /** The value fields shown as columns: none on phones. */
    get shown() {
      return shown
    },
    get resized() {
      return Object.keys(widths).length > 0
    },
    get offset() {
      return offset
    },
    get time() {
      return time
    },
    get key() {
      return key
    },
    field,
    /** The narrowest the list can be with every column at its width. */
    get listMin() {
      return listMin
    },
    /** Columns in a row, for colspans. */
    get count() {
      return count
    },
    add,
    remove,
    move,
    setWidth,
    keep,
    reset,
    fit,
  }
}

export type MessageColumns = ReturnType<typeof messageColumns>
