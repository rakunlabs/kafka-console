// Column resizing for any table: <table use:resizable={'topics'}>.
//
// Every header with a name gets a grab handle on its right edge: drag to
// resize, double-click to fit the longest value shown, right-click to go
// back to the automatic width. Arrow keys and Enter do the same from the
// keyboard. Widths are kept in localStorage per table id and column name,
// so a column that comes and goes (an edit column on read-only clusters)
// does not shift the others' widths.
//
// Columns nobody resized keep the table's own layout. A resized one is held
// at its width through a stylesheet rule rather than inline styles, so rows
// added later (paging, a refresh) follow it without being touched.

const storeKey = (id: string) => `kc.tw.${id}`
const minW = 40

function load(id: string): Record<string, number> {
  try {
    const v = JSON.parse(localStorage.getItem(storeKey(id)) ?? '{}')
    if (!v || typeof v !== 'object' || Array.isArray(v)) return {}
    return Object.fromEntries(Object.entries(v).filter(([, w]) => typeof w === 'number' && Number.isFinite(w) && w > 0)) as Record<string, number>
  } catch {
    return {}
  }
}

function save(id: string, widths: Record<string, number>) {
  if (Object.keys(widths).length) localStorage.setItem(storeKey(id), JSON.stringify(widths))
  else localStorage.removeItem(storeKey(id))
}

let seq = 0

export function resizable(table: HTMLTableElement, id: string) {
  let tableId = id
  let widths = load(tableId)
  const uid = `rsz${++seq}`
  table.dataset.rsz = uid
  const sheet = document.createElement('style')
  document.head.appendChild(sheet)

  // A header's name: its text, without the handle. Empty headers (a checkbox
  // or an actions column) are not resizable.
  const nameOf = (th: HTMLTableCellElement) => (th.dataset.col ?? th.textContent ?? '').replace(/\s+/g, ' ').trim()

  const headers = () => {
    const row = table.tHead?.rows
    if (!row || row.length !== 1) return []
    return [...row[0].cells].filter((c): c is HTMLTableCellElement => c.tagName === 'TH' && c.colSpan === 1)
  }

  function apply() {
    const sel = `table[data-rsz="${uid}"]`
    const rules: string[] = []
    for (const th of headers()) {
      const w = widths[nameOf(th)]
      if (!w) continue
      const n = th.cellIndex + 1
      rules.push(
        `${sel} > colgroup > col:nth-child(${n}) { width: ${w}px !important; }`,
        `${sel} > thead > tr > :nth-child(${n}), ${sel} > tbody > tr > :nth-child(${n}):not([colspan]) ` +
          `{ width: ${w}px !important; min-width: ${w}px !important; max-width: ${w}px !important; overflow: hidden; text-overflow: ellipsis; }`,
      )
    }
    sheet.textContent = rules.join('\n')
    table.classList.toggle('rsz-on', rules.length > 0)
  }

  function maxFor() {
    const box = table.parentElement?.clientWidth ?? 1200
    return Math.max(minW, Math.min(1200, box - 60))
  }
  const clamp = (px: number) => Math.round(Math.min(Math.max(px, minW), maxFor()))

  function set(name: string, px: number | undefined, keep: boolean) {
    if (px === undefined) delete widths[name]
    else widths[name] = clamp(px)
    widths = { ...widths }
    apply()
    if (keep) save(tableId, widths)
  }

  // The widest content in the column, header included: a range measures the
  // text even where the cell cuts it off.
  function fit(th: HTMLTableCellElement) {
    const idx = th.cellIndex
    const cells: HTMLTableCellElement[] = [th]
    for (const body of table.tBodies) {
      for (const row of body.rows) {
        const c = row.cells[idx]
        if (c && c.colSpan === 1 && row.cells.length === th.parentElement!.children.length) cells.push(c)
      }
    }
    const range = document.createRange()
    let px = 0
    for (const c of cells) {
      const parts = [...c.childNodes].filter((n) => !(n instanceof HTMLElement && n.classList.contains('rsz-h')))
      if (!parts.length) continue
      range.setStartBefore(parts[0])
      range.setEndAfter(parts[parts.length - 1])
      const cs = getComputedStyle(c)
      px = Math.max(px, range.getBoundingClientRect().width + parseFloat(cs.paddingLeft) + parseFloat(cs.paddingRight))
    }
    if (px) set(nameOf(th), Math.ceil(px) + 4, true)
  }

  function handleFor(th: HTMLTableCellElement) {
    const h = document.createElement('span')
    h.className = 'rsz-h'
    h.tabIndex = 0
    h.setAttribute('role', 'separator')
    h.setAttribute('aria-orientation', 'vertical')
    h.setAttribute('aria-label', `Resize ${nameOf(th)} column`)
    h.title = 'Drag to resize · double-click to fit · right-click to reset'

    h.addEventListener('pointerdown', (e) => {
      if (e.button !== 0) return
      e.preventDefault()
      e.stopPropagation()
      h.setPointerCapture(e.pointerId)
      const name = nameOf(th)
      const startX = e.clientX
      const startW = th.getBoundingClientRect().width
      document.documentElement.classList.add('col-resizing')
      const move = (ev: PointerEvent) => set(name, startW + ev.clientX - startX, false)
      const end = () => {
        h.removeEventListener('pointermove', move)
        h.removeEventListener('pointerup', end)
        h.removeEventListener('pointercancel', end)
        document.documentElement.classList.remove('col-resizing')
        save(tableId, widths)
      }
      h.addEventListener('pointermove', move)
      h.addEventListener('pointerup', end)
      h.addEventListener('pointercancel', end)
    })
    // The header may hold a sort button; resizing must not sort.
    h.addEventListener('click', (e) => e.stopPropagation())
    h.addEventListener('dblclick', (e) => {
      e.stopPropagation()
      fit(th)
    })
    h.addEventListener('contextmenu', (e) => {
      e.preventDefault()
      set(nameOf(th), undefined, true)
    })
    h.addEventListener('keydown', (e) => {
      const by = e.key === 'ArrowRight' ? 16 : e.key === 'ArrowLeft' ? -16 : 0
      if (e.key === 'Enter') fit(th)
      else if (e.key === 'Delete' || e.key === 'Backspace') set(nameOf(th), undefined, true)
      else if (by) set(nameOf(th), (widths[nameOf(th)] ?? th.getBoundingClientRect().width) + by, true)
      else return
      e.preventDefault()
      e.stopPropagation()
    })
    return h
  }

  // Headers can be re-rendered (a column appearing, a sort arrow); handles
  // are added to whichever lack one.
  function decorate() {
    for (const th of headers()) {
      if (th.querySelector(':scope > .rsz-h') || !nameOf(th) || th.hasAttribute('data-noresize')) continue
      th.classList.add('rsz-th')
      th.appendChild(handleFor(th))
    }
    apply()
  }

  decorate()
  const observer = new MutationObserver(() => decorate())
  if (table.tHead) observer.observe(table.tHead, { childList: true, subtree: true, characterData: true })

  return {
    update(next: string) {
      if (next === tableId) return
      tableId = next
      widths = load(tableId)
      apply()
    },
    destroy() {
      observer.disconnect()
      sheet.remove()
    },
  }
}
