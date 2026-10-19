import { untrack } from 'svelte'
import { isAbort, withSignal } from './api'
import { cached, remember } from './cache'

// Server-paged list: the server filters, sorts and pages, and only the rows
// on screen get their expensive numbers (offsets, lag, versions) computed.
// load() restarts from the top, more() appends the next page. Starting a
// load aborts the one still running, on the server too, so typing a search
// leaves no queue of stale listings behind; stop() aborts it by hand.
export type Page<T> = { items: T[]; total: number }

export function paged<T, Q>(opts: {
  key: string
  fetch: (q: Q, offset: number, limit: number) => Promise<Page<T>>
  query: () => Q
  id: (item: T) => string
  /** Cache only this query's result, so a plain revisit opens instantly. */
  cacheable: () => boolean
  size?: number
}) {
  const size = opts.size ?? 50
  const initial = cached<Page<T>>(opts.key)

  let items = $state<T[]>(initial?.items ?? [])
  let total = $state(initial?.total ?? 0)
  let loading = $state(true)
  let more = $state(false)
  let error = $state('')
  let ctl: AbortController | undefined

  // Abort whatever is in flight and hand out a fresh controller.
  function restart() {
    ctl?.abort()
    ctl = new AbortController()
    return ctl
  }

  // keep reloads as many rows as are shown, for Refresh.
  async function load(keep = false) {
    const c = restart()
    loading = true
    more = false
    error = ''
    try {
      const limit = keep ? Math.min(Math.max(items.length, size), 500) : size
      const q = untrack(opts.query)
      const res = await withSignal(c.signal, () => opts.fetch(q, 0, limit))
      items = res.items
      total = res.total
      if (untrack(opts.cacheable)) remember(opts.key, res)
    } catch (e: any) {
      if (!isAbort(e)) error = e.message
    } finally {
      if (ctl === c) loading = false
    }
  }

  async function loadMore() {
    // Shares the current load's controller, so a new search cancels it.
    const c = ctl ?? restart()
    more = true
    try {
      const res = await withSignal(c.signal, () => opts.fetch(untrack(opts.query), items.length, size))
      // Items created or deleted between pages shift the offsets; skip ones
      // already shown instead of listing them twice.
      const seen = new Set(items.map(opts.id))
      items = [...items, ...res.items.filter((t) => !seen.has(opts.id(t)))]
      total = res.total
    } catch (e: any) {
      if (!isAbort(e)) error = e.message
    } finally {
      if (ctl === c) more = false
    }
  }

  /** Abort the load in flight; the rows already shown stay. */
  function stop() {
    ctl?.abort()
    ctl = undefined
    loading = false
    more = false
  }

  // Reload when the query changes; typing in search waits for a pause.
  function watch(search: () => string) {
    let first = true
    $effect(() => {
      opts.query()
      const q = search()
      if (first) {
        first = false
        load()
        return
      }
      const t = setTimeout(load, q.trim() ? 250 : 0)
      return () => clearTimeout(t)
    })
    // Navigation aborts the page's reads anyway; this also covers the list
    // unmounting while its page stays (another tab of the same topic).
    $effect(() => () => ctl?.abort())
  }

  return {
    size,
    get items() { return items },
    set items(v: T[]) { items = v },
    get total() { return total },
    get loading() { return loading },
    get more() { return more },
    get error() { return error },
    load,
    loadMore,
    stop,
    watch,
  }
}
