import type { ConsumeResult, Message, MessageStreamEvent, TailEvent } from './api'
import { errorMessage } from './http.ts'

export type MessageQuery = Record<string, string | number | boolean | undefined>
export const liveCap = 2000

// Live tail is always from now on. Browse position, time range and limit do
// not apply; search, partition, filter and decoding still do.
export function tailQuery({ start: _start, offset: _offset, since: _since, until: _until, limit: _limit, exact: _exact, ...query }: MessageQuery): MessageQuery {
  return { ...query, start: 'newest' }
}
export type FeedState = {
  result?: ConsumeResult
  query: MessageQuery
  loading: boolean
  loadingMore: boolean
  error: string
  searched: string
  live: boolean
  paused: boolean
  liveError: string
  liveStats: { scanned: number; matched: number; scriptErrors: number; firstError: string }
}

export const initialFeed = (): FeedState => ({
  query: {}, loading: false, loadingMore: false, error: '', searched: '', live: false, paused: false, liveError: '',
  liveStats: { scanned: 0, matched: 0, scriptErrors: 0, firstError: '' },
})

export type FeedTransport = {
  read: (cluster: string, topic: string, query: MessageQuery, event: (e: MessageStreamEvent) => void, signal: AbortSignal) => Promise<void>
  tail: (cluster: string, topic: string, query: MessageQuery,
    on: { event: (e: TailEvent) => void; error: (message: string) => void; end: () => void }) => () => void
}

// The transport and snapshot publisher are injected so browse/tail races can
// be tested without a browser or Svelte. The view owns only selection/layout.
export function createMessageFeed(
  transport: FeedTransport,
  source: () => { cluster: string; topic: string },
  publish: (state: FeedState) => void,
  hooks: { reset: () => void; trim: (kept: Set<string>) => void },
) {
  let state = initialFeed()
  const update = (patch: Partial<FeedState>) => {
    state = { ...state, ...patch }
    publish(state)
  }
  let generation = 0
  let reading: AbortController | undefined
  let closeTail: (() => void) | undefined
  let incoming: Message[] = []
  let frame: ReturnType<typeof setTimeout> | undefined
  const fresh = new Set<string>()
  const key = (m: Message) => `${m.partition}:${m.offset}`

  function flush() {
    clearTimeout(frame)
    frame = undefined
    if (!state.result || !incoming.length) return
    const all = [...incoming.reverse(), ...state.result.messages]
    incoming = []
    const messages = all.slice(0, liveCap)
    update({ result: { ...state.result, messages } })
    if (all.length > messages.length) {
      const kept = new Set(messages.map(key))
      for (const k of fresh) if (!kept.has(k)) fresh.delete(k)
      hooks.trim(kept)
    }
  }

  function stopLive() {
    closeTail?.()
    closeTail = undefined
    update({ live: false })
    flush()
  }

  function stopReading() {
    reading?.abort()
    reading = undefined
    update({ loading: false, loadingMore: false })
  }

  async function load(query: MessageQuery, more = false) {
    stopLive()
    reading?.abort()
    const ctl = (reading = new AbortController())
    const gen = ++generation
    const current = () => gen === generation
    const prev = more ? state.result : undefined
    if (!more) {
      fresh.clear()
      hooks.reset()
      update({ query: { ...query }, searched: String(query.q ?? '') })
    }
    const shown = state.query
    const newestFirst = shown.start === 'newest'
    const after = more && prev ? Object.entries(prev.next ?? {}).map(([p, o]) => `${p}:${o}`).join(',') : undefined
    update({ loading: true, loadingMore: more, error: '' })
    const began = performance.now()
    const base = prev?.messages ?? []
    const got: Message[] = []
    let scanned = 0
    let next = { ...(prev?.next ?? {}) }
    let timer: ReturnType<typeof setTimeout> | undefined
    let finished = false
    const show = (stoppedBy: string, extra: Partial<ConsumeResult> = {}) => {
      if (!current()) return
      update({ result: {
        messages: got.length ? [...base, ...got] : base,
        scanned: (prev?.scanned ?? 0) + scanned,
        script_errors: (prev?.script_errors ?? 0) + (extra.script_errors ?? 0),
        first_error: extra.first_error ?? prev?.first_error,
        stopped_by: stoppedBy, elapsed_ms: Math.round(performance.now() - began), next,
      } })
    }
    const schedule = () => {
      timer ??= setTimeout(() => { timer = undefined; show('reading') }, 100)
    }
    if (!more) show('reading')
    const { cluster, topic } = source()
    try {
      await transport.read(cluster, topic, { ...shown, after }, (e) => {
        if (!current() || ctl.signal.aborted) return
        scanned = e.scanned
        switch (e.type) {
          case 'message': {
            got.push(e.message)
            const p = String(e.message.partition)
            const at = newestFirst ? e.message.offset : e.message.offset + 1
            if (next[p] === undefined || (newestFirst ? at < next[p] : at > next[p])) next[p] = at
            schedule()
            break
          }
          case 'progress':
            if (e.next) next = e.next
            schedule()
            break
          case 'done':
            finished = true
            clearTimeout(timer)
            next = e.result.next ?? {}
            show(e.result.stopped_by, e.result)
            break
          case 'error': throw new Error(e.error)
        }
      }, ctl.signal)
      if (!finished) show('cancelled')
    } catch (e) {
      if (current()) {
        if ((e as Error | undefined)?.name === 'AbortError') show('cancelled')
        else update({ error: errorMessage(e), ...(!more ? { result: undefined } : {}) })
      }
    } finally {
      clearTimeout(timer)
      if (reading === ctl) {
        reading = undefined
        update({ loading: false, loadingMore: false })
      }
    }
  }

  function startLive(query: MessageQuery) {
    stopLive()
    stopReading()
    const gen = ++generation
    fresh.clear()
    hooks.reset()
    update({ live: true, paused: false, error: '', liveError: '', searched: String(query.q ?? ''),
      liveStats: { scanned: 0, matched: 0, scriptErrors: 0, firstError: '' },
      result: { messages: [], scanned: 0, script_errors: 0, stopped_by: 'live', elapsed_ms: 0 },
    })
    const current = () => gen === generation && state.live
    const { cluster, topic } = source()
    closeTail = transport.tail(cluster, topic, query, {
      event: (e) => {
        if (!current()) return
        update({ liveStats: { scanned: e.scanned, matched: e.matched, scriptErrors: e.script_errors ?? 0, firstError: e.first_error ?? '' },
          liveError: e.error ?? (e.message ? '' : state.liveError) })
        if (e.message && !state.paused) {
          fresh.add(key(e.message))
          incoming.push(e.message)
          frame ??= setTimeout(flush, 250)
        }
      },
      error: (error) => {
        if (!current()) return
        closeTail = undefined
        update({ error, live: false })
        flush()
      },
      end: () => {
        if (!current()) return
        closeTail = undefined
        update({ live: false })
        flush()
      },
    })
  }

  function dispose() {
    generation++
    reading?.abort()
    reading = undefined
    closeTail?.()
    closeTail = undefined
    clearTimeout(frame)
    incoming = []
    fresh.clear()
  }

  return {
    load, startLive, stopLive, stopReading, dispose, fresh,
    pause: (paused: boolean) => update({ paused }),
    fail: (error: string) => update({ error }),
  }
}
