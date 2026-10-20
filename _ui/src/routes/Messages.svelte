<script lang="ts">
  import { onDestroy, untrack } from 'svelte'
  import { api, tail, messageStream, isAbort, rawURL, type Cluster, type ConsumeResult, type Message } from '../lib/api'
  import Pending from '../lib/Pending.svelte'
  import { fmt } from '../lib/format'
  import { prefs, loadColumns, saveColumns } from '../lib/prefs.svelte'
  import ViewSettings from '../lib/ViewSettings.svelte'
  import { showTime, pick as pickField, cell } from '../lib/viewer'
  import ValueView from '../lib/ValueView.svelte'
  import ResendDialog from '../lib/ResendDialog.svelte'
  import ReplayDialog from '../lib/ReplayDialog.svelte'
  import ImportDialog from '../lib/ImportDialog.svelte'
  import { messageDraft } from '../lib/produce'
  import { narrow } from '../lib/media.svelte'
  import Overflow from '../lib/Overflow.svelte'
  import { Pencil } from '@lucide/svelte'
  import { Play, Pause, Square, Radio, Download, Copy, Check, ChevronUp, ChevronDown, Plus, Minus, ChevronsUpDown, ChevronsDownUp, Repeat, X, Code, Binary, Search, Upload, History, Loader, ChevronsDown } from '@lucide/svelte'

  let { cluster, topic, partitions, onedit }: { cluster: Cluster; topic: string; partitions: number[]; onedit: (message: Message) => void } = $props()

  let editing = $state(false)
  let editError = $state('')
  let disposed = false
  onDestroy(() => (disposed = true))

  async function edit(message: Message) {
    if (editing || cluster.read_only) return
    editing = true
    editError = ''
    try {
      const full = await api.message(cluster.name, topic, message.partition, message.offset)
      messageDraft(full)
      if (!disposed) onedit(full)
    } catch (e: any) {
      if (!disposed) editError = e.message
    } finally {
      editing = false
    }
  }

  let start = $state('newest')
  let partition = $state('')
  let offset = $state(0)
  // For "last": how many of the newest records of each partition.
  let lastN = $state(100)
  let timestamp = $state('')
  let limit = $state(50)
  let maxScan = $state(10000)
  let filter = $state('')
  let search = $state('')
  // The term the current result was fetched with; highlighting follows the
  // result, not whatever is being typed next.
  let searched = $state('')
  let showFilter = $state(false)
  let raw = $state(false)
  // How keys and values are read when the automatic choice is wrong, e.g.
  // MessagePack values that would otherwise show as base64.
  let keyAs = $state('')
  let valueAs = $state('')
  const decoders: [string, string][] = [
    ['', 'Auto'], ['json', 'JSON'], ['text', 'Text'], ['msgpack', 'MessagePack'], ['cbor', 'CBOR'],
    ['hex', 'Hex'], ['base64', 'Base64'], ['uint', 'Unsigned int'],
  ]

  // Raw: results are replaced, never edited, so their (up to thousands of)
  // messages need no deep proxies.
  let result = $state.raw<ConsumeResult>()
  let loading = $state(false)
  let error = $state('')
  let selected = $state.raw<Message>()
  // The read in flight. A new Fetch or Stop aborts it, on the server too.
  let reading: AbortController | undefined

  // The query of the shown result, so "Load more" continues that read even
  // after the form was edited.
  let shownQuery = $state.raw<Record<string, string | number | boolean | undefined>>({})
  // Bumped by whatever replaces the shown result (a fetch, a tail), so a
  // read it cut off does not write over it.
  let shownGen = 0

  // more continues the shown read from where it stopped, appending to it.
  // Matches are shown as the server finds them; Stop keeps what arrived and
  // the read can still be continued from where it was stopped.
  async function load(more = false) {
    // Enter in the search box or ⌘Enter in the filter can fetch while a
    // tail runs; the fetch replaces the list, so the tail must not feed it.
    if (live) stopLive()
    reading?.abort()
    const ctl = (reading = new AbortController())
    const gen = ++shownGen
    const current = () => gen === shownGen
    const prev = more ? result : undefined
    if (!more) {
      searched = search.trim()
      shownQuery = {
        start,
        partitions: partition,
        offset: start === 'offset' ? offset : start === 'last' ? lastN : undefined,
        timestamp: start === 'timestamp' && timestamp ? new Date(timestamp).getTime() : undefined,
        limit,
        max_scan: maxScan,
        filter: showFilter ? filter : undefined,
        q: search.trim() || undefined,
        raw,
        key_as: keyAs || undefined,
        value_as: valueAs || undefined,
      }
    }
    const query = shownQuery
    const newestFirst = query.start === 'newest'
    const after = more && prev ? Object.entries(prev.next ?? {}).map(([p, o]) => `${p}:${o}`).join(',') : undefined
    loading = true
    loadingMore = more
    error = ''

    const began = performance.now()
    const base = prev?.messages ?? []
    let got: Message[] = []
    let scanned = 0
    // Where a stopped read continues: the server's last report, moved past
    // the matches that arrived after it.
    let next: Record<string, number> = { ...(prev?.next ?? {}) }
    let pending = false
    let finished = false
    const show = (stoppedBy: string, extra: Partial<ConsumeResult> = {}) => {
      result = {
        messages: got.length ? [...base, ...got] : base,
        scanned: (prev?.scanned ?? 0) + scanned,
        script_errors: (prev?.script_errors ?? 0) + (extra.script_errors ?? 0),
        first_error: extra.first_error ?? prev?.first_error,
        stopped_by: stoppedBy,
        elapsed_ms: Math.round(performance.now() - began),
        next,
      }
    }
    // Rows are added a few times a second, not per message.
    let timer: ReturnType<typeof setTimeout> | undefined
    const schedule = () => {
      if (pending) return
      pending = true
      timer = setTimeout(() => {
        pending = false
        if (current()) show('reading')
      }, 100)
    }

    if (!more) {
      selected = undefined
      expanded = new Set()
      picked = new Set()
      show('reading')
    }

    try {
      await messageStream(cluster.name, topic, { ...query, after }, (e) => {
        scanned = e.scanned
        switch (e.type) {
          case 'message': {
            const m = e.message
            got.push(m)
            const p = String(m.partition)
            const at = newestFirst ? m.offset : m.offset + 1
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
            next = e.result.next ?? {}
            show(e.result.stopped_by, e.result)
            break
          case 'error':
            throw new Error(e.error)
        }
      }, ctl.signal)
      if (!finished && current()) show('cancelled')
    } catch (e: any) {
      if (!current()) {
        // Replaced by a newer read or a tail; it owns the list now.
      } else if (isAbort(e)) {
        // Stopped: keep what arrived, and let Load more continue from there.
        show('cancelled')
      } else {
        error = e.message
        if (!more) result = undefined
      }
    } finally {
      clearTimeout(timer)
      if (reading === ctl) {
        reading = undefined
        loading = false
        loadingMore = false
      }
    }
  }
  let loadingMore = $state(false)
  onDestroy(() => reading?.abort())

  function stopReading() {
    reading?.abort()
    reading = undefined
    loading = false
    loadingMore = false
  }

  // Rows that arrived over live tail. They flash once when inserted; the set
  // is not reactive on purpose, it is only read as each row is created.
  const fresh = new Set<string>()
  function arrived(node: HTMLElement, key: string) {
    if (!fresh.delete(key)) return
    node.classList.add('arrived')
    node.addEventListener('animationend', () => node.classList.remove('arrived'), { once: true })
  }

  // Live tail: newest first, capped so a busy topic cannot exhaust memory.
  const liveCap = 2000
  let live = $state(false)
  let liveStats = $state({ scanned: 0, matched: 0 })
  let paused = $state(false)
  let stop: (() => void) | undefined

  // A busy topic sends hundreds of messages a second; they are added in
  // batches a few times a second, so the list re-renders per batch, not per
  // message. Fast enough to read as live.
  let incoming: Message[] = []
  let frame: ReturnType<typeof setTimeout> | undefined
  const liveEvery = 250
  let liveError = $state('')
  function flush() {
    frame = undefined
    if (!result || !incoming.length) return
    const add = incoming.reverse()
    incoming = []
    const all = [...add, ...result.messages]
    result = { ...result, messages: all.slice(0, liveCap) }
    // Rows past the cap are gone: they can no longer be selected, resent
    // or flashed, so nothing may keep pointing at them.
    if (all.length > liveCap) {
      const kept = new Set(result.messages.map(mkey))
      for (const k of fresh) if (!kept.has(k)) fresh.delete(k)
      if ([...picked].some((k) => !kept.has(k))) picked = new Set([...picked].filter((k) => kept.has(k)))
      if (selected && !kept.has(mkey(selected))) selected = undefined
    }
  }

  function startLive() {
    stopLive()
    stopReading()
    shownGen++
    searched = search.trim()
    error = ''
    liveError = ''
    expanded = new Set()
    picked = new Set()
    live = true
    paused = false
    liveStats = { scanned: 0, matched: 0 }
    result = { messages: [], scanned: 0, script_errors: 0, stopped_by: 'live', elapsed_ms: 0 }
    selected = undefined
    // "newest" in live mode means only messages that arrive from now on.
    stop = tail(cluster.name, topic, {
      start,
      partitions: partition,
      offset: start === 'offset' ? offset : start === 'last' ? lastN : undefined,
      timestamp: start === 'timestamp' && timestamp ? new Date(timestamp).getTime() : undefined,
      filter: showFilter ? filter : undefined,
      q: search.trim() || undefined,
      raw,
      key_as: keyAs || undefined,
      value_as: valueAs || undefined,
    }, {
      event: (e) => {
        liveStats = { scanned: e.scanned, matched: e.matched }
        // A fetch error mid-stream is passing; the tail keeps going, so it
        // is shown above the rows instead of in place of them.
        if (e.error) liveError = e.error
        else if (e.message) liveError = ''
        if (e.message && !paused) {
          fresh.add(`${e.message.partition}:${e.message.offset}`)
          incoming.push(e.message)
          frame ??= setTimeout(flush, liveEvery)
        }
      },
      error: (msg) => {
        error = msg
        stop = undefined
        live = false
      },
      end: () => {
        stop = undefined
        live = false
      },
    })
  }

  function stopLive() {
    stop?.()
    stop = undefined
    live = false
    clearTimeout(frame)
    frame = undefined
    flush()
  }

  onDestroy(stopLive)

  const hasMore = $derived(!live && !!result && result.stopped_by !== 'live' && (loadingMore || (!loading && Object.keys(result.next ?? {}).length > 0)))

  // Exports the picked messages when any are picked, otherwise every message
  // shown, in list order either way.
  function exportMessages(kind: 'json' | 'csv') {
    if (!result) return
    const list = pickedMessages.length ? pickedMessages : result.messages
    let body: string
    let type: string
    if (kind === 'json') {
      // JSON values are spliced in as their text: parsing them would round
      // large integers, and Import reads this file back.
      body = '[\n' + list.map((m) => {
        const raw = rawJSON(m)
        const rest = JSON.stringify({ ...m, value: raw === undefined ? m.value : null }, null, 2)
        return '  ' + (raw === undefined ? rest : rest.replace(/"value": null/, () => `"value": ${raw}`)).replaceAll('\n', '\n  ')
      }).join(',\n') + '\n]\n'
      type = 'application/json'
    } else {
      const esc = (v: unknown) => {
        const s = v === null || v === undefined ? '' : String(v)
        return /[",\n]/.test(s) ? `"${s.replaceAll('"', '""')}"` : s
      }
      // Binary keys and values are base64 text; the encoding columns tell
      // Import to decode them back to the original bytes.
      const rows = [['partition', 'offset', 'timestamp', 'key', 'value', 'headers', 'key_encoding', 'value_encoding']]
      for (const m of list) {
        rows.push([String(m.partition), String(m.offset), m.timestamp, m.key ?? '', m.value ?? '', JSON.stringify(m.headers),
          m.key_encoding === 'base64' ? 'base64' : '', m.value_encoding === 'base64' ? 'base64' : ''])
      }
      body = rows.map((r) => r.map(esc).join(',')).join('\n')
      type = 'text/csv'
    }
    const a = document.createElement('a')
    a.href = URL.createObjectURL(new Blob([body], { type }))
    a.download = `${topic}-${new Date().toISOString().replaceAll(':', '-')}.${kind}`
    a.click()
    // Revoking at once can cancel the download before it starts.
    setTimeout(() => URL.revokeObjectURL(a.href), 1000)
  }

  // The value's own JSON text when it is a JSON document, else undefined.
  function rawJSON(m: Message) {
    if (m.value === null || m.truncated || !m.value_format || m.value_format === 'text' || m.value_format === 'binary') return undefined
    try {
      JSON.parse(m.value)
      return m.value.trim()
    } catch {
      return undefined
    }
  }


  const stopped: Record<string, string> = {
    end: 'Reached end of range',
    limit: 'Limit reached',
    max_scan: 'Scan limit reached',
    timeout: 'Timed out',
    cancelled: 'Stopped',
    reading: 'Reading…',
  }

  const examples = [
    'return value.status === "FAILED"',
    'return key === "user-42"',
    'return headers["trace-id"] !== undefined',
    'return value.amount > 500 && timestamp > new Date("2026-10-01")',
  ]

  // Value preview. With a search, the window is moved so the first match is
  // visible even when it sits deep in a long value.
  function preview(m: Message) {
    if (m.value === null) return 'null'
    const v = m.value
    const q = searched.toLowerCase()
    const at = q ? v.toLowerCase().indexOf(q) : -1
    if (at > 120) {
      const from = Math.max(0, at - 40)
      const out = v.slice(from, from + 160)
      return '…' + out + (from + 160 < v.length ? '…' : '')
    }
    return v.length > 160 ? v.slice(0, 160) + '…' : v
  }

  // Splits text around the first case-insensitive match of the search term.
  function hit(text: string): [string, string, string] {
    const q = searched
    const i = q ? text.toLowerCase().indexOf(q.toLowerCase()) : -1
    return i < 0 ? [text, '', ''] : [text.slice(0, i), text.slice(i, i + q.length), text.slice(i + q.length)]
  }

  // Rows ticked for a bulk action (resend), keyed by partition:offset.
  let picked = $state(new Set<string>())
  function pick(m: Message) {
    const k = `${m.partition}:${m.offset}`
    const next = new Set(picked)
    if (next.has(k)) next.delete(k)
    else next.add(k)
    picked = next
  }
  const pickedMessages = $derived(picked.size ? (result?.messages ?? []).filter((m) => picked.has(`${m.partition}:${m.offset}`)) : [])
  const allPicked = $derived(!!result?.messages.length && result.messages.every((m) => picked.has(`${m.partition}:${m.offset}`)))
  const pickAll = () => (picked = allPicked ? new Set() : new Set(result?.messages.map((m) => `${m.partition}:${m.offset}`)))

  let resending = $state(false)
  let replaying = $state(false)
  let importing = $state(false)
  let resendList = $state<Message[]>([])
  function resend(list: Message[]) {
    // Oldest first, so per-key order survives the copy.
    resendList = [...list].sort((a, b) => a.timestamp.localeCompare(b.timestamp) || a.partition - b.partition || a.offset - b.offset)
    resending = true
  }

  // Rows expanded in place, keyed by partition:offset.
  let expanded = $state(new Set<string>())
  const mkey = (m: Message) => `${m.partition}:${m.offset}`
  function toggle(m: Message) {
    const k = mkey(m)
    const next = new Set(expanded)
    if (next.has(k)) next.delete(k)
    else next.add(k)
    expanded = next
  }
  // Each open row renders a full value tree; opening a thousand of them at
  // once would freeze the tab, so Expand all opens the first expandCap.
  const expandCap = 50
  const allOpen = $derived(!!result?.messages.length && expanded.size >= Math.min(result.messages.length, expandCap))
  function toggleAll() {
    expanded = allOpen ? new Set() : new Set(result?.messages.slice(0, expandCap).map(mkey))
  }

  // Which field was copied last, so only its button shows the tick.
  let copied = $state<'key' | 'value' | null>(null)
  function copy(text: string, what: 'key' | 'value' = 'value') {
    navigator.clipboard?.writeText(text).then(() => {
      copied = what
      setTimeout(() => (copied = null), 1200)
    }, () => {})
  }

  // j/k (or arrows) walk the list without leaving the detail pane.
  function step(d: number) {
    if (!result?.messages.length) return
    const i = selected ? result.messages.indexOf(selected) : -1
    const next = result.messages[Math.min(Math.max(i + d, 0), result.messages.length - 1)]
    if (next) {
      selected = next
      const row = document.getElementById(`m-${next.partition}-${next.offset}`)
      // Keyboard focus follows the selection when it is on a row, so the
      // focus ring does not stay behind on the row that was clicked.
      if (document.activeElement?.matches('.list tr[tabindex]')) row?.focus({ preventScroll: true })
      row?.scrollIntoView({ block: 'nearest' })
    }
  }
  function onkey(e: KeyboardEvent) {
    // Esc stops a running read wherever focus is, even in the search box.
    // Another dialog (a modal, the command palette) owns the keys while open.
    if (document.querySelector('dialog[open]')) return
    if (e.key === 'Escape' && loading && !resending && !replaying && !importing) {
      e.preventDefault()
      stopReading()
      return
    }
    const t = e.target as HTMLElement
    if (resending || replaying || importing || t.closest('input, textarea, select, [contenteditable], dialog') || e.metaKey || e.ctrlKey || e.altKey) return
    // Arrows walk the list only once a message is open; otherwise they
    // scroll the page, and on a control (a button, a link) keep their use.
    const arrow = e.key === 'ArrowDown' || e.key === 'ArrowUp'
    if (arrow && (!selected || (t.closest('button, a') && !t.closest('.list, .detail')))) return
    if (e.key === 'j' || e.key === 'ArrowDown') { e.preventDefault(); step(1) }
    else if (e.key === 'k' || e.key === 'ArrowUp') { e.preventDefault(); step(-1) }
    else if (e.key === 'Escape') selected = undefined
  }

  // On phones the detail is a full-screen sheet; the page behind must not
  // scroll with it, and the app header (a separate stacking layer) steps
  // aside so the sheet can cover the whole screen.
  const sheet = $derived(narrow.matches && !!selected)
  $effect(() => {
    const html = document.documentElement
    html.style.overflow = sheet ? 'hidden' : ''
    html.classList.toggle('sheet-open', sheet)
    return () => {
      html.style.overflow = ''
      html.classList.remove('sheet-open')
    }
  })
  // Value fields shown as their own columns, chosen per topic.
  let columns = $state<string[]>(untrack(() => loadColumns(topic)))
  function addColumn(path: string) {
    if (columns.includes(path)) return
    columns = [...columns, path]
    saveColumns(topic, columns)
  }
  function removeColumn(path: string) {
    columns = columns.filter((c) => c !== path)
    saveColumns(topic, columns)
  }
  function moveColumn(path: string, by: number) {
    const i = columns.indexOf(path)
    const j = i + by
    if (i < 0 || j < 0 || j >= columns.length) return
    const next = [...columns]
    ;[next[i], next[j]] = [next[j], next[i]]
    columns = next
    saveColumns(topic, columns)
  }
  const showColumns = $derived(!narrow.matches && columns.length > 0)
  // Each JSON value is parsed once per result, and only with columns shown.
  const parsed = $derived.by(() => {
    const out = new Map<Message, unknown>()
    if (!showColumns || !result) return out
    for (const m of result.messages) {
      if (m.value === null || m.value_format !== 'json' && !/^\s*[{[]/.test(m.value)) continue
      try {
        out.set(m, JSON.parse(m.value))
      } catch {}
    }
    return out
  })
  const field = (m: Message, path: string) => cell(pickField(parsed.get(m), path))

  // Relative times tick; the others never change.
  let now = $state(Date.now())
  $effect(() => {
    if (prefs.time !== 'relative') return
    const t = setInterval(() => (now = Date.now()), 15_000)
    return () => clearInterval(t)
  })
  const when = (iso: string) => showTime(iso, prefs.time, now)
  // The other clock, on hover: UTC for local times, local otherwise.
  const other = (iso: string) => showTime(iso, prefs.time === 'local' ? 'utc' : 'local')

  const cols = $derived((cluster.read_only ? 0 : 1) + (narrow.matches ? 3 : 6) + (showColumns ? columns.length : 0))

  // The key column fits the longest key shown, so keys read whole; the value
  // preview takes what is left. Capped so the value never gets squeezed out.
  const keyWidth = $derived.by(() => {
    let longest = 3
    for (const m of result?.messages ?? []) longest = Math.max(longest, (m.key ?? '—').length)
    const px = Math.max(96, Math.ceil(longest * 7.6) + 30)
    return Math.min(px, selected ? 200 : 380)
  })
  const timeWidth = $derived(prefs.time === 'relative' || prefs.time === 'unix' ? 130 : prefs.time === 'utc' ? 210 : 200)

  // The list's columns have fixed widths, so it needs at least their sum (plus
  // room for the value preview) before the detail can sit beside it. When the
  // split is narrower than both, the detail goes below the list instead of
  // overlapping it.
  const detailMin = 380
  let splitWidth = $state(0)
  const listMin = $derived(
    (cluster.read_only ? 0 : 34) + 40 + 44 + 84 + timeWidth + keyWidth + (showColumns ? columns.length * 140 : 0) + 160,
  )
  const stacked = $derived(narrow.matches || splitWidth < listMin + 12 + detailMin)
  // Too narrow for the side-by-side split but wider than a phone, the detail
  // slides over the list from the right instead of opening below it.
  const drawer = $derived(!narrow.matches && stacked)

  // Last: load() resets state declared throughout this script before its
  // first await.
  load()
</script>

<svelte:window onkeydown={onkey} />

<ResendDialog {cluster} {topic} messages={resendList} bind:open={resending} />
<ReplayDialog {cluster} {topic} {partitions} bind:open={replaying}
  seed={{ partition, start, offset, timestamp, search: search.trim(), filter: showFilter ? filter : '' }} />
<ImportDialog {cluster} {topic} {partitions} bind:open={importing} />
{#if editError}<div class="error" role="alert">{editError}</div>{/if}

<form class="controls" onsubmit={(e) => { e.preventDefault(); load() }}>
  <!-- One strip of joined cells: label inside on the left, control on the
       right, matching the topic stat strip. Actions sit at the far end. -->
  <div class="fields">
    <label class="f search" title="Keep messages whose key, value or headers contain this text (case-insensitive). Schema-encoded values are searched in their decoded form.">
      <Search size={14} class="text-ink-3" />
      <input type="search" placeholder="Search messages…" bind:value={search} aria-label="Search messages"
        onkeydown={(e) => { if (e.key === 'Enter') { e.preventDefault(); load() } }} />
      {#if search}
        <button type="button" class="ghost clear" aria-label="Clear search" onclick={() => { search = ''; load() }}><X size={13} /></button>
      {/if}
    </label>
    <label class="f">
      <span class="fl">Start</span>
      <select bind:value={start}>
        <option value="newest">Newest</option>
        <option value="oldest">Oldest</option>
        <option value="offset">From offset</option>
        <option value="timestamp">From time</option>
        <option value="last">Last N per partition</option>
      </select>
    </label>
    {#if start === 'offset'}
      <label class="f"><span class="fl">Offset</span><input type="number" min="0" class="w-num" bind:value={offset} /></label>
    {:else if start === 'timestamp'}
      <label class="f"><span class="fl">Time</span><input type="datetime-local" step="1" bind:value={timestamp} /></label>
    {:else if start === 'last'}
      <label class="f" title="Read the newest N records of each partition, oldest first"><span class="fl">N</span><input type="number" min="1" class="w-sm" bind:value={lastN} /></label>
    {/if}
    <label class="f">
      <span class="fl">Partition</span>
      <select bind:value={partition}>
        <option value="">All</option>
        {#each partitions as p (p)}<option value={String(p)}>{p}</option>{/each}
      </select>
    </label>
    <label class="f" title="Messages to return"><span class="fl">Limit</span><input type="number" min="1" max="1000" class="w-sm" bind:value={limit} /></label>
    <label class="f" title="Stop after reading this many records, matched or not"><span class="fl">Max scan</span><input type="number" min="1" class="w-num" bind:value={maxScan} /></label>
    <label class="f switch" class:on={showFilter} title="Filter with a JavaScript expression">
      <input type="checkbox" bind:checked={showFilter} /><Code size={13} />JS filter
    </label>
    <label class="f" title="How keys are read. Auto: Schema Registry, then JSON, text, or base64 for binary. Any other choice reads the bytes in that format and skips the registry.">
      <span class="fl">Key</span>
      <select bind:value={keyAs}>{#each decoders as [id, label] (id)}<option value={id}>{label}</option>{/each}</select>
    </label>
    <label class="f" title="How values are read. Auto: Schema Registry, then JSON, text, or base64 for binary. Any other choice reads the bytes in that format and skips the registry.">
      <span class="fl">Value</span>
      <select bind:value={valueAs}>{#each decoders as [id, label] (id)}<option value={id}>{label}</option>{/each}</select>
    </label>
    {#if cluster.schema_registry}
      <label class="f switch" class:on={raw}
        title="Skip Schema Registry decoding. Avro, Protobuf and JSON Schema messages are shown as their stored bytes (base64, starting with the schema ID) instead of decoded JSON. Use it when decoding fails or to check which schema ID a producer wrote. Plain JSON and text messages look the same either way.">
        <input type="checkbox" bind:checked={raw} /><Binary size={13} />Raw
      </label>
    {/if}
  </div>
  <span class="spacer"></span>
  <div class="actions">
    {#if live}
      <button type="button" onclick={() => (paused = !paused)}>{#if paused}<Play size={14} />Resume{:else}<Pause size={14} />Pause{/if}</button>
      <button type="button" class="danger" onclick={stopLive}><Square size={13} />Stop</button>
    {:else if loading}
      <button type="button" class="danger" onclick={stopReading} title="Stop reading; the server stops scanning too (Esc)"><Square size={13} />Stop</button>
      <button class="primary" type="submit" disabled><Loader size={14} class="animate-spin" />Reading…</button>
    {:else}
      <button type="button" onclick={startLive}
        title={start === 'newest' ? 'Watch new messages as they arrive' : 'Read from the selected start and keep following'}><Radio size={14} />Live tail</button>
      <button class="primary" type="submit"><Play size={14} />Fetch</button>
    {/if}
  </div>

  {#if showFilter}
    <div class="filter">
      {#await import('../lib/FilterEditor.svelte')}
        <div class="cm-loading"></div>
      {:then { default: FilterEditor }}
        <FilterEditor bind:value={filter} onrun={() => load()} placeholder={'return value.status === "FAILED"'} />
      {/await}
      <div class="hint faint">
        <span>Return <code>true</code> to keep a message · <code>value</code> <code>key</code> <code>headers</code> <code>partition</code> <code>offset</code> <code>timestamp</code> <code>raw</code> · <kbd>Ctrl/⌘ Enter</kbd> runs</span>
        <span class="spacer"></span>
        <span class="examples">
          {#each examples as ex (ex)}<button type="button" class="ghost ex mono" onclick={() => (filter = ex)}>{ex}</button>{/each}
        </span>
      </div>
    </div>
  {/if}
</form>

{#if error}
  <div class="error">{error}</div>
{:else if result}
  {#if live && liveError}<div class="error small-err">{liveError}</div>{/if}
  <div class="meta faint">
    {#if live || result.stopped_by === 'live'}
      {#if live}<span class="badge ok"><span class="sq live"></span>live</span>{:else}<span class="badge"><span class="sq idle"></span>stopped</span>{/if}
      {fmt(result.messages.length)} messages shown · {fmt(liveStats.matched)} matched · {fmt(liveStats.scanned)} scanned
      {#if paused}<span class="badge warn">paused</span>{/if}
      {#if result.messages.length >= liveCap}<span>· keeping last {fmt(liveCap)}</span>{/if}
    {:else}
      {fmt(result.messages.length)} {searched ? `matching “${searched}”` : 'messages'} · {fmt(result.scanned)} scanned · {result.elapsed_ms} ms · {stopped[result.stopped_by] ?? result.stopped_by}
    {/if}
    {#if result.script_errors > 0}
      <span class="badge danger" title={result.first_error}>{result.script_errors} filter errors</span>
      <span class="mono err">{result.first_error}</span>
    {/if}
    <span class="spacer"></span>
    {#if picked.size}
      <span class="pickbar"><b>{picked.size}</b> selected
        <button class="ghost small" onclick={() => (picked = new Set())}><X size={12} />Clear</button>
        <button class="primary small" disabled={cluster.read_only} onclick={() => resend(pickedMessages)}><Repeat size={13} />Resend…</button>
      </span>
    {/if}
    {#if !cluster.read_only || result.messages.length}
      {#snippet tools()}
        {#if !cluster.read_only}
          <button class="ghost small" onclick={() => (replaying = true)} title="Copy a range of this topic to another topic or back into it"><History size={13} />Replay range…</button>
          <button class="ghost small" onclick={() => (importing = true)} title="Write records from a JSON, CSV or text file"><Upload size={13} />Import…</button>
        {/if}
        {#if result?.messages.length}
          {@const what = pickedMessages.length ? `the ${fmt(pickedMessages.length)} selected messages` : `all ${fmt(result.messages.length)} messages shown`}
          <button class="ghost small" title="Download {what} as JSON" onclick={() => exportMessages('json')}><Download size={13} />{narrow.matches ? 'Export JSON' : 'JSON'}{#if pickedMessages.length} ({pickedMessages.length}){/if}</button>
          <button class="ghost small" title="Download {what} as CSV" onclick={() => exportMessages('csv')}><Download size={13} />{narrow.matches ? 'Export CSV' : 'CSV'}{#if pickedMessages.length} ({pickedMessages.length}){/if}</button>
        {/if}
      {/snippet}
      {#if narrow.matches}
        <Overflow label="Message tools">{@render tools()}</Overflow>
      {:else}
        <ViewSettings {columns} onadd={addColumn} onremove={removeColumn} onmove={moveColumn} />
        {@render tools()}
      {/if}
    {/if}
  </div>

  <div class="split" class:open={!!selected} class:stacked bind:clientWidth={splitWidth}
    style:--list-min="{listMin}px" style:--detail-min="{detailMin}px">
    <div class="panel list">
      <table class="manual">
        <colgroup>
          {#if !cluster.read_only}<col style:width="34px" />{/if}
          {#if narrow.matches}
            <col />
          {:else}
            <col style:width="40px" /><col style:width="44px" /><col style:width="84px" /><col style:width="{timeWidth}px" />
            <col style:width="{keyWidth}px" />
            {#if showColumns}{#each columns as c (c)}<col style:width="140px" />{/each}{/if}
            <col />
          {/if}
        </colgroup>
        <thead>
          <tr>
            {#if !cluster.read_only}
              <th class="pk"><input type="checkbox" checked={allPicked} indeterminate={picked.size > 0 && !allPicked}
                disabled={!result.messages.length} onchange={pickAll} aria-label="Select all messages" /></th>
            {/if}
            {#if narrow.matches}
              <th>Message</th>
            {:else}
              <th class="tog">
                {#if result.messages.length}
                  <button type="button" class="ghost xbtn" onclick={toggleAll} title={allOpen ? 'Collapse all' : result.messages.length > expandCap ? `Expand the first ${expandCap}` : 'Expand all'} aria-label={allOpen ? 'Collapse all' : 'Expand all'}>
                    {#if allOpen}<ChevronsDownUp size={13} />{:else}<ChevronsUpDown size={13} />{/if}
                  </button>
                {/if}
              </th>
              <th class="num" title="Partition"><abbr>P</abbr></th><th class="num">Offset</th><th>Time</th><th>Key</th>
              {#if showColumns}{#each columns as c (c)}<th class="mono vcol" title="value.{c}">{c}</th>{/each}{/if}
              <th>Value</th>
            {/if}
          </tr>
        </thead>
        <tbody>
          {#each result.messages as m, i (mkey(m))}
            {@const open = !narrow.matches && expanded.has(mkey(m))}
            {@const k = hit(m.key ?? '—')}
            {@const v = hit(preview(m))}
            <tr id="m-{m.partition}-{m.offset}" class:alt={i % 2 === 1} class:sel={selected === m} class:bad={!!m.decode_error} class:opened={open} class:picked={picked.has(mkey(m))} onclick={() => (selected = m)}
              tabindex="0" aria-selected={selected === m} onkeydown={(e) => { if ((e.key === 'Enter' || e.key === ' ') && e.target === e.currentTarget) { e.preventDefault(); selected = m } }} use:arrived={mkey(m)}>
              {#if !cluster.read_only}
                <td class="pk"><input type="checkbox" checked={picked.has(mkey(m))} onclick={(e) => e.stopPropagation()}
                  onchange={() => pick(m)} aria-label="Select message {m.partition}:{m.offset}" /></td>
              {/if}
              {#if narrow.matches}
                <td class="card">
                  <div class="card-meta mono">
                    <span title="Partition {m.partition}">P{m.partition}</span><span title="Offset {m.offset}">#{m.offset}</span>
                    <span class="faint">{when(m.timestamp)}</span>
                  </div>
                  <div class="mono key">{k[0]}{#if k[1]}<mark>{k[1]}</mark>{/if}{k[2]}</div>
                  <div class="mono val">{v[0]}{#if v[1]}<mark>{v[1]}</mark>{/if}{v[2]}</div>
                </td>
              {:else}
                <td class="tog">
                  <button type="button" class="xbtn" class:on={open} aria-expanded={open}
                    aria-label={open ? 'Collapse message' : 'Expand message'}
                    onclick={(e) => { e.stopPropagation(); toggle(m) }}>
                    {#if open}<Minus size={12} strokeWidth={2.2} />{:else}<Plus size={12} strokeWidth={2.2} />{/if}
                  </button>
                </td>
                <td class="num mono">{m.partition}</td>
                <td class="num mono">{m.offset}</td>
                <td class="mono nowrap time" title={other(m.timestamp)}>{when(m.timestamp)}</td>
                <td class="mono key" title={m.key ?? undefined}>{k[0]}{#if k[1]}<mark>{k[1]}</mark>{/if}{k[2]}</td>
                {#if showColumns}{#each columns as c (c)}{@const f = field(m, c)}<td class="mono vcol" title={f}>{f}</td>{/each}{/if}
                <td class="mono val">{v[0]}{#if v[1]}<mark>{v[1]}</mark>{/if}{v[2]}</td>
              {/if}
            </tr>
            {#if open}
              <tr class="xrow" class:alt={i % 2 === 1} class:sel={selected === m}>
                {#if !cluster.read_only}<td></td>{/if}
                <td></td>
                <td colspan={5 + (showColumns ? columns.length : 0)}>
                  <div class="inl">
                    <div class="inl-meta">
                      <span class="badge" class:accent={!!m.value_schema_id}>{m.value_format ?? 'null'}</span>
                      {#if m.value_schema_id}<span class="faint">schema #{m.value_schema_id}</span>{/if}
                      <span class="faint">{fmt(m.value_bytes)} bytes</span>
                      {#if m.truncated}<span class="badge warn">truncated</span>{/if}
                      {#if m.key !== null}<span class="faint">key</span><code>{m.key}</code>{/if}
                      {#each m.headers as h, i (i)}<span class="hdr"><span class="faint">{h.key}</span>{h.value}</span>{/each}
                      <span class="spacer"></span>
                       {#if !cluster.read_only}
                         <button type="button" class="ghost small" disabled={editing} onclick={() => edit(m)}><Pencil size={12} />{editing ? 'Loading…' : 'Edit & resend'}</button>
                         <button type="button" class="ghost small" onclick={() => resend([m])}><Repeat size={12} />Resend…</button>
                       {/if}
                      {#if m.key !== null}<button type="button" class="ghost small" onclick={() => copy(m.key ?? '', 'key')}><Copy size={12} />Copy key</button>{/if}
                      <button type="button" class="ghost small" onclick={() => copy(m.value ?? '')}><Copy size={12} />Copy value</button>
                    </div>
                    {#if m.decode_error}<div class="error small-err">Schema decoding failed: {m.decode_error}</div>{/if}
                    <ValueView text={m.value} format={m.value_format} maxHeight={420} />
                  </div>
                </td>
              </tr>
            {/if}
          {:else}
            <tr><td colspan={cols} class="empty">{#if live}<Pending label="Waiting for new messages…" />{:else if loading}<Pending label="Reading…" />{:else}No messages found.{/if}</td></tr>
          {/each}
        </tbody>
      </table>
      {#if hasMore}
        <div class="more-rows">
          {#if loadingMore}
            <button type="button" class="danger" onclick={stopReading}><Square size={12} />Stop</button>
            <span class="faint"><Loader size={13} class="animate-spin inline" /> Reading…</span>
          {:else}
            <button type="button" onclick={() => load(true)} disabled={loading}
              title="Continue the same read from where it stopped"><ChevronsDown size={14} />{shownQuery.start === 'newest' ? 'Load older' : 'Load more'}</button>
          {/if}
        </div>
      {/if}
    </div>

    {#if selected}
      <div class="panel detail" class:sheet class:drawer role={sheet || drawer ? 'dialog' : undefined} aria-modal={sheet ? 'true' : undefined}
        aria-label={sheet || drawer ? `Message ${selected.partition}:${selected.offset}` : undefined}>
        <div class="dhead">
          <div class="dtop">
            <div class="dtitle">
              <b><span class="faint">Partition</span> {selected.partition} <span class="faint">·</span> <span class="faint">Offset</span> <span class="mono">{fmt(selected.offset)}</span></b>
              <span class="faint mono time" title={other(selected.timestamp)}>{when(selected.timestamp)}</span>
            </div>
            <div class="seg" role="group" aria-label="Walk messages">
              <button title="Previous (k)" aria-label="Previous message" onclick={() => step(-1)}><ChevronUp size={15} /></button>
              <button title="Next (j)" aria-label="Next message" onclick={() => step(1)}><ChevronDown size={15} /></button>
            </div>
            <button class="ghost icon" title="Close (Esc)" aria-label="Close" onclick={() => (selected = undefined)}><X size={16} /></button>
          </div>
          {#if !cluster.read_only}
            <div class="dacts">
              <button class="small" disabled={editing} title="Open this message in Produce" onclick={() => selected && edit(selected)}><Pencil size={13} />{editing ? 'Loading…' : 'Edit & resend'}</button>
              <button class="small" title="Resend this message byte for byte" onclick={() => selected && resend([selected])}><Repeat size={13} />Resend…</button>
            </div>
          {/if}
        </div>

        <div class="dbody">
        {#if selected.decode_error}
          <section><div class="error small-err">Schema decoding failed: {selected.decode_error}</div></section>
        {/if}

        <section>
          <div class="label">
            Key
            {#if selected.key_format}<span class="badge accent">{selected.key_format}</span>{/if}
            {#if selected.key_schema_id}<span class="faint">schema #{selected.key_schema_id}</span>{/if}
            {#if selected.key_encoding === 'base64'}<span class="badge">base64</span>{/if}
            {#if selected.key !== null}<span class="faint">{fmt(selected.key_bytes)} bytes</span>{/if}
            {#if selected.key !== null}
              <span class="spacer"></span>
              <a class="btn ghost small" href={rawURL(cluster.name, topic, selected.partition, selected.offset, 'key')} download
                title="Download the key's bytes exactly as stored"><Download size={13} />Raw</a>
              <button class="ghost small" onclick={() => copy(selected?.key ?? '', 'key')}>{#if copied === 'key'}<Check size={13} />Copied{:else}<Copy size={13} />Copy{/if}</button>
            {/if}
          </div>
          <ValueView text={selected.key} format={selected.key_format} depth={2} />
        </section>

        <section>
          <div class="label">
            Value
            <span class="badge" class:accent={!!selected.value_schema_id}>{selected.value_format ?? 'null'}</span>
            {#if selected.value_schema_id}<span class="faint">schema #{selected.value_schema_id}</span>{/if}
            <span class="faint">{fmt(selected.value_bytes)} bytes</span>
            {#if selected.truncated}<span class="badge warn">truncated</span>{/if}
            <span class="spacer"></span>
            {#if selected.value !== null}
              <a class="btn ghost small" href={rawURL(cluster.name, topic, selected.partition, selected.offset, 'value')} download
                title={selected.truncated ? 'Only the first 64 KiB is shown; download the whole value as stored' : "Download the value's bytes exactly as stored"}><Download size={13} />{selected.truncated ? 'Download full' : 'Raw'}</a>
            {/if}
            <button class="ghost small" onclick={() => copy(selected?.value ?? '')}>{#if copied === 'value'}<Check size={13} />Copied{:else}<Copy size={13} />Copy{/if}</button>
          </div>
          {#key selected}<ValueView text={selected.value} format={selected.value_format} />{/key}
        </section>

        <section>
          <div class="label">Headers <span class="faint">{selected.headers.length}</span></div>
          {#if selected.headers.length}
            <table class="headers">
              <tbody>
                {#each selected.headers as h, i (i)}
                  <tr><td class="mono">{h.key}</td><td class="mono">{h.value}</td></tr>
                {/each}
              </tbody>
            </table>
          {:else}
            <div class="faint">No headers.</div>
          {/if}
        </section>
        </div>
      </div>
    {/if}
  </div>
{:else if loading}
  <div class="empty"><Pending label="Reading…" /></div>
{/if}

<style>
  .controls {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 8px 12px;
    margin-bottom: 14px;
  }
  .fields {
    display: flex;
    flex-wrap: wrap;
    background: var(--surface);
    border: 1px solid var(--border-strong);
  }
  .f {
    display: flex;
    flex-direction: row;
    align-items: center;
    gap: 8px;
    height: 32px;
    padding: 0 4px 0 10px;
    margin: 0 -1px -1px 0;
    border-right: 1px solid var(--border);
    border-bottom: 1px solid var(--border);
    font-size: 13px;
    font-weight: 400;
    color: var(--text);
    white-space: nowrap;
  }
  .f:focus-within { background: var(--row-hover); }
  .fl { color: var(--text-3); font-size: 12px; }
  /* Controls sit flush inside their cell; the cell is the visible box. */
  .f select, .f input:not([type='checkbox']) {
    height: 30px;
    padding: 0 6px;
    border: 0;
    background: transparent;
    font-size: 13px;
    font-weight: 500;
  }
  .f select { padding-right: 2px; cursor: pointer; }
  .f input:not([type='checkbox']):focus, .f select:focus { outline: none; }
  .f:has(input:focus-visible, select:focus-visible) { box-shadow: inset 0 -2px 0 var(--focus); }
  .w-sm { width: 56px; }
  .list mark { background: var(--cta); color: var(--cta-text); padding: 0 1px; border-radius: 1px; }
  .search { gap: 6px; padding-right: 2px; min-width: 240px; flex: 1 1 240px; }
  .search input[type='search'] { flex: 1; min-width: 0; font-weight: 400; }
  .search input::-webkit-search-cancel-button { display: none; }
  .clear { padding: 3px 5px; color: var(--text-3); }
  .clear:hover:not(:disabled) { color: var(--text); background: transparent; }
  .fields { flex: 1 1 auto; }
  .w-num { width: 84px; }
  /* Toggle cells: the cell is the switch, filled in the highlight when on. */
  .switch { position: relative; gap: 6px; padding: 0 11px; color: var(--text-2); cursor: pointer; }
  .switch:hover { color: var(--text); background: var(--row-hover); }
  .switch.on { color: var(--cta-text); background: var(--cta); }
  .switch input { position: absolute; opacity: 0; pointer-events: none; }
  .switch:has(input:focus-visible) { box-shadow: inset 0 -2px 0 var(--focus); }
  .actions { display: flex; gap: 8px; }
  .filter {
    flex-basis: 100%;
    display: grid;
    gap: 6px;
  }
  .filter :global(.cm) { margin-top: 0; }
  kbd { font: 11px var(--mono); padding: 0 4px; border: 1px solid var(--border-strong); border-radius: 2px; background: var(--surface); }
  .hint { display: flex; flex-wrap: wrap; align-items: center; gap: 4px 10px; font-size: 12px; }
  .hint code { font-size: 12px; }
  .examples { display: flex; flex-wrap: wrap; gap: 4px; }
  .ex { font-size: 12px; padding: 1px 7px; border: 1px solid var(--border-strong); background: var(--surface); color: var(--text-2); }
  .meta { font-size: 13px; margin-bottom: 8px; display: flex; gap: 8px; align-items: center; flex-wrap: wrap; min-height: 26px; }
  .err { color: var(--danger); font-size: 12px; }
  .split { display: grid; grid-template-columns: minmax(0, 1fr); gap: 12px; align-items: start; }
  .split.open:not(.stacked) { grid-template-columns: minmax(var(--list-min), 1fr) minmax(var(--detail-min), 1fr); }
  .list > table { min-width: 0; }
  /* The list grows with its rows and the page scrolls; the header row sticks
     under the top edge (under the app header below lg) while reading. */
  .list { --stick: 0px; }
  .list thead th { top: var(--stick); z-index: 2; }
  .list tbody tr { scroll-margin-top: calc(var(--stick) + 40px); scroll-margin-bottom: 12px; }
  @media (max-width: 1023.98px) { .list { --stick: 48px; } }
  /* Between phone and lg the panel scrolls sideways, so it is the header's
     sticky container and the offset would push the header onto the rows. */
  @media (min-width: 640px) and (max-width: 1023.98px) { .list thead th { position: static; } }
  .list table { table-layout: fixed; }
  .list tr { cursor: pointer; }
  .list tr:focus-visible { outline: 2px solid var(--focus); outline-offset: -2px; }
  .list tr.sel td { background: var(--sel); }
  .list tr.bad td:first-child { box-shadow: inset 2px 0 0 var(--danger); }
  /* A message that just arrived over live tail lights up in the highlight
     colour and settles into its row, so a burst reads as a burst. With
     reduced motion it is marked for a moment and then simply unmarked. */
  .list :global(tr.arrived > td) { animation: arrive 1.6s cubic-bezier(.16, 1, .3, 1) both; }
  .list :global(tr.arrived > td:first-child) { animation-name: arrive, arrive-edge; }
  @keyframes arrive { from { background-color: var(--cta); } }
  @keyframes arrive-edge { from { box-shadow: inset 3px 0 0 var(--cta-text); } to { box-shadow: inset 3px 0 0 transparent; } }
  @media (prefers-reduced-motion: reduce) {
    .list :global(tr.arrived > td) { animation-timing-function: steps(1, end); }
  }
  .small-err { font-size: 12px; padding: 6px 8px; }
  .nowrap { white-space: nowrap; }
  .time { overflow: hidden; text-overflow: ellipsis; }
  .key, .val { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .val { color: var(--text-2); }
  /* The header stays put; the body under it scrolls. */
  .detail { position: sticky; top: 12px; max-height: calc(100vh - 24px); display: flex; flex-direction: column; overflow: hidden; }
  .dbody { flex: 1 1 auto; min-height: 0; overflow: auto; overscroll-behavior: contain; }
  /* Header: who the message is, how to walk and close, on one line that
     never wraps; the write actions sit on their own row under it. */
  .dhead { flex: none; display: grid; grid-template-columns: minmax(0, 1fr); border-bottom: 1px solid var(--border-strong); background: var(--surface-2); }
  .dtop { display: flex; align-items: center; gap: 8px; padding: 4px 4px 4px 12px; }
  /* Title and time on one baseline; the time gives way first when narrow. */
  .dtitle { display: flex; align-items: baseline; gap: 10px; flex: 1; min-width: 0; }
  .dtitle > * { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .dtitle b { flex: none; max-width: 100%; font-weight: 600; }
  .dtitle b .faint { font-weight: 400; }
  .dtitle b .mono { font-size: 14px; font-weight: 600; }
  .dtitle .time { flex: 0 1 auto; min-width: 0; font-size: 12px; }
  .seg { display: flex; flex: none; }
  .seg button { padding: 2px 5px; border-radius: 0; color: var(--text-2); background: var(--surface); }
  .seg button + button { margin-left: -1px; }
  .seg button:first-child { border-radius: var(--radius) 0 0 var(--radius); }
  .seg button:last-child { border-radius: 0 var(--radius) var(--radius) 0; }
  .seg button:hover:not(:disabled) { color: var(--text); }
  .icon { flex: none; padding: 3px; color: var(--text-2); }
  .icon:hover:not(:disabled) { color: var(--text); }
  .dacts { display: flex; flex-wrap: wrap; gap: 6px; padding: 4px 12px; border-top: 1px dashed var(--line-dash); }
  section { padding: 12px; border-bottom: 1px solid var(--border); }
  section:last-child { border-bottom: 0; }
  .label { display: flex; align-items: center; gap: 6px; font-size: 12px; font-weight: 500; color: var(--text-2); margin-bottom: 6px; }
  .headers td { padding: 4px 8px; font-size: 12px; }

  .pk { padding-right: 0; }
  td.pk { padding-top: calc(var(--cell-y) + 1px); }
  .pk input { display: block; margin: 0 0 0 2px; }
  .list tr.picked td { background: var(--sel); }
  .pickbar { display: inline-flex; align-items: center; gap: 6px; padding: 1px 4px 1px 10px; margin-right: 6px; color: var(--text); background: var(--sel); border: 1px solid var(--cta-border); border-radius: var(--radius); }
  .pickbar b { font-weight: 600; }
  .tog { width: 1%; padding-right: 0; }
  td.tog { padding-top: calc(var(--cell-y) - 1px); }
  .xbtn {
    width: 20px;
    height: 20px;
    padding: 0;
    justify-content: center;
    border-radius: 2px;
    color: var(--text-2);
  }
  .xbtn.on { background: var(--cta); border-color: var(--cta-border); color: var(--cta-text); }
  .xbtn.on:hover:not(:disabled) { background: var(--cta-hover); }
  th .xbtn { border: 0; color: var(--text-3); }
  tr.opened td { border-bottom-color: transparent; }
  tr.xrow td { padding-top: 0; cursor: default; }
  .vcol { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .more-rows { display: flex; align-items: center; justify-content: center; gap: 10px; padding: 10px; border-top: 1px solid var(--border); }
  tbody tr.xrow:hover td { background: transparent; }
  tbody tr.xrow.alt:hover td { background: var(--stripe); }
  .inl { display: grid; gap: 6px; padding-bottom: 4px; }
  .inl-meta { display: flex; flex-wrap: wrap; align-items: center; gap: 6px 8px; font-size: 12px; }
  .inl-meta code { font-size: 12px; }
  .hdr { display: inline-flex; gap: 4px; font: 12px var(--mono); padding: 0 6px; border: 1px solid var(--border); border-radius: 2px; }

  /* The drawer leaves the list's left side visible, so rows stay clickable
     and j/k keep walking it; under lg it starts below the app header. */
  .detail.drawer {
    position: fixed;
    top: 0;
    right: 0;
    bottom: 0;
    z-index: 32;
    width: min(560px, calc(100vw - 64px));
    max-height: none;
    border-width: 0 0 0 1px;
    border-radius: 0;
    box-shadow: -12px 0 32px rgb(0 0 0 / .18);
    animation: drawer-in .18s cubic-bezier(.16, 1, .3, 1);
  }
  @keyframes drawer-in { from { transform: translateX(24px); opacity: 0; } }
  @media (max-width: 1023.98px) { .detail.drawer { top: 48px; } }
  @media (prefers-reduced-motion: reduce) { .detail.drawer { animation: none; } }

  /* Phones. The field strip becomes a two-column grid with search on its own
     row, the actions fill the width, rows are two-line cards, and the
     selected message opens as a full-screen sheet. */
  @media (max-width: 639.98px) {
    .controls { gap: 8px; }
    .fields { display: grid; grid-template-columns: 1fr 1fr; width: 100%; }
    .f { height: 40px; min-width: 0; }
    .f select, .f input:not([type='checkbox']) { height: 38px; min-width: 0; flex: 1; font-size: 16px; }
    .search { grid-column: 1 / -1; min-width: 0; }
    .switch { justify-content: center; }
    .w-sm, .w-num { width: auto; }
    .controls > .spacer { display: none; }
    .actions { width: 100%; }
    .actions > button { flex: 1; justify-content: center; padding-block: 8px; }
    .hint .examples { display: none; }
    .meta { gap: 6px; }
    .pickbar { order: 1; width: 100%; margin: 0; justify-content: space-between; }

    .list { overflow: visible; }
    .card { display: grid; gap: 2px; min-width: 0; padding-block: 9px; }
    .card-meta { display: flex; gap: 10px; font-size: 12px; color: var(--text-2); }
    .card-meta .faint { margin-left: auto; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .card .key { font-size: 12px; color: var(--text); }
    .card .val { font-size: 12px; }
    td.pk { padding-top: 12px; }

    .detail.sheet {
      position: fixed;
      inset: 0;
      z-index: 50;
      max-height: none;
      border: 0;
      border-radius: 0;
      overscroll-behavior: contain;
      padding-bottom: env(safe-area-inset-bottom);
    }
    .sheet .dtop { padding: 10px 8px 10px 14px; }
    .sheet .dacts { padding-inline: 14px; }
    .sheet .seg button { padding: 6px 9px; }
    .sheet .icon { padding: 7px; }
  }
</style>
