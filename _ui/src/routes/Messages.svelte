<script lang="ts">
  import { onDestroy, untrack, type ComponentProps } from 'svelte'
  import { errorMessage } from '../lib/http'
  import { api, type Cluster, type Message } from '../lib/api'
  import { messageFeed } from '../lib/messageFeed.svelte'
  import { liveCap, tailQuery } from '../lib/messageFeed'
  import Pending from '../lib/Pending.svelte'
  import { copier } from '../lib/resource.svelte'
  import { fmt, localInput } from '../lib/format'
  import { exportJSON, exportCSV } from '../lib/messageExport'
  import { download, fileStamp } from '../lib/download'
  import { resendOrder } from '../lib/resend'
  import { messageStart, startProblem } from '../lib/messageStart'
  import { prefs, loadDetailWidth } from '../lib/prefs.svelte'
  import { messageColumns } from '../lib/messageColumns.svelte'
  import { DETAIL_MIN, SPLIT_GAP } from '../lib/messageLayout'
  import ViewSettings from '../lib/ViewSettings.svelte'
  import { showTime } from '../lib/viewer'
  import MessageControls, { messageForm, startsAtTime } from '../lib/MessageControls.svelte'
  import MessageTable from '../lib/MessageTable.svelte'
  import MessageDetail from '../lib/MessageDetail.svelte'
  import ResendDialog from '../lib/ResendDialog.svelte'
  import ReplayDialog from '../lib/ReplayDialog.svelte'
  import ImportDialog from '../lib/ImportDialog.svelte'
  import { messageDraft } from '../lib/produce'
  import { narrow } from '../lib/media.svelte'
  import Overflow from '../lib/Overflow.svelte'
  import { Download, X, Repeat, Upload, History } from '@lucide/svelte'

  let {
    cluster,
    topic,
    partitions,
    onedit,
    at,
  }: {
    cluster: Cluster
    topic: string
    partitions: number[]
    onedit: (message: Message) => void
    // Open on this message: the read starts there and it is selected.
    at?: { partition: number; offset: number }
  } = $props()

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
    } catch (e) {
      if (!disposed) editError = errorMessage(e)
    } finally {
      editing = false
    }
  }

  let form = $state(untrack(() => messageForm(at)))
  const rangeError = $derived(startProblem(form.start, form.since, form.until))

  let selected = $state.raw<Message>()
  // Rows ticked for a bulk action (resend) and rows expanded in place, keyed
  // by partition:offset.
  let picked = $state(new Set<string>())
  let expanded = $state(new Set<string>())
  const mkey = (m: Message) => `${m.partition}:${m.offset}`
  const feed = messageFeed(() => ({ cluster: cluster.name, topic }), {
    reset: () => { selected = undefined; expanded = new Set(); picked = new Set() },
    trim: (kept) => {
      if ([...picked].some((k) => !kept.has(k))) picked = new Set([...picked].filter((k) => kept.has(k)))
      if (selected && !kept.has(mkey(selected))) selected = undefined
    },
  })
  const fs = $derived(feed.state)
  onDestroy(feed.dispose)

  // What to show, apart from where to start; live tail uses only this.
  function view() {
    return {
      partitions: form.partition, filter: form.showFilter ? form.filter : undefined,
      q: form.search.trim() || undefined, raw: form.raw, key_as: form.keyAs || undefined, value_as: form.valueAs || undefined,
    }
  }
  function query() {
    return {
      ...messageStart(form.start, form), ...view(),
      exact: startsAtTime(form.start) && form.exactTime ? true : undefined,
      limit: form.limit,
    }
  }
  function load(more = false) {
    if (more) return feed.load({}, true)
    if (rangeError) { feed.fail(rangeError); return }
    return feed.load(query())
  }

  const startLive = () => feed.startLive(tailQuery(view()))

  const messages = $derived(fs.result?.messages ?? [])
  const hasMore = $derived(!fs.live && !!fs.result && fs.result.stopped_by !== 'live' && (fs.loadingMore || (!fs.loading && Object.keys(fs.result.next ?? {}).length > 0)))

  // Exports the picked messages when any are picked, otherwise every message
  // shown, in list order either way.
  function exportMessages(kind: 'json' | 'csv') {
    if (!fs.result) return
    const list = pickedMessages.length ? pickedMessages : messages
    const body = kind === 'json' ? exportJSON(list) : exportCSV(list)
    download(`${topic}-${fileStamp()}.${kind}`, body, kind === 'json' ? 'application/json' : 'text/csv')
  }

  const stopped: Record<string, string> = {
    end: 'Reached end of range',
    limit: 'Limit reached',
    timeout: 'Timed out',
    cancelled: 'Stopped',
    incomplete: 'Broker stopped sending; load more to continue',
    reading: 'Reading…',
  }

  const pickedMessages = $derived(picked.size ? messages.filter((m) => picked.has(mkey(m))) : [])

  let resending = $state(false)
  let replaying = $state(false)
  let importing = $state(false)
  let resendList = $state<Message[]>([])
  function resend(list: Message[]) {
    resendList = resendOrder(list)
    resending = true
  }

  // Which field was copied last, so only its button shows the tick.
  const clip = copier()

  // j/k (or arrows) walk the list without leaving the detail pane.
  function step(d: number) {
    if (!messages.length) return
    const i = selected ? messages.indexOf(selected) : -1
    const next = messages[Math.min(Math.max(i + d, 0), messages.length - 1)]
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
    // Another dialog (a modal, the command palette) owns the keys while open.
    if (resending || replaying || importing || document.querySelector('dialog[open]')) return
    // Esc stops a running read wherever focus is, even in the search box.
    if (e.key === 'Escape' && fs.loading) {
      e.preventDefault()
      feed.stopReading()
      return
    }
    const t = e.target as HTMLElement
    if (t.closest('input, textarea, select, [contenteditable], dialog') || e.metaKey || e.ctrlKey || e.altKey) return
    // Arrows walk the list only once a message is open; otherwise they
    // scroll the page, and on a control (a button, a link) keep their use.
    const arrow = e.key === 'ArrowDown' || e.key === 'ArrowUp'
    if (arrow && (!selected || (t.closest('button, a') && !t.closest('.list, .detail')))) return
    if (e.key === 'j' || e.key === 'ArrowDown') { e.preventDefault(); step(1) }
    else if (e.key === 'k' || e.key === 'ArrowUp') { e.preventDefault(); step(-1) }
    else if (e.key === 'Escape') selected = undefined
  }

  const cols = messageColumns(untrack(() => topic), {
    readOnly: () => cluster.read_only,
    messages: () => messages,
    detailOpen: () => !!selected,
  })

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

  // When the split is narrower than the list's columns plus the detail, the
  // detail goes below the list (or over it as a drawer) instead of
  // overlapping it.
  let splitWidth = $state(0)
  const stacked = $derived(narrow.matches || splitWidth < cols.listMin + SPLIT_GAP + DETAIL_MIN)
  let detailWidth = $state<number | null>(loadDetailWidth())
  let resizing = $state(false)
  const detailMax = $derived(Math.max(DETAIL_MIN, splitWidth - cols.listMin - SPLIT_GAP))
  const detailW = $derived(detailWidth === null ? null : Math.round(Math.min(Math.max(detailWidth, DETAIL_MIN), detailMax)))

  // The replay dialog starts from the range shown here, taken when it opens
  // so "Last 15 min" means the 15 minutes before opening.
  function rangeSeed() {
    if (rangeError) return { since: '', until: '' }
    const t = messageStart(form.start, form)
    return { since: localInput(t.since), until: localInput(t.until) }
  }
  let replaySeed = $state<ComponentProps<typeof ReplayDialog>['seed']>()
  function openReplay() {
    replaySeed = { partition: form.partition, start: form.start, offset: form.offset, ...rangeSeed(), search: form.search.trim(), filter: form.showFilter ? form.filter : '' }
    replaying = true
  }

  // Last: load() resets state declared throughout this script before its
  // first await.
  load()

  // Opened on a message (from a group's committed offset): select it once
  // the first read brings it in. Any other read or a pick of its own ends it.
  let seekAt = $state(untrack(() => at))
  $effect(() => {
    const result = fs.result
    if (!seekAt || !result) return
    if (selected) {
      seekAt = undefined
      return
    }
    const m = result.messages.find((x) => x.partition === seekAt!.partition && x.offset === seekAt!.offset)
    if (m) {
      selected = m
      seekAt = undefined
      requestAnimationFrame(() => document.getElementById(`m-${m.partition}-${m.offset}`)?.scrollIntoView({ block: 'nearest' }))
    } else if (result.stopped_by !== 'reading') {
      seekAt = undefined
    }
  })
</script>

<svelte:window onkeydown={onkey} />

{#if resending}<ResendDialog {cluster} {topic} messages={resendList} bind:open={resending} />{/if}
{#if replaying && replaySeed}
  <ReplayDialog {cluster} {topic} {partitions} bind:open={replaying} seed={replaySeed} />
{/if}
{#if importing}<ImportDialog {cluster} {topic} {partitions} bind:open={importing} />{/if}
{#if editError}<div class="error" role="alert">{editError}</div>{/if}

<MessageControls {cluster} {partitions} bind:form {rangeError} live={fs.live} paused={fs.paused} loading={fs.loading}
  onload={() => load()} onlive={startLive} onpause={feed.pause} onstoplive={feed.stopLive} onstop={feed.stopReading} />

{#if fs.error}
  <div class="error">{fs.error}</div>
{:else if fs.result}
  {@const result = fs.result}
  {#if fs.live && fs.liveError}<div class="error small-err">{fs.liveError}</div>{/if}
  <div class="meta faint">
    {#if fs.live || result.stopped_by === 'live'}
      {#if fs.live}<span class="badge ok"><span class="sq live"></span>live</span>{:else}<span class="badge"><span class="sq idle"></span>stopped</span>{/if}
      {fmt(messages.length)} messages shown · {fmt(fs.liveStats.matched)} matched · {fmt(fs.liveStats.scanned)} scanned
      {#if fs.paused}<span class="badge warn">paused</span>{/if}
      {#if fs.liveStats.scriptErrors > 0}<span class="badge danger" title={fs.liveStats.firstError}>{fmt(fs.liveStats.scriptErrors)} filter errors</span>{/if}
      {#if messages.length >= liveCap}<span>· keeping last {fmt(liveCap)}</span>{/if}
    {:else}
      {fmt(messages.length)} {fs.searched ? `matching “${fs.searched}”` : 'messages'} · {fmt(result.scanned)} scanned · {result.elapsed_ms} ms · {stopped[result.stopped_by] ?? result.stopped_by}
    {/if}
    {#if result.script_errors > 0}
      <span class="badge danger" title={result.first_error}>{result.script_errors} filter errors</span>
      <span class="mono err">{result.first_error}</span>
    {/if}
    <span class="spacer"></span>
    {#if picked.size}
      <span class="pickbar"><b>{picked.size}</b> selected
        <button class="ghost small" onclick={() => (picked = new Set())}><X size={12} />Clear</button>
        {#if !cluster.read_only}<button class="primary small" onclick={() => resend(pickedMessages)}><Repeat size={13} />Resend…</button>{/if}
      </span>
    {/if}
    {#if !cluster.read_only || messages.length}
      {#snippet tools()}
        {#if !cluster.read_only}
          <button class="ghost small" onclick={openReplay} title="Copy a range of this topic to another topic or back into it"><History size={13} />Replay range…</button>
          <button class="ghost small" onclick={() => (importing = true)} title="Write records from a JSON, CSV or text file"><Upload size={13} />Import…</button>
        {/if}
        {#if messages.length}
          {@const what = pickedMessages.length ? `the ${fmt(pickedMessages.length)} selected messages` : `all ${fmt(messages.length)} messages shown`}
          <button class="ghost small" title="Download {what} as JSON" onclick={() => exportMessages('json')}><Download size={13} />{narrow.matches ? 'Export JSON' : 'JSON'}{#if pickedMessages.length} ({pickedMessages.length}){/if}</button>
          <button class="ghost small" title="Download {what} as CSV" onclick={() => exportMessages('csv')}><Download size={13} />{narrow.matches ? 'Export CSV' : 'CSV'}{#if pickedMessages.length} ({pickedMessages.length}){/if}</button>
        {/if}
      {/snippet}
      {#if narrow.matches}
        <Overflow label="Message tools">{@render tools()}</Overflow>
      {:else}
        <ViewSettings columns={cols.columns} onadd={cols.add} onremove={cols.remove} onmove={cols.move}
          resized={cols.resized} onresetwidths={cols.reset} />
        {@render tools()}
      {/if}
    {/if}
  </div>

  <div class="split" class:open={!!selected} class:stacked class:resizing bind:clientWidth={splitWidth}
    style:--list-min="{cols.listMin}px" style:--detail-min="{DETAIL_MIN}px" style:--detail-w={detailW === null ? undefined : `${detailW}px`}>
    <MessageTable {cluster} {messages} {cols} searched={fs.searched} bind:selected bind:picked bind:expanded
      live={fs.live} loading={fs.loading} loadingMore={fs.loadingMore} {hasMore}
      moreLabel={fs.query.start === 'newest' ? 'Load older' : 'Load more'} {editing} fresh={feed.fresh} {when} {other}
      onedit={edit} onresend={(m) => resend([m])} oncopy={(text, what) => clip.copy(text, what)} onmore={() => load(true)} onstop={feed.stopReading} />

    {#if selected}
      <MessageDetail {cluster} {topic} message={selected} {clip} {stacked} max={detailMax} bind:width={detailWidth} bind:resizing
        {editing} {when} {other} onedit={edit} onresend={(m) => resend([m])} onstep={step} onclose={() => (selected = undefined)} />
    {/if}
  </div>
{:else if fs.loading}
  <div class="empty"><Pending label="Reading…" /></div>
{/if}

<style>
  .meta { font-size: 13px; margin-bottom: 8px; display: flex; gap: 8px; align-items: center; flex-wrap: wrap; min-height: 26px; }
  .err { color: var(--danger); font-size: 12px; }
  .small-err { font-size: 12px; padding: 6px 8px; }
  .split { display: grid; grid-template-columns: minmax(0, 1fr); gap: 12px; align-items: start; }
  .split.open:not(.stacked) { grid-template-columns: minmax(var(--list-min), 1fr) var(--detail-w, minmax(var(--detail-min), 1.35fr)); }
  .split.resizing { cursor: col-resize; user-select: none; }
  .pickbar { display: inline-flex; align-items: center; gap: 6px; padding: 1px 4px 1px 10px; margin-right: 6px; color: var(--text); background: var(--sel); border: 1px solid var(--cta-border); border-radius: var(--radius); }
  .pickbar b { font-weight: 600; }

  @media (max-width: 639.98px) {
    .meta { gap: 6px; }
    .pickbar { order: 1; width: 100%; margin: 0; justify-content: space-between; }
  }
</style>
