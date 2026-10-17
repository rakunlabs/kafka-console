<script lang="ts">
  import { onDestroy } from 'svelte'
  import { api, tail, type Cluster, type ConsumeResult, type Message } from '../lib/api'
  import { fmt, time } from '../lib/format'
  import ValueView from '../lib/ValueView.svelte'
  import ResendDialog from '../lib/ResendDialog.svelte'
  import { Play, Pause, Square, Radio, Download, Copy, Check, ChevronUp, ChevronDown, Plus, Minus, ChevronsUpDown, ChevronsDownUp, Repeat, X, Code, Binary, Search } from '@lucide/svelte'

  let { cluster, topic, partitions }: { cluster: Cluster; topic: string; partitions: number[] } = $props()

  let start = $state('newest')
  let partition = $state('')
  let offset = $state(0)
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

  let result = $state<ConsumeResult>()
  let loading = $state(false)
  let error = $state('')
  let selected = $state<Message>()

  async function load() {
    searched = search.trim()
    loading = true
    error = ''
    try {
      result = await api.messages(cluster.name, topic, {
        start,
        partitions: partition,
        offset: start === 'offset' ? offset : undefined,
        timestamp: start === 'timestamp' && timestamp ? new Date(timestamp).getTime() : undefined,
        limit,
        max_scan: maxScan,
        filter: showFilter ? filter : undefined,
        q: search.trim() || undefined,
        raw,
      })
      selected = undefined
      expanded = new Set()
      picked = new Set()
    } catch (e: any) {
      error = e.message
      result = undefined
    } finally {
      loading = false
    }
  }
  load()

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

  function startLive() {
    stopLive()
    searched = search.trim()
    error = ''
    live = true
    paused = false
    liveStats = { scanned: 0, matched: 0 }
    result = { messages: [], scanned: 0, script_errors: 0, stopped_by: 'live', elapsed_ms: 0 }
    selected = undefined
    // "newest" in live mode means only messages that arrive from now on.
    stop = tail(cluster.name, topic, {
      start,
      partitions: partition,
      offset: start === 'offset' ? offset : undefined,
      timestamp: start === 'timestamp' && timestamp ? new Date(timestamp).getTime() : undefined,
      filter: showFilter ? filter : undefined,
      q: search.trim() || undefined,
      raw,
    }, {
      event: (e) => {
        liveStats = { scanned: e.scanned, matched: e.matched }
        if (e.error) error = e.error
        if (e.message && result && !paused) {
          fresh.add(`${e.message.partition}:${e.message.offset}`)
          result.messages = [e.message, ...result.messages].slice(0, liveCap)
        }
      },
      error: (msg) => {
        error = msg
        live = false
      },
      end: () => (live = false),
    })
  }

  function stopLive() {
    stop?.()
    stop = undefined
    live = false
  }

  onDestroy(stopLive)

  function exportMessages(kind: 'json' | 'csv') {
    if (!result) return
    let body: string
    let type: string
    if (kind === 'json') {
      body = JSON.stringify(result.messages.map((m) => ({ ...m, value: parsed(m) })), null, 2)
      type = 'application/json'
    } else {
      const esc = (v: unknown) => {
        const s = v === null || v === undefined ? '' : String(v)
        return /[",\n]/.test(s) ? `"${s.replaceAll('"', '""')}"` : s
      }
      const rows = [['partition', 'offset', 'timestamp', 'key', 'value', 'headers']]
      for (const m of result.messages) {
        rows.push([String(m.partition), String(m.offset), m.timestamp, m.key ?? '', m.value ?? '', JSON.stringify(m.headers)])
      }
      body = rows.map((r) => r.map(esc).join(',')).join('\n')
      type = 'text/csv'
    }
    const a = document.createElement('a')
    a.href = URL.createObjectURL(new Blob([body], { type }))
    a.download = `${topic}-${new Date().toISOString().replaceAll(':', '-')}.${kind}`
    a.click()
    URL.revokeObjectURL(a.href)
  }

  function parsed(m: Message) {
    if (m.value === null) return null
    if (m.value_format && m.value_format !== 'text' && m.value_format !== 'binary') {
      try {
        return JSON.parse(m.value)
      } catch {}
    }
    return m.value
  }


  const stopped: Record<string, string> = {
    end: 'Reached end of range',
    limit: 'Limit reached',
    max_scan: 'Scan limit reached',
    timeout: 'Timed out',
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
  const pickedMessages = $derived((result?.messages ?? []).filter((m) => picked.has(`${m.partition}:${m.offset}`)))
  const allPicked = $derived(!!result?.messages.length && result.messages.every((m) => picked.has(`${m.partition}:${m.offset}`)))
  const pickAll = () => (picked = allPicked ? new Set() : new Set(result?.messages.map((m) => `${m.partition}:${m.offset}`)))

  let resending = $state(false)
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
  const allOpen = $derived(!!result?.messages.length && result.messages.every((m) => expanded.has(mkey(m))))
  function toggleAll() {
    expanded = allOpen ? new Set() : new Set(result?.messages.map(mkey))
  }

  let copied = $state(false)
  function copy(text: string) {
    navigator.clipboard?.writeText(text)
    copied = true
    setTimeout(() => (copied = false), 1200)
  }

  // j/k (or arrows) walk the list without leaving the detail pane.
  function step(d: number) {
    if (!result?.messages.length) return
    const i = selected ? result.messages.indexOf(selected) : -1
    const next = result.messages[Math.min(Math.max(i + d, 0), result.messages.length - 1)]
    if (next) {
      selected = next
      document.getElementById(`m-${next.partition}-${next.offset}`)?.scrollIntoView({ block: 'nearest' })
    }
  }
  function onkey(e: KeyboardEvent) {
    const t = e.target as HTMLElement
    if (resending || t.closest('input, textarea, select, [contenteditable], dialog') || e.metaKey || e.ctrlKey || e.altKey) return
    if (e.key === 'j' || e.key === 'ArrowDown') { e.preventDefault(); step(1) }
    else if (e.key === 'k' || e.key === 'ArrowUp') { e.preventDefault(); step(-1) }
    else if (e.key === 'Escape') selected = undefined
  }
</script>

<svelte:window onkeydown={onkey} />

<ResendDialog {cluster} {topic} messages={resendList} bind:open={resending} />

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
      </select>
    </label>
    {#if start === 'offset'}
      <label class="f"><span class="fl">Offset</span><input type="number" min="0" class="w-num" bind:value={offset} /></label>
    {:else if start === 'timestamp'}
      <label class="f"><span class="fl">Time</span><input type="datetime-local" step="1" bind:value={timestamp} /></label>
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
    {:else}
      <button type="button" onclick={startLive} disabled={loading}
        title={start === 'newest' ? 'Watch new messages as they arrive' : 'Read from the selected start and keep following'}><Radio size={14} />Live tail</button>
      <button class="primary" type="submit" disabled={loading}><Play size={14} />{loading ? 'Reading…' : 'Fetch'}</button>
    {/if}
  </div>

  {#if showFilter}
    <div class="filter">
      {#await import('../lib/FilterEditor.svelte')}
        <div class="cm-loading"></div>
      {:then { default: FilterEditor }}
        <FilterEditor bind:value={filter} onrun={load} placeholder={'return value.status === "FAILED"'} />
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
    {#if result.messages.length}
      <button class="ghost small" onclick={() => exportMessages('json')}><Download size={13} />JSON</button>
      <button class="ghost small" onclick={() => exportMessages('csv')}><Download size={13} />CSV</button>
    {/if}
  </div>

  <div class="split" class:open={!!selected}>
    <div class="panel list">
      <table class="manual">
        <colgroup>
          {#if !cluster.read_only}<col style:width="34px" />{/if}<col style:width="40px" /><col style:width="44px" /><col style:width="84px" /><col style:width="200px" />
          <col style:width={selected ? '120px' : '170px'} /><col />
        </colgroup>
        <thead>
          <tr>
            {#if !cluster.read_only}
              <th class="pk"><input type="checkbox" checked={allPicked} indeterminate={picked.size > 0 && !allPicked}
                disabled={!result.messages.length} onchange={pickAll} aria-label="Select all messages" /></th>
            {/if}
            <th class="tog">
              {#if result.messages.length}
                <button type="button" class="ghost xbtn" onclick={toggleAll} title={allOpen ? 'Collapse all' : 'Expand all'} aria-label={allOpen ? 'Collapse all' : 'Expand all'}>
                  {#if allOpen}<ChevronsDownUp size={13} />{:else}<ChevronsUpDown size={13} />{/if}
                </button>
              {/if}
            </th>
            <th class="num">P</th><th class="num">Offset</th><th>Time</th><th>Key</th><th>Value</th>
          </tr>
        </thead>
        <tbody>
          {#each result.messages as m, i (mkey(m))}
            {@const open = expanded.has(mkey(m))}
            {@const k = hit(m.key ?? '—')}
            {@const v = hit(preview(m))}
            <tr id="m-{m.partition}-{m.offset}" class:alt={i % 2 === 1} class:sel={selected === m} class:bad={!!m.decode_error} class:opened={open} class:picked={picked.has(mkey(m))} onclick={() => (selected = m)} use:arrived={mkey(m)}>
              {#if !cluster.read_only}
                <td class="pk"><input type="checkbox" checked={picked.has(mkey(m))} onclick={(e) => e.stopPropagation()}
                  onchange={() => pick(m)} aria-label="Select message {m.partition}:{m.offset}" /></td>
              {/if}
              <td class="tog">
                <button type="button" class="xbtn" class:on={open} aria-expanded={open}
                  aria-label={open ? 'Collapse message' : 'Expand message'}
                  onclick={(e) => { e.stopPropagation(); toggle(m) }}>
                  {#if open}<Minus size={12} strokeWidth={2.2} />{:else}<Plus size={12} strokeWidth={2.2} />{/if}
                </button>
              </td>
              <td class="num mono">{m.partition}</td>
              <td class="num mono">{m.offset}</td>
              <td class="mono nowrap time">{time(m.timestamp)}</td>
              <td class="mono key">{k[0]}{#if k[1]}<mark>{k[1]}</mark>{/if}{k[2]}</td>
              <td class="mono val">{v[0]}{#if v[1]}<mark>{v[1]}</mark>{/if}{v[2]}</td>
            </tr>
            {#if open}
              <tr class="xrow" class:alt={i % 2 === 1} class:sel={selected === m}>
                {#if !cluster.read_only}<td></td>{/if}
                <td></td>
                <td colspan="5">
                  <div class="inl">
                    <div class="inl-meta">
                      <span class="badge" class:accent={!!m.value_schema_id}>{m.value_format ?? 'null'}</span>
                      {#if m.value_schema_id}<span class="faint">schema #{m.value_schema_id}</span>{/if}
                      <span class="faint">{fmt(m.value_bytes)} bytes</span>
                      {#if m.truncated}<span class="badge warn">truncated</span>{/if}
                      {#if m.key !== null}<span class="faint">key</span><code>{m.key}</code>{/if}
                      {#each m.headers as h, i (i)}<span class="hdr"><span class="faint">{h.key}</span>{h.value}</span>{/each}
                      <span class="spacer"></span>
                      {#if !cluster.read_only}<button type="button" class="ghost small" onclick={() => resend([m])}><Repeat size={12} />Resend…</button>{/if}
                      <button type="button" class="ghost small" onclick={() => copy(m.value ?? '')}><Copy size={12} />Copy</button>
                    </div>
                    {#if m.decode_error}<div class="error small-err">Schema decoding failed: {m.decode_error}</div>{/if}
                    <ValueView text={m.value} format={m.value_format} maxHeight={420} />
                  </div>
                </td>
              </tr>
            {/if}
          {:else}
            <tr><td colspan={cluster.read_only ? 6 : 7} class="empty">{live ? 'Waiting for new messages…' : 'No messages found.'}</td></tr>
          {/each}
        </tbody>
      </table>
    </div>

    {#if selected}
      <div class="panel detail">
        <div class="dhead">
          <div class="grid">
            <b>Partition {selected.partition} · Offset {fmt(selected.offset)}</b>
            <span class="faint mono">{time(selected.timestamp)}</span>
          </div>
          <span class="spacer"></span>
          <button class="ghost small" title="Previous (k)" aria-label="Previous message" onclick={() => step(-1)}><ChevronUp size={15} /></button>
          <button class="ghost small" title="Next (j)" aria-label="Next message" onclick={() => step(1)}><ChevronDown size={15} /></button>
          {#if !cluster.read_only}<button class="ghost small" title="Resend this message" onclick={() => selected && resend([selected])}><Repeat size={13} />Resend…</button>{/if}
          <button class="ghost small" title="Close (Esc)" onclick={() => (selected = undefined)}>Close</button>
        </div>

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
            <button class="ghost small" onclick={() => copy(selected?.value ?? '')}>{#if copied}<Check size={13} />Copied{:else}<Copy size={13} />Copy{/if}</button>
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
    {/if}
  </div>
{:else if loading}
  <div class="empty">Reading…</div>
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
    font-size: 12.5px;
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
  .hint code { font-size: 11.5px; }
  .examples { display: flex; flex-wrap: wrap; gap: 4px; }
  .ex { font-size: 12px; padding: 1px 7px; border: 1px solid var(--border-strong); background: var(--surface); color: var(--text-2); }
  .meta { font-size: 12.5px; margin-bottom: 8px; display: flex; gap: 8px; align-items: center; flex-wrap: wrap; min-height: 26px; }
  .err { color: var(--danger); font-size: 12px; }
  .split { display: grid; grid-template-columns: minmax(0, 1fr); gap: 12px; align-items: start; }
  .split.open { grid-template-columns: minmax(0, 1fr) minmax(0, 1fr); }
  .list { max-height: calc(100vh - 300px); overflow: auto; }
  .list table { table-layout: fixed; }
  .list tr { cursor: pointer; }
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
  .detail { position: sticky; top: 12px; max-height: calc(100vh - 300px); overflow: auto; }
  .dhead { display: flex; align-items: center; gap: 4px; padding: 10px 12px; border-bottom: 1px solid var(--border-strong); background: var(--surface-2); position: sticky; top: 0; z-index: 1; }
  section { padding: 12px; border-bottom: 1px solid var(--border); }
  section:last-child { border-bottom: 0; }
  .label { display: flex; align-items: center; gap: 6px; font-size: 12px; font-weight: 500; color: var(--text-2); margin-bottom: 6px; }
  .headers td { padding: 4px 8px; font-size: 12px; }

  .pk { padding-right: 0; }
  td.pk { padding-top: 9px; }
  .pk input { display: block; margin: 0 0 0 2px; }
  .list tr.picked td { background: var(--sel); }
  .pickbar { display: inline-flex; align-items: center; gap: 6px; padding: 1px 4px 1px 10px; margin-right: 6px; color: var(--text); background: var(--sel); border: 1px solid var(--cta-border); border-radius: var(--radius); }
  .pickbar b { font-weight: 600; }
  .tog { width: 1%; padding-right: 0; }
  td.tog { padding-top: 7px; }
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
  tbody tr.xrow:hover td { background: transparent; }
  tbody tr.xrow.alt:hover td { background: var(--stripe); }
  .inl { display: grid; gap: 6px; padding-bottom: 4px; }
  .inl-meta { display: flex; flex-wrap: wrap; align-items: center; gap: 6px 8px; font-size: 12px; }
  .inl-meta code { font-size: 12px; }
  .hdr { display: inline-flex; gap: 4px; font: 11.5px var(--mono); padding: 0 6px; border: 1px solid var(--border); border-radius: 2px; }

  @media (max-width: 1100px) {
    .split.open { grid-template-columns: 1fr; }
    .detail { position: static; }
  }
</style>
