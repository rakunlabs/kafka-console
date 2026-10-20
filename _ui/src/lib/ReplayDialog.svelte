<script lang="ts">
  import { names } from './names'
  import { replay, type Cluster, type TopicName, type ReplayBody, type ReplayEvent, type ReplayRange } from './api'
  import { fmt } from './format'
  import { href } from './router.svelte'
  import Modal from './Modal.svelte'
  import TopicPicker from './TopicPicker.svelte'
  import { Send, Repeat, Check, TriangleAlert, Square, ListChecks } from '@lucide/svelte'

  // Copies a range of this topic, byte for byte, to this or another topic.
  // The current search, filter and partition from the Messages view are
  // offered as a starting point.
  let {
    cluster,
    topic,
    partitions,
    seed,
    open = $bindable(false),
  }: {
    cluster: Cluster
    topic: string
    partitions: number[]
    seed: { partition: string; start: string; offset: number; timestamp: string; search: string; filter: string }
    open: boolean
  } = $props()

  type Bound = { type: string; offset: number; timestamp: string }

  let from = $state<Bound>({ type: 'oldest', offset: 0, timestamp: '' })
  let to = $state<Bound>({ type: 'newest', offset: 0, timestamp: '' })
  let partition = $state('')
  let search = $state('')
  let filter = $state('')
  let useFilter = $state(false)
  let elsewhere = $state(true)
  let target = $state('')
  let keepPartition = $state(false)
  let keepTimestamp = $state(false)
  let mark = $state(true)
  let limit = $state<number | ''>('')
  let confirm = $state('')
  let topics = $state.raw<TopicName[]>([])

  let phase = $state<'edit' | 'counting' | 'counted' | 'running' | 'done'>('edit')
  let plan = $state<ReplayRange[]>([])
  let ev = $state<ReplayEvent>()
  let counted = $state<ReplayEvent>()
  let error = $state('')
  let abort: AbortController | undefined

  const dest = $derived(elsewhere ? target : topic)
  const destInfo = $derived(topics.find((t) => t.name === dest))
  const missing = $derived.by(() => {
    if (!keepPartition || !destInfo) return []
    const used = partition === '' ? partitions : [Number(partition)]
    return used.filter((p) => p >= destInfo.partitions)
  })
  const busy = $derived(phase === 'counting' || phase === 'running')

  let wasOpen = false
  $effect(() => {
    if (open && !wasOpen) {
      from = seed.start === 'offset' ? { type: 'offset', offset: seed.offset, timestamp: '' }
        : seed.start === 'timestamp' && seed.timestamp ? { type: 'timestamp', offset: 0, timestamp: seed.timestamp }
        : { type: 'oldest', offset: 0, timestamp: '' }
      to = { type: 'newest', offset: 0, timestamp: '' }
      partition = seed.partition
      search = seed.search
      filter = seed.filter
      useFilter = !!seed.filter.trim()
      elsewhere = true
      target = ''
      keepPartition = false
      keepTimestamp = false
      mark = true
      limit = ''
      confirm = ''
      phase = 'edit'
      plan = []
      ev = counted = undefined
      error = ''
      names(cluster.name).then((n) => (topics = n.topics)).catch(() => {})
    }
    if (!open && wasOpen) abort?.abort()
    wasOpen = open
  })

  function bound(b: Bound) {
    if (b.type === 'offset') return { type: 'offset', offset: Number(b.offset) }
    if (b.type === 'timestamp') return { type: 'timestamp', timestamp: b.timestamp ? new Date(b.timestamp).getTime() : 0 }
    return { type: b.type }
  }

  function body(dry: boolean): ReplayBody {
    return {
      partitions: partition === '' ? undefined : [Number(partition)],
      from: bound(from),
      to: bound(to),
      search: search.trim() || undefined,
      filter: useFilter && filter.trim() ? filter : undefined,
      target: dest,
      keep_partition: keepPartition,
      keep_timestamp: keepTimestamp,
      mark_source: mark,
      limit: limit === '' ? undefined : Number(limit),
      dry_run: dry,
    }
  }

  async function run(dry: boolean) {
    abort?.abort()
    const ctl = new AbortController()
    abort = ctl
    error = ''
    ev = undefined
    phase = dry ? 'counting' : 'running'
    try {
      await replay(cluster.name, topic, body(dry), (e) => {
        if (e.type === 'plan') plan = e.plan ?? []
        ev = e
      }, ctl.signal)
      if (dry) {
        counted = ev
        phase = 'counted'
      } else phase = 'done'
    } catch (e: any) {
      const last = ev as ReplayEvent | undefined
      if (ctl.signal.aborted) {
        phase = dry ? 'edit' : 'done'
        if (!dry && last) ev = { ...last, type: 'done', stopped_by: 'cancelled' }
      } else {
        error = e.message
        phase = dry ? 'edit' : 'done'
      }
    }
  }

  function edit() {
    phase = 'edit'
    counted = undefined
  }

  const invalidRange = $derived(
    (from.type === 'timestamp' && !from.timestamp) || (to.type === 'timestamp' && !to.timestamp) ||
      (from.type === 'offset' && to.type === 'offset' && Number(to.offset) < Number(from.offset)) ||
      (from.type === 'timestamp' && to.type === 'timestamp' && to.timestamp < from.timestamp),
  )
  const canCount = $derived(!busy && !invalidRange && !!dest && missing.length === 0 && !cluster.read_only)
  const canSend = $derived(phase === 'counted' && !!counted?.matched && (!cluster.confirm_writes || confirm === dest))

  const pct = $derived.by(() => {
    if (!ev) return 0
    const goal = phase === 'running' || phase === 'done' ? counted?.matched || ev.total : ev.total
    const n = phase === 'running' || phase === 'done' ? ev.written + ev.failed : ev.scanned
    return goal ? Math.min(100, Math.round((n / goal) * 100)) : 0
  })

  const stopped: Record<string, string> = {
    end: 'Finished',
    limit: 'Limit reached',
    error: 'Stopped on an error',
    timeout: 'Timed out after 30 minutes',
    cancelled: 'Cancelled',
  }
  const span = (r: ReplayRange) => (r.end > r.start ? `${fmt(r.start)} – ${fmt(r.end - 1)}` : 'nothing')
</script>

<Modal title="Replay a range" bind:open width={680} locked={phase === 'running'}>
  {#if phase === 'edit' || phase === 'counting'}
    <p class="lead">
      Copies every record in the range, oldest first per partition, with key, value and headers byte for byte.
      The range is fixed when the replay starts, so records written meanwhile are not included.
    </p>

    <div class="grid2">
      <label>From
        <select bind:value={from.type} disabled={busy}>
          <option value="oldest">Oldest</option>
          <option value="offset">Offset</option>
          <option value="timestamp">Time</option>
        </select>
      </label>
      <label>To (inclusive)
        <select bind:value={to.type} disabled={busy}>
          <option value="newest">Newest now</option>
          <option value="offset">Offset</option>
          <option value="timestamp">Time</option>
        </select>
      </label>
      {#if from.type === 'offset'}<label>From offset <input type="number" min="0" bind:value={from.offset} disabled={busy} /></label>
      {:else if from.type === 'timestamp'}<label>From time <input type="datetime-local" step="1" bind:value={from.timestamp} disabled={busy} /></label>
      {:else if to.type !== 'newest'}<div></div>{/if}
      {#if to.type === 'offset'}<label>To offset <input type="number" min="0" bind:value={to.offset} disabled={busy} /></label>
      {:else if to.type === 'timestamp'}<label>To time <input type="datetime-local" step="1" bind:value={to.timestamp} disabled={busy} /></label>{/if}
    </div>
    {#if from.type === 'offset' || to.type === 'offset'}<p class="hint0">An offset applies to each selected partition.</p>{/if}

    <div class="grid2">
      <label>Partition
        <select bind:value={partition} disabled={busy}>
          <option value="">All</option>
          {#each partitions as p (p)}<option value={String(p)}>{p}</option>{/each}
        </select>
      </label>
      <label>Stop after (matching records)
        <input type="number" min="1" placeholder="No limit" bind:value={limit} disabled={busy} />
      </label>
    </div>

    <label>Only records containing <input bind:value={search} placeholder="Any record" disabled={busy} /></label>
    <label class="check"><input type="checkbox" bind:checked={useFilter} disabled={busy} /> Only records the JS filter keeps</label>
    {#if useFilter}
      {#await import('./FilterEditor.svelte')}
        <div class="cm-loading"></div>
      {:then { default: FilterEditor }}
        <FilterEditor bind:value={filter} placeholder={'return value.status === "FAILED"'} />
      {/await}
    {/if}

    <fieldset class="modes" aria-label="Destination">
      <label class="mode" class:on={elsewhere}><input type="radio" name="replay-dest" checked={elsewhere} disabled={busy} onchange={() => (elsewhere = true)} /><Send size={14} />Another topic</label>
      <label class="mode" class:on={!elsewhere}><input type="radio" name="replay-dest" checked={!elsewhere} disabled={busy} onchange={() => (elsewhere = false)} /><Repeat size={14} />Same topic</label>
    </fieldset>
    {#if elsewhere}
      <TopicPicker {topics} bind:value={target} id="replay-target" />
    {:else}
      <div class="warnbox"><TriangleAlert size={15} /><span>Consumers of <span class="mono">{topic}</span> will receive these records again, as new records at the end.</span></div>
    {/if}

    <label class="check"><input type="checkbox" bind:checked={keepPartition} disabled={busy} /> Keep partition numbers
      <span class="hint">Off: the key decides the partition, as for any producer.</span></label>
    {#if missing.length}<p class="err">{dest} has {destInfo?.partitions} partitions; partition {missing.join(', ')} does not exist there.</p>{/if}
    <label class="check"><input type="checkbox" bind:checked={keepTimestamp} disabled={busy} /> Keep original timestamps
      <span class="hint">Off: records get the time they are written. Topics with LogAppendTime always use the broker time.</span></label>
    <label class="check"><input type="checkbox" bind:checked={mark} disabled={busy} /> Add an <code>x-resent-from</code> header
      <span class="hint">e.g. <code>{topic}/0/42</code>, so a replay can be told apart from the original.</span></label>

    {#if phase === 'counting' && ev}
      <div class="progress" role="status">
        <div class="bar"><span style:width="{pct}%"></span></div>
        <span class="faint">Counting… {fmt(ev.scanned)} of up to {fmt(ev.total)} read · {fmt(ev.matched)} match</span>
      </div>
    {/if}
  {:else}
    {@const shown = phase === 'counted' ? counted : ev}
    <div class="panel tbl">
      <table>
        <thead><tr><th class="num" title="Partition"><abbr>P</abbr></th><th>Offsets</th><th class="num">Offsets in range</th></tr></thead>
        <tbody>
          {#each plan as r (r.partition)}
            <tr><td class="num mono">{r.partition}</td><td class="mono">{span(r)}</td><td class="num mono">{fmt(r.end - r.start)}</td></tr>
          {/each}
        </tbody>
      </table>
    </div>

    {#if phase === 'counted' && shown}
      {#if shown.matched}
        <p class="sum"><ListChecks size={15} /><span><b>{fmt(shown.matched)}</b> {shown.matched === 1 ? 'record' : 'records'} will be written to
          <b class="mono">{dest}</b>{shown.scanned ? ` (${fmt(shown.scanned)} read)` : ''}.</span></p>
      {:else}
        <p class="sum">No record in the range matches. Nothing would be written.</p>
      {/if}
      {#if shown.stopped_by === 'limit'}<p class="faint small">Stopped counting at the limit of {fmt(Number(limit))}.</p>{/if}
      {#if shown.script_errors}<p class="err">The filter failed on {fmt(shown.script_errors)} records, which are skipped: {shown.first_error}</p>{/if}
      {#if cluster.confirm_writes && shown.matched}
        <label>Type <b class="mono">{dest}</b> to confirm
          <input class="mono" bind:value={confirm} autocomplete="off" />
        </label>
      {/if}
    {:else if shown}
      <div class="progress" role="status">
        <div class="bar" class:fail={shown.failed > 0}><span style:width="{pct}%"></span></div>
        <span class="faint">{fmt(shown.written)} written{shown.failed ? ` · ${fmt(shown.failed)} failed` : ''} · {fmt(shown.scanned)} read · {(shown.elapsed_ms / 1000).toFixed(1)} s</span>
      </div>
      {#if phase === 'done'}
        {#if shown.stopped_by === 'end' || shown.stopped_by === 'limit'}
          <div class="donemsg"><Check size={15} /><span>{stopped[shown.stopped_by]}: <b>{fmt(shown.written)}</b> {shown.written === 1 ? 'record' : 'records'} written to <a class="mono" href={href(cluster.name, 'topics', dest)}>{dest}</a>.</span></div>
        {:else}
          <p class="err">{stopped[shown.stopped_by ?? ''] ?? shown.stopped_by}. {fmt(shown.written)} {shown.written === 1 ? 'record was' : 'records were'} written before it stopped; records already handed to the producer may still arrive.</p>
        {/if}
        {#if shown.first_error}<p class="err mono">{shown.first_error}</p>{/if}
      {/if}
    {/if}
  {/if}

  {#if error}<div class="error" role="alert">{error}</div>{/if}

  {#snippet footer()}
    {#if phase === 'edit' || phase === 'counting'}
      <button onclick={() => (open = false)}>Cancel</button>
      {#if phase === 'counting'}
        <button onclick={() => abort?.abort()}><Square size={13} />Stop counting</button>
      {/if}
      <button class="primary" disabled={!canCount} onclick={() => run(true)}><ListChecks size={14} />{phase === 'counting' ? 'Counting…' : 'Review'}</button>
    {:else if phase === 'counted'}
      <button onclick={edit}>Back</button>
      <button class="primary" disabled={!canSend} onclick={() => run(false)}><Send size={14} />Write {fmt(counted?.matched ?? 0)} to {dest}</button>
    {:else if phase === 'running'}
      <button class="danger" onclick={() => abort?.abort()}><Square size={13} />Stop</button>
    {:else}
      <button class="primary" onclick={() => (open = false)}>Done</button>
    {/if}
  {/snippet}
</Modal>

<style>
  .lead { font-size: 13px; color: var(--text-2); }
  .hint { display: block; margin-left: 22px; font-size: 12px; color: var(--text-3); }
  .hint0 { font-size: 12px; color: var(--text-3); margin-top: -6px; }
  .check { align-items: flex-start; flex-wrap: wrap; }
  .err { color: var(--danger); font-size: 13px; overflow-wrap: anywhere; }
  .tbl { max-height: 220px; overflow: auto; }
  .sum { display: flex; align-items: center; gap: 6px; font-size: 14px; }
  .sum b { font-weight: 600; }
  .donemsg { display: flex; align-items: center; gap: 6px; font-size: 14px; color: var(--ok); }
  .donemsg b, .donemsg a { color: var(--text); }
  .warnbox { display: flex; gap: 8px; align-items: flex-start; padding: 9px 11px; font-size: 13px; color: var(--warn); background: var(--warn-soft); border-radius: var(--radius); }
  .warnbox :global(svg) { flex: none; margin-top: 1px; }
  .progress { display: grid; gap: 6px; font-size: 13px; }
  .bar { height: 6px; background: var(--surface-2); border: 1px solid var(--border); overflow: hidden; }
  .bar span { display: block; height: 100%; background: var(--cta-text); transition: width .3s ease-out; }
  .bar.fail span { background: var(--danger); }
  .cm-loading { height: 80px; border: 1px solid var(--border-strong); background: var(--surface-2); }
</style>
