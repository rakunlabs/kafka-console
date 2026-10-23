<script lang="ts">
  import { api, type Cluster, type ResetPlan, type TopicConsumer } from './api'
  import { fmt } from './format'
  import { reviewedOffsets } from './offsetReset'
  import Modal from './Modal.svelte'
  import { TriangleAlert, Eye, Check } from '@lucide/svelte'

  // Moves a group's committed offsets for some partitions of one topic.
  // Always previews first; applying commits exactly what was previewed.
  let {
    cluster,
    group,
    topic,
    partitions,
    activeMembers = 0,
    open = $bindable(false),
    ondone,
  }: {
    cluster: Cluster
    group: string
    topic: string
    // Empty means every partition of the topic.
    partitions: number[]
    activeMembers?: number
    open: boolean
    ondone?: () => void
  } = $props()

  type Strategy = 'offset' | 'shift' | 'earliest' | 'latest' | 'timestamp' | 'copy'
  let strategy = $state<Strategy>('offset')
  let offset = $state(0)
  let shift = $state(-10)
  let timestamp = $state('')
  // Copy: other groups with commits on this topic, and the one picked.
  let sources = $state.raw<TopicConsumer[]>()
  let sourcesLoading = $state(false)
  let sourceError = $state('')
  let source = $state('')
  let plan = $state<ResetPlan[]>()
  let planKey = $state('')
  let error = $state('')
  let busy = $state(false)
  let done = $state(false)

  const single = $derived(partitions.length === 1)
  const target = $derived(single ? `partition ${partitions[0]}` : partitions.length ? `${partitions.length} partitions` : 'all partitions')

  // Fresh form on every open; a single partition starts from its current commit.
  let wasOpen = false
  // Each open is a generation: a seed answering after the dialog closed,
  // reopened for another partition, or after the user typed is dropped.
  let generation = 0
  let touched = false
  $effect(() => {
    if (open && !wasOpen) {
      generation++
      touched = false
      strategy = single ? 'offset' : 'latest'
      shift = -10
      timestamp = ''
      sources = undefined
      sourcesLoading = false
      sourceError = ''
      source = ''
      plan = undefined
      error = ''
      done = false
      if (single) void seed(generation)
    }
    wasOpen = open
  })

  async function seed(gen: number) {
    try {
      const p = await api.resetOffsets(cluster.name, group, { topic, partitions, strategy: 'shift', shift: 0, dry_run: true })
      if (gen !== generation || !open || touched) return
      const row = p.partitions[0]
      if (row) offset = row.from >= 0 ? row.from : row.low
    } catch {}
  }

  const key = $derived(JSON.stringify([cluster.name, group, topic, partitions, strategy, offset, shift, timestamp, source]))
  // A preview describes the inputs it ran with; any edit voids it.
  $effect(() => {
    if (plan && key !== planKey) plan = undefined
  })

  // The groups to copy from are listed the first time copy is picked: only
  // groups with a commit on this topic, so the copy is always same-topic.
  $effect(() => {
    if (strategy !== 'copy' || sources || !open) return
    const gen = generation
    sources = []
    sourcesLoading = true
    api.topicConsumers(cluster.name, topic).then(
      (list) => {
        if (gen !== generation) return
        sources = list.filter((c) => c.group !== group && c.partitions.some((p) => p.committed >= 0))
        source = sources[0]?.group ?? ''
      },
      (e) => {
        if (gen === generation) sourceError = e.message
      },
    ).finally(() => {
      if (gen === generation) sourcesLoading = false
    })
  })

  // The source's commits for the partitions this dialog moves.
  const copied = $derived.by(() => {
    const src = sources?.find((s) => s.group === source)
    if (!src) return []
    const want = new Set(partitions)
    return src.partitions
      .filter((p) => p.committed >= 0 && (!want.size || want.has(p.partition)))
      .map((p) => ({ partition: p.partition, offset: p.committed }))
  })
  // Partitions asked for that the source never committed; they stay put.
  const uncopied = $derived.by(() => {
    const src = sources?.find((s) => s.group === source)
    if (!src) return 0
    const has = new Set(copied.map((c) => c.partition))
    return partitions.length
      ? partitions.filter((p) => !has.has(p)).length
      : src.partitions.filter((p) => !has.has(p.partition)).length
  })

  function body(dry: boolean) {
    if (strategy === 'copy') return { topic, strategy: 'offsets', offsets: copied, dry_run: dry }
    return {
      topic,
      partitions,
      strategy,
      offset: strategy === 'offset' ? Number(offset) : undefined,
      shift: strategy === 'shift' ? Number(shift) : undefined,
      timestamp: strategy === 'timestamp' && timestamp ? new Date(timestamp).getTime() : undefined,
      dry_run: dry,
    }
  }

  async function preview() {
    const gen = generation
    const previewKey = key
    busy = true
    error = ''
    plan = undefined
    try {
      const result = await api.resetOffsets(cluster.name, group, body(true))
      if (gen !== generation || !open || previewKey !== key) return
      planKey = previewKey
      plan = result.partitions
    } catch (e: any) {
      error = e.message
    } finally {
      busy = false
    }
  }

  async function apply() {
    busy = true
    error = ''
    try {
      plan = (await api.resetOffsets(cluster.name, group, reviewedOffsets(topic, plan))).partitions
      done = true
      ondone?.()
    } catch (e: any) {
      error = e.message
    } finally {
      busy = false
    }
  }

  const moved = $derived(plan?.filter((p) => p.from !== p.to) ?? [])
  const skip = $derived(moved.reduce((s, p) => s + Math.max(p.to - Math.max(p.from, p.low), 0), 0))
  const reread = $derived(moved.reduce((s, p) => s + (p.from >= 0 ? Math.max(p.from - p.to, 0) : 0), 0))
  const clamped = $derived(
    strategy === 'offset' && plan?.some((p) => p.to !== Number(offset)) ? plan.find((p) => p.to !== Number(offset)) : undefined,
  )
  const invalid = $derived(
    (strategy === 'offset' && (!Number.isFinite(Number(offset)) || Number(offset) < 0)) ||
      (strategy === 'shift' && (!Number.isInteger(Number(shift)) || Number(shift) === 0)) ||
      (strategy === 'timestamp' && !timestamp) ||
      (strategy === 'copy' && !copied.length),
  )

  const delta = (p: ResetPlan) => (p.from < 0 ? '—' : `${p.to - p.from > 0 ? '+' : ''}${(p.to - p.from).toLocaleString()}`)
</script>

<Modal title="Change offsets" bind:open width={600} locked={busy}>
  <p class="lead">
    <span class="mono">{group}</span> on <span class="mono">{topic}</span> · <b>{target}</b>
  </p>

  {#if activeMembers > 0}
    <div class="warnbox">
      <TriangleAlert size={15} />
      <span>This group has {activeMembers} active {activeMembers === 1 ? 'member' : 'members'}. Kafka only accepts offset changes for a group with no running consumers, so stop them first. You can still preview.</span>
    </div>
  {/if}

  <fieldset class="modes" aria-label="Target" disabled={busy}>
    {#each [
      ['offset', 'Specific offset'],
      ['shift', 'Shift by'],
      ['earliest', 'Earliest'],
      ['latest', 'Latest'],
      ['timestamp', 'Timestamp'],
      ['copy', 'Copy a group'],
    ] as [v, l] (v)}
      <label class="mode" class:on={strategy === v}>
        <input type="radio" name="strategy" value={v} bind:group={strategy} />{l}
      </label>
    {/each}
  </fieldset>

  {#if strategy === 'offset'}
    <label>Offset
      <input type="number" min="0" class="mono" bind:value={offset} disabled={busy} oninput={() => (touched = true)} />
      {#if !single}<span class="hint">Applied to every selected partition, clamped to each one's range.</span>{/if}
    </label>
  {:else if strategy === 'shift'}
    <label>Messages to move
      <input type="number" class="mono" bind:value={shift} disabled={busy} />
      <span class="hint">Negative re-reads that many messages, positive skips ahead. Counted from each partition's committed offset.</span>
    </label>
  {:else if strategy === 'timestamp'}
    <label>Time
      <input type="datetime-local" step="1" bind:value={timestamp} disabled={busy} />
      <span class="hint">Each partition moves to the first message at or after this time.</span>
    </label>
  {:else if strategy === 'copy'}
    {#if sourceError}
      <div class="error">{sourceError}</div>
    {:else if sourcesLoading}
      <p class="hint">Loading groups…</p>
    {:else if !sources?.length}
      <p class="hint">No other group has committed offsets on <span class="mono">{topic}</span>.</p>
    {:else}
      <label>Source group
        <select bind:value={source} disabled={busy}>
          {#each sources as s (s.group)}
            <option value={s.group}>{s.group} · {s.state || 'unknown'} · lag {fmt(s.lag)}</option>
          {/each}
        </select>
        <span class="hint">
          Its committed offsets on <span class="mono">{topic}</span> are copied as they are now.
          {#if uncopied}{uncopied} {uncopied === 1 ? 'partition it never committed stays' : 'partitions it never committed stay'} where {uncopied === 1 ? 'it is' : 'they are'}.{/if}
        </span>
      </label>
    {/if}
  {:else}
    <p class="hint">{strategy === 'earliest' ? 'Move to the oldest retained message; everything still on disk is consumed again.' : 'Move to the end; existing messages are skipped and only new ones are consumed.'}</p>
  {/if}

  {#if plan}
    <div class="panel plan">
      <table>
        <thead><tr><th class="num" title="Partition"><abbr>P</abbr></th><th class="num">Range</th><th class="num">Current</th><th class="num">New</th><th class="num">Change</th></tr></thead>
        <tbody>
          {#each plan as p (p.partition)}
            <tr class:same={p.from === p.to}>
              <td class="num mono">{p.partition}</td>
              <td class="num mono faint">{fmt(p.low)}–{fmt(p.high)}</td>
              <td class="num mono">{p.from < 0 ? 'none' : fmt(p.from)}</td>
              <td class="num mono"><b>{fmt(p.to)}</b></td>
              <td class="num mono" class:up={p.to > p.from && p.from >= 0} class:down={p.to < p.from}>{delta(p)}</td>
            </tr>
          {/each}
        </tbody>
      </table>
    </div>
    <p class="effect">
      {#if !moved.length}
        No change: every partition is already there.
      {:else}
        {#if skip}<span><b>{fmt(skip)}</b> messages will be skipped</span>{/if}
        {#if skip && reread} · {/if}
        {#if reread}<span><b>{fmt(reread)}</b> messages will be consumed again</span>{/if}
        {#if !skip && !reread}{moved.length} partitions will get their first commit{/if}
      {/if}
      {#if clamped}<span class="faint"> · offset {fmt(Number(offset))} is outside partition {clamped.partition}'s range, so it was clamped to {fmt(clamped.to)}</span>{/if}
    </p>
  {/if}

  {#if done}<div class="ok"><Check size={14} />Offsets committed.</div>{/if}
  {#if error}<div class="error">{error}</div>{/if}

  {#snippet footer()}
    <button onclick={() => (open = false)} disabled={busy}>{done ? 'Close' : 'Cancel'}</button>
    {#if !done}
      <button onclick={preview} disabled={busy || invalid}><Eye size={14} />Preview</button>
      <button class="danger solid" disabled={busy || !plan || !moved.length || activeMembers > 0 || cluster.read_only}
        title={!plan ? 'Preview first' : activeMembers > 0 ? 'Stop the consumers first' : ''}
        onclick={apply}>{busy ? 'Applying…' : `Apply to ${target}`}</button>
    {/if}
  {/snippet}
</Modal>

<style>
  .lead { font-size: 13px; color: var(--text-2); }
  .lead b { color: var(--text); font-weight: 600; }
  .warnbox {
    display: flex;
    gap: 8px;
    align-items: flex-start;
    padding: 9px 11px;
    font-size: 13px;
    color: var(--warn);
    background: var(--warn-soft);
    border-radius: var(--radius);
  }
  .warnbox :global(svg) { flex: none; margin-top: 1px; }
  label input[type='number'], label input[type='datetime-local'] { max-width: 240px; }
  .hint { font-size: 12px; font-weight: 400; color: var(--text-3); }
  .plan { max-height: 260px; overflow: auto; }
  tr.same td { color: var(--text-3); }
  .up { color: var(--warn); }
  .down { color: var(--accent); }
  .effect { font-size: 13px; color: var(--text-2); }
  .effect b { color: var(--text); }
  .ok { display: flex; align-items: center; gap: 6px; }
</style>
