<script lang="ts">
  import { names, forgetNames } from './names'
  import { api, type Cluster, type TopicName, type TopicConsumer, type ResetPlan } from './api'
  import { fmt } from './format'
  import { go } from './router.svelte'
  import Modal from './Modal.svelte'
  import TopicPicker from './TopicPicker.svelte'
  import { ArrowLeft, Check, Copy, PencilLine, TriangleAlert } from '@lucide/svelte'

  // Creates a group, or (with `group`) gives an existing group a topic it does
  // not read yet. Either way it commits a first offset for every chosen
  // partition: set by hand or copied from a group that already reads the topic.
  let {
    cluster,
    open = $bindable(false),
    group: fixedGroup,
    existingTopics = [],
    activeMembers = 0,
    ondone,
  }: {
    cluster: Cluster
    open: boolean
    group?: string
    existingTopics?: string[]
    activeMembers?: number
    ondone?: () => void
  } = $props()

  type Row = { partition: number; low: number; high: number; offset: number; on: boolean }
  type Mode = 'manual' | 'copy'
  type Start = 'earliest' | 'latest' | 'timestamp'

  let step = $state<1 | 2>(1)
  let name = $state('')
  let topic = $state('')
  let topics = $state.raw<TopicName[]>([])
  let groupNames = $state<string[]>([])
  let mode = $state<Mode>('manual')
  let start = $state<Start>('latest')
  let timestamp = $state('')
  let rows = $state<Row[]>([])
  let sources = $state<TopicConsumer[]>([])
  let source = $state('')
  let error = $state('')
  let busy = $state(false)
  let loadingTopic = $state(false)
  let result = $state<ResetPlan[]>()

  const adding = $derived(!!fixedGroup)
  const groupName = $derived(adding ? fixedGroup! : name.trim())
  const nameTaken = $derived(!adding && groupNames.includes(name.trim()))

  let wasOpen = false
  $effect(() => {
    if (open && !wasOpen) {
      step = 1
      name = ''
      topic = ''
      mode = 'manual'
      start = 'latest'
      timestamp = ''
      rows = []
      sources = []
      source = ''
      error = ''
      result = undefined
      names(cluster.name).then((n) => (topics = n.topics)).catch((e) => (error = e.message))
      if (!adding) names(cluster.name, true).then((n) => (groupNames = n.groups)).catch(() => {})
    }
    wasOpen = open
  })

  async function next() {
    error = ''
    loadingTopic = true
    try {
      const [detail, consumers] = await Promise.all([api.topic(cluster.name, topic), api.topicConsumers(cluster.name, topic)])
      rows = detail.partitions.map((p) => ({ partition: p.id, low: p.low, high: p.high, offset: p.high, on: true }))
      sources = consumers.filter((c) => c.group !== groupName && c.partitions.some((p) => p.committed >= 0))
      source = sources[0]?.group ?? ''
      mode = 'manual'
      applyStart('latest')
      step = 2
    } catch (e: any) {
      error = e.message
    } finally {
      loadingTopic = false
    }
  }

  async function applyStart(s: Start) {
    start = s
    if (s === 'earliest') rows = rows.map((r) => ({ ...r, offset: r.low }))
    else if (s === 'latest') rows = rows.map((r) => ({ ...r, offset: r.high }))
  }

  // Time lookup goes through a dry-run reset on a throwaway name, which uses
  // the broker's own offset-for-time index without committing anything.
  async function applyTime() {
    if (!timestamp) return
    busy = true
    error = ''
    try {
      const plan = await api.resetOffsets(cluster.name, `__kafka_console_probe__`, {
        topic,
        strategy: 'timestamp',
        timestamp: new Date(timestamp).getTime(),
        dry_run: true,
      })
      const at = new Map(plan.partitions.map((p) => [p.partition, p.to]))
      rows = rows.map((r) => ({ ...r, offset: at.get(r.partition) ?? r.high }))
    } catch (e: any) {
      error = e.message
    } finally {
      busy = false
    }
  }

  function copyFrom(g: string) {
    source = g
    const src = sources.find((s) => s.group === g)
    if (!src) return
    const by = new Map(src.partitions.map((p) => [p.partition, p.committed]))
    rows = rows.map((r) => {
      const c = by.get(r.partition)
      return c === undefined || c < 0 ? { ...r, on: false } : { ...r, on: true, offset: Math.min(Math.max(c, r.low), r.high) }
    })
  }

  function setMode(m: Mode) {
    mode = m
    if (m === 'copy' && source) copyFrom(source)
    if (m === 'manual') {
      rows = rows.map((r) => ({ ...r, on: true }))
      applyStart(start === 'timestamp' ? 'latest' : start)
    }
  }

  const chosen = $derived(rows.filter((r) => r.on))
  const bad = $derived(chosen.filter((r) => !Number.isInteger(Number(r.offset)) || r.offset < r.low || r.offset > r.high))
  const backlog = $derived(chosen.reduce((s, r) => s + Math.max(r.high - r.offset, 0), 0))
  const allOn = $derived(rows.length > 0 && rows.every((r) => r.on))

  const nameError = $derived(
    adding ? '' : !name.trim() ? '' : nameTaken ? 'A group with this name already exists.' : /\s/.test(name.trim()) ? 'Group names cannot contain spaces.' : '',
  )
  const canNext = $derived(!!topic && (adding || (!!name.trim() && !nameError)))
  const canSave = $derived(!busy && chosen.length > 0 && !bad.length && !(adding && activeMembers > 0) && !cluster.read_only)

  async function save() {
    busy = true
    error = ''
    const offsets = chosen.map((r) => ({ partition: r.partition, offset: Number(r.offset) }))
    try {
      result = adding
        ? await api.addGroupTopic(cluster.name, groupName, { topic, offsets })
        : await api.createGroup(cluster.name, { group: groupName, topic, offsets })
      forgetNames(cluster.name)
      ondone?.()
    } catch (e: any) {
      error = e.message
    } finally {
      busy = false
    }
  }

  function finish() {
    open = false
    if (!adding) go(cluster.name, 'groups', groupName)
  }
</script>

<Modal title={adding ? `Add topic to ${fixedGroup}` : 'New consumer group'} bind:open width={680}>
  {#if result}
    <div class="ok"><Check size={15} />
      {adding ? `${topic} added to` : 'Created'} <b class="mono">{groupName}</b> with {result.length} {result.length === 1 ? 'partition' : 'partitions'}.
    </div>
    <p class="faint small">A consumer that joins <span class="mono">{groupName}</span> will start from these offsets.</p>
  {:else if step === 1}
    {#if !adding}
      <label>Group name
        <!-- svelte-ignore a11y_autofocus -->
        <input class="mono" bind:value={name} placeholder="analytics-replay" aria-invalid={!!nameError} />
        {#if nameError}<span class="field-err">{nameError}</span>{/if}
      </label>
    {:else if activeMembers > 0}
      <div class="warnbox"><TriangleAlert size={15} /><span>This group has {activeMembers} active {activeMembers === 1 ? 'member' : 'members'}. Kafka refuses offset commits while consumers are running, so stop them first.</span></div>
    {/if}
    <div class="field">
      <span class="lbl">Topic</span>
      <TopicPicker {topics} bind:value={topic} exclude={adding ? existingTopics : []} />
    </div>
  {:else}
    <div class="crumb">
      <button class="ghost small" onclick={() => (step = 1)}><ArrowLeft size={13} />Back</button>
      <span class="mono">{groupName}</span><span class="faint">on</span><span class="mono">{topic}</span>
    </div>

    <fieldset class="modes" aria-label="Starting offsets">
      <label class="mode" class:on={mode === 'manual'}><input type="radio" name="mode" value="manual" checked={mode === 'manual'} onchange={() => setMode('manual')} /><PencilLine size={14} />Set offsets</label>
      <label class="mode" class:on={mode === 'copy'} class:off={!sources.length}
        title={sources.length ? '' : 'No other group has committed offsets for this topic'}>
        <input type="radio" name="mode" value="copy" disabled={!sources.length} checked={mode === 'copy'} onchange={() => setMode('copy')} /><Copy size={14} />Copy from a group
      </label>
    </fieldset>

    {#if mode === 'manual'}
      <div class="starts">
        <span class="lbl">Start every partition at</span>
        {#each [['earliest', 'Earliest'], ['latest', 'Latest'], ['timestamp', 'Time…']] as [v, l] (v)}
          <button type="button" class="small" class:pressed={start === v} aria-pressed={start === v} onclick={() => applyStart(v as Start)}>{l}</button>
        {/each}
        {#if start === 'timestamp'}
          <input type="datetime-local" step="1" bind:value={timestamp} />
          <button type="button" class="small" disabled={!timestamp || busy} onclick={applyTime}>Apply</button>
        {/if}
        <span class="hint">then edit any row.</span>
      </div>
    {:else}
      <label>Source group
        <select value={source} onchange={(e) => copyFrom(e.currentTarget.value)}>
          {#each sources as s (s.group)}
            <option value={s.group}>{s.group} · {s.state || 'unknown'} · lag {fmt(s.lag)}</option>
          {/each}
        </select>
        <span class="hint">Its committed offsets are copied as a snapshot; partitions it never committed are left out. You can still edit rows.</span>
      </label>
    {/if}

    <div class="panel tbl">
      <table>
        <thead>
          <tr>
            <th class="ck"><input type="checkbox" checked={allOn} indeterminate={!allOn && chosen.length > 0}
              onchange={() => (rows = rows.map((r) => ({ ...r, on: !allOn })))} aria-label="Include all partitions" /></th>
            <th class="num">P</th><th class="num">Range</th><th class="num">Start offset</th><th class="num">Backlog</th>
          </tr>
        </thead>
        <tbody>
          {#each rows as r (r.partition)}
            {@const wrong = r.on && (!Number.isInteger(Number(r.offset)) || r.offset < r.low || r.offset > r.high)}
            <tr class:dim={!r.on}>
              <td class="ck"><input type="checkbox" bind:checked={r.on} aria-label="Include partition {r.partition}" /></td>
              <td class="num mono">{r.partition}</td>
              <td class="num mono faint">{fmt(r.low)}–{fmt(r.high)}</td>
              <td class="num">
                <input type="number" class="mono off" class:wrong min={r.low} max={r.high} disabled={!r.on}
                  bind:value={r.offset} aria-label="Start offset for partition {r.partition}" aria-invalid={wrong} />
              </td>
              <td class="num mono">{r.on ? fmt(Math.max(r.high - r.offset, 0)) : '—'}</td>
            </tr>
          {/each}
        </tbody>
      </table>
    </div>
    <p class="effect">
      {#if bad.length}
        <span class="field-err">Partition {bad[0].partition}: offset must be between {fmt(bad[0].low)} and {fmt(bad[0].high)}.</span>
      {:else if !chosen.length}
        Pick at least one partition.
      {:else}
        <b>{chosen.length}</b> {chosen.length === 1 ? 'partition' : 'partitions'} · <b>{fmt(backlog)}</b> messages waiting to be consumed
      {/if}
    </p>
  {/if}

  {#if error}<div class="error">{error}</div>{/if}

  {#snippet footer()}
    {#if result}
      <button class="primary" onclick={finish}>{adding ? 'Done' : 'Open group'}</button>
    {:else if step === 1}
      <button onclick={() => (open = false)}>Cancel</button>
      <button class="primary" disabled={!canNext || loadingTopic} onclick={next}>{loadingTopic ? 'Loading…' : 'Next'}</button>
    {:else}
      <button onclick={() => (open = false)}>Cancel</button>
      <button class="primary" disabled={!canSave} onclick={save}
        title={adding && activeMembers > 0 ? 'Stop the consumers first' : ''}>
        {busy ? 'Saving…' : adding ? 'Add topic' : 'Create group'}
      </button>
    {/if}
  {/snippet}
</Modal>

<style>
  .lbl { font-size: 12px; font-weight: 500; color: var(--text-2); }
  .field { display: grid; gap: 4px; }
  .hint { font-size: 12px; font-weight: 400; color: var(--text-3); }
  .field-err { font-size: 12px; font-weight: 400; color: var(--danger); }
  .crumb { display: flex; align-items: center; gap: 8px; font-size: 13px; }
  .ok { display: flex; align-items: center; gap: 6px; font-size: 14px; color: var(--ok); }
  .ok b { color: var(--text); }
  .warnbox { display: flex; gap: 8px; align-items: flex-start; padding: 9px 11px; font-size: 13px; color: var(--warn); background: var(--warn-soft); border-radius: var(--radius); }
  .warnbox :global(svg) { flex: none; margin-top: 1px; }


  .starts { display: flex; flex-wrap: wrap; align-items: center; gap: 6px; }
  .starts .lbl { margin-right: 4px; }
  button.pressed { background: var(--surface-2); border-color: var(--text-3); color: var(--text); }

  .tbl { max-height: 280px; overflow: auto; }
  .ck { width: 34px; padding-right: 0; }
  .off { width: 120px; text-align: right; padding: 3px 7px; }
  .off.wrong { border-color: var(--danger); color: var(--danger); }
  tr.dim td { color: var(--text-3); }
  td.num:has(.off) { padding-top: 4px; padding-bottom: 4px; }
  .effect { font-size: 13px; color: var(--text-2); }
  .effect b { color: var(--text); }
</style>
