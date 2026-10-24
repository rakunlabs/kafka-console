<script lang="ts" module>
  import type { GroupDetail } from './api'
  import { download, fileStamp } from './download'
  import { errorMessage } from './http'

  // A group's committed offsets as a file. Restoring it commits exactly
  // these offsets again; partitions not in the file are left alone.
  export type OffsetBackup = {
    format: 'kafka-console.offsets'
    version: 1
    cluster: string
    group: string
    taken_at: string
    topics: { topic: string; partitions: { partition: number; offset: number }[] }[]
  }

  export function backupOf(cluster: string, g: GroupDetail): OffsetBackup {
    const by = new Map<string, { partition: number; offset: number }[]>()
    for (const p of g.partitions) {
      if (p.no_access || p.committed < 0) continue
      const rows = by.get(p.topic) ?? []
      rows.push({ partition: p.partition, offset: p.committed })
      by.set(p.topic, rows)
    }
    return {
      format: 'kafka-console.offsets',
      version: 1,
      cluster,
      group: g.name,
      taken_at: new Date().toISOString(),
      topics: [...by].map(([topic, partitions]) => ({ topic, partitions: partitions.sort((a, b) => a.partition - b.partition) })),
    }
  }

  export function downloadBackup(b: OffsetBackup) {
    download(`${b.group}-offsets-${fileStamp(b.taken_at)}.json`, JSON.stringify(b, null, 2), 'application/json')
  }

  function parseBackup(text: string): OffsetBackup {
    let v: any
    try {
      v = JSON.parse(text)
    } catch {
      throw new Error('Not a JSON file.')
    }
    if (v?.format !== 'kafka-console.offsets' || !Array.isArray(v.topics)) throw new Error('Not an offsets backup from Kafka Console.')
    for (const t of v.topics) {
      if (typeof t?.topic !== 'string' || !Array.isArray(t.partitions)) throw new Error('The backup is damaged: a topic entry is malformed.')
      for (const p of t.partitions) {
        if (!Number.isInteger(p?.partition) || !Number.isInteger(p?.offset) || p.partition < 0 || p.offset < 0) {
          throw new Error(`The backup is damaged: ${t.topic} has an invalid partition offset.`)
        }
      }
    }
    return v
  }
</script>

<script lang="ts">
  import { api, type Cluster, type ResetOffsetsBody, type ResetPlan } from './api'
  import { fmt, offsetDelta } from './format'
  import { reviewedOffsets } from './offsetReset'
  import Modal from './Modal.svelte'
  import { TriangleAlert, Eye, Check, Upload } from '@lucide/svelte'

  let {
    cluster,
    group,
    activeMembers = 0,
    open = $bindable(false),
    ondone,
  }: { cluster: Cluster; group: string; activeMembers?: number; open: boolean; ondone?: () => void } = $props()

  type Row = { topic: string; on: boolean; plan?: ResetPlan[]; error?: string; done?: boolean }

  let backup = $state.raw<OffsetBackup>()
  let fileName = $state('')
  let rows = $state<Row[]>([])
  let error = $state('')
  let busy = $state(false)
  let previewed = $state(false)
  let applied = $state(false)

  async function pick(e: Event) {
    const file = (e.currentTarget as HTMLInputElement).files?.[0]
    if (!file) return
    error = ''
    previewed = false
    applied = false
    try {
      backup = parseBackup(await file.text())
      fileName = file.name
      rows = backup.topics.map((t) => ({ topic: t.topic, on: true }))
    } catch (err) {
      backup = undefined
      rows = []
      error = errorMessage(err)
    }
  }

  const offsetsOf = (topic: string) => backup?.topics.find((t) => t.topic === topic)?.partitions ?? []

  // One request per topic; each answers for itself, so one missing topic
  // does not hide what would happen to the others.
  async function run(dry: boolean) {
    busy = true
    error = ''
    for (const r of rows) {
      if (!r.on || (!dry && (!r.plan || r.error || r.done))) continue
      try {
        const request: ResetOffsetsBody = dry ? { topic: r.topic, strategy: 'offsets', offsets: offsetsOf(r.topic), dry_run: true }
          : reviewedOffsets(r.topic, r.plan)
        const res = await api.resetOffsets(cluster.name, group, request)
        r.plan = res.partitions
        r.error = undefined
        if (!dry) r.done = true
      } catch (err) {
        r.error = errorMessage(err)
        if (dry) r.plan = undefined
      }
    }
    busy = false
    if (dry) previewed = true
    else {
      applied = true
      ondone?.()
    }
  }

  const chosen = $derived(rows.filter((r) => r.on))
  const ready = $derived(chosen.filter((r) => r.plan && !r.error))
  const moves = $derived(ready.reduce((s, r) => s + r.plan!.filter((p) => p.from !== p.to).length, 0))
  const clamped = $derived(
    ready.reduce((s, r) => s + r.plan!.filter((p) => offsetsOf(r.topic).find((o) => o.partition === p.partition)?.offset !== p.to).length, 0),
  )
  const failed = $derived(chosen.filter((r) => r.error).length)
</script>

<Modal title="Restore offsets" bind:open width={640} locked={busy}>
  {#if activeMembers > 0}
    <div class="warnbox">
      <TriangleAlert size={15} />
      <span>This group has {activeMembers} active {activeMembers === 1 ? 'member' : 'members'}. Kafka only accepts offset changes for a group with no running consumers, so stop them first. You can still preview.</span>
    </div>
  {/if}

  <label class="file">
    <span class="btn small"><Upload size={13} />{backup ? 'Choose another file…' : 'Choose a backup file…'}</span>
    <input type="file" accept="application/json,.json" onchange={pick} disabled={busy} />
    {#if fileName}<span class="mono small">{fileName}</span>{/if}
  </label>

  {#if backup}
    <p class="lead">
      Taken from <span class="mono">{backup.group}</span>{#if backup.cluster !== cluster.name} on cluster <span class="mono">{backup.cluster}</span>{/if}
      at {new Date(backup.taken_at).toLocaleString()}.
      Restoring into <b class="mono">{group}</b>.
    </p>
    {#if backup.group !== group || backup.cluster !== cluster.name}
      <div class="warnbox">
        <TriangleAlert size={15} />
        <span>This backup was taken from {backup.group !== group ? 'another group' : 'another cluster'}. Offsets only mean the same messages on the same topic of the same cluster.</span>
      </div>
    {/if}

    <div class="panel tbl">
      <table>
        <thead><tr><th class="ck"></th><th>Topic</th><th class="num" title="Partition"><abbr>P</abbr></th><th class="num">Current</th><th class="num">Backup</th><th class="num">Change</th></tr></thead>
        <tbody>
          {#each rows as r (r.topic)}
            {@const offs = offsetsOf(r.topic)}
            {#if r.plan && !r.error}
              {#each r.plan as p, i (p.partition)}
                <tr class:same={p.from === p.to} class:dim={!r.on}>
                  {#if i === 0}
                    <td class="ck" rowspan={r.plan.length}><input type="checkbox" bind:checked={r.on} disabled={busy || r.done} aria-label="Restore {r.topic}" /></td>
                    <td class="mono" rowspan={r.plan.length}>{r.topic}{#if r.done}<Check size={13} class="okicon" />{/if}</td>
                  {/if}
                  <td class="num mono">{p.partition}</td>
                  <td class="num mono">{p.from < 0 ? 'none' : fmt(p.from)}</td>
                  <td class="num mono"><b>{fmt(p.to)}</b>{#if offs.find((o) => o.partition === p.partition)?.offset !== p.to}<span class="faint" title="Outside the partition's current range; clamped"> *</span>{/if}</td>
                  <td class="num mono" class:up={p.to > p.from && p.from >= 0} class:down={p.to < p.from}>{offsetDelta(p)}</td>
                </tr>
              {/each}
            {:else}
              <tr class:dim={!r.on}>
                <td class="ck"><input type="checkbox" bind:checked={r.on} disabled={busy} aria-label="Restore {r.topic}" /></td>
                <td class="mono">{r.topic}</td>
                <td colspan="4" class:err={!!r.error} class:faint={!r.error}>
                  {r.error ?? `${offs.length} ${offs.length === 1 ? 'partition' : 'partitions'}`}
                </td>
              </tr>
            {/if}
          {/each}
        </tbody>
      </table>
    </div>

    {#if previewed}
      <p class="effect">
        {#if applied}
          <span class="okline"><Check size={14} />Restored {ready.filter((r) => r.done).length} of {chosen.length} {chosen.length === 1 ? 'topic' : 'topics'}.</span>
        {:else if !ready.length}
          Nothing can be restored.
        {:else}
          <b>{moves}</b> {moves === 1 ? 'partition moves' : 'partitions move'} across <b>{ready.length}</b> {ready.length === 1 ? 'topic' : 'topics'}.
          {#if clamped}<span class="faint"> * {clamped} {clamped === 1 ? 'offset is' : 'offsets are'} outside the retained range and will be clamped.</span>{/if}
        {/if}
        {#if failed}<span class="err"> {failed} {failed === 1 ? 'topic' : 'topics'} cannot be restored and will be skipped.</span>{/if}
      </p>
    {/if}
  {/if}

  {#if error}<div class="error">{error}</div>{/if}

  {#snippet footer()}
    <button onclick={() => (open = false)} disabled={busy}>{applied ? 'Close' : 'Cancel'}</button>
    {#if !applied}
      <button onclick={() => run(true)} disabled={busy || !chosen.length}><Eye size={14} />Preview</button>
      <button class="danger solid" disabled={busy || !previewed || !moves || activeMembers > 0 || cluster.read_only}
        title={!previewed ? 'Preview first' : activeMembers > 0 ? 'Stop the consumers first' : ''}
        onclick={() => run(false)}>{busy ? 'Working…' : 'Restore'}</button>
    {/if}
  {/snippet}
</Modal>

<style>
  .file { display: flex; align-items: center; gap: 10px; cursor: pointer; }
  .file input { position: absolute; width: 1px; height: 1px; opacity: 0; }
  .file:focus-within .btn { outline: 2px solid var(--focus); outline-offset: 1px; }
  .tbl { max-height: 320px; overflow: auto; }
  .ck { width: 34px; padding-right: 0; vertical-align: top; }
  td[rowspan] { vertical-align: top; }
  tr.same td { color: var(--text-3); }
  tr.dim td { opacity: .5; }
  .up { color: var(--warn); }
  .down { color: var(--accent); }
  .effect { font-size: 13px; color: var(--text-2); }
  .effect b { color: var(--text); }
  .okline { display: inline-flex; align-items: center; gap: 6px; color: var(--ok); }
  :global(.okicon) { margin-left: 6px; color: var(--ok); vertical-align: -2px; }
</style>
