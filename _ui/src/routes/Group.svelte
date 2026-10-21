<script lang="ts">
  import { resizable } from '../lib/resizable'
  import Pending from '../lib/Pending.svelte'
  import { api, type Cluster, type GroupDetail, type ResetPlan } from '../lib/api'
  import { fmt, stateBadge, partitionList } from '../lib/format'
  import { href, go } from '../lib/router.svelte'
  import PartitionOffsets from '../lib/PartitionOffsets.svelte'
  import Modal from '../lib/Modal.svelte'
  import { forgetNames } from '../lib/names'
  import { RefreshCw, Trash2, RotateCcw, Plus, Lock, Download, Upload } from '@lucide/svelte'
  import GroupTopicDialog from '../lib/GroupTopicDialog.svelte'
  import RestoreOffsetsDialog, { backupOf, downloadBackup } from '../lib/RestoreOffsetsDialog.svelte'
  import LagEta from '../lib/LagEta.svelte'
  import MoreRows from '../lib/MoreRows.svelte'
  import ResourceACLs from '../lib/ResourceACLs.svelte'

  let { cluster, name }: { cluster: Cluster; name: string } = $props()

  let data = $state<GroupDetail>()
  let error = $state('')
  let loading = $state(false)

  // Refreshes can overlap (Refresh, after a reset); the last one started wins.
  let seq = 0
  async function load() {
    const id = ++seq
    loading = true
    error = ''
    try {
      const g = await api.group(cluster.name, name)
      if (id === seq) data = g
    } catch (e: any) {
      if (id === seq) error = e.message
    } finally {
      if (id === seq) loading = false
    }
  }
  load()

  const topics = $derived([...new Set((data?.partitions ?? []).map((p) => p.topic))])
  // Grouped in one pass; a group can read hundreds of topics.
  const byTopic = $derived.by(() => {
    const m = new Map<string, NonNullable<typeof data>['partitions']>()
    for (const p of data?.partitions ?? []) {
      const rows = m.get(p.topic)
      if (rows) rows.push(p)
      else m.set(p.topic, [p])
    }
    return topics.map((t) => ({ topic: t, rows: m.get(t)! }))
  })
  const topicPage = 20
  let shownTopics = $state(topicPage)

  let addingTopic = $state(false)
  let restoring = $state(false)

  // The backup is taken from fresh offsets, not what the page loaded earlier.
  let backingUp = $state(false)
  async function backup() {
    backingUp = true
    try {
      const g = await api.group(cluster.name, name)
      data = g
      downloadBackup(backupOf(cluster.name, g))
    } catch (e: any) {
      error = e.message
    } finally {
      backingUp = false
    }
  }
  const hasCommits = $derived(!!data?.partitions.some((p) => p.committed >= 0 && !p.no_access))

  // Fresh readings for the catch-up estimate; the page's own data is left alone.
  async function reading(topic?: string) {
    const g = await api.group(cluster.name, name)
    const rows = g.partitions.filter((p) => !p.no_access && (!topic || p.topic === topic))
    return {
      lag: rows.reduce((s, p) => s + Math.max(p.lag, 0), 0),
      committed: rows.reduce((s, p) => s + Math.max(p.committed, 0), 0),
      high: rows.reduce((s, p) => s + p.high, 0),
      parts: rows.map((p) => ({ topic: p.topic, partition: p.partition, committed: p.committed, lag: p.lag })),
    }
  }
  const messageAt = (topic: string, partition: number, offset: number) =>
    href(cluster.name, 'topics', topic, { p: String(partition), o: String(offset) })

  // Offset reset
  let resetting = $state(false)
  let reset = $state({ topic: '', strategy: 'latest', offset: 0, timestamp: '', partitions: '' })
  let plan = $state<ResetPlan[]>()
  let resetError = $state('')
  let busy = $state(false)
  let done = $state(false)

  function openReset(topic: string) {
    reset = { topic, strategy: 'latest', offset: 0, timestamp: '', partitions: '' }
    plan = undefined
    resetError = ''
    done = false
    resetting = true
  }

  const resetParts = $derived(partitionList(reset.partitions))

  function body(dry: boolean) {
    return {
      topic: reset.topic,
      strategy: reset.strategy,
      offset: reset.strategy === 'offset' ? Number(reset.offset) : undefined,
      timestamp: reset.strategy === 'timestamp' && reset.timestamp ? new Date(reset.timestamp).getTime() : undefined,
      partitions: resetParts ?? [],
      dry_run: dry,
    }
  }

  async function preview() {
    busy = true
    resetError = ''
    try {
      plan = (await api.resetOffsets(cluster.name, name, body(true))).partitions
    } catch (e: any) {
      resetError = e.message
    } finally {
      busy = false
    }
  }

  async function apply() {
    busy = true
    resetError = ''
    try {
      plan = (await api.resetOffsets(cluster.name, name, body(false))).partitions
      done = true
      load()
    } catch (e: any) {
      resetError = e.message
    } finally {
      busy = false
    }
  }

  // Delete committed offsets for one topic
  let dropTopic = $state('')
  let dropping = $state(false)
  let dropError = $state('')

  async function dropOffsets() {
    busy = true
    dropError = ''
    try {
      await api.deleteGroupOffsets(cluster.name, name, dropTopic)
      dropping = false
      load()
    } catch (e: any) {
      dropError = e.message
    } finally {
      busy = false
    }
  }

  // Delete group
  let deleting = $state(false)
  let deleteError = $state('')

  async function remove() {
    busy = true
    deleteError = ''
    try {
      await api.deleteGroup(cluster.name, name)
      forgetNames(cluster.name)
      deleting = false
      go(cluster.name, 'groups')
    } catch (e: any) {
      deleteError = e.message
    } finally {
      busy = false
    }
  }
</script>

<div class="head">
  <h1 class="crumbed"><a class="parent" href={href(cluster.name, 'groups')}>Consumer groups</a><span class="sep" aria-hidden="true">/</span><span class="mono">{name}</span></h1>
  {#if data}<span class="badge {stateBadge(data.state)}"><span class="sq {stateBadge(data.state) || 'idle'}"></span>{data.state}</span>{/if}
  <span class="spacer"></span>
  <div class="acts">
  <button onclick={load} disabled={loading}><RefreshCw size={14} class={loading ? 'animate-spin' : ''} />Refresh</button>
  <button disabled={cluster.read_only || !data} title={cluster.read_only ? 'This cluster is read-only' : ''} onclick={() => (addingTopic = true)}><Plus size={14} />Add topic</button>
  <button disabled={!hasCommits || backingUp} title="Download the committed offsets as a JSON file" onclick={backup}><Download size={14} />Back up offsets</button>
  <button disabled={cluster.read_only || !data} title={cluster.read_only ? 'This cluster is read-only' : 'Commit offsets from a backup file'} onclick={() => (restoring = true)}><Upload size={14} />Restore…</button>
  {#if cluster.features.delete_group}
    <button class="danger" disabled={cluster.read_only || !data || data.members.length > 0}
      title={data && data.members.length > 0 ? 'Cannot delete a group with active members' : ''}
      onclick={() => { deleteError = ''; deleting = true }}><Trash2 size={14} />Delete group</button>
  {/if}
  </div>
</div>

{#if error && !data}
  <div class="error">{error}</div>
{:else if !data}
  <div class="empty"><Pending /></div>
{:else}
  {#if error}<div class="error mb-3">{error}</div>{/if}
  <div class="summary">
    <span>Total lag <b class:lagnum={data.lag > 0}>{fmt(data.lag)}</b></span>
    <span><b>{data.members.length}</b> members</span>
    <span>Protocol <b>{data.protocol || '—'}</b></span>
    <span>Coordinator <b>{data.coordinator}</b></span>
    {#if data.lag > 0}<span class="spacer"></span><LagEta read={() => reading()} link={messageAt} />{/if}
  </div>

  {#each byTopic.slice(0, shownTopics) as t (t.topic)}
    {@const denied = t.rows.every((r) => r.no_access)}
    <div class="thead">
      <h2><a class="mono" href={href(cluster.name, 'topics', t.topic)}>{t.topic}</a></h2>
      {#if denied}
        <span class="badge gap-1"><Lock size={11} />no access</span>
      {:else}
        <span class="faint">lag {fmt(t.rows.reduce((s, r) => s + Math.max(r.lag, 0), 0))}</span>
        {#if t.rows.some((r) => r.lag > 0) && byTopic.length > 1}<LagEta read={() => reading(t.topic)} link={messageAt} />{/if}
      {/if}
      <span class="spacer"></span>
      <span class="acts">
      <button disabled={cluster.read_only || denied} title={denied ? 'No permission to read this topic' : ''} onclick={() => openReset(t.topic)}><RotateCcw size={14} />Reset all partitions…</button>
      {#if cluster.features.delete_group}
      <button class="danger" disabled={cluster.read_only || (data?.members.length ?? 0) > 0}
        title={(data?.members.length ?? 0) > 0 ? 'Cannot delete offsets for a group with active members' : 'Deletes committed offsets for this topic'}
        onclick={() => { dropError = ''; dropTopic = t.topic; dropping = true }}>Delete offsets</button>
      {/if}
      </span>
    </div>
    {#if denied}
      <div class="panel noaccess">
        The console's credentials may not read <b class="mono">{t.topic}</b>, so its offsets and lag are unknown.
        This group reads it on {t.rows.length === 1 ? '1 partition' : `${t.rows.length} partitions`}. Total lag above leaves it out.
      </div>
    {:else}
      <PartitionOffsets {cluster} group={name} topic={t.topic} rows={t.rows} activeMembers={data.members.length} showLow onchanged={load} />
    {/if}
  {:else}
    <div class="panel empty">
      This group has no committed offsets.
      {#if !cluster.read_only}<div class="mt-3"><button onclick={() => (addingTopic = true)}><Plus size={14} />Add a topic</button></div>{/if}
    </div>
  {/each}
  {#if byTopic.length > shownTopics}
    <div class="panel mt-3"><MoreRows total={byTopic.length} bind:shown={shownTopics} step={topicPage} /></div>
  {/if}

  <div class="section-title"><h2>Members</h2><span class="faint">{data.members.length}</span></div>
  <div class="panel">
    <table use:resizable={'group.members'}>
      <thead><tr><th>Client ID</th><th>Host</th><th>Member ID</th></tr></thead>
      <tbody>
        {#each data.members as m (m.id)}
          <tr><td class="mono">{m.client_id}</td><td class="mono">{m.host}</td><td class="mono faint small grow" title={m.id}>{m.id}</td></tr>
        {:else}
          <tr><td colspan="3" class="faint">No active members.</td></tr>
        {/each}
      </tbody>
    </table>
  </div>

  {#if cluster.features.security}
    <div class="section-title"><h2>ACL</h2></div>
    <ResourceACLs {cluster} type="GROUP" {name} />
  {/if}
{/if}

<GroupTopicDialog {cluster} group={name} existingTopics={topics} activeMembers={data?.members.length ?? 0} bind:open={addingTopic} ondone={load} />
<RestoreOffsetsDialog {cluster} group={name} activeMembers={data?.members.length ?? 0} bind:open={restoring} ondone={load} />

<Modal title="Reset offsets: {reset.topic}" bind:open={resetting} width={560}>
  {#if data && data.members.length > 0}
    <div class="error">This group has {data.members.length} active members. Stop the consumers before resetting offsets. You can still preview.</div>
  {/if}
  <div class="grid2">
    <label>Target
      <select bind:value={reset.strategy} onchange={() => (plan = undefined)}>
        <option value="earliest">Earliest</option>
        <option value="latest">Latest</option>
        <option value="offset">Specific offset</option>
        <option value="timestamp">Timestamp</option>
      </select>
    </label>
    {#if reset.strategy === 'offset'}
      <label>Offset <input type="number" min="0" bind:value={reset.offset} oninput={() => (plan = undefined)} /></label>
    {:else if reset.strategy === 'timestamp'}
      <label>Time <input type="datetime-local" step="1" bind:value={reset.timestamp} oninput={() => (plan = undefined)} /></label>
    {:else}
      <div></div>
    {/if}
  </div>
  <label>Partitions (empty = all) <input placeholder="0, 1, 2" bind:value={reset.partitions} oninput={() => (plan = undefined)} aria-invalid={!resetParts} /></label>
  {#if !resetParts}<div class="error small">Use partition numbers separated by commas, e.g. 0, 1, 2.</div>{/if}

  {#if plan}
    <div class="panel plan">
      <table>
        <thead><tr><th class="num" title="Partition"><abbr>P</abbr></th><th class="num">Current</th><th class="num">New</th><th class="num">Diff</th></tr></thead>
        <tbody>
          {#each plan as p (p.partition)}
            <tr>
              <td class="num mono">{p.partition}</td>
              <td class="num mono">{fmt(p.from)}</td>
              <td class="num mono">{fmt(p.to)}</td>
              <td class="num mono">{p.from < 0 ? '—' : (p.to - p.from > 0 ? '+' : '') + (p.to - p.from).toLocaleString()}</td>
            </tr>
          {/each}
        </tbody>
      </table>
    </div>
  {/if}
  {#if done}<div class="ok">Offsets committed.</div>{/if}
  {#if resetError}<div class="error">{resetError}</div>{/if}

  {#snippet footer()}
    <button onclick={() => (resetting = false)}>{done ? 'Close' : 'Cancel'}</button>
    {#if !done}
      <button onclick={preview} disabled={busy || !resetParts}>Preview</button>
      <button class="danger solid" disabled={busy || !plan || !resetParts || (data?.members.length ?? 0) > 0} onclick={apply}>Apply</button>
    {/if}
  {/snippet}
</Modal>

<Modal title="Delete offsets: {dropTopic}" bind:open={dropping}>
  <p>All offsets committed by group <b class="mono">{name}</b> for topic <b class="mono">{dropTopic}</b> will be deleted. When the consumer restarts, it will start according to <code>auto.offset.reset</code>.</p>
  {#if dropError}<div class="error">{dropError}</div>{/if}
  {#snippet footer()}
    <button onclick={() => (dropping = false)}>Cancel</button>
    <button class="danger solid" disabled={busy} onclick={dropOffsets}>Delete</button>
  {/snippet}
</Modal>

<Modal title="Delete group" bind:open={deleting}>
  <p>Group <b class="mono">{name}</b> and all its committed offsets will be deleted.</p>
  {#if deleteError}<div class="error">{deleteError}</div>{/if}
  {#snippet footer()}
    <button onclick={() => (deleting = false)}>Cancel</button>
    <button class="danger solid" disabled={busy} onclick={remove}>Delete</button>
  {/snippet}
</Modal>

<style>
  .thead { display: flex; flex-wrap: wrap; align-items: center; gap: 8px 10px; margin: 18px 0 8px; }
  .thead h2 { min-width: 0; overflow-wrap: anywhere; }
  .acts { display: flex; flex-wrap: wrap; gap: 8px; }
  .noaccess { padding: 12px 14px; font-size: 13px; color: var(--text-2); }
  .mtop { margin: 24px 0 8px; }
  .lagnum { background: var(--sel); padding: 0 4px; }
  .plan { max-height: 260px; overflow: auto; }
</style>
