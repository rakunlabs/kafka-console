<script lang="ts">
  import { lagReading } from '../lib/lag'
  import { errorMessage } from '../lib/http'
  import { resizable } from '../lib/resizable'
  import Pending from '../lib/Pending.svelte'
  import { api, isAbort, type Cluster } from '../lib/api'
  import { resource } from '../lib/resource.svelte'
  import { fmt, stateBadge } from '../lib/format'
  import { href, go } from '../lib/router.svelte'
  import PartitionOffsets from '../lib/PartitionOffsets.svelte'
  import OffsetDialog from '../lib/OffsetDialog.svelte'
  import ConfirmDialog from '../lib/ConfirmDialog.svelte'
  import { forgetNames } from '../lib/names'
  import { RefreshCw, Trash2, RotateCcw, Plus, Lock, Download, Upload } from '@lucide/svelte'
  import GroupTopicDialog from '../lib/GroupTopicDialog.svelte'
  import RestoreOffsetsDialog, { backupOf, downloadBackup } from '../lib/RestoreOffsetsDialog.svelte'
  import LagEta from '../lib/LagEta.svelte'
  import MoreRows from '../lib/MoreRows.svelte'
  import ResourceACLs from '../lib/ResourceACLs.svelte'

  let { cluster, name }: { cluster: Cluster; name: string } = $props()

  // A reload (Refresh, after a reset) replaces one still running.
  const detail = resource(() => api.group(cluster.name, name))
  // A failed backup is shown like a failed refresh, until the next load.
  let backupError = $state('')
  function load() {
    backupError = ''
    return detail.load()
  }
  load()
  const data = $derived(detail.data)
  const error = $derived(backupError || detail.error)

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
    backupError = ''
    const keep = detail.since()
    try {
      const g = await api.group(cluster.name, name)
      keep(g)
      downloadBackup(backupOf(cluster.name, g))
    } catch (e) {
      if (!isAbort(e)) backupError = errorMessage(e)
    } finally {
      backingUp = false
    }
  }
  const hasCommits = $derived(!!data?.partitions.some((p) => p.committed >= 0 && !p.no_access))

  // Fresh readings for the catch-up estimate; the page's own data is left alone.
  async function reading(topic?: string) {
    const g = await api.group(cluster.name, name)
    return lagReading(g.partitions, (p) => !topic || p.topic === topic)
  }
  const messageAt = (topic: string, partition: number, offset: number) =>
    href(cluster.name, 'topics', topic, { p: String(partition), o: String(offset) })

  // Offset reset for every partition of one topic (or the ones typed).
  let resetTopic = $state('')
  let resetting = $state(false)

  function openReset(topic: string) {
    resetTopic = topic
    resetting = true
  }

  // Delete committed offsets for one topic
  let dropTopic = $state('')
  let dropping = $state(false)

  async function dropOffsets() {
    await api.deleteGroupOffsets(cluster.name, name, dropTopic)
    load()
  }

  // Delete group
  let deleting = $state(false)

  async function remove() {
    await api.deleteGroup(cluster.name, name)
    forgetNames(cluster.name)
    go(cluster.name, 'groups')
  }
</script>

<div class="head">
  <h1 class="crumbed"><a class="parent" href={href(cluster.name, 'groups')}>Consumer groups</a><span class="sep" aria-hidden="true">/</span><span class="mono">{name}</span></h1>
  {#if data}<span class="badge {stateBadge(data.state)}"><span class="sq {stateBadge(data.state) || 'idle'}"></span>{data.state}</span>{/if}
  <span class="spacer"></span>
  <div class="acts">
  <button onclick={load} disabled={detail.loading}><RefreshCw size={14} class={detail.loading ? 'animate-spin' : ''} />Refresh</button>
  {#if !cluster.read_only}<button disabled={!data} onclick={() => (addingTopic = true)}><Plus size={14} />Add topic</button>{/if}
  <button disabled={!hasCommits || backingUp} title="Download the committed offsets as a JSON file" onclick={backup}><Download size={14} />Back up offsets</button>
  {#if !cluster.read_only}<button disabled={!data} title="Commit offsets from a backup file" onclick={() => (restoring = true)}><Upload size={14} />Restore…</button>{/if}
  {#if cluster.features.delete_group && !cluster.read_only}
    <button class="danger" disabled={!data || data.members.length > 0}
      title={data && data.members.length > 0 ? 'Cannot delete a group with active members' : ''}
      onclick={() => (deleting = true)}><Trash2 size={14} />Delete group</button>
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
      {#if !cluster.read_only}<button disabled={denied} title={denied ? 'No permission to read this topic' : ''} onclick={() => openReset(t.topic)}><RotateCcw size={14} />Reset all partitions…</button>{/if}
      {#if cluster.features.delete_group && !cluster.read_only}
      <button class="danger" disabled={(data?.members.length ?? 0) > 0}
        title={(data?.members.length ?? 0) > 0 ? 'Cannot delete offsets for a group with active members' : 'Deletes committed offsets for this topic'}
        onclick={() => { dropTopic = t.topic; dropping = true }}>Delete offsets</button>
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

{#if addingTopic}
  <GroupTopicDialog {cluster} group={name} existingTopics={topics} activeMembers={data?.members.length ?? 0} bind:open={addingTopic} ondone={load} />
{/if}
{#if restoring}
  <RestoreOffsetsDialog {cluster} group={name} activeMembers={data?.members.length ?? 0} bind:open={restoring} ondone={load} />
{/if}

{#if resetting}
  <OffsetDialog {cluster} group={name} topic={resetTopic} partitions={[]} pickPartitions
    activeMembers={data?.members.length ?? 0} bind:open={resetting} ondone={load} />
{/if}

{#if dropping}
  <ConfirmDialog title="Delete offsets: {dropTopic}" bind:open={dropping} onconfirm={dropOffsets}>
    <p>All offsets committed by group <b class="mono">{name}</b> for topic <b class="mono">{dropTopic}</b> will be deleted. When the consumer restarts, it will start according to <code>auto.offset.reset</code>.</p>
  </ConfirmDialog>
{/if}

{#if deleting}
  <ConfirmDialog title="Delete group" bind:open={deleting} onconfirm={remove}>
    <p>Group <b class="mono">{name}</b> and all its committed offsets will be deleted.</p>
  </ConfirmDialog>
{/if}

<style>
  .thead { display: flex; flex-wrap: wrap; align-items: center; gap: 8px 10px; margin: 18px 0 8px; }
  .thead h2 { min-width: 0; overflow-wrap: anywhere; }
  .acts { display: flex; flex-wrap: wrap; gap: 8px; }
  .noaccess { padding: 12px 14px; font-size: 13px; color: var(--text-2); }
  .lagnum { background: var(--sel); padding: 0 4px; }
</style>
