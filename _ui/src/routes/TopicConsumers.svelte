<script lang="ts">
  import { untrack } from 'svelte'
  import { lagReading } from '../lib/lag'
  import Pending from '../lib/Pending.svelte'
  import { api, isAbort, type Cluster, type TopicConsumer } from '../lib/api'
  import { errorMessage } from '../lib/http'
  import { resource } from '../lib/resource.svelte'
  import { fmt, stateBadge } from '../lib/format'
  import { href, router, replaceQuery } from '../lib/router.svelte'
  import PartitionOffsets from '../lib/PartitionOffsets.svelte'
  import LagEta from '../lib/LagEta.svelte'
  import UnreadAge from '../lib/UnreadAge.svelte'
  import Overflow from '../lib/Overflow.svelte'
  import OffsetDialog from '../lib/OffsetDialog.svelte'
  import GroupTopicDialog from '../lib/GroupTopicDialog.svelte'
  import ConfirmDialog from '../lib/ConfirmDialog.svelte'
  import { backupOf, downloadBackup } from '../lib/RestoreOffsetsDialog.svelte'
  import { forgetNames } from '../lib/names'
  import { toggled } from '../lib/sets'
  import {
    filterConsumers, groupIssues, nextConsumerSort, pushTrend, sortConsumers, sparkPoints,
    type ConsumerSort, type ConsumerSortKey, type TrendPoint,
  } from '../lib/consumers'
  import {
    RefreshCw, ArrowRight, Users, ChevronRight, ChevronsUpDown, ChevronsDownUp, ArrowUp, ArrowDown,
    TriangleAlert, Plus, RotateCcw, Download, Copy, Trash2, Flame, ExternalLink,
  } from '@lucide/svelte'

  let { cluster, topic }: { cluster: Cluster; topic: string } = $props()

  const consumers = resource(() => api.topicConsumers(cluster.name, topic))
  const load = consumers.load
  load()
  const groups = $derived(consumers.data)

  // ─── Sort and filter ───
  let sort = $state<ConsumerSort>({ key: 'lag', desc: true })
  let search = $state('')
  let onlyLagging = $state(false)
  const shown = $derived(sortConsumers(filterConsumers(groups ?? [], search, onlyLagging), sort))
  const setSort = (k: ConsumerSortKey) => (sort = nextConsumerSort(sort, k))

  // ─── Auto-refresh: off until picked, and paused while the tab is hidden ───
  const intervals = [0, 5, 15, 30] as const
  let every = $state<(typeof intervals)[number]>(0)
  $effect(() => {
    if (!every) return
    const t = setInterval(() => {
      if (!document.hidden && !consumers.loading) load()
    }, every * 1000)
    return () => clearInterval(t)
  })

  // ─── Lag trend across readings ───
  let trend = $state.raw(new Map<string, TrendPoint[]>())
  $effect(() => {
    const g = consumers.data
    if (g) trend = pushTrend(untrack(() => trend), g, Date.now())
  })
  function trendOf(group: string) {
    const pts = trend.get(group) ?? []
    if (pts.length < 2) return undefined
    const first = pts[0].lag
    const last = pts[pts.length - 1].lag
    return { points: sparkPoints(pts.map((p) => p.lag), 56, 18), first, last, n: pts.length, dir: last > first ? 'up' : last < first ? 'down' : 'flat' }
  }

  // ─── Open rows, kept in the URL so a reload or a shared link keeps them ───
  let open = $state(new Set(untrack(() => router.route.query.getAll('open'))))
  $effect(() => replaceQuery({ open: [...open] }))
  const toggle = (g: string) => (open = toggled(open, g))
  const allOpen = $derived(shown.length > 0 && shown.every((g) => open.has(g.group)))
  const toggleAll = () => (open = allOpen ? new Set() : new Set(shown.map((g) => g.group)))
  // The row toggles on a click anywhere except its own links, buttons and menu.
  function rowClick(e: MouseEvent, g: string) {
    if ((e.target as Element).closest('a, button, summary, details')) return
    toggle(g)
  }

  // Clients that own at least one partition of this topic, per group.
  function clients(g: TopicConsumer) {
    const m = new Map<string, { id: string; client: string; host: string; partitions: number[]; lag: number }>()
    for (const p of g.partitions) {
      if (!p.member_id) continue
      const c = m.get(p.member_id) ?? { id: p.member_id, client: p.client_id ?? '', host: p.host ?? '', partitions: [], lag: 0 }
      c.partitions.push(p.partition)
      if (p.lag > 0) c.lag += p.lag
      m.set(p.member_id, c)
    }
    return [...m.values()]
  }
  // One picked client per group: its partitions are marked in the table.
  let picked = $state<Record<string, string>>({})
  const pick = (group: string, member: string) => (picked = { ...picked, [group]: picked[group] === member ? '' : member })
  function highlight(g: TopicConsumer) {
    const id = picked[g.group]
    return id ? new Set(g.partitions.filter((p) => p.member_id === id).map((p) => p.partition)) : undefined
  }

  // One group's numbers: the group endpoint computes lag for that group
  // alone, where the topic's consumer list would redo every group's.
  async function reading(group: string) {
    const g = await api.group(cluster.name, group)
    return lagReading(g.partitions, (p) => p.topic === topic)
  }
  const messageAt = (t: string, partition: number, offset: number) =>
    href(cluster.name, 'topics', t, { p: String(partition), o: String(offset) })

  const total = $derived((groups ?? []).reduce((s, g) => s + g.lag, 0))
  const writable = $derived(!cluster.read_only)
  const canDelete = $derived(writable && cluster.features.delete_group)

  // ─── Row actions ───
  let resetting = $state<TopicConsumer>()
  let dropping = $state<TopicConsumer>()
  let creating = $state(false)
  let copyFrom = $state('')
  function newGroup(from = '') {
    copyFrom = from
    creating = true
  }

  let actionError = $state('')
  let backingUp = $state('')
  async function backup(group: string) {
    backingUp = group
    actionError = ''
    try {
      const g = await api.group(cluster.name, group)
      downloadBackup(backupOf(cluster.name, { ...g, partitions: g.partitions.filter((p) => p.topic === topic) }))
    } catch (e) {
      if (!isAbort(e)) actionError = errorMessage(e)
    } finally {
      backingUp = ''
    }
  }
  async function dropOffsets() {
    await api.deleteGroupOffsets(cluster.name, dropping!.group, topic)
    forgetNames(cluster.name)
    load()
  }
</script>

<div class="bar">
  {#if groups}
    <span class="muted">
      <b>{groups.length}</b> {groups.length === 1 ? 'group' : 'groups'} reading this topic
      {#if groups.length}· total lag <b class:lagged={total > 0}>{fmt(total)}</b>{/if}
    </span>
  {/if}
  <span class="spacer"></span>
  {#if shown.length > 1}
    <button class="ghost" onclick={toggleAll}>
      {#if allOpen}<ChevronsDownUp size={14} />Collapse all{:else}<ChevronsUpDown size={14} />Expand all{/if}
    </button>
  {/if}
  {#if writable}<button onclick={() => newGroup()}><Plus size={14} />New group</button>{/if}
  <span class="refresh">
    <button onclick={load} disabled={consumers.loading}><RefreshCw size={14} class={consumers.loading ? 'animate-spin' : ''} />Refresh</button>
    <select bind:value={every} aria-label="Auto-refresh" title="Refresh on its own every few seconds">
      {#each intervals as s (s)}<option value={s}>{s ? `Every ${s}s` : 'Auto: off'}</option>{/each}
    </select>
  </span>
</div>

{#if groups && groups.length > 0}
  <div class="filters">
    <input type="search" placeholder="Search groups or clients…" bind:value={search} />
    <label class="check"><input type="checkbox" bind:checked={onlyLagging} /> Only groups with lag</label>
  </div>
{/if}

{#if actionError}<div class="error mb-3">{actionError}</div>{/if}

{#if consumers.error && !groups}
  <div class="error">{consumers.error}</div>
{:else if !groups}
  <div class="empty"><Pending /></div>
{:else if !groups.length}
  <div class="panel empty">
    <Users size={20} class="mx-auto mb-2 text-ink-3" />
    No consumer group has committed offsets for this topic yet.
    {#if writable}<div class="mt-3"><button onclick={() => newGroup()}><Plus size={14} />Create a group for this topic</button></div>{/if}
  </div>
{:else}
  {#if consumers.error}<div class="error mb-3">{consumers.error}</div>{/if}
  <div class="panel list">
    <div class="lhead">
      <span></span>
      {#snippet head(k: ConsumerSortKey, label: string, cls = '')}
        <span class={cls}>
          <button class="ghost th" onclick={() => setSort(k)}>{label}{#if sort.key === k}{#if sort.desc}<ArrowDown size={12} />{:else}<ArrowUp size={12} />{/if}{/if}</button>
        </span>
      {/snippet}
      {@render head('name', 'Group')}
      {@render head('state', 'State')}
      {@render head('members', 'Members', 'num')}
      <span>Partitions caught up</span>
      <span title="Lag across refreshes; turn on auto-refresh to fill it">Trend</span>
      {@render head('lag', 'Lag', 'num')}
      <span></span>
    </div>
    {#each shown as g, gi (g.group)}
      {@const cs = clients(g)}
      {@const isOpen = open.has(g.group)}
      {@const caught = g.partitions.filter((p) => p.lag <= 0 && (p.committed >= 0 || p.high <= p.low)).length}
      {@const issues = groupIssues(g)}
      {@const tr = trendOf(g.group)}
      <div class="item" class:alt={gi % 2 === 1} class:open={isOpen}>
        <!-- The chevron is the keyboard toggle; the rest of the row is a mouse shortcut for it. -->
        <!-- svelte-ignore a11y_click_events_have_key_events, a11y_no_static_element_interactions -->
        <div class="row" onclick={(e) => rowClick(e, g.group)}>
          <button
            type="button"
            class="chevbtn"
            aria-expanded={isOpen}
            aria-controls="cg-{g.group}"
            aria-label="{isOpen ? 'Collapse' : 'Expand'} {g.group}"
            onclick={() => toggle(g.group)}
          ><ChevronRight size={15} class="chev" /></button>
          <span class="cell">
            <a class="mono gname" href={href(cluster.name, 'groups', g.group)} title="Open group {g.group}">{g.group}</a>
            {#if issues.stalled}
              <span class="badge danger" title="Messages are waiting but no consumer is running to read them">
                <TriangleAlert size={11} />no consumers
              </span>
            {/if}
            {#if issues.unassigned}
              <span class="badge warn" title="{issues.unassigned} {issues.unassigned === 1 ? 'partition has' : 'partitions have'} no consumer assigned, though the group has members">
                {issues.unassigned} unassigned
              </span>
            {/if}
          </span>
          <span><span class="badge {stateBadge(g.state)}"><span class="sq {stateBadge(g.state) || 'idle'}"></span>{g.state || '—'}</span></span>
          <span class="num muted">{g.members}</span>
          <span class="caught">
            <span class="track"><span style:width="{g.partitions.length ? (caught / g.partitions.length) * 100 : 0}%"></span></span>
            <span class="faint">{caught}/{g.partitions.length}</span>
          </span>
          <span class="trend">
            {#if tr}
              <svg class="spark {tr.dir}" width="56" height="18" viewBox="0 0 56 18" role="img"
                aria-label="Lag {tr.dir === 'up' ? 'rising' : tr.dir === 'down' ? 'falling' : 'steady'}: {fmt(tr.first)} to {fmt(tr.last)} over {tr.n} readings">
                <title>Lag {fmt(tr.first)} → {fmt(tr.last)} over the last {tr.n} readings</title>
                <polyline points={tr.points} />
              </svg>
            {/if}
          </span>
          <span class="num mono">{#if g.lag > 0}<span class="lagpill">{fmt(g.lag)}</span>{:else}<span class="faint">0</span>{/if}</span>
          <span class="acts">
            <Overflow label="Actions for {g.group}">
              {#if writable}
                <button onclick={() => (resetting = g)} title={g.members > 0 ? 'Stop the consumers to change offsets (you can still preview)' : ''}>
                  <RotateCcw size={14} />Reset offsets…
                </button>
              {/if}
              <button disabled={backingUp === g.group || !g.partitions.some((p) => p.committed >= 0 && !p.no_access)}
                title="Download this group's committed offsets for {topic} as a JSON file" onclick={() => backup(g.group)}>
                <Download size={14} />Back up offsets
              </button>
              {#if writable}
                <button onclick={() => newGroup(g.group)} title="Create a new group that starts where this one is">
                  <Copy size={14} />Copy into a new group…
                </button>
              {/if}
              {#if canDelete}
                <button class="danger" disabled={g.members > 0}
                  title={g.members > 0 ? 'Cannot delete offsets for a group with active members' : `Deletes this group's committed offsets for ${topic}`}
                  onclick={() => (dropping = g)}><Trash2 size={14} />Delete offsets</button>
              {/if}
              <a class="menulink" href={href(cluster.name, 'groups', g.group)}><ExternalLink size={14} />Open group</a>
            </Overflow>
          </span>
        </div>

        {#if isOpen}
          {@const hl = highlight(g)}
          <div class="detail" id="cg-{g.group}">
            {#if g.error}<div class="error small-err">{g.error}</div>{/if}
            <div class="dbar">
              {#if cs.length}
                <div class="clients">
                  {#each cs as c (c.id)}
                    <span class="client" class:on={picked[g.group] === c.id}>
                      <button type="button" class="ghost pickc" aria-pressed={picked[g.group] === c.id}
                        title="Mark this client's partitions in the table" onclick={() => pick(g.group, c.id)}>
                        <span class="mono">{c.client || 'unknown client'}</span>
                        <span class="faint mono">{c.host}</span>
                        <span class="faint">· {c.partitions.length === 1 ? 'partition' : 'partitions'} {c.partitions.join(', ')}</span>
                        {#if c.lag > 0}<span class="lagpill">{fmt(c.lag)}</span>{/if}
                      </button>
                      <a class="memberlink" href={href(cluster.name, 'groups', g.group, { member: c.id })} title="Show this member in the group" aria-label="Show {c.client || 'this client'} in the group"><ArrowRight size={12} /></a>
                    </span>
                  {/each}
                </div>
              {:else}
                <span class="faint">No active members; showing committed offsets.</span>
              {/if}
              <span class="spacer"></span>
              {#if g.lag > 0}
                <UnreadAge {cluster} {topic} rows={g.partitions} />
                <LagEta read={() => reading(g.group)} link={messageAt} />
              {/if}
              <a class="openlink" href={href(cluster.name, 'groups', g.group)}>Open group <ArrowRight size={13} /></a>
            </div>
            {#if issues.hot}
              <div class="hint"><Flame size={13} />{Math.round(issues.hot.share * 100)}% of this group's lag is on partition {issues.hot.partition}. A slow consumer or a hot key may be holding it back.</div>
            {/if}

            <PartitionOffsets {cluster} group={g.group} topic={topic} rows={g.partitions} activeMembers={g.members} highlight={hl} onchanged={load} />
          </div>
        {/if}
      </div>
    {:else}
      <div class="empty nomatch">No groups match your filters.</div>
    {/each}
  </div>
{/if}

{#if resetting}
  <OffsetDialog {cluster} group={resetting.group} {topic} partitions={[]} pickPartitions
    activeMembers={resetting.members} ondone={load}
    bind:open={() => !!resetting, (v) => { if (!v) resetting = undefined }} />
{/if}

{#if creating}
  <GroupTopicDialog {cluster} {topic} sourceGroup={copyFrom} bind:open={creating} ondone={load} />
{/if}

{#if dropping}
  <ConfirmDialog title="Delete offsets: {dropping.group}" onconfirm={dropOffsets}
    bind:open={() => !!dropping, (v) => { if (!v) dropping = undefined }}>
    <p>All offsets committed by group <b class="mono">{dropping.group}</b> for topic <b class="mono">{topic}</b> will be deleted. When the consumer restarts, it will start according to <code>auto.offset.reset</code>.</p>
  </ConfirmDialog>
{/if}

<style>
  .bar { display: flex; flex-wrap: wrap; align-items: center; gap: 10px; margin-bottom: 14px; font-size: 13px; }
  .bar b { color: var(--text); font-weight: 600; }
  .lagged { background: var(--sel); padding: 0 4px; }
  .refresh { display: inline-flex; gap: 6px; }
  .refresh select { width: auto; }

  .list { --cols: 28px minmax(0, 2.2fr) 130px 80px minmax(120px, 1fr) 64px 100px 36px; }
  .lhead, .row {
    display: grid;
    grid-template-columns: var(--cols);
    align-items: center;
    gap: 12px;
    padding: 0 6px 0 8px;
  }
  .lhead {
    height: 34px;
    font-size: 12px;
    font-weight: 500;
    color: var(--text-3);
    background: var(--surface-2);
    border-bottom: 1px solid var(--border-strong);
  }
  .lhead .num { justify-self: end; }
  .lhead button.th { padding: 0; font-size: 12px; color: inherit; font-weight: 500; gap: 4px; }
  .lhead button.th:hover:not(:disabled) { background: transparent; color: var(--text); }
  .item + .item { border-top: 1px solid var(--border); }
  .row {
    min-height: 46px;
    cursor: pointer;
  }
  .item.alt > .row { background: var(--stripe); }
  .row:hover { background: var(--row-hover); }
  .item.open > .row,
  .item.open > .row:hover { background: var(--sel); }
  .chevbtn {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    justify-self: center;
    width: 24px;
    height: 24px;
    padding: 0;
    border: 0;
    border-radius: 2px;
    background: transparent;
  }
  .chevbtn:hover:not(:disabled) { background: transparent; }
  .row :global(.chev) { color: var(--text-3); transition: transform .16s cubic-bezier(.2, .7, .2, 1); }
  .open > .row :global(.chev) { transform: rotate(90deg); color: var(--text); }
  .gname { font-size: 14px; font-weight: 500; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .cell .badge { gap: 4px; }
  .caught { display: flex; align-items: center; gap: 8px; font-size: 12px; }
  .caught .track { flex: 1; max-width: 160px; }
  .caught .track > span { background: var(--ok); }
  .spark { display: block; }
  .spark polyline { fill: none; stroke: var(--text-3); stroke-width: 1.4; stroke-linejoin: round; }
  .spark.up polyline { stroke: var(--danger); }
  .spark.down polyline { stroke: var(--ok); }
  .acts { justify-self: end; cursor: default; }
  .menulink { display: flex; align-items: center; gap: 6px; padding: 7px 10px; font-size: 13px; color: var(--text); border-radius: 2px; }
  .menulink:hover { background: var(--row-hover); text-decoration: none; }
  .nomatch { padding: 18px 14px; }

  .detail { padding: 12px 14px 16px 44px; border-top: 1px solid var(--border); background: var(--bg); }
  .dbar { display: flex; flex-wrap: wrap; align-items: center; gap: 8px 12px; margin-bottom: 10px; font-size: 13px; }
  .openlink { display: inline-flex; align-items: center; gap: 4px; font-size: 13px; color: var(--text-2); }
  .openlink:hover { color: var(--text); text-decoration: none; }
  .clients { display: flex; flex-wrap: wrap; gap: 6px; }
  .client {
    display: inline-flex;
    align-items: center;
    font-size: 12px;
    border: 1px solid var(--border-strong);
    border-radius: 2px;
    background: var(--surface);
  }
  .client.on { border-color: var(--accent); box-shadow: inset 3px 0 0 var(--accent); }
  .pickc { display: inline-flex; align-items: center; gap: 6px; padding: 2px 6px 2px 8px; font-size: 12px; font-weight: 400; border: 0; color: var(--text); }
  .memberlink { display: inline-flex; align-items: center; align-self: stretch; padding: 0 6px; border-left: 1px solid var(--border); color: var(--text-3); }
  .memberlink:hover { color: var(--text); background: var(--row-hover); }
  .hint { display: flex; align-items: center; gap: 6px; margin-bottom: 10px; font-size: 13px; color: var(--warn); }
  .lagpill { display: inline-block; padding: 0 6px; background: var(--sel); border: 1px solid var(--cta-border); border-radius: 2px; font-weight: 600; }
  .small-err { font-size: 12px; padding: 6px 8px; margin-bottom: 8px; }

  @media (max-width: 760px) {
    .list { --cols: 24px minmax(0, 1fr) auto 80px 36px; }
    .lhead > :nth-child(4), .lhead > :nth-child(5), .lhead > :nth-child(6),
    .row > :nth-child(4), .row > :nth-child(5), .row > :nth-child(6) { display: none; }
    .detail { padding-left: 14px; }
  }
</style>
