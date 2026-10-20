<script lang="ts">
  import { api, type Cluster, type TopicConsumer } from '../lib/api'
  import { fmt, stateBadge } from '../lib/format'
  import { href } from '../lib/router.svelte'
  import PartitionOffsets from '../lib/PartitionOffsets.svelte'
  import LagEta from '../lib/LagEta.svelte'
  import { RefreshCw, ArrowRight, Users, ChevronRight, ChevronsUpDown, ChevronsDownUp } from '@lucide/svelte'

  let { cluster, topic }: { cluster: Cluster; topic: string } = $props()

  let groups = $state<TopicConsumer[]>()
  let error = $state('')
  let loading = $state(false)

  async function load() {
    loading = true
    try {
      groups = await api.topicConsumers(cluster.name, topic)
      error = ''
    } catch (e: any) {
      error = e.message
    } finally {
      loading = false
    }
  }
  load()

  // Clients that own at least one partition of this topic, per group.
  function clients(g: TopicConsumer) {
    const m = new Map<string, { client: string; host: string; partitions: number[]; lag: number }>()
    for (const p of g.partitions) {
      if (!p.member_id) continue
      const c = m.get(p.member_id) ?? { client: p.client_id ?? '', host: p.host ?? '', partitions: [], lag: 0 }
      c.partitions.push(p.partition)
      if (p.lag > 0) c.lag += p.lag
      m.set(p.member_id, c)
    }
    return [...m.values()]
  }

  // Every group starts collapsed; the row is the toggle.
  let open = $state(new Set<string>())
  function toggle(g: string) {
    const next = new Set(open)
    if (next.has(g)) next.delete(g)
    else next.add(g)
    open = next
  }
  const allOpen = $derived(!!groups?.length && groups.every((g) => open.has(g.group)))
  const toggleAll = () => (open = allOpen ? new Set() : new Set(groups?.map((g) => g.group)))

  // One group's numbers: the group endpoint computes lag for that group
  // alone, where the topic's consumer list would redo every group's.
  async function reading(group: string) {
    const g = await api.group(cluster.name, group)
    const rows = g.partitions.filter((p) => p.topic === topic)
    return {
      lag: rows.reduce((s, p) => s + Math.max(p.lag, 0), 0),
      committed: rows.reduce((s, p) => s + Math.max(p.committed, 0), 0),
      high: rows.reduce((s, p) => s + p.high, 0),
    }
  }

  const total = $derived((groups ?? []).reduce((s, g) => s + g.lag, 0))
</script>

<div class="bar">
  {#if groups}
    <span class="muted">
      <b>{groups.length}</b> {groups.length === 1 ? 'group' : 'groups'} reading this topic
      {#if groups.length}· total lag <b class:lagged={total > 0}>{fmt(total)}</b>{/if}
    </span>
  {/if}
  <span class="spacer"></span>
  {#if groups && groups.length > 1}
    <button class="ghost" onclick={toggleAll}>
      {#if allOpen}<ChevronsDownUp size={14} />Collapse all{:else}<ChevronsUpDown size={14} />Expand all{/if}
    </button>
  {/if}
  <button onclick={load} disabled={loading}><RefreshCw size={14} class={loading ? 'animate-spin' : ''} />Refresh</button>
</div>

{#if error}
  <div class="error">{error}</div>
{:else if !groups}
  <div class="empty">Loading…</div>
{:else if !groups.length}
  <div class="panel empty">
    <Users size={20} class="mx-auto mb-2 text-ink-3" />
    No consumer group has committed offsets for this topic yet.
  </div>
{:else}
  <div class="panel list">
    <div class="lhead" aria-hidden="true">
      <span></span><span>Group</span><span>State</span><span class="num">Members</span><span>Partitions caught up</span><span class="num">Lag</span>
    </div>
    {#each groups as g, gi (g.group)}
      {@const cs = clients(g)}
      {@const isOpen = open.has(g.group)}
      {@const caught = g.partitions.filter((p) => p.lag <= 0 && (p.committed >= 0 || p.high <= p.low)).length}
      <div class="item" class:alt={gi % 2 === 1} class:open={isOpen}>
        <button
          type="button"
          class="row"
          aria-expanded={isOpen}
          aria-controls="cg-{g.group}"
          onclick={() => toggle(g.group)}
        >
          <ChevronRight size={15} class="chev" />
          <span class="mono gname">{g.group}</span>
          <span><span class="badge {stateBadge(g.state)}"><span class="sq {stateBadge(g.state) || 'idle'}"></span>{g.state || '—'}</span></span>
          <span class="num muted">{g.members}</span>
          <span class="caught">
            <span class="track"><span style:width="{g.partitions.length ? (caught / g.partitions.length) * 100 : 0}%"></span></span>
            <span class="faint">{caught}/{g.partitions.length}</span>
          </span>
          <span class="num mono">{#if g.lag > 0}<span class="lagpill">{fmt(g.lag)}</span>{:else}<span class="faint">0</span>{/if}</span>
        </button>

        {#if isOpen}
          <div class="detail" id="cg-{g.group}">
            {#if g.error}<div class="error small-err">{g.error}</div>{/if}
            <div class="dbar">
              {#if cs.length}
                <div class="clients">
                  {#each cs as c (c.client + c.host + c.partitions.join())}
                    <span class="client">
                      <span class="mono">{c.client || 'unknown client'}</span>
                      <span class="faint mono">{c.host}</span>
                      <span class="faint">· {c.partitions.length === 1 ? 'partition' : 'partitions'} {c.partitions.join(', ')}</span>
                      {#if c.lag > 0}<span class="lagpill">{fmt(c.lag)}</span>{/if}
                    </span>
                  {/each}
                </div>
              {:else}
                <span class="faint">No active members; showing committed offsets.</span>
              {/if}
              <span class="spacer"></span>
              {#if g.lag > 0}<LagEta read={() => reading(g.group)} />{/if}
              <a class="openlink" href={href(cluster.name, 'groups', g.group)}>Open group <ArrowRight size={13} /></a>
            </div>

            <PartitionOffsets {cluster} group={g.group} topic={topic} rows={g.partitions} activeMembers={g.members} onchanged={load} />
          </div>
        {/if}
      </div>
    {/each}
  </div>
{/if}

<style>
  .bar { display: flex; align-items: center; gap: 10px; margin-bottom: 14px; font-size: 13px; }
  .bar b { color: var(--text); font-weight: 600; }
  .lagged { background: var(--sel); padding: 0 4px; }

  .list { --cols: 28px minmax(0, 2.2fr) 130px 80px minmax(140px, 1.2fr) 110px; }
  .lhead, .row {
    display: grid;
    grid-template-columns: var(--cols);
    align-items: center;
    gap: 12px;
    padding: 0 14px 0 8px;
  }
  .lhead {
    height: 34px;
    font-size: 12px;
    font-weight: 500;
    color: var(--text-3);
    background: var(--surface-2);
    border-bottom: 1px solid var(--border-strong);
  }
  .item + .item { border-top: 1px solid var(--border); }
  .row {
    width: 100%;
    min-height: 46px;
    border: 0;
    border-radius: 0;
    background: transparent;
    font-weight: 400;
    text-align: left;
    white-space: normal;
  }
  .item.alt > .row { background: var(--stripe); }
  .row:hover:not(:disabled) { background: var(--row-hover); }
  .row:focus-visible { outline-offset: -2px; }
  .item.open > .row,
  .item.open > .row:hover:not(:disabled) { background: var(--sel); }
  .row :global(.chev) { color: var(--text-3); transition: transform .16s cubic-bezier(.2, .7, .2, 1); justify-self: center; }
  .open > .row :global(.chev) { transform: rotate(90deg); color: var(--text); }
  .gname { font-size: 14px; font-weight: 500; color: var(--text); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .caught { display: flex; align-items: center; gap: 8px; font-size: 12px; }
  .caught .track { flex: 1; max-width: 160px; }
  .caught .track > span { background: var(--ok); }

  .detail { padding: 12px 14px 16px 44px; border-top: 1px solid var(--border); background: var(--bg); }
  .dbar { display: flex; flex-wrap: wrap; align-items: center; gap: 8px; margin-bottom: 10px; font-size: 13px; }
  .openlink { display: inline-flex; align-items: center; gap: 4px; font-size: 13px; color: var(--text-2); }
  .openlink:hover { color: var(--text); text-decoration: none; }
  .clients { display: flex; flex-wrap: wrap; gap: 6px; }
  .client {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    font-size: 12px;
    padding: 2px 8px;
    border: 1px solid var(--border-strong);
    border-radius: 2px;
    background: var(--surface);
  }
  .lagpill { display: inline-block; padding: 0 6px; background: var(--sel); border: 1px solid var(--cta-border); border-radius: 2px; font-weight: 600; }
  .small-err { font-size: 12px; padding: 6px 8px; margin-bottom: 8px; }

  @media (max-width: 760px) {
    .list { --cols: 24px minmax(0, 1fr) auto 80px; }
    .lhead > :nth-child(4), .lhead > :nth-child(5), .row > :nth-child(4), .row > :nth-child(5) { display: none; }
    .detail { padding-left: 14px; }
  }
</style>
