<script lang="ts">
  import { resizable } from '../lib/resizable'
  import Pending from '../lib/Pending.svelte'
  import { untrack } from 'svelte'
  import { api, type Cluster, type GroupQuery, type GroupSummary } from '../lib/api'
  import { fmt, stateBadge } from '../lib/format'
  import { href } from '../lib/router.svelte'
  import { Plus, Lock } from '@lucide/svelte'
  import GroupTopicDialog from '../lib/GroupTopicDialog.svelte'
  import MorePages from '../lib/MorePages.svelte'
  import RefreshStop from '../lib/RefreshStop.svelte'
  import { paged } from '../lib/paged.svelte'

  let { cluster }: { cluster: Cluster } = $props()

  let search = $state('')
  let sortByLag = $state(false)

  // The page remounts per cluster ({#key} in App), so the first value is the one.
  const name = untrack(() => cluster.name)
  const list = paged<GroupSummary, GroupQuery>({
    key: `groups:${name}`,
    query: () => ({ q: search.trim(), sort: sortByLag ? 'lag' : 'name' }),
    fetch: async (q, offset, limit) => {
      const r = await api.groups(name, { ...q, offset, limit })
      return { items: r.groups, total: r.total }
    },
    id: (g) => g.name,
    cacheable: () => !search.trim() && !sortByLag,
  })
  list.watch(() => search)

  let creating = $state(false)
</script>

<div class="head">
  <h1>Consumer groups</h1>
  <span class="faint">{fmt(list.total)}</span>
  <span class="spacer"></span>
  <RefreshStop loading={list.loading} onrefresh={() => list.load(true)} onstop={list.stop} />
  {#if !cluster.read_only}<button class="primary" onclick={() => (creating = true)}><Plus size={14} />New group</button>{/if}
</div>

<GroupTopicDialog {cluster} bind:open={creating} ondone={() => list.load()} />

<div class="filters">
  <input type="search" placeholder="Search groups or topics…" bind:value={search} />
  <label class="check" title="Computes lag for every matching group; slower on large clusters"><input type="checkbox" bind:checked={sortByLag} /> Sort by lag</label>
</div>

{#if list.error}
  <div class="error">{list.error}</div>
{:else}
  <div class="panel" class:stale={list.loading && list.items.length > 0}>
    <table use:resizable={'groups'}>
      <thead><tr><th>Group</th><th>State</th><th class="num">Members</th><th>Topics</th><th class="num">Lag</th></tr></thead>
      <tbody>
        {#each list.items as g (g.name)}
          <tr>
            <td class="grow"><a class="mono" href={href(cluster.name, 'groups', g.name)} title={g.name}>{g.name}</a></td>
            <td><span class="badge {stateBadge(g.state)}"><span class="sq {stateBadge(g.state) || 'idle'}"></span>{g.state || '—'}</span></td>
            <td class="num">{g.members}</td>
            <td class="grow" title={g.topics.join(', ')}>
              {#each g.topics as t (t)}
                {#if g.unreadable?.includes(t)}
                  <span class="mono noaccess" title="No permission to read this topic's offsets; its lag is not counted"><Lock size={11} />{t}</span>
                {:else}
                  <a class="mono" href={href(cluster.name, 'topics', t)}>{t}</a>
                {/if}
              {/each}
              {#if !g.topics.length}<span class="faint">—</span>{/if}
              {#if g.error}<div class="gerr" title={g.error}>{g.error}</div>{/if}
            </td>
            <td class="num mono" class:lag={g.lag > 0}>
              {#if g.lag > 0}<span class="lagpill">{fmt(g.lag)}</span>{:else if g.error}<span class="faint">—</span>{:else}<span class="faint">0</span>{/if}
              {#if g.unreadable?.length}<span class="faint partial" title="Lag of {g.unreadable.join(', ')} is unknown: no permission">+?</span>{/if}
            </td>
          </tr>
        {:else}
          <tr><td colspan="5" class="empty">{#if list.loading}<Pending />{:else}{search.trim() ? 'No groups match your search.' : 'No consumer groups found.'}{/if}</td></tr>
        {/each}
      </tbody>
    </table>
    <MorePages shown={list.items.length} total={list.total} step={list.size} busy={list.more || list.loading} onmore={list.loadMore} />
  </div>
{/if}

<style>
  td.grow > a + a, td.grow > a + .noaccess, td.grow > .noaccess + a, td.grow > .noaccess + .noaccess { margin-left: 10px; }
  .lagpill { display: inline-block; padding: 0 6px; background: var(--sel); box-shadow: inset 0 0 0 1px var(--cta-border); border-radius: 2px; font-weight: 600; }
  .noaccess { display: inline-flex; align-items: center; gap: 3px; color: var(--text-3); cursor: help; vertical-align: bottom; }
  .partial { margin-left: 4px; cursor: help; }
  .gerr { margin-top: 3px; font-size: 12px; color: var(--danger); overflow: hidden; text-overflow: ellipsis; }
</style>
