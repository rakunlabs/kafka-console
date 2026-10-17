<script lang="ts">
  import { api, type Cluster, type GroupSummary } from '../lib/api'
  import { fmt, stateBadge } from '../lib/format'
  import { href } from '../lib/router.svelte'
  import { RefreshCw, Plus } from '@lucide/svelte'
  import GroupTopicDialog from '../lib/GroupTopicDialog.svelte'
  import MoreRows from '../lib/MoreRows.svelte'

  let { cluster }: { cluster: Cluster } = $props()

  let groups = $state<GroupSummary[]>([])
  let loading = $state(true)
  let error = $state('')
  let search = $state('')
  let sortByLag = $state(false)

  async function load() {
    loading = true
    error = ''
    try {
      groups = await api.groups(cluster.name)
    } catch (e: any) {
      error = e.message
    } finally {
      loading = false
    }
  }
  load()

  let creating = $state(false)

  const rows = $derived.by(() => {
    const q = search.trim().toLowerCase()
    const list = groups.filter((g) => !q || g.name.toLowerCase().includes(q) || g.topics.some((t) => t.toLowerCase().includes(q)))
    return sortByLag ? list.sort((a, b) => b.lag - a.lag) : list
  })

  const page = 200
  let shown = $state(page)
  $effect(() => {
    void search, sortByLag
    shown = page
  })
</script>

<div class="head">
  <h1>Consumer groups</h1>
  <span class="faint">{rows.length}</span>
  <span class="spacer"></span>
  <button onclick={load} disabled={loading}><RefreshCw size={14} class={loading ? 'animate-spin' : ''} />Refresh</button>
  <button class="primary" disabled={cluster.read_only} title={cluster.read_only ? 'This cluster is read-only' : ''} onclick={() => (creating = true)}><Plus size={14} />New group</button>
</div>

<GroupTopicDialog {cluster} bind:open={creating} ondone={load} />

<div class="filters">
  <input type="search" placeholder="Search groups or topics…" bind:value={search} />
  <label class="check"><input type="checkbox" bind:checked={sortByLag} /> Sort by lag</label>
</div>

{#if error}
  <div class="error">{error}</div>
{:else}
  <div class="panel">
    <table>
      <thead><tr><th>Group</th><th>State</th><th class="num">Members</th><th>Topics</th><th class="num">Lag</th></tr></thead>
      <tbody>
        {#each rows.slice(0, shown) as g (g.name)}
          <tr>
            <td><a class="mono" href={href(cluster.name, 'groups', g.name)}>{g.name}</a></td>
            <td><span class="badge {stateBadge(g.state)}"><span class="sq {stateBadge(g.state) || 'idle'}"></span>{g.state || '—'}</span></td>
            <td class="num">{g.members}</td>
            <td>
              <div class="topics">
                {#each g.topics as t (t)}<a class="mono" href={href(cluster.name, 'topics', t)}>{t}</a>{/each}
                {#if !g.topics.length}<span class="faint">—</span>{/if}
              </div>
            </td>
            <td class="num mono" class:lag={g.lag > 0}>{#if g.lag > 0}<span class="lagpill">{fmt(g.lag)}</span>{:else}<span class="faint">0</span>{/if}</td>
          </tr>
        {:else}
          <tr><td colspan="5" class="empty">{loading ? 'Loading…' : 'No consumer groups found.'}</td></tr>
        {/each}
      </tbody>
    </table>
    <MoreRows total={rows.length} bind:shown step={page} />
  </div>
{/if}

<style>
  .topics { display: flex; flex-wrap: wrap; gap: 4px 10px; }
  .lagpill { display: inline-block; padding: 0 6px; background: var(--sel); border: 1px solid var(--cta-border); border-radius: 2px; font-weight: 600; }
</style>
