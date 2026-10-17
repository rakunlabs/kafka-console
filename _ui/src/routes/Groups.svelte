<script lang="ts">
  import { api, type Cluster, type GroupSummary } from '../lib/api'
  import { fmt, stateBadge } from '../lib/format'
  import { href } from '../lib/router.svelte'

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

  const rows = $derived.by(() => {
    const q = search.trim().toLowerCase()
    const list = groups.filter((g) => !q || g.name.toLowerCase().includes(q) || g.topics.some((t) => t.toLowerCase().includes(q)))
    return sortByLag ? list.sort((a, b) => b.lag - a.lag) : list
  })
</script>

<div class="head">
  <h1>Consumer groups</h1>
  <span class="faint">{rows.length}</span>
  <span class="spacer"></span>
  <button onclick={load} disabled={loading}>Yenile</button>
</div>

<div class="row filters">
  <input type="search" placeholder="Group ya da topic ara…" bind:value={search} />
  <label class="check"><input type="checkbox" bind:checked={sortByLag} /> Lag'e göre sırala</label>
</div>

{#if error}
  <div class="error">{error}</div>
{:else}
  <div class="panel">
    <table>
      <thead><tr><th>Group</th><th>Durum</th><th class="num">Üye</th><th>Topic'ler</th><th class="num">Lag</th></tr></thead>
      <tbody>
        {#each rows as g (g.name)}
          <tr>
            <td><a class="mono" href={href(cluster.name, 'groups', g.name)}>{g.name}</a></td>
            <td><span class="badge {stateBadge(g.state)}">{g.state || '—'}</span></td>
            <td class="num">{g.members}</td>
            <td class="topics">
              {#each g.topics as t (t)}<a class="mono" href={href(cluster.name, 'topics', t)}>{t}</a>{/each}
              {#if !g.topics.length}<span class="faint">—</span>{/if}
            </td>
            <td class="num mono" class:lag={g.lag > 0}>{fmt(g.lag)}</td>
          </tr>
        {:else}
          <tr><td colspan="5" class="empty">{loading ? 'Yükleniyor…' : 'Consumer group bulunamadı.'}</td></tr>
        {/each}
      </tbody>
    </table>
  </div>
{/if}

<style>
  .head { display: flex; align-items: center; gap: 10px; margin-bottom: 14px; }
  .filters { margin-bottom: 12px; }
  .filters input[type='search'] { width: 320px; }
  .check { display: flex; flex-direction: row; align-items: center; gap: 6px; font-weight: 400; }
  .topics { display: flex; flex-wrap: wrap; gap: 4px 10px; }
  .lag { font-weight: 600; }
</style>
