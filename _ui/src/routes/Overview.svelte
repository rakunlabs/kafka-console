<script lang="ts">
  import { api, type Cluster, type Overview } from '../lib/api'
  import { fmt } from '../lib/format'
  import { href } from '../lib/router.svelte'

  let { cluster }: { cluster: Cluster } = $props()

  let data = $state<Overview>()
  let error = $state('')

  function load() {
    api
      .overview(cluster.name)
      .then((o) => (data = o))
      .catch((e) => (error = e.message))
  }
  load()
</script>

<div class="head">
  <h1>{cluster.name}</h1>
  {#if data?.cluster_id}<span class="faint mono">{data.cluster_id}</span>{/if}
</div>

{#if error}
  <div class="error">{error}</div>
{:else if !data}
  <div class="empty">Yükleniyor…</div>
{:else}
  <div class="stats">
    <a class="panel stat" href={href(cluster.name, 'topics')}><span class="muted">Topics</span><b>{fmt(data.topics)}</b><small class="faint">+{data.internal_topics} internal</small></a>
    <div class="panel stat"><span class="muted">Partitions</span><b>{fmt(data.partitions)}</b></div>
    <div class="panel stat"><span class="muted">Under-replicated</span><b class:warn={data.under_replicated > 0}>{fmt(data.under_replicated)}</b></div>
    <div class="panel stat"><span class="muted">Offline</span><b class:bad={data.offline_partitions > 0}>{fmt(data.offline_partitions)}</b></div>
    <a class="panel stat" href={href(cluster.name, 'groups')}><span class="muted">Consumer groups</span><b>{fmt(data.groups)}</b></a>
  </div>

  <h2 class="section">Brokers</h2>
  <div class="panel">
    <table>
      <thead><tr><th>ID</th><th>Host</th><th class="num">Port</th><th>Rack</th><th></th></tr></thead>
      <tbody>
        {#each data.brokers as b (b.id)}
          <tr>
            <td class="mono">{b.id}</td>
            <td class="mono">{b.host}</td>
            <td class="num mono">{b.port}</td>
            <td class="muted">{b.rack ?? '—'}</td>
            <td>{#if b.controller}<span class="badge accent">controller</span>{/if}</td>
          </tr>
        {/each}
      </tbody>
    </table>
  </div>
{/if}

<style>
  .head { display: flex; align-items: baseline; gap: 12px; margin-bottom: 16px; }
  .stats { display: grid; grid-template-columns: repeat(auto-fill, minmax(170px, 1fr)); gap: 12px; margin-bottom: 24px; }
  .stat { display: grid; gap: 2px; padding: 12px 14px; color: var(--text); }
  a.stat:hover { border-color: var(--border-strong); text-decoration: none; }
  .stat span { font-size: 12px; }
  .stat b { font-size: 20px; font-weight: 600; font-variant-numeric: tabular-nums; }
  .stat small { font-size: 11px; }
  .warn { color: var(--warn); }
  .bad { color: var(--danger); }
  .section { margin-bottom: 8px; }
</style>
