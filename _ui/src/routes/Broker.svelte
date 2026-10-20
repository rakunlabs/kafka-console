<script lang="ts">
  import { api, type BrokerDetail, type Cluster, type ConfigChange } from '../lib/api'
  import { bytes, fmt } from '../lib/format'
  import { href } from '../lib/router.svelte'
  import ConfigTable from '../lib/ConfigTable.svelte'

  let { cluster, id }: { cluster: Cluster; id: number } = $props()

  let data = $state<BrokerDetail>()
  let error = $state('')

  function load() {
    api
      .broker(cluster.name, id)
      .then((b) => (data = b))
      .catch((e) => (error = e.message))
  }
  load()

  async function save(changes: ConfigChange[]) {
    await api.alterBrokerConfigs(cluster.name, id, changes)
    data = await api.broker(cluster.name, id)
  }

  const used = (d: { total_bytes: number; usable_bytes: number }) =>
    d.total_bytes > 0 ? 1 - d.usable_bytes / d.total_bytes : -1
</script>

<div class="head">
  <h1 class="crumbed"><a class="parent" href={href(cluster.name, 'overview')}>Overview</a><span class="sep" aria-hidden="true">/</span><span>Broker <span class="mono">#{id}</span></span></h1>
  {#if data?.controller}<span class="badge accent">controller</span>{/if}
</div>

{#if error}
  <div class="error">{error}</div>
{:else if !data}
  <div class="empty">Loading…</div>
{:else}
  <div class="summary">
    <span class="mono">{data.host}:{data.port}</span>
    {#if data.rack}<span>Rack <b>{data.rack}</b></span>{/if}
    <span><b>{fmt(data.leaders)}</b> leaders</span>
    <span><b>{fmt(data.replicas)}</b> replicas</span>
  </div>

  <h2 class="section">Log directories</h2>
  <div class="panel">
    <table>
      <thead><tr><th>Directory</th><th class="num">Partitions</th><th class="num">Data</th><th class="disk">Disk</th></tr></thead>
      <tbody>
        {#each data.log_dirs as d (d.dir)}
          <tr>
            <td class="mono grow wrap">{d.dir}{#if d.error}<span class="badge danger">{d.error}</span>{/if}</td>
            <td class="num mono">{fmt(d.partitions)}</td>
            <td class="num mono">{bytes(d.size)}</td>
            <td class="disk">
              {#if used(d) >= 0}
                <div class="usage"><div class="fill" class:hot={used(d) > 0.85} style:width="{used(d) * 100}%"></div></div>
                <span class="faint small">{bytes(d.total_bytes - d.usable_bytes)} / {bytes(d.total_bytes)}</span>
              {:else}
                <span class="faint small">Not reported by broker</span>
              {/if}
            </td>
          </tr>
        {:else}
          <tr><td colspan="4" class="faint">Could not fetch log directory info.</td></tr>
        {/each}
      </tbody>
    </table>
  </div>

  <h2 class="section mtop">Config</h2>
  <ConfigTable configs={data.configs} readOnly={cluster.read_only || !cluster.features.alter_broker} {save} />
{/if}

<style>
  .section { margin-bottom: 8px; }
  .mtop { margin-top: 24px; }
  .disk { width: 30%; }
  .usage { height: 6px; border-radius: 1px; background: var(--surface-2); box-shadow: inset 0 0 0 1px var(--border); overflow: hidden; margin: 6px 0 2px; }
  .fill { height: 100%; background: var(--text-2); }
  .fill.hot { background: var(--danger); }
</style>
