<script lang="ts">
  import { api, type Cluster, type RegistryOverview } from '../lib/api'
  import { href, go } from '../lib/router.svelte'
  import Modal from '../lib/Modal.svelte'
  import SchemaEditor from '../lib/SchemaEditor.svelte'
  import MoreRows from '../lib/MoreRows.svelte'
  import { RefreshCw, Plus } from '@lucide/svelte'

  let { cluster }: { cluster: Cluster } = $props()

  let data = $state<RegistryOverview>()
  let error = $state('')
  let loading = $state(false)
  let search = $state('')
  let showDeleted = $state(false)
  let typeFilter = $state('')

  async function load() {
    loading = true
    error = ''
    try {
      data = await api.registry(cluster.name, showDeleted)
    } catch (e: any) {
      error = e.message
    } finally {
      loading = false
    }
  }
  load()

  const rows = $derived.by(() => {
    const q = search.trim().toLowerCase()
    return (data?.subjects ?? []).filter((s) => (!q || s.name.toLowerCase().includes(q)) && (!typeFilter || s.type === typeFilter))
  })

  const page = 200
  let shown = $state(page)
  $effect(() => {
    void search, typeFilter
    shown = page
  })

  const levels = ['NONE', 'BACKWARD', 'BACKWARD_TRANSITIVE', 'FORWARD', 'FORWARD_TRANSITIVE', 'FULL', 'FULL_TRANSITIVE']

  let compatOpen = $state(false)
  let compatLevel = $state('')
  let compatError = $state('')
  let busy = $state(false)

  async function saveCompat() {
    busy = true
    compatError = ''
    try {
      await api.setGlobalCompatibility(cluster.name, compatLevel)
      compatOpen = false
      load()
    } catch (e: any) {
      compatError = e.message
    } finally {
      busy = false
    }
  }

  let creating = $state(false)
</script>

<div class="head">
  <h1>Schema Registry</h1>
  <span class="faint">{rows.length}</span>
  <span class="spacer"></span>
  <button onclick={load} disabled={loading}><RefreshCw size={14} class={loading ? 'animate-spin' : ''} />Refresh</button>
  <button class="primary" disabled={cluster.read_only || !data} onclick={() => (creating = true)}><Plus size={14} />Create schema</button>
</div>

{#if error}
  <div class="error">{error}</div>
{:else if data}
  <div class="summary">
    <span>Global compatibility <b>{data.compatibility || '—'}</b>
      {#if !cluster.read_only}<button class="ghost small" onclick={() => { compatLevel = data?.compatibility ?? 'BACKWARD'; compatError = ''; compatOpen = true }}>Change</button>{/if}
    </span>
    <span>Mode <b>{data.mode || '—'}</b></span>
    <span>Types <b>{data.types.join(', ')}</b></span>
  </div>

  <div class="filters">
    <input type="search" placeholder="Search subjects…" bind:value={search} />
    <select bind:value={typeFilter}>
      <option value="">All types</option>
      {#each data.types as t (t)}<option value={t}>{t}</option>{/each}
    </select>
    <label class="check"><input type="checkbox" bind:checked={showDeleted} onchange={load} /> Show deleted</label>
  </div>

  <div class="panel">
    <table>
      <thead><tr><th>Subject</th><th>Type</th><th class="num">Latest version</th><th class="num">Versions</th><th class="num">ID</th><th>Compatibility</th></tr></thead>
      <tbody>
        {#each rows.slice(0, shown) as s (s.name)}
          <tr class:deleted={s.deleted}>
            <td>
              <a class="mono" href={href(cluster.name, 'schemas', s.name)}>{s.name}</a>
              {#if s.deleted}<span class="badge danger">deleted</span>{/if}
            </td>
            <td><span class="badge">{s.type || '—'}</span></td>
            <td class="num mono">{s.latest_version || '—'}</td>
            <td class="num mono">{s.versions}</td>
            <td class="num mono faint">{s.latest_id || '—'}</td>
            <td class="faint small">{s.compatibility ?? '—'}</td>
          </tr>
        {:else}
          <tr><td colspan="6" class="empty">{loading ? 'Loading…' : search ? 'No subjects match your search.' : 'No schemas registered.'}</td></tr>
        {/each}
      </tbody>
    </table>
    <MoreRows total={rows.length} bind:shown step={page} />
  </div>
{:else}
  <div class="empty">Loading…</div>
{/if}

<Modal title="Global compatibility" bind:open={compatOpen}>
  <label>Level
    <select bind:value={compatLevel}>{#each levels as l (l)}<option>{l}</option>{/each}</select>
  </label>
  <p class="faint small">Applies to all subjects without their own compatibility level.</p>
  {#if compatError}<div class="error">{compatError}</div>{/if}
  {#snippet footer()}
    <button onclick={() => (compatOpen = false)}>Cancel</button>
    <button class="primary" disabled={busy} onclick={saveCompat}>Save</button>
  {/snippet}
</Modal>

<SchemaEditor {cluster} bind:open={creating} types={data?.types ?? []} onsaved={(subject) => go(cluster.name, 'schemas', subject)} />

<style>
  button.small { padding: 1px 6px; }
  td:nth-child(2) .badge { margin-left: 0; }
  tr.deleted td { color: var(--text-3); }
</style>
