<script lang="ts">
  import { untrack } from 'svelte'
  import { api, type Cluster, type RegistryOverview, type RegistryQuery, type SubjectSummary } from '../lib/api'
  import { fmt } from '../lib/format'
  import { href, go } from '../lib/router.svelte'
  import Modal from '../lib/Modal.svelte'
  import SchemaEditor from '../lib/SchemaEditor.svelte'
  import MorePages from '../lib/MorePages.svelte'
  import RefreshStop from '../lib/RefreshStop.svelte'
  import { paged } from '../lib/paged.svelte'
  import { cached, remember } from '../lib/cache'
  import { Plus } from '@lucide/svelte'

  let { cluster }: { cluster: Cluster } = $props()

  let search = $state('')
  let showDeleted = $state(false)
  let typeFilter = $state('')
  let contextFilter = $state('')

  // The page remounts per cluster ({#key} in App), so the first value is the one.
  const name = untrack(() => cluster.name)
  // Mode, compatibility and types come with every page; the latest wins.
  const infoKey = `registry-info:${name}`
  let info = $state<Omit<RegistryOverview, 'subjects' | 'total'> | undefined>(cached(infoKey))

  const list = paged<SubjectSummary, RegistryQuery>({
    key: `schemas:${name}`,
    query: () => ({ q: search.trim(), type: typeFilter, context: contextFilter, deleted: showDeleted }),
    fetch: async (q, offset, limit) => {
      const { subjects, total, ...rest } = await api.registry(name, { ...q, offset, limit })
      info = remember(infoKey, rest)
      return { items: subjects, total }
    },
    id: (s) => s.name,
    cacheable: () => !search.trim() && !typeFilter && !contextFilter && !showDeleted,
  })
  list.watch(() => search)

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
      list.load(true)
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
  <span class="faint">{fmt(list.total)}</span>
  <span class="spacer"></span>
  <RefreshStop loading={list.loading} onrefresh={() => list.load(true)} onstop={list.stop} />
  <button class="primary" disabled={cluster.read_only || !info} onclick={() => (creating = true)}><Plus size={14} />Create schema</button>
</div>

{#if list.error}
  <div class="error">{list.error}</div>
{:else if info}
  <div class="summary">
    <span>Global compatibility <b>{info.compatibility || '—'}</b>
      {#if !cluster.read_only}<button class="ghost small" onclick={() => { compatLevel = info?.compatibility ?? 'BACKWARD'; compatError = ''; compatOpen = true }}>Change</button>{/if}
    </span>
    <span>Mode <b>{info.mode || '—'}</b></span>
    <span>Types <b>{info.types.join(', ')}</b></span>
  </div>

  <div class="filters">
    <input type="search" placeholder="Search subjects…" bind:value={search} />
    <select bind:value={typeFilter} title="Filtering by type reads every subject's latest schema; slower on large registries">
      <option value="">All types</option>
      {#each info.types as t (t)}<option value={t}>{t}</option>{/each}
    </select>
    {#if (info.contexts?.length ?? 0) > 1}
      <select bind:value={contextFilter} title="Registry context">
        <option value="">All contexts</option>
        {#each info.contexts ?? [] as c (c)}<option value={c}>{c === '.' ? 'Default context' : c}</option>{/each}
      </select>
    {/if}
    <label class="check"><input type="checkbox" bind:checked={showDeleted} /> Show deleted</label>
  </div>

  <div class="panel" class:stale={list.loading && list.items.length > 0}>
    <table>
      <thead><tr><th>Subject</th><th>Type</th><th class="num">Latest version</th><th class="num">Versions</th><th class="num">ID</th><th>Compatibility</th></tr></thead>
      <tbody>
        {#each list.items as s (s.name)}
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
          <tr><td colspan="6" class="empty">{list.loading ? 'Loading…' : search.trim() || typeFilter || contextFilter ? 'No subjects match your filters.' : 'No schemas registered.'}</td></tr>
        {/each}
      </tbody>
    </table>
    <MorePages shown={list.items.length} total={list.total} step={list.size} busy={list.more || list.loading} onmore={list.loadMore} />
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

<SchemaEditor {cluster} bind:open={creating} types={info?.types ?? []} onsaved={(subject) => go(cluster.name, 'schemas', subject)} />

<style>
  button.small { padding: 1px 6px; }
  td:nth-child(2) .badge { margin-left: 0; }
  tr.deleted td { color: var(--text-3); }
</style>
