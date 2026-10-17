<script lang="ts">
  import { api, type Cluster, type TopicSummary } from '../lib/api'
  import { fmt, bytes } from '../lib/format'
  import { href, go } from '../lib/router.svelte'
  import Modal from '../lib/Modal.svelte'
  import MoreRows from '../lib/MoreRows.svelte'
  import { forgetNames } from '../lib/names'
  import { ArrowDown, ArrowUp, RefreshCw, Plus } from '@lucide/svelte'

  let { cluster }: { cluster: Cluster } = $props()

  let topics = $state<TopicSummary[]>([])
  let loading = $state(true)
  let error = $state('')
  let search = $state('')
  let showInternal = $state(false)
  let sortKey = $state<keyof TopicSummary>('name')
  let sortDesc = $state(false)

  async function load() {
    loading = true
    error = ''
    try {
      topics = await api.topics(cluster.name)
    } catch (e: any) {
      error = e.message
    } finally {
      loading = false
    }
  }
  load()

  const rows = $derived.by(() => {
    const q = search.trim().toLowerCase()
    const list = topics.filter((t) => (showInternal || !t.internal) && (!q || t.name.toLowerCase().includes(q)))
    return list.sort((a, b) => {
      const av = a[sortKey] ?? ''
      const bv = b[sortKey] ?? ''
      const r = typeof av === 'string' ? av.localeCompare(bv as string) : (av as number) - (bv as number)
      return sortDesc ? -r : r
    })
  })

  const page = 200
  let shown = $state(page)
  $effect(() => {
    void search, showInternal, sortKey, sortDesc
    shown = page
  })

  function sort(key: keyof TopicSummary) {
    if (sortKey === key) sortDesc = !sortDesc
    else {
      sortKey = key
      sortDesc = key !== 'name'
    }
  }

  // Create topic
  let creating = $state(false)
  let form = $state({ name: '', partitions: 3, replication_factor: 0, configs: '' })
  let createError = $state('')
  let busy = $state(false)

  async function create() {
    busy = true
    createError = ''
    try {
      const configs: Record<string, string> = {}
      for (const line of form.configs.split('\n')) {
        const [k, ...v] = line.split('=')
        if (k.trim()) configs[k.trim()] = v.join('=').trim()
      }
      await api.createTopic(cluster.name, {
        name: form.name.trim(),
        partitions: Number(form.partitions),
        replication_factor: Number(form.replication_factor),
        configs,
      })
      creating = false
      forgetNames(cluster.name)
      go(cluster.name, 'topics', form.name.trim())
    } catch (e: any) {
      createError = e.message
    } finally {
      busy = false
    }
  }
</script>

<div class="head">
  <h1>Topics</h1>
  <span class="faint">{rows.length}</span>
  <span class="spacer"></span>
  <button onclick={load} disabled={loading}><RefreshCw size={14} class={loading ? 'animate-spin' : ''} />Refresh</button>
  <button class="primary" disabled={cluster.read_only} onclick={() => {
    form = { name: '', partitions: 3, replication_factor: 0, configs: '' }
    createError = ''
    creating = true
  }}><Plus size={14} />Create topic</button>
</div>

<div class="filters">
  <input type="search" placeholder="Search topics…" bind:value={search} />
  <label class="check"><input type="checkbox" bind:checked={showInternal} /> Show internal topics</label>
</div>

{#if error}
  <div class="error">{error}</div>
{:else}
  <div class="panel">
    <table>
      <thead>
        <tr>
          <th><button class="ghost th" onclick={() => sort('name')}>Name {#if sortKey === 'name'}{#if sortDesc}<ArrowDown size={12} />{:else}<ArrowUp size={12} />{/if}{/if}</button></th>
          <th class="num"><button class="ghost th" onclick={() => sort('partitions')}>Partitions {#if sortKey === 'partitions'}{#if sortDesc}<ArrowDown size={12} />{:else}<ArrowUp size={12} />{/if}{/if}</button></th>
          <th class="num">RF</th>
          <th class="num"><button class="ghost th" onclick={() => sort('messages')}>Messages {#if sortKey === 'messages'}{#if sortDesc}<ArrowDown size={12} />{:else}<ArrowUp size={12} />{/if}{/if}</button></th>
          <th class="num"><button class="ghost th" onclick={() => sort('size')}>Size {#if sortKey === 'size'}{#if sortDesc}<ArrowDown size={12} />{:else}<ArrowUp size={12} />{/if}{/if}</button></th>
          <th>Cleanup</th>
          <th class="num"><button class="ghost th" onclick={() => sort('under_replicated')}>Under-replicated {#if sortKey === 'under_replicated'}{#if sortDesc}<ArrowDown size={12} />{:else}<ArrowUp size={12} />{/if}{/if}</button></th>
        </tr>
      </thead>
      <tbody>
        {#each rows.slice(0, shown) as t (t.name)}
          <tr>
            <td>
              <a class="mono" href={href(cluster.name, 'topics', t.name)}>{t.name}</a>
              {#if t.internal}<span class="badge">internal</span>{/if}
            </td>
            <td class="num">{t.partitions}</td>
            <td class="num">{t.replication_factor}</td>
            <td class="num">{fmt(t.messages)}</td>
            <td class="num">{t.size >= 0 ? bytes(t.size) : '—'}</td>
            <td class="muted">{t.cleanup_policy ?? '—'}</td>
            <td class="num">
              {#if t.under_replicated > 0}<span class="badge warn">{t.under_replicated}</span>{:else}<span class="faint">0</span>{/if}
            </td>
          </tr>
        {:else}
          <tr><td colspan="7" class="empty">{loading ? 'Loading…' : search ? 'No topics match your search.' : 'This cluster has no topics.'}</td></tr>
        {/each}
      </tbody>
    </table>
    <MoreRows total={rows.length} bind:shown step={page} />
  </div>
{/if}

<Modal title="Create topic" bind:open={creating}>
  <label>Name <input bind:value={form.name} placeholder="orders.created" /></label>
  <div class="grid2">
    <label>Partitions <input type="number" min="1" bind:value={form.partitions} /></label>
    <label>Replication factor <input type="number" min="0" bind:value={form.replication_factor} placeholder="0 = broker default" /></label>
  </div>
  <label>Config (one key=value per line)
    <textarea rows="4" bind:value={form.configs} placeholder={'retention.ms=604800000\ncleanup.policy=delete'}></textarea>
  </label>
  <p class="faint hint">A replication factor of 0 uses the broker default.</p>
  {#if createError}<div class="error">{createError}</div>{/if}
  {#snippet footer()}
    <button onclick={() => (creating = false)}>Cancel</button>
    <button class="primary" disabled={busy || !form.name.trim()} onclick={create}>{busy ? 'Creating…' : 'Create'}</button>
  {/snippet}
</Modal>

<style>
  .hint { font-size: 12px; }
</style>
