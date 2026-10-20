<script lang="ts">
  import Pending from '../lib/Pending.svelte'
  import { untrack } from 'svelte'
  import { api, type Cluster, type TopicQuery, type TopicSummary } from '../lib/api'
  import { fmt, bytes } from '../lib/format'
  import { href, go } from '../lib/router.svelte'
  import Modal from '../lib/Modal.svelte'
  import MorePages from '../lib/MorePages.svelte'
  import RefreshStop from '../lib/RefreshStop.svelte'
  import { forgetNames } from '../lib/names'
  import { paged } from '../lib/paged.svelte'
  import { ArrowDown, ArrowUp, Plus } from '@lucide/svelte'

  let { cluster }: { cluster: Cluster } = $props()

  type SortKey = NonNullable<TopicQuery['sort']>

  let search = $state('')
  let showInternal = $state(false)
  let sortKey = $state<SortKey>('name')
  let sortDesc = $state(false)

  // The page remounts per cluster ({#key} in App), so the first value is the one.
  const name = untrack(() => cluster.name)
  const list = paged<TopicSummary, TopicQuery>({
    key: `topics:${name}`,
    query: () => ({ q: search.trim(), internal: showInternal, sort: sortKey, desc: sortDesc }),
    fetch: async (q, offset, limit) => {
      const r = await api.topics(name, { ...q, offset, limit })
      return { items: r.topics, total: r.total }
    },
    id: (t) => t.name,
    cacheable: () => !search.trim() && !showInternal && sortKey === 'name' && !sortDesc,
  })
  list.watch(() => search)

  function sort(key: SortKey) {
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
  <span class="faint">{fmt(list.total)}</span>
  <span class="spacer"></span>
  <RefreshStop loading={list.loading} onrefresh={() => list.load(true)} onstop={list.stop} />
  {#if cluster.features.create_topic}
    <button class="primary" disabled={cluster.read_only} onclick={() => {
      form = { name: '', partitions: 3, replication_factor: 0, configs: '' }
      createError = ''
      creating = true
    }}><Plus size={14} />Create topic</button>
  {/if}
</div>

<div class="filters">
  <input type="search" placeholder="Search topics…" bind:value={search} />
  <label class="check"><input type="checkbox" bind:checked={showInternal} /> Show internal topics</label>
</div>

{#if list.error}
  <div class="error">{list.error}</div>
{:else}
  <div class="panel" class:stale={list.loading && list.items.length > 0}>
    <table>
      <thead>
        <tr>
          <th><button class="ghost th" onclick={() => sort('name')}>Name {#if sortKey === 'name'}{#if sortDesc}<ArrowDown size={12} />{:else}<ArrowUp size={12} />{/if}{/if}</button></th>
          <th class="num"><button class="ghost th" onclick={() => sort('partitions')}>Partitions {#if sortKey === 'partitions'}{#if sortDesc}<ArrowDown size={12} />{:else}<ArrowUp size={12} />{/if}{/if}</button></th>
          <th class="num" title="Replication factor: copies of each partition"><abbr>RF</abbr></th>
          <th class="num"><button class="ghost th" onclick={() => sort('messages')} title="Reads offsets of every matching topic; slower on large clusters">Messages {#if sortKey === 'messages'}{#if sortDesc}<ArrowDown size={12} />{:else}<ArrowUp size={12} />{/if}{/if}</button></th>
          <th class="num"><button class="ghost th" onclick={() => sort('size')} title="Reads log sizes of every matching topic; slower on large clusters">Size {#if sortKey === 'size'}{#if sortDesc}<ArrowDown size={12} />{:else}<ArrowUp size={12} />{/if}{/if}</button></th>
          <th>Cleanup</th>
          <th class="num"><button class="ghost th" onclick={() => sort('under_replicated')}>Under-replicated {#if sortKey === 'under_replicated'}{#if sortDesc}<ArrowDown size={12} />{:else}<ArrowUp size={12} />{/if}{/if}</button></th>
        </tr>
      </thead>
      <tbody>
        {#each list.items as t (t.name)}
          <tr>
            <td class="grow">
              <span class="cell">
                <a class="mono" href={href(cluster.name, 'topics', t.name)} title={t.name}>{t.name}</a>
                {#if t.internal}<span class="badge">internal</span>{/if}
              </span>
            </td>
            <td class="num">{t.partitions}</td>
            <td class="num">{t.replication_factor}</td>
            <td class="num">{fmt(t.messages)}</td>
            <td class="num">{t.size >= 0 ? bytes(t.size) : '—'}</td>
            <td class="muted">{t.cleanup_policy || '—'}</td>
            <td class="num">
              {#if t.under_replicated > 0}<span class="badge warn">{t.under_replicated}</span>{:else}<span class="faint">0</span>{/if}
            </td>
          </tr>
        {:else}
          <tr><td colspan="7" class="empty">{#if list.loading}<Pending />{:else}{search.trim() ? 'No topics match your search.' : 'This cluster has no topics.'}{/if}</td></tr>
        {/each}
      </tbody>
    </table>
    <MorePages shown={list.items.length} total={list.total} step={list.size} busy={list.more || list.loading} onmore={list.loadMore} />
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
