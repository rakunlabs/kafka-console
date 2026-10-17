<script lang="ts">
  import { api, type Cluster, type TopicSummary } from '../lib/api'
  import { fmt } from '../lib/format'
  import { href, go } from '../lib/router.svelte'
  import Modal from '../lib/Modal.svelte'

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
      const av = a[sortKey]
      const bv = b[sortKey]
      const r = typeof av === 'string' ? av.localeCompare(bv as string) : (av as number) - (bv as number)
      return sortDesc ? -r : r
    })
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
  <button onclick={load} disabled={loading}>Yenile</button>
  <button class="primary" disabled={cluster.read_only} onclick={() => {
    form = { name: '', partitions: 3, replication_factor: 0, configs: '' }
    createError = ''
    creating = true
  }}>Topic oluştur</button>
</div>

<div class="row filters">
  <input type="search" placeholder="Topic ara…" bind:value={search} />
  <label class="check"><input type="checkbox" bind:checked={showInternal} /> Internal topic'leri göster</label>
</div>

{#if error}
  <div class="error">{error}</div>
{:else}
  <div class="panel">
    <table>
      <thead>
        <tr>
          <th><button class="ghost th" onclick={() => sort('name')}>Ad {sortKey === 'name' ? (sortDesc ? '↓' : '↑') : ''}</button></th>
          <th class="num"><button class="ghost th" onclick={() => sort('partitions')}>Partitions {sortKey === 'partitions' ? (sortDesc ? '↓' : '↑') : ''}</button></th>
          <th class="num">RF</th>
          <th class="num"><button class="ghost th" onclick={() => sort('messages')}>Mesaj {sortKey === 'messages' ? (sortDesc ? '↓' : '↑') : ''}</button></th>
          <th class="num"><button class="ghost th" onclick={() => sort('under_replicated')}>Under-replicated {sortKey === 'under_replicated' ? (sortDesc ? '↓' : '↑') : ''}</button></th>
        </tr>
      </thead>
      <tbody>
        {#each rows as t (t.name)}
          <tr>
            <td>
              <a class="mono" href={href(cluster.name, 'topics', t.name)}>{t.name}</a>
              {#if t.internal}<span class="badge">internal</span>{/if}
            </td>
            <td class="num">{t.partitions}</td>
            <td class="num">{t.replication_factor}</td>
            <td class="num">{fmt(t.messages)}</td>
            <td class="num">
              {#if t.under_replicated > 0}<span class="badge warn">{t.under_replicated}</span>{:else}<span class="faint">0</span>{/if}
            </td>
          </tr>
        {:else}
          <tr><td colspan="5" class="empty">{loading ? 'Yükleniyor…' : search ? 'Aramayla eşleşen topic yok.' : 'Bu cluster\'da topic yok.'}</td></tr>
        {/each}
      </tbody>
    </table>
  </div>
{/if}

<Modal title="Topic oluştur" bind:open={creating}>
  <label>Ad <input bind:value={form.name} placeholder="orders.created" /></label>
  <div class="grid2">
    <label>Partitions <input type="number" min="1" bind:value={form.partitions} /></label>
    <label>Replication factor <input type="number" min="0" bind:value={form.replication_factor} placeholder="0 = broker varsayılanı" /></label>
  </div>
  <label>Config (her satıra bir key=value)
    <textarea rows="4" bind:value={form.configs} placeholder={'retention.ms=604800000\ncleanup.policy=delete'}></textarea>
  </label>
  <p class="faint hint">Replication factor 0 olursa broker varsayılanı kullanılır.</p>
  {#if createError}<div class="error">{createError}</div>{/if}
  {#snippet footer()}
    <button onclick={() => (creating = false)}>Vazgeç</button>
    <button class="primary" disabled={busy || !form.name.trim()} onclick={create}>{busy ? 'Oluşturuluyor…' : 'Oluştur'}</button>
  {/snippet}
</Modal>

<style>
  .head { display: flex; align-items: center; gap: 10px; margin-bottom: 14px; }
  .filters { margin-bottom: 12px; }
  .filters input[type='search'] { width: 320px; }
  .check { display: flex; flex-direction: row; align-items: center; gap: 6px; font-weight: 400; }
  .th { padding: 0; font-size: 12px; color: inherit; font-weight: 500; }
  td .badge { margin-left: 6px; }
  .grid2 { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
  .hint { font-size: 12px; }
</style>
