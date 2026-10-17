<script lang="ts">
  import { api, type Cluster, type TopicDetail } from '../lib/api'
  import { fmt } from '../lib/format'
  import { href, go, router } from '../lib/router.svelte'
  import Modal from '../lib/Modal.svelte'
  import Messages from './Messages.svelte'
  import Produce from './Produce.svelte'

  let { cluster, name }: { cluster: Cluster; name: string } = $props()

  let data = $state<TopicDetail>()
  let error = $state('')

  function load() {
    api
      .topic(cluster.name, name)
      .then((t) => (data = t))
      .catch((e) => (error = e.message))
  }
  load()

  const tabs = [
    { id: 'messages', label: 'Mesajlar' },
    { id: 'partitions', label: 'Partitions' },
    { id: 'config', label: 'Config' },
    { id: 'produce', label: 'Mesaj gönder' },
  ]
  const tab = $derived(router.route.query.get('tab') ?? 'messages')

  const totals = $derived.by(() => {
    if (!data) return { messages: 0, urp: 0 }
    let messages = 0
    let urp = 0
    for (const p of data.partitions) {
      messages += p.high - p.low
      if (p.isr.length < p.replicas.length) urp++
    }
    return { messages, urp }
  })

  let configFilter = $state('')
  let onlyOverrides = $state(true)
  const configs = $derived(
    (data?.configs ?? []).filter(
      (c) =>
        (!onlyOverrides || c.source !== 'DEFAULT_CONFIG') &&
        (!configFilter || c.key.toLowerCase().includes(configFilter.toLowerCase())),
    ),
  )

  // Delete
  let deleting = $state(false)
  let confirmName = $state('')
  let deleteError = $state('')
  let busy = $state(false)

  async function remove() {
    busy = true
    deleteError = ''
    try {
      await api.deleteTopic(cluster.name, name)
      deleting = false
      go(cluster.name, 'topics')
    } catch (e: any) {
      deleteError = e.message
    } finally {
      busy = false
    }
  }
</script>

<div class="crumb"><a href={href(cluster.name, 'topics')}>Topics</a> /</div>
<div class="head">
  <h1 class="mono">{name}</h1>
  {#if data?.internal}<span class="badge">internal</span>{/if}
  <span class="spacer"></span>
  <button class="danger" disabled={cluster.read_only || !data} onclick={() => {
    confirmName = ''
    deleteError = ''
    deleting = true
  }}>Topic'i sil</button>
</div>

{#if error}
  <div class="error">{error}</div>
{:else if !data}
  <div class="empty">Yükleniyor…</div>
{:else}
  <div class="summary muted">
    <span><b>{data.partitions.length}</b> partition</span>
    <span><b>RF {Math.max(0, ...data.partitions.map((p) => p.replicas.length))}</b></span>
    <span><b>{fmt(totals.messages)}</b> mesaj</span>
    {#if totals.urp > 0}<span class="badge warn">{totals.urp} under-replicated</span>{/if}
    {#if data.groups?.length}
      <span>Consumer group'lar:
        {#each data.groups as g, i (g)}<a href={href(cluster.name, 'groups', g)}>{g}</a>{i < data.groups.length - 1 ? ', ' : ''}{/each}
      </span>
    {/if}
  </div>

  <div class="tabs" role="tablist">
    {#each tabs as t (t.id)}
      <a role="tab" aria-selected={tab === t.id} class:active={tab === t.id} href={href(cluster.name, 'topics', name, { tab: t.id })}>{t.label}</a>
    {/each}
  </div>

  {#if tab === 'partitions'}
    <div class="panel">
      <table>
        <thead>
          <tr><th class="num">Partition</th><th class="num">Leader</th><th>Replicas</th><th>ISR</th><th class="num">Low</th><th class="num">High</th><th class="num">Mesaj</th></tr>
        </thead>
        <tbody>
          {#each data.partitions as p (p.id)}
            <tr>
              <td class="num mono">{p.id}</td>
              <td class="num mono">{p.leader < 0 ? '—' : p.leader}</td>
              <td class="mono">{p.replicas.join(', ')}</td>
              <td class="mono">
                {p.isr.join(', ')}
                {#if p.isr.length < p.replicas.length}<span class="badge warn">URP</span>{/if}
              </td>
              <td class="num mono">{fmt(p.low)}</td>
              <td class="num mono">{fmt(p.high)}</td>
              <td class="num mono">{fmt(p.high - p.low)}</td>
            </tr>
          {/each}
        </tbody>
      </table>
    </div>
  {:else if tab === 'config'}
    <div class="row filters">
      <input type="search" placeholder="Config ara…" bind:value={configFilter} />
      <label class="check"><input type="checkbox" bind:checked={onlyOverrides} /> Sadece varsayılandan farklı olanlar</label>
    </div>
    <div class="panel">
      <table>
        <thead><tr><th>Key</th><th>Value</th><th>Source</th></tr></thead>
        <tbody>
          {#each configs as c (c.key)}
            <tr>
              <td class="mono">{c.key}</td>
              <td class="mono">{c.sensitive ? '••••••' : (c.value ?? '—')}</td>
              <td class="faint">{c.source}</td>
            </tr>
          {:else}
            <tr><td colspan="3" class="empty">Gösterilecek config yok.</td></tr>
          {/each}
        </tbody>
      </table>
    </div>
  {:else if tab === 'produce'}
    <Produce {cluster} topic={name} partitions={data.partitions.map((p) => p.id)} />
  {:else}
    <Messages {cluster} topic={name} partitions={data.partitions.map((p) => p.id)} />
  {/if}
{/if}

<Modal title="Topic'i sil" bind:open={deleting}>
  <p><b class="mono">{name}</b> topic'i ve içindeki tüm mesajlar kalıcı olarak silinecek. Bu işlem geri alınamaz.</p>
  <label>Onaylamak için topic adını yaz <input bind:value={confirmName} class="mono" /></label>
  {#if deleteError}<div class="error">{deleteError}</div>{/if}
  {#snippet footer()}
    <button onclick={() => (deleting = false)}>Vazgeç</button>
    <button class="danger solid" disabled={busy || confirmName !== name} onclick={remove}>{busy ? 'Siliniyor…' : 'Kalıcı olarak sil'}</button>
  {/snippet}
</Modal>

<style>
  .crumb { font-size: 12px; color: var(--text-3); }
  .head { display: flex; align-items: center; gap: 10px; margin: 2px 0 10px; }
  h1 { overflow-wrap: anywhere; }
  .summary { display: flex; flex-wrap: wrap; gap: 6px 18px; font-size: 13px; margin-bottom: 16px; }
  .summary b { color: var(--text); font-weight: 600; }
  .tabs { display: flex; gap: 4px; border-bottom: 1px solid var(--border); margin-bottom: 16px; overflow-x: auto; }
  .tabs a {
    white-space: nowrap;
    padding: 8px 12px;
    color: var(--text-2);
    font-weight: 500;
    border-bottom: 2px solid transparent;
    margin-bottom: -1px;
  }
  .tabs a:hover { color: var(--text); text-decoration: none; }
  .tabs a.active { color: var(--text); border-bottom-color: var(--accent); }
  .filters { margin-bottom: 12px; }
  .filters input[type='search'] { width: 320px; }
  .check { display: flex; flex-direction: row; align-items: center; gap: 6px; font-weight: 400; }
  td .badge { margin-left: 6px; }
</style>
