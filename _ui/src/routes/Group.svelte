<script lang="ts">
  import { api, type Cluster, type GroupDetail, type ResetPlan } from '../lib/api'
  import { fmt, stateBadge } from '../lib/format'
  import { href, go } from '../lib/router.svelte'
  import LagBar from '../lib/LagBar.svelte'
  import Modal from '../lib/Modal.svelte'

  let { cluster, name }: { cluster: Cluster; name: string } = $props()

  let data = $state<GroupDetail>()
  let error = $state('')
  let loading = $state(false)

  async function load() {
    loading = true
    error = ''
    try {
      data = await api.group(cluster.name, name)
    } catch (e: any) {
      error = e.message
    } finally {
      loading = false
    }
  }
  load()

  const topics = $derived([...new Set((data?.partitions ?? []).map((p) => p.topic))])
  const byTopic = $derived(topics.map((t) => ({ topic: t, rows: data!.partitions.filter((p) => p.topic === t) })))

  // Offset reset
  let resetting = $state(false)
  let reset = $state({ topic: '', strategy: 'latest', offset: 0, timestamp: '', partitions: '' })
  let plan = $state<ResetPlan[]>()
  let resetError = $state('')
  let busy = $state(false)
  let done = $state(false)

  function openReset(topic: string) {
    reset = { topic, strategy: 'latest', offset: 0, timestamp: '', partitions: '' }
    plan = undefined
    resetError = ''
    done = false
    resetting = true
  }

  function body(dry: boolean) {
    return {
      topic: reset.topic,
      strategy: reset.strategy,
      offset: reset.strategy === 'offset' ? Number(reset.offset) : undefined,
      timestamp: reset.strategy === 'timestamp' && reset.timestamp ? new Date(reset.timestamp).getTime() : undefined,
      partitions: reset.partitions
        .split(',')
        .map((s) => s.trim())
        .filter(Boolean)
        .map(Number),
      dry_run: dry,
    }
  }

  async function preview() {
    busy = true
    resetError = ''
    try {
      plan = (await api.resetOffsets(cluster.name, name, body(true))).partitions
    } catch (e: any) {
      resetError = e.message
    } finally {
      busy = false
    }
  }

  async function apply() {
    busy = true
    resetError = ''
    try {
      plan = (await api.resetOffsets(cluster.name, name, body(false))).partitions
      done = true
      load()
    } catch (e: any) {
      resetError = e.message
    } finally {
      busy = false
    }
  }

  // Delete group
  let deleting = $state(false)
  let deleteError = $state('')

  async function remove() {
    busy = true
    deleteError = ''
    try {
      await api.deleteGroup(cluster.name, name)
      deleting = false
      go(cluster.name, 'groups')
    } catch (e: any) {
      deleteError = e.message
    } finally {
      busy = false
    }
  }
</script>

<div class="crumb"><a href={href(cluster.name, 'groups')}>Consumer groups</a> /</div>
<div class="head">
  <h1 class="mono">{name}</h1>
  {#if data}<span class="badge {stateBadge(data.state)}">{data.state}</span>{/if}
  <span class="spacer"></span>
  <button onclick={load} disabled={loading}>Yenile</button>
  <button class="danger" disabled={cluster.read_only || !data || data.members.length > 0}
    title={data && data.members.length > 0 ? 'Aktif üyesi olan group silinemez' : ''}
    onclick={() => { deleteError = ''; deleting = true }}>Group'u sil</button>
</div>

{#if error}
  <div class="error">{error}</div>
{:else if !data}
  <div class="empty">Yükleniyor…</div>
{:else}
  <div class="summary muted">
    <span>Toplam lag <b>{fmt(data.lag)}</b></span>
    <span><b>{data.members.length}</b> üye</span>
    <span>Protokol <b>{data.protocol || '—'}</b></span>
    <span>Coordinator <b>{data.coordinator}</b></span>
  </div>

  {#each byTopic as t (t.topic)}
    <div class="thead">
      <h2><a class="mono" href={href(cluster.name, 'topics', t.topic)}>{t.topic}</a></h2>
      <span class="faint">lag {fmt(t.rows.reduce((s, r) => s + Math.max(r.lag, 0), 0))}</span>
      <span class="spacer"></span>
      <button disabled={cluster.read_only} onclick={() => openReset(t.topic)}>Offset reset…</button>
    </div>
    <div class="panel">
      <table>
        <thead>
          <tr><th class="num">P</th><th class="bar">İlerleme</th><th class="num">Low</th><th class="num">Committed</th><th class="num">High</th><th class="num">Lag</th><th>Üye</th></tr>
        </thead>
        <tbody>
          {#each t.rows as p (p.partition)}
            <tr>
              <td class="num mono">{p.partition}</td>
              <td class="bar"><LagBar low={p.low} high={p.high} committed={p.committed} /></td>
              <td class="num mono faint">{fmt(p.low)}</td>
              <td class="num mono">{fmt(p.committed)}</td>
              <td class="num mono">{fmt(p.high)}</td>
              <td class="num mono" class:lag={p.lag > 0}>{fmt(p.lag)}</td>
              <td class="mono small">{p.client_id ? `${p.client_id} · ${p.host}` : '—'}{#if p.error}<span class="badge danger" title={p.error}>hata</span>{/if}</td>
            </tr>
          {/each}
        </tbody>
      </table>
    </div>
  {:else}
    <div class="panel empty">Bu group'un commit edilmiş offset'i yok.</div>
  {/each}

  <h2 class="mtop">Üyeler</h2>
  <div class="panel">
    <table>
      <thead><tr><th>Client ID</th><th>Host</th><th>Member ID</th></tr></thead>
      <tbody>
        {#each data.members as m (m.id)}
          <tr><td class="mono">{m.client_id}</td><td class="mono">{m.host}</td><td class="mono faint small">{m.id}</td></tr>
        {:else}
          <tr><td colspan="3" class="faint">Aktif üye yok.</td></tr>
        {/each}
      </tbody>
    </table>
  </div>
{/if}

<Modal title="Offset reset: {reset.topic}" bind:open={resetting} width={560}>
  {#if data && data.members.length > 0}
    <div class="error">Group'un {data.members.length} aktif üyesi var. Offset reset için önce consumer'ları durdur. Önizleme yine de yapılabilir.</div>
  {/if}
  <div class="grid2">
    <label>Hedef
      <select bind:value={reset.strategy} onchange={() => (plan = undefined)}>
        <option value="earliest">En eski (earliest)</option>
        <option value="latest">En yeni (latest)</option>
        <option value="offset">Belirli offset</option>
        <option value="timestamp">Zaman</option>
      </select>
    </label>
    {#if reset.strategy === 'offset'}
      <label>Offset <input type="number" min="0" bind:value={reset.offset} oninput={() => (plan = undefined)} /></label>
    {:else if reset.strategy === 'timestamp'}
      <label>Zaman <input type="datetime-local" step="1" bind:value={reset.timestamp} oninput={() => (plan = undefined)} /></label>
    {:else}
      <div></div>
    {/if}
  </div>
  <label>Partition'lar (boş = hepsi) <input placeholder="0, 1, 2" bind:value={reset.partitions} oninput={() => (plan = undefined)} /></label>

  {#if plan}
    <div class="panel plan">
      <table>
        <thead><tr><th class="num">P</th><th class="num">Şu an</th><th class="num">Yeni</th><th class="num">Fark</th></tr></thead>
        <tbody>
          {#each plan as p (p.partition)}
            <tr>
              <td class="num mono">{p.partition}</td>
              <td class="num mono">{fmt(p.from)}</td>
              <td class="num mono">{fmt(p.to)}</td>
              <td class="num mono">{p.from < 0 ? '—' : (p.to - p.from > 0 ? '+' : '') + (p.to - p.from).toLocaleString()}</td>
            </tr>
          {/each}
        </tbody>
      </table>
    </div>
  {/if}
  {#if done}<div class="ok">Offset'ler commit edildi.</div>{/if}
  {#if resetError}<div class="error">{resetError}</div>{/if}

  {#snippet footer()}
    <button onclick={() => (resetting = false)}>{done ? 'Kapat' : 'Vazgeç'}</button>
    {#if !done}
      <button onclick={preview} disabled={busy}>Önizle</button>
      <button class="danger solid" disabled={busy || !plan || (data?.members.length ?? 0) > 0} onclick={apply}>Uygula</button>
    {/if}
  {/snippet}
</Modal>

<Modal title="Group'u sil" bind:open={deleting}>
  <p><b class="mono">{name}</b> group'u ve commit edilmiş tüm offset'leri silinecek.</p>
  {#if deleteError}<div class="error">{deleteError}</div>{/if}
  {#snippet footer()}
    <button onclick={() => (deleting = false)}>Vazgeç</button>
    <button class="danger solid" disabled={busy} onclick={remove}>Sil</button>
  {/snippet}
</Modal>

<style>
  .crumb { font-size: 12px; color: var(--text-3); }
  .head { display: flex; align-items: center; gap: 10px; margin: 2px 0 10px; }
  h1 { overflow-wrap: anywhere; }
  .summary { display: flex; flex-wrap: wrap; gap: 6px 18px; font-size: 13px; margin-bottom: 18px; }
  .summary b { color: var(--text); font-weight: 600; }
  .thead { display: flex; align-items: center; gap: 10px; margin: 18px 0 8px; }
  .mtop { margin: 24px 0 8px; }
  th.bar, td.bar { width: 30%; }
  .lag { font-weight: 600; color: var(--accent); }
  .small { font-size: 12px; }
  .grid2 { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
  .plan { max-height: 260px; overflow: auto; }
  .ok { color: var(--ok); font-size: 13px; }
</style>
