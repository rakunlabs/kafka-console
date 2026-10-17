<script lang="ts">
  import { api, type Cluster } from '../lib/api'
  import { time } from '../lib/format'

  let { cluster, topic, partitions }: { cluster: Cluster; topic: string; partitions: number[] } = $props()

  let key = $state('')
  let nullKey = $state(false)
  let value = $state('')
  let partition = $state('')
  let headers = $state<{ key: string; value: string }[]>([])

  let busy = $state(false)
  let error = $state('')
  let sent = $state<{ partition: number; offset: number; timestamp: string }[]>([])

  const jsonError = $derived.by(() => {
    const v = value.trim()
    if (!v || !(v.startsWith('{') || v.startsWith('['))) return ''
    try {
      JSON.parse(v)
      return ''
    } catch (e: any) {
      return e.message
    }
  })

  function format() {
    try {
      value = JSON.stringify(JSON.parse(value), null, 2)
    } catch {}
  }

  async function send() {
    busy = true
    error = ''
    try {
      const res = await api.produce(cluster.name, topic, {
        key: nullKey ? null : key,
        value,
        headers: headers.filter((h) => h.key.trim()),
        partition: partition === '' ? null : Number(partition),
      })
      sent = [res, ...sent].slice(0, 10)
    } catch (e: any) {
      error = e.message
    } finally {
      busy = false
    }
  }
</script>

{#if cluster.read_only}
  <div class="error">Bu cluster read-only olarak yapılandırılmış; mesaj gönderilemez.</div>
{:else}
  <div class="layout">
    <form class="panel form" onsubmit={(e) => { e.preventDefault(); send() }}>
      <div class="grid">
        <label>Key
          <input bind:value={key} disabled={nullKey} class="mono" placeholder="(boş)" />
        </label>
        <label>Partition
          <select bind:value={partition}>
            <option value="">Otomatik (key'e göre)</option>
            {#each partitions as p (p)}<option value={String(p)}>{p}</option>{/each}
          </select>
        </label>
      </div>
      <label class="check"><input type="checkbox" bind:checked={nullKey} /> Key gönderme (null)</label>

      <label>
        <span class="row">Value <span class="spacer"></span>
          <button type="button" class="ghost small" onclick={format} disabled={!!jsonError || !value.trim()}>JSON formatla</button>
        </span>
        <textarea rows="12" bind:value spellcheck="false" placeholder={'{"id": 1, "status": "NEW"}'}></textarea>
      </label>
      {#if jsonError}<div class="warn">JSON geçersiz görünüyor: {jsonError}</div>{/if}

      <div>
        <div class="row hhead"><span class="lbl">Headers</span><span class="spacer"></span>
          <button type="button" class="ghost small" onclick={() => (headers = [...headers, { key: '', value: '' }])}>+ Header ekle</button>
        </div>
        {#each headers as h, i (i)}
          <div class="row hrow">
            <input placeholder="key" class="mono" bind:value={h.key} />
            <input placeholder="value" class="mono" bind:value={h.value} />
            <button type="button" class="ghost" aria-label="Header'ı kaldır" onclick={() => (headers = headers.filter((_, j) => j !== i))}>✕</button>
          </div>
        {/each}
      </div>

      {#if error}<div class="error">{error}</div>{/if}

      <div class="row">
        {#if cluster.production}<span class="prodnote">Production cluster'a gönderiyorsun.</span>{/if}
        <span class="spacer"></span>
        <button class="primary" type="submit" disabled={busy}>{busy ? 'Gönderiliyor…' : 'Gönder'}</button>
      </div>
    </form>

    <div class="panel">
      <table>
        <thead><tr><th colspan="3">Bu oturumda gönderilenler</th></tr></thead>
        <tbody>
          {#each sent as s (`${s.partition}:${s.offset}`)}
            <tr><td class="mono">p{s.partition}</td><td class="mono num">{s.offset}</td><td class="mono faint">{time(s.timestamp)}</td></tr>
          {:else}
            <tr><td colspan="3" class="faint">Henüz mesaj gönderilmedi.</td></tr>
          {/each}
        </tbody>
      </table>
    </div>
  </div>
{/if}

<style>
  .layout { display: grid; grid-template-columns: minmax(0, 2fr) minmax(0, 1fr); gap: 12px; align-items: start; }
  .form { padding: 14px; display: grid; gap: 12px; }
  .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
  .check { display: flex; flex-direction: row; align-items: center; gap: 6px; font-weight: 400; }
  .small { padding: 2px 8px; font-size: 12px; }
  .warn { font-size: 12px; color: var(--warn); }
  .lbl { font-size: 12px; font-weight: 500; color: var(--text-2); }
  .hhead { margin-bottom: 6px; }
  .hrow { margin-bottom: 6px; }
  .hrow input { flex: 1; }
  .prodnote { font-size: 12px; color: var(--danger); }
  @media (max-width: 1000px) { .layout { grid-template-columns: 1fr; } }
</style>
