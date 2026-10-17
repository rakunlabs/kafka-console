<script lang="ts">
  import { api, type Cluster, type ConsumeResult, type Message } from '../lib/api'
  import { fmt, time, pretty } from '../lib/format'

  let { cluster, topic, partitions }: { cluster: Cluster; topic: string; partitions: number[] } = $props()

  let start = $state('newest')
  let partition = $state('')
  let offset = $state(0)
  let timestamp = $state('')
  let limit = $state(50)
  let maxScan = $state(10000)
  let filter = $state('')
  let showFilter = $state(false)

  let result = $state<ConsumeResult>()
  let loading = $state(false)
  let error = $state('')
  let selected = $state<Message>()

  async function load() {
    loading = true
    error = ''
    try {
      result = await api.messages(cluster.name, topic, {
        start,
        partitions: partition,
        offset: start === 'offset' ? offset : undefined,
        timestamp: start === 'timestamp' && timestamp ? new Date(timestamp).getTime() : undefined,
        limit,
        max_scan: maxScan,
        filter: showFilter ? filter : undefined,
      })
      selected = result.messages[0]
    } catch (e: any) {
      error = e.message
      result = undefined
    } finally {
      loading = false
    }
  }
  load()

  const stopped: Record<string, string> = {
    end: 'Aralığın sonuna ulaşıldı',
    limit: 'Limite ulaşıldı',
    max_scan: 'Tarama sınırına ulaşıldı',
    timeout: 'Zaman aşımı',
  }

  const examples = [
    'return value.status === "FAILED"',
    'return key === "user-42"',
    'return headers["trace-id"] !== undefined',
    'return value.amount > 500 && timestamp > new Date("2026-10-01")',
  ]

  function preview(m: Message) {
    if (m.value === null) return 'null'
    return m.value.length > 160 ? m.value.slice(0, 160) + '…' : m.value
  }

  function copy(text: string) {
    navigator.clipboard?.writeText(text)
  }
</script>

<form class="controls panel" onsubmit={(e) => { e.preventDefault(); load() }}>
  <label>Başlangıç
    <select bind:value={start}>
      <option value="newest">En yeni</option>
      <option value="oldest">En eski</option>
      <option value="offset">Offset'ten</option>
      <option value="timestamp">Zamandan</option>
    </select>
  </label>
  {#if start === 'offset'}
    <label>Offset <input type="number" min="0" bind:value={offset} /></label>
  {:else if start === 'timestamp'}
    <label>Zaman <input type="datetime-local" step="1" bind:value={timestamp} /></label>
  {/if}
  <label>Partition
    <select bind:value={partition}>
      <option value="">Tümü</option>
      {#each partitions as p (p)}<option value={String(p)}>{p}</option>{/each}
    </select>
  </label>
  <label>Limit <input type="number" min="1" max="1000" bind:value={limit} /></label>
  <label>Max tarama <input type="number" min="1" bind:value={maxScan} /></label>
  <label class="check"><input type="checkbox" bind:checked={showFilter} /> JS filtre</label>
  <span class="spacer"></span>
  <button class="primary" type="submit" disabled={loading}>{loading ? 'Okunuyor…' : 'Getir'}</button>

  {#if showFilter}
    <div class="filter">
      <textarea rows="3" bind:value={filter} spellcheck="false" placeholder={'return value.status === "FAILED"'}
        onkeydown={(e) => { if (e.key === 'Enter' && (e.metaKey || e.ctrlKey)) load() }}></textarea>
      <div class="hint faint">
        Fonksiyon gövdesi yaz, eşleşen mesaj için <code>true</code> dön. Değişkenler:
        <code>value</code> (JSON ise parse edilmiş), <code>key</code>, <code>headers</code>, <code>partition</code>,
        <code>offset</code>, <code>timestamp</code> (Date), <code>raw</code>. Ctrl+Enter ile çalıştır.
        <div class="examples">
          {#each examples as ex (ex)}<button type="button" class="ghost ex mono" onclick={() => (filter = ex)}>{ex}</button>{/each}
        </div>
      </div>
    </div>
  {/if}
</form>

{#if error}
  <div class="error">{error}</div>
{:else if result}
  <div class="meta faint">
    {fmt(result.messages.length)} mesaj · {fmt(result.scanned)} tarandı · {result.elapsed_ms} ms · {stopped[result.stopped_by] ?? result.stopped_by}
    {#if result.script_errors > 0}
      <span class="badge danger" title={result.first_error}>{result.script_errors} filtre hatası</span>
      <span class="mono err">{result.first_error}</span>
    {/if}
  </div>

  <div class="split">
    <div class="panel list">
      <table>
        <thead><tr><th class="num">P</th><th class="num">Offset</th><th>Zaman</th><th>Key</th><th>Value</th></tr></thead>
        <tbody>
          {#each result.messages as m (`${m.partition}:${m.offset}`)}
            <tr class:sel={selected === m} onclick={() => (selected = m)}>
              <td class="num mono">{m.partition}</td>
              <td class="num mono">{m.offset}</td>
              <td class="mono nowrap">{time(m.timestamp)}</td>
              <td class="mono key">{m.key ?? '—'}</td>
              <td class="mono val">{preview(m)}</td>
            </tr>
          {:else}
            <tr><td colspan="5" class="empty">Mesaj bulunamadı.</td></tr>
          {/each}
        </tbody>
      </table>
    </div>

    {#if selected}
      <div class="panel detail">
        <div class="dhead">
          <b>Partition {selected.partition} · Offset {selected.offset}</b>
          <span class="faint mono">{time(selected.timestamp)}</span>
        </div>

        <section>
          <div class="label">Key {#if selected.key_encoding === 'base64'}<span class="badge">base64</span>{/if}</div>
          <pre>{selected.key ?? 'null'}</pre>
        </section>

        <section>
          <div class="label">
            Value
            <span class="badge">{selected.value_format ?? 'null'}</span>
            <span class="faint">{fmt(selected.value_bytes)} byte</span>
            {#if selected.truncated}<span class="badge warn">kesildi</span>{/if}
            <span class="spacer"></span>
            <button class="ghost small" onclick={() => copy(selected?.value ?? '')}>Kopyala</button>
          </div>
          <pre class="value">{pretty(selected.value, selected.value_format)}</pre>
        </section>

        <section>
          <div class="label">Headers <span class="faint">{selected.headers.length}</span></div>
          {#if selected.headers.length}
            <table class="headers">
              <tbody>
                {#each selected.headers as h, i (i)}
                  <tr><td class="mono">{h.key}</td><td class="mono">{h.value}</td></tr>
                {/each}
              </tbody>
            </table>
          {:else}
            <div class="faint">Header yok.</div>
          {/if}
        </section>
      </div>
    {/if}
  </div>
{:else if loading}
  <div class="empty">Okunuyor…</div>
{/if}

<style>
  .controls { display: flex; flex-wrap: wrap; align-items: flex-end; gap: 10px; padding: 12px; margin-bottom: 12px; }
  .controls input[type='number'] { width: 110px; }
  .check { display: flex; flex-direction: row; align-items: center; gap: 6px; font-weight: 400; padding-bottom: 6px; }
  .filter { flex-basis: 100%; display: grid; gap: 6px; }
  .filter textarea { width: 100%; }
  .hint { font-size: 12px; }
  .examples { display: flex; flex-wrap: wrap; gap: 4px; margin-top: 6px; }
  .ex { font-size: 12px; padding: 2px 6px; background: var(--surface-2); color: var(--text-2); }
  .meta { font-size: 12px; margin-bottom: 8px; display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }
  .err { color: var(--danger); font-size: 12px; }
  .split { display: grid; grid-template-columns: minmax(0, 1fr) minmax(0, 1fr); gap: 12px; align-items: start; }
  .list { max-height: calc(100vh - 300px); overflow: auto; }
  .list tr { cursor: pointer; }
  .list tr.sel td { background: var(--accent-soft); }
  .nowrap { white-space: nowrap; }
  .key { max-width: 140px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .val { max-width: 0; width: 100%; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; color: var(--text-2); }
  .detail { position: sticky; top: 12px; max-height: calc(100vh - 300px); overflow: auto; }
  .dhead { display: flex; justify-content: space-between; gap: 8px; padding: 10px 12px; border-bottom: 1px solid var(--border); }
  section { padding: 10px 12px; border-bottom: 1px solid var(--border); }
  section:last-child { border-bottom: 0; }
  .label { display: flex; align-items: center; gap: 6px; font-size: 12px; font-weight: 500; color: var(--text-2); margin-bottom: 6px; }
  pre {
    margin: 0;
    font: 12.5px/1.5 var(--mono);
    background: var(--surface-2);
    border-radius: var(--radius);
    padding: 8px 10px;
    white-space: pre-wrap;
    overflow-wrap: anywhere;
  }
  .small { padding: 2px 8px; font-size: 12px; }
  .headers td { padding: 4px 8px; font-size: 12px; }

  @media (max-width: 1100px) {
    .split { grid-template-columns: 1fr; }
    .detail { position: static; }
  }
</style>
