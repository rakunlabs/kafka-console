<script lang="ts">
  import { api, type Cluster, type TopicDetail } from '../lib/api'
  import { fmt, bytes, duration, sizeIEC } from '../lib/format'
  import { Mail, Columns3, SlidersHorizontal, Send, Users, ArrowRight, Plus, Eraser, Trash2, RefreshCw } from '@lucide/svelte'
  import { href, go, router } from '../lib/router.svelte'
  import Modal from '../lib/Modal.svelte'
  import { forgetNames } from '../lib/names'
  import ConfigTable from '../lib/ConfigTable.svelte'
  import Messages from './Messages.svelte'
  import Produce from './Produce.svelte'
  import TopicConsumers from './TopicConsumers.svelte'
  import PartitionMap, { type Cell } from '../lib/PartitionMap.svelte'

  let { cluster, name }: { cluster: Cluster; name: string } = $props()

  let data = $state<TopicDetail>()
  let error = $state('')

  let loading = $state(false)

  function load() {
    loading = true
    api
      .topic(cluster.name, name)
      .then((t) => {
        data = t
        error = ''
      })
      .catch((e) => (error = e.message))
      .finally(() => (loading = false))
  }
  load()

  const tabs = [
    { id: 'messages', label: 'Messages', icon: Mail },
    { id: 'partitions', label: 'Partitions', icon: Columns3 },
    { id: 'consumers', label: 'Consumers', icon: Users },
    { id: 'config', label: 'Config', icon: SlidersHorizontal },
    { id: 'produce', label: 'Produce', icon: Send },
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

  async function saveConfigs(changes: Parameters<typeof api.alterTopicConfigs>[2]) {
    await api.alterTopicConfigs(cluster.name, name, changes)
    data = await api.topic(cluster.name, name)
  }

  // Add partitions
  let growing = $state(false)
  let total = $state(0)
  let growError = $state('')

  async function grow() {
    busy = true
    growError = ''
    try {
      await api.addPartitions(cluster.name, name, Number(total))
      growing = false
      load()
    } catch (e: any) {
      growError = e.message
    } finally {
      busy = false
    }
  }

  // Delete records
  let truncating = $state(false)
  let trunc = $state({ strategy: 'all', offset: 0, timestamp: '', partitions: '' })
  let truncError = $state('')
  let truncConfirm = $state('')
  let truncResult = $state<{ partition: number; low_watermark: number; error?: string }[]>()

  async function truncate() {
    busy = true
    truncError = ''
    try {
      truncResult = await api.deleteRecords(cluster.name, name, {
        strategy: trunc.strategy,
        offset: trunc.strategy === 'offset' ? Number(trunc.offset) : undefined,
        timestamp: trunc.strategy === 'timestamp' && trunc.timestamp ? new Date(trunc.timestamp).getTime() : undefined,
        partitions: trunc.partitions.split(',').map((s) => s.trim()).filter(Boolean).map(Number),
      })
      load()
    } catch (e: any) {
      truncError = e.message
    } finally {
      busy = false
    }
  }

  // The handful of settings people check first, read from the topic config.
  const cfg = $derived(new Map((data?.configs ?? []).map((c) => [c.key, c])))
  const num = (k: string) => {
    const v = cfg.get(k)?.value
    return v === null || v === undefined || v === '' ? undefined : Number(v)
  }
  const isSet = (k: string) => {
    const src = cfg.get(k)?.source
    return !!src && src.startsWith('DYNAMIC_TOPIC')
  }
  const settings = $derived.by(() => {
    if (!data?.configs) return []
    const policy = cfg.get('cleanup.policy')?.value ?? 'delete'
    const deletes = policy.includes('delete')
    const out: { key: string; label: string; value: string; note?: string }[] = []
    const rms = num('retention.ms')
    const rb = num('retention.bytes')
    if (deletes) {
      out.push({ key: 'retention.ms', label: 'Retention', value: rms === undefined ? '—' : duration(rms) })
      out.push({
        key: 'retention.bytes',
        label: 'Retention size',
        value: rb === undefined ? '—' : sizeIEC(rb),
        note: rb !== undefined && rb >= 0 ? 'per partition' : undefined,
      })
    }
    out.push({ key: 'cleanup.policy', label: 'Cleanup policy', value: policy })
    if (policy.includes('compact')) {
      const lag = num('min.compaction.lag.ms')
      out.push({ key: 'min.compaction.lag.ms', label: 'Min compaction lag', value: lag === undefined ? '—' : duration(lag) })
    }
    const mx = num('max.message.bytes')
    out.push({ key: 'max.message.bytes', label: 'Max message', value: mx === undefined ? '—' : sizeIEC(mx) })
    const isr = num('min.insync.replicas')
    if (isr !== undefined) out.push({ key: 'min.insync.replicas', label: 'Min in-sync replicas', value: String(isr) })
    const comp = cfg.get('compression.type')?.value
    if (comp) out.push({ key: 'compression.type', label: 'Compression', value: comp })
    const ts = cfg.get('message.timestamp.type')?.value
    if (ts) out.push({ key: 'message.timestamp.type', label: 'Timestamp', value: ts === 'CreateTime' ? 'Create time' : ts === 'LogAppendTime' ? 'Log append time' : ts })
    return out
  })
  // Partition map: fill shows each partition's share of the topic's messages
  // relative to the busiest one, so a hot or empty partition stands out.
  const partitionCells = $derived.by<Cell[]>(() => {
    const parts = data?.partitions ?? []
    const max = Math.max(1, ...parts.map((p) => p.high - p.low))
    return parts.map((p) => {
      const n = p.high - p.low
      const urp = p.isr.length < p.replicas.length
      const off = p.leader < 0
      return {
        label: String(p.id),
        tone: off ? 'danger' : urp ? 'warn' : 'ok',
        level: off || urp ? 1 : n / max,
        title: `partition ${p.id} · ${fmt(n)} messages${off ? ' · offline' : urp ? ' · under-replicated' : ''}`,
      }
    })
  })
  const skew = $derived.by(() => {
    const parts = data?.partitions ?? []
    if (parts.length < 2) return undefined
    const counts = parts.map((p) => p.high - p.low)
    const total = counts.reduce((a, b) => a + b, 0)
    if (!total) return undefined
    const top = Math.max(...counts)
    const share = top / total
    const fair = 1 / parts.length
    return share >= fair * 2 ? { id: parts[counts.indexOf(top)].id, pct: Math.round(share * 100) } : undefined
  })

  const rf = $derived(Math.max(0, ...(data?.partitions.map((p) => p.replicas.length) ?? [0])))

  const compacted = $derived((data?.configs ?? []).some((c) => c.key === 'cleanup.policy' && c.value?.includes('compact') && !c.value?.includes('delete')))

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
      forgetNames(cluster.name)
      deleting = false
      go(cluster.name, 'topics')
    } catch (e: any) {
      deleteError = e.message
    } finally {
      busy = false
    }
  }
</script>

<div class="head">
  <h1 class="crumbed"><a class="parent" href={href(cluster.name, 'topics')}>Topics</a><span class="sep" aria-hidden="true">/</span><span class="mono">{name}</span></h1>
  {#if data?.internal}<span class="badge">internal</span>{/if}
  {#if totals.urp > 0}<span class="badge warn">{totals.urp} under-replicated</span>{/if}
  <span class="spacer"></span>
  <button onclick={load} disabled={loading}><RefreshCw size={14} class={loading ? 'animate-spin' : ''} />Refresh</button>
  <button disabled={cluster.read_only || !data} onclick={() => {
    total = (data?.partitions.length ?? 0) + 1
    growError = ''
    growing = true
  }}><Plus size={14} />Add partitions</button>
  <button class="danger" disabled={cluster.read_only || !data || compacted}
    title={compacted ? 'Records cannot be deleted from compact-only topics' : ''} onclick={() => {
    trunc = { strategy: 'all', offset: 0, timestamp: '', partitions: '' }
    truncError = ''
    truncConfirm = ''
    truncResult = undefined
    truncating = true
  }}><Eraser size={14} />Delete records…</button>
  <button class="danger" disabled={cluster.read_only || !data} onclick={() => {
    confirmName = ''
    deleteError = ''
    deleting = true
  }}><Trash2 size={14} />Delete topic</button>
</div>

{#if error}
  <div class="error">{error}</div>
{:else if !data}
  <div class="empty">Loading…</div>
{:else}
  <!-- Compact stat strip: label left, value right. The one qualifier worth
       keeping (replication, replicated size) sits beside the label; plain
       explanations moved to tooltips. -->
  <section class="corners mb-3 grid grid-cols-[repeat(auto-fit,minmax(170px,1fr))] overflow-hidden border border-line-strong bg-surface [--c:var(--text-2)] [--k:7px]">
    {#each [
      { label: 'Messages', value: fmt(totals.messages), help: 'Sum of high minus low watermark across partitions' },
      { label: 'Partitions', value: fmt(data.partitions.length), aside: `RF ${rf}`, help: `Replication factor ${rf}` },
      { label: 'Size', value: data.size >= 0 ? bytes(data.size) : '—', aside: data.replicated_size > data.size ? `${bytes(data.replicated_size)} total` : undefined,
        help: data.replicated_size > data.size ? `${bytes(data.size)} on leaders, ${bytes(data.replicated_size)} including replicas` : 'On disk, one copy' },
      { label: 'Consumers', value: fmt(data.groups?.length ?? 0), help: data.groups?.length ? 'Consumer groups with offsets on this topic' : 'No group has committed offsets yet',
        link: href(cluster.name, 'topics', name, { tab: 'consumers' }) },
    ] as s (s.label)}
      <svelte:element
        this={'link' in s ? 'a' : 'div'}
        href={'link' in s ? s.link : undefined}
        title={s.help}
        class="group -mr-px flex items-baseline gap-2 border-r border-line px-4 py-2.5 text-ink {'link' in s ? 'hover:bg-hover hover:no-underline' : ''}"
      >
        <span class="text-[12.5px] text-ink-2">{s.label}</span>
        {#if 'aside' in s && s.aside}<span class="font-mono text-[11px] text-ink-3">{s.aside}</span>{/if}
        <span class="flex-1"></span>
        <b class="text-[19px] leading-none font-[450] tracking-tight tabular-nums">{s.value}</b>
        {#if 'link' in s}<ArrowRight size={13} class="self-center text-ink-3 transition-transform duration-150 ease-out group-hover:translate-x-0.5 group-hover:text-ink" />{/if}
      </svelte:element>
    {/each}
  </section>

  {#if settings.length}
    <a
      class="mb-3 flex flex-wrap overflow-hidden border border-line-strong bg-surface text-ink transition-colors hover:border-ink-3 hover:bg-hover hover:no-underline"
      href={href(cluster.name, 'topics', name, { tab: 'config' })}
      title="Open topic config"
    >
      {#each settings as st (st.key)}
        <div class="-mr-px -mb-px flex min-w-[150px] flex-1 items-baseline gap-2 border-r border-b border-line px-3.5 py-2"
          title={st.note ? `${st.label}: ${st.value} ${st.note}` : undefined}>
          <span class="flex items-center gap-1.5 text-xs whitespace-nowrap text-ink-3">
            {st.label}
            {#if isSet(st.key)}<span class="sq size-[5px] bg-ink-2" title="Set on this topic (overrides the broker default)"></span>{/if}
          </span>
          <span class="flex-1"></span>
          <span class="text-[13px] font-medium whitespace-nowrap">{st.value}</span>
        </div>
      {/each}
    </a>
  {/if}

  <div class="mb-5"></div>

  <div class="tabs" role="tablist">
    {#each tabs as t (t.id)}
      <a role="tab" aria-selected={tab === t.id} class:active={tab === t.id} href={href(cluster.name, 'topics', name, { tab: t.id })}><t.icon size={15} strokeWidth={1.7} />{t.label}{#if t.id === 'consumers' && data.groups?.length}<span class="count">{data.groups.length}</span>{/if}</a>
    {/each}
  </div>

  {#if tab === 'partitions'}
    <div class="panel mb-3 flex flex-wrap items-center gap-x-6 gap-y-2 px-4 py-3">
      <PartitionMap cells={partitionCells} size={data.partitions.length > 120 ? 10 : 16} />
      <span class="text-[12.5px] text-ink-2">
        Fill = messages relative to the busiest partition.
        {#if skew}<span class="text-warn">Partition {skew.id} holds {skew.pct}% of all messages; check the key distribution.</span>{/if}
      </span>
    </div>
    <div class="panel">
      <table>
        <thead>
          <tr><th class="num">Partition</th><th class="num">Leader</th><th>Replicas</th><th>ISR</th><th class="num">Low</th><th class="num">High</th><th class="num">Messages</th><th class="num">Size</th></tr>
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
              <td class="num mono">{p.size >= 0 ? bytes(p.size) : '—'}</td>
            </tr>
          {/each}
        </tbody>
      </table>
    </div>
  {:else if tab === 'consumers'}
    <TopicConsumers {cluster} topic={name} />
  {:else if tab === 'config'}
    <ConfigTable configs={data.configs ?? []} readOnly={cluster.read_only} save={saveConfigs} />
  {:else if tab === 'produce'}
    <Produce {cluster} topic={name} partitions={data.partitions.map((p) => p.id)} />
  {:else}
    <Messages {cluster} topic={name} partitions={data.partitions.map((p) => p.id)} />
  {/if}
{/if}

<Modal title="Delete topic" bind:open={deleting}>
  <p>Topic <b class="mono">{name}</b> and all of its messages will be permanently deleted. This cannot be undone.</p>
  <label>Type the topic name to confirm <input bind:value={confirmName} class="mono" /></label>
  {#if deleteError}<div class="error">{deleteError}</div>{/if}
  {#snippet footer()}
    <button onclick={() => (deleting = false)}>Cancel</button>
    <button class="danger solid" disabled={busy || confirmName !== name} onclick={remove}>{busy ? 'Deleting…' : 'Delete permanently'}</button>
  {/snippet}
</Modal>

<Modal title="Add partitions" bind:open={growing}>
  <p>The partition count can only be increased. For producers that pick partitions by key, the same key may now land on a different partition, breaking ordering guarantees.</p>
  <label>New total partition count <input type="number" min={(data?.partitions.length ?? 0) + 1} bind:value={total} /></label>
  {#if growError}<div class="error">{growError}</div>{/if}
  {#snippet footer()}
    <button onclick={() => (growing = false)}>Cancel</button>
    <button class="primary" disabled={busy || Number(total) <= (data?.partitions.length ?? 0)} onclick={grow}>{busy ? 'Applying…' : 'Apply'}</button>
  {/snippet}
</Modal>

<Modal title="Delete records" bind:open={truncating} width={520}>
  {#if truncResult}
    <div class="ok">Records deleted. New start offsets:</div>
    <div class="panel">
      <table>
        <thead><tr><th class="num">P</th><th class="num">New low</th><th></th></tr></thead>
        <tbody>
          {#each truncResult as r (r.partition)}
            <tr><td class="num mono">{r.partition}</td><td class="num mono">{fmt(r.low_watermark)}</td><td>{#if r.error}<span class="badge danger">{r.error}</span>{/if}</td></tr>
          {/each}
        </tbody>
      </table>
    </div>
  {:else}
    <p>All records <b>before</b> the target offset in the selected partitions will be permanently deleted. This cannot be undone.</p>
    <div class="grid2">
      <label>Range to delete
        <select bind:value={trunc.strategy}>
          <option value="all">All existing records</option>
          <option value="offset">Up to this offset</option>
          <option value="timestamp">Up to this time</option>
        </select>
      </label>
      {#if trunc.strategy === 'offset'}
        <label>Offset <input type="number" min="0" bind:value={trunc.offset} /></label>
      {:else if trunc.strategy === 'timestamp'}
        <label>Time <input type="datetime-local" step="1" bind:value={trunc.timestamp} /></label>
      {:else}
        <div></div>
      {/if}
    </div>
    <label>Partitions (empty = all) <input placeholder="0, 1, 2" bind:value={trunc.partitions} /></label>
    <label>Type the topic name to confirm <input bind:value={truncConfirm} class="mono" /></label>
  {/if}
  {#if truncError}<div class="error">{truncError}</div>{/if}
  {#snippet footer()}
    <button onclick={() => (truncating = false)}>{truncResult ? 'Close' : 'Cancel'}</button>
    {#if !truncResult}
      <button class="danger solid" disabled={busy || truncConfirm !== name} onclick={truncate}>{busy ? 'Deleting…' : 'Delete records'}</button>
    {/if}
  {/snippet}
</Modal>
