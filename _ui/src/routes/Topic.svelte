<script lang="ts">
  import { resizable } from '../lib/resizable'
  import { errorMessage } from '../lib/http'
  import Pending from '../lib/Pending.svelte'
  import { api, type Cluster, type ConfigChange, type Message } from '../lib/api'
  import { resource } from '../lib/resource.svelte'
  import { fmt, bytes } from '../lib/format'
  import { Mail, Columns3, SlidersHorizontal, Send, Users, ArrowRight, Plus, Eraser, Trash2, RefreshCw, ShieldCheck, ArrowRightLeft, BookOpen } from '@lucide/svelte'
  import { href, go, router } from '../lib/router.svelte'
  import Modal from '../lib/Modal.svelte'
  import ConfirmDialog from '../lib/ConfirmDialog.svelte'
  import Overflow from '../lib/Overflow.svelte'
  import { narrow } from '../lib/media.svelte'
  import { forgetNames } from '../lib/names'
  import ConfigTable from '../lib/ConfigTable.svelte'
  import MoreRows from '../lib/MoreRows.svelte'
  import ResourceACLs from '../lib/ResourceACLs.svelte'
  import ReassignDialog from '../lib/ReassignDialog.svelte'
  import DeleteRecordsDialog from '../lib/DeleteRecordsDialog.svelte'
  import { compactOnly, keySettings, partitionCells, partitionSkew, replicationFactor, topicTotals } from '../lib/topicStats'
  import TopicDocs from '../lib/TopicDocs.svelte'
  import Messages from './Messages.svelte'
  import Produce from './Produce.svelte'
  import TopicConsumers from './TopicConsumers.svelte'
  import PartitionMap, { type Cell } from '../lib/PartitionMap.svelte'

  let { cluster, name }: { cluster: Cluster; name: string } = $props()

  // A reload (Refresh, after growing or truncating) replaces one still running.
  const detail = resource(() => api.topic(cluster.name, name))
  const load = detail.load
  load()
  const data = $derived(detail.data)
  let partitionsShown = $state(200)
  let moving = $state(false)

  const tabs = $derived([
    { id: 'messages', label: 'Messages', icon: Mail },
    { id: 'partitions', label: 'Partitions', icon: Columns3 },
    { id: 'consumers', label: 'Consumers', icon: Users },
    { id: 'config', label: 'Config', icon: SlidersHorizontal },
    ...(cluster.read_only ? [] : [{ id: 'produce', label: 'Produce', icon: Send }]),
    ...(cluster.features.security ? [{ id: 'acl', label: 'ACL', icon: ShieldCheck }] : []),
    ...(cluster.docs ? [{ id: 'docs', label: 'Docs', icon: BookOpen }] : []),
  ])
  const tab = $derived(router.route.query.get('tab') ?? 'messages')
  let editMessage = $state<Message>()

  function edit(message: Message) {
    if (cluster.read_only) return
    editMessage = message
    go(cluster.name, 'topics', name, { tab: 'produce' })
  }

  const totals = $derived(topicTotals(data?.partitions ?? []))

  async function saveConfigs(changes: ConfigChange[]) {
    await api.alterTopicConfigs(cluster.name, name, changes)
    await load()
    // The table keeps the row in edit mode and shows why.
    if (detail.error) throw new Error(detail.error)
  }

  // Add partitions
  let growing = $state(false)
  let total = $state(0)
  let growError = $state('')
  let busy = $state(false)

  async function grow() {
    busy = true
    growError = ''
    try {
      await api.addPartitions(cluster.name, name, Number(total))
      growing = false
      load()
    } catch (e) {
      growError = errorMessage(e)
    } finally {
      busy = false
    }
  }

  // Delete records
  let truncating = $state(false)

  const settings = $derived(data?.configs ? keySettings(data.configs) : [])
  const cells = $derived<Cell[]>(partitionCells(data?.partitions ?? []))
  const skew = $derived(partitionSkew(data?.partitions ?? []))
  const rf = $derived(replicationFactor(data?.partitions ?? []))
  const compacted = $derived(compactOnly(data?.configs ?? []))
  // The "⋯" menu would be empty when every management feature is off.
  const hasManage = $derived(!cluster.read_only && (cluster.features.alter_topic || cluster.features.delete_records || cluster.features.delete_topic))

  // Delete
  let deleting = $state(false)

  async function remove() {
    await api.deleteTopic(cluster.name, name)
    forgetNames(cluster.name)
    go(cluster.name, 'topics')
  }
</script>

<div class="head">
  <h1 class="crumbed"><a class="parent" href={href(cluster.name, 'topics')}>Topics</a><span class="sep" aria-hidden="true">/</span><span class="mono">{name}</span></h1>
  {#if data?.internal}<span class="badge">internal</span>{/if}
  {#if totals.urp > 0}<span class="badge warn">{totals.urp} under-replicated</span>{/if}
  <span class="spacer"></span>
  <div class="acts">
  <button onclick={load} disabled={detail.loading}><RefreshCw size={14} class={detail.loading ? 'animate-spin' : ''} />Refresh</button>
  {#snippet manage()}
  {#if cluster.features.alter_topic}
    <button disabled={!data} onclick={() => {
      total = (data?.partitions.length ?? 0) + 1
      growError = ''
      growing = true
    }}><Plus size={14} />Add partitions</button>
  {/if}
  {#if cluster.features.delete_records}
    <button class="danger" disabled={!data || compacted}
      title={compacted ? 'Records cannot be deleted from compact-only topics' : ''} onclick={() => (truncating = true)}><Eraser size={14} />Delete records…</button>
  {/if}
  {#if cluster.features.delete_topic}
    <button class="danger" disabled={!data} onclick={() => (deleting = true)}><Trash2 size={14} />Delete topic</button>
  {/if}
  {/snippet}
  {#if hasManage}
    {#if narrow.matches}
      <Overflow label="Topic actions">{@render manage()}</Overflow>
    {:else}
      {@render manage()}
    {/if}
  {/if}
  </div>
</div>

<!-- A failed refresh keeps what is shown (and a running live tail in
     it); only a first load that failed has nothing else to show. -->
{#if detail.error && !data}
  <div class="error">{detail.error}</div>
{:else if !data}
  <div class="empty"><Pending /></div>
{:else}
  {#if detail.error}<div class="error mb-3">{detail.error}</div>{/if}
  <!-- Compact stat strip: label left, value right. The one qualifier worth
       keeping (replication, replicated size) sits beside the label; plain
       explanations moved to tooltips. -->
  <!-- Stats and the key settings share one frame: the settings row is the
       stats' footnote, not a box of its own. -->
  <div class="ticks relative mb-5 overflow-hidden border border-line-strong bg-surface">
  <section class="grid grid-cols-[repeat(auto-fit,minmax(170px,1fr))]">
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
        <span class="text-[13px] text-ink-2">{s.label}</span>
        {#if 'aside' in s && s.aside}<span class="font-mono text-[11px] text-ink-3">{s.aside}</span>{/if}
        <span class="flex-1"></span>
        <b class="text-xl leading-none font-medium tracking-tight tabular-nums">{s.value}</b>
        {#if 'link' in s}<ArrowRight size={13} class="self-center text-ink-3 transition-transform duration-150 ease-out group-hover:translate-x-0.5 group-hover:text-ink" />{/if}
      </svelte:element>
    {/each}
  </section>

  {#if settings.length}
    <a
      class="grid grid-cols-[repeat(auto-fill,minmax(190px,1fr))] border-t border-dashed border-line-dash bg-bg text-ink transition-colors hover:bg-hover hover:no-underline"
      href={href(cluster.name, 'topics', name, { tab: 'config' })}
      title="Open topic config"
    >
      {#each settings as st (st.key)}
        <div class="-mr-px flex min-w-0 items-baseline gap-2 border-r border-line px-3.5 py-2"
          title={st.note ? `${st.label}: ${st.value} ${st.note}` : undefined}>
          <span class="flex items-center gap-1.5 text-xs whitespace-nowrap text-ink-3">
            {st.label}
            {#if st.overridden}<span class="sq size-[5px] bg-ink-2" title="Set on this topic (overrides the broker default)"></span>{/if}
          </span>
          <span class="flex-1"></span>
          <span class="text-[13px] font-medium whitespace-nowrap">{st.value}</span>
        </div>
      {/each}
    </a>
  {/if}
  </div>

  <div class="tabs" role="tablist">
    {#each tabs as t (t.id)}
      <a role="tab" aria-selected={tab === t.id} class:active={tab === t.id} href={href(cluster.name, 'topics', name, { tab: t.id })}><t.icon size={15} strokeWidth={1.7} />{t.label}{#if t.id === 'consumers' && data.groups?.length}<span class="count">{data.groups.length}</span>{/if}</a>
    {/each}
  </div>

  {#if tab === 'partitions'}
    <div class="mb-3 flex flex-wrap items-center gap-x-6 gap-y-2">
      <PartitionMap {cells} size={data.partitions.length > 120 ? 10 : 16} />
      <span class="text-[13px] text-ink-2">
        Fill = messages relative to the busiest partition.
        {#if skew}<span class="text-warn">Partition {skew.id} holds {skew.pct}% of all messages; check the key distribution.</span>{/if}
      </span>
      {#if cluster.features.alter_broker && !cluster.read_only}
        <span class="flex-1"></span>
        <button onclick={() => (moving = true)}
          title="Move replicas between brokers, or balance them"><ArrowRightLeft size={14} />Move replicas…</button>
      {/if}
    </div>
    {#if moving}<ReassignDialog {cluster} topic={name} bind:open={moving} ondone={load} />{/if}
    <div class="panel">
      <table use:resizable={'topic.partitions'}>
        <thead>
          <tr><th class="num">Partition</th><th class="num">Leader</th><th>Replicas</th><th title="In-sync replicas: the replicas fully caught up with the leader"><abbr>ISR</abbr></th><th class="num">Low</th><th class="num">High</th><th class="num">Messages</th><th class="num">Size</th></tr>
        </thead>
        <tbody>
          {#each data.partitions.slice(0, partitionsShown) as p (p.id)}
            <tr>
              <td class="num mono">{p.id}</td>
              <td class="num mono">{p.leader < 0 ? '—' : p.leader}</td>
              <td class="mono">{p.replicas.join(', ')}</td>
              <td class="mono">
                {p.isr.join(', ')}
                {#if p.isr.length < p.replicas.length}<span class="badge warn" title="Under-replicated partition: fewer in-sync replicas than replicas">URP</span>{/if}
              </td>
              <td class="num mono">{fmt(p.low)}</td>
              <td class="num mono">{fmt(p.high)}</td>
              <td class="num mono">{fmt(p.high - p.low)}</td>
              <td class="num mono">{p.size >= 0 ? bytes(p.size) : '—'}</td>
            </tr>
          {/each}
        </tbody>
      </table>
      <MoreRows total={data.partitions.length} bind:shown={partitionsShown} />
    </div>
  {:else if tab === 'docs' && cluster.docs}
    <TopicDocs {cluster} topic={name} />
  {:else if tab === 'acl' && cluster.features.security}
    <ResourceACLs {cluster} type="TOPIC" name={name} />
  {:else if tab === 'consumers'}
    <TopicConsumers {cluster} topic={name} />
  {:else if tab === 'config'}
    <ConfigTable configs={data.configs ?? []} readOnly={cluster.read_only || !cluster.features.alter_topic} save={saveConfigs} />
  {:else if tab === 'produce'}
    {#key editMessage}
      <Produce {cluster} topic={name} partitions={data.partitions.map((p) => p.id)} source={editMessage} onclear={() => (editMessage = undefined)} />
    {/key}
  {:else}
    {@const p = router.route.query.get('p')}
    {@const o = router.route.query.get('o')}
    {@const at = p !== null && o !== null && /^\d+$/.test(p) && /^\d+$/.test(o) ? { partition: Number(p), offset: Number(o) } : undefined}
    {#key at && `${at.partition}:${at.offset}`}
      <Messages {cluster} topic={name} partitions={data.partitions.map((p) => p.id)} onedit={edit} {at} />
    {/key}
  {/if}
{/if}

{#if deleting}
  <ConfirmDialog title="Delete topic" bind:open={deleting} requireText={name} requirePrompt="Type the topic name to confirm"
    confirmLabel="Delete permanently" busyLabel="Deleting…" onconfirm={remove}>
    <p>Topic <b class="mono">{name}</b> and all of its messages will be permanently deleted. This cannot be undone.</p>
  </ConfirmDialog>
{/if}

<Modal title="Add partitions" bind:open={growing}>
  <p>The partition count can only be increased. For producers that pick partitions by key, the same key may now land on a different partition, breaking ordering guarantees.</p>
  <label>New total partition count <input type="number" min={(data?.partitions.length ?? 0) + 1} bind:value={total} /></label>
  {#if growError}<div class="error">{growError}</div>{/if}
  {#snippet footer()}
    <button onclick={() => (growing = false)}>Cancel</button>
    <button class="primary" disabled={busy || Number(total) <= (data?.partitions.length ?? 0)} onclick={grow}>{busy ? 'Applying…' : 'Apply'}</button>
  {/snippet}
</Modal>

{#if truncating}
  <DeleteRecordsDialog {cluster} topic={name} partitionCount={data?.partitions.length ?? 0} bind:open={truncating} ondone={load} />
{/if}
