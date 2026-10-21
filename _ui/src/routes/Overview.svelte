<script lang="ts">
  import Pending from '../lib/Pending.svelte'
  import { untrack } from 'svelte'
  import { api, type Cluster, type Overview, type DiskUsage } from '../lib/api'
  import { fmt, bytes } from '../lib/format'
  import { href } from '../lib/router.svelte'
  import { cached, remember } from '../lib/cache'
  import { ArrowRight, RefreshCw, Check, Copy } from '@lucide/svelte'

  let { cluster }: { cluster: Cluster } = $props()

  // The page remounts per cluster ({#key} in App), so the first value is the one.
  const key = untrack(() => `overview:${cluster.name}`)
  const diskKey = untrack(() => `disk-usage:${cluster.name}`)
  // A revisit shows the last result at once and refreshes behind it.
  let data = $state<Overview | undefined>(cached(key))
  let error = $state('')
  let loading = $state(false)
  // Disk usage scans every broker's log dirs, which can take far longer than
  // the rest, so it loads on its own and fills its tile when it arrives.
  let disk = $state<DiskUsage | undefined>(cached(diskKey))
  let diskLoading = $state(false)
  let diskFailed = $state(false)

  // The server keeps both for a few seconds; Refresh asks it for new ones.
  function load(fresh = false) {
    loading = true
    api
      .overview(cluster.name, fresh)
      .then((o) => {
        data = remember(key, o)
        error = ''
      })
      .catch((e) => (error = e.message))
      .finally(() => (loading = false))

    diskLoading = true
    api
      .diskUsage(cluster.name, fresh)
      .then((d) => {
        disk = remember(diskKey, d)
        diskFailed = false
      })
      .catch(() => (diskFailed = true))
      .finally(() => (diskLoading = false))
  }
  load()

  let copied = $state(false)
  function copyBootstrap() {
    navigator.clipboard?.writeText(cluster.brokers.join(','))
    copied = true
    setTimeout(() => (copied = false), 1200)
  }

  const health = $derived.by(() => {
    if (!data) return undefined
    if (data.offline_partitions > 0)
      return { cls: 'bg-danger-soft text-danger', text: `${fmt(data.offline_partitions)} partitions offline` }
    if (data.under_replicated > 0)
      return { cls: 'bg-warn-soft text-warn', text: `${fmt(data.under_replicated)} partitions under-replicated` }
    return { cls: 'bg-ok-soft text-ok', text: 'All partitions in sync' }
  })
  // Only topics with a problem come from the server, so this stays small on
  // clusters with thousands of topics. Worst first, then by name.
  const problems = $derived.by(() =>
    (data?.problems ?? [])
      .map((t) => {
        const offline: number[] = []
        const under: number[] = []
        for (let i = 0; i < t.partitions.length; i++) {
          if (t.partitions[i] === 'x') offline.push(i)
          else if (t.partitions[i] === 'u') under.push(i)
        }
        return { topic: t.topic, total: t.partitions.length, offline, under }
      })
      .sort((a, b) => b.offline.length - a.offline.length || b.under.length - a.under.length || a.topic.localeCompare(b.topic)),
  )
  const problemLimit = 50
  let showAllProblems = $state(false)
  const shownProblems = $derived(showAllProblems ? problems : problems.slice(0, problemLimit))
  const ids = (xs: number[]) => (xs.length > 8 ? `${xs.slice(0, 8).join(', ')} +${xs.length - 8}` : xs.join(', '))

  const maxLeaders = $derived(Math.max(1, ...(data?.brokers.map((b) => b.leaders) ?? [1])))
</script>

{#snippet stat(label: string, value: string, note: string, link?: string, tone = '')}
  <svelte:element
    this={link ? 'a' : 'div'}
    href={link}
    class="group -mr-px -mb-px grid gap-0.5 border-r border-b border-line px-[18px] pt-4 pb-3.5 text-ink {link ? 'hover:bg-hover hover:no-underline' : ''}"
  >
    <span class="flex items-center gap-1.5 text-[13px] text-ink-2">
      {label}
      {#if link}<ArrowRight size={13} class="text-ink-3 transition-transform duration-150 ease-out group-hover:translate-x-0.5 group-hover:text-ink" />{/if}
    </span>
    <b class="text-3xl leading-tight font-medium tracking-tight tabular-nums {tone}">{value}</b>
    <small class="text-xs {tone || 'text-ink-3'}">{note}</small>
  </svelte:element>
{/snippet}

<header class="mb-6 grid gap-1.5 border-b border-dashed border-line-dash pb-[18px]">
  <div class="flex flex-wrap items-center gap-3.5">
    <h1>{cluster.name}</h1>
    {#if health}
      <span class="inline-flex items-center gap-[7px] rounded-[2px] px-2.5 py-0.5 text-[13px] font-medium {health.cls}">
        <span class="sq"></span>{health.text}
      </span>
    {/if}
  </div>
  <div class="flex flex-wrap items-center gap-4 text-[13px]">
    {#if data?.cluster_id}<span class="mono faint min-w-0 break-all">cluster-id {data.cluster_id}</span>{/if}
    {#if data}<span class="faint">{data.brokers.length} {data.brokers.length === 1 ? 'broker' : 'brokers'} · controller #{data.controller}</span>{/if}
    <span class="spacer"></span>
    <button onclick={() => load(true)} disabled={loading}>
      <RefreshCw size={14} class={loading ? 'animate-spin' : ''} />{loading ? 'Refreshing…' : 'Refresh'}
    </button>
  </div>
  <div class="flex min-w-0 flex-wrap items-center gap-x-2 gap-y-1 text-[13px]">
    <span class="faint" title="The addresses this console connects to, from its configuration">Bootstrap servers</span>
    {#each cluster.brokers as b (b)}<code class="break-all text-ink-2">{b}</code>{/each}
    <button class="ghost small" onclick={copyBootstrap} title="Copy as a comma-separated list" aria-label="Copy bootstrap servers">
      {#if copied}<Check size={13} />{:else}<Copy size={13} />{/if}
    </button>
  </div>
</header>

{#if cluster.description}
  <section class="panel markdown mb-6 px-4 py-3 text-[14px]" aria-label="Cluster description">
    <!-- eslint-disable-next-line svelte/no-at-html-tags -- rendered server-side without raw HTML -->
    {@html cluster.description}
  </section>
{/if}

{#if error}
  <div class="error">{error}</div>
{:else if !data}
  <div class="empty"><Pending /></div>
{:else}
  <div class="ticks relative mb-8 overflow-hidden border border-line-strong bg-surface [--k:8px]">
  <section class="grid grid-cols-2 sm:grid-cols-3 2xl:grid-cols-6">
    {@render stat('Topics', fmt(data.topics), `+${fmt(data.internal_topics)} internal`, href(cluster.name, 'topics'))}
    {@render stat('Partitions', fmt(data.partitions), data.brokers.length ? `~${fmt(Math.round(data.partitions / data.brokers.length))} per broker` : '—')}
    {@render stat('Under-replicated', fmt(data.under_replicated), data.under_replicated > 0 ? 'replicas out of ISR' : 'healthy', undefined, data.under_replicated > 0 ? 'text-warn' : '')}
    {@render stat('Offline', fmt(data.offline_partitions), data.offline_partitions > 0 ? 'no leader' : 'healthy', undefined, data.offline_partitions > 0 ? 'text-danger' : '')}
    {@render stat('Consumer groups', fmt(data.groups), 'view lag', href(cluster.name, 'groups'))}
    {#if disk}{@render stat('Disk usage', bytes(disk.size), 'all replicas')}
    {:else if diskLoading}{@render stat('Disk usage', '…', 'scanning brokers')}
    {:else if diskFailed}{@render stat('Disk usage', '—', 'not available')}{/if}
  </section>

  {#if data.has_registry}
    <a
      href={href(cluster.name, 'schemas')}
      class="-mt-px flex flex-wrap items-center gap-2.5 border-t border-dashed border-line-dash bg-bg px-[18px] py-2.5 text-[13px] text-ink hover:bg-sel hover:no-underline"
    >
      <span class="sq ok"></span>
      <span>Schema Registry connected</span>
      <span class="faint">Avro, Protobuf and JSON Schema payloads are decoded automatically</span>
      <span class="spacer"></span>
      <span class="inline-flex items-center gap-1.5 font-medium">Browse subjects <ArrowRight size={14} /></span>
    </a>
  {/if}
  </div>

  <div class="mb-2.5 flex items-baseline gap-2">
    <h2>Partition health</h2>
    {#if problems.length}<span class="faint">{fmt(problems.length)} {problems.length === 1 ? 'topic needs' : 'topics need'} attention</span>{/if}
  </div>
  {#if !problems.length}
    <div class="mb-8 flex items-center gap-2.5 border-y border-dashed border-line-dash px-1 py-2.5 text-[13px]">
      <span class="sq ok"></span>
      <span>All {fmt(data.partitions)} partitions across {fmt(data.topics + data.internal_topics)} topics have a leader and a full ISR.</span>
      <Check size={14} class="text-ok" />
    </div>
  {:else}
    <div class="panel mb-8 overflow-x-auto">
      <table>
        <thead><tr><th>Topic</th><th class="num">Partitions</th><th class="num">Offline</th><th class="num">Under-replicated</th><th>Affected partitions</th></tr></thead>
        <tbody>
          {#each shownProblems as t (t.topic)}
            <tr>
              <td class="grow"><a class="mono" href={href(cluster.name, 'topics', t.topic, { tab: 'partitions' })} title={t.topic}>{t.topic}</a></td>
              <td class="num">{fmt(t.total)}</td>
              <td class="num">{#if t.offline.length}<span class="badge danger">{fmt(t.offline.length)}</span>{:else}<span class="faint">0</span>{/if}</td>
              <td class="num">{#if t.under.length}<span class="badge warn">{fmt(t.under.length)}</span>{:else}<span class="faint">0</span>{/if}</td>
              <td class="mono text-xs text-ink-2">
                {#if t.offline.length}<span class="text-danger">{ids(t.offline)}</span>{/if}
                {#if t.offline.length && t.under.length}<span class="faint"> · </span>{/if}
                {#if t.under.length}<span>{ids(t.under)}</span>{/if}
              </td>
            </tr>
          {/each}
        </tbody>
      </table>
      {#if problems.length > problemLimit}
        <div class="border-t border-line px-4 py-2 text-[13px]">
          <button class="ghost" onclick={() => (showAllProblems = !showAllProblems)}>
            {showAllProblems ? `Show the first ${problemLimit}` : `Show all ${fmt(problems.length)} topics`}
          </button>
        </div>
      {/if}
    </div>
  {/if}

  <div class="mb-2.5 flex items-baseline gap-2">
    <h2>Brokers</h2>
    <span class="faint">{data.brokers.length}</span>
  </div>
  <div class="panel overflow-x-auto">
    <table>
      <thead>
        <tr>
          <th>ID</th><th>Host</th><th>Rack</th>
          <th class="w-[22%] max-lg:hidden" title="How many partitions this broker leads, relative to the broker with the most. Producers and consumers talk to the leader, so even bars mean even load; one long bar means that broker does most of the work.">
            <span class="help">Leader share</span>
          </th>
          <th class="num" title="Partitions whose leader is this broker. All reads and writes for a partition go through its leader."><span class="help">Leaders</span></th>
          <th class="num" title="Partition copies stored on this broker, leaders and followers together."><span class="help">Replicas</span></th>
          <th>Version</th><th></th>
        </tr>
      </thead>
      <tbody>
        {#each data.brokers as b (b.id)}
          <tr>
            <td class="mono"><a href={href(cluster.name, 'brokers', String(b.id))}>#{b.id}</a></td>
            <td class="mono">{b.host}<span class="faint">:{b.port}</span></td>
            <td class="muted">{b.rack ?? '—'}</td>
            <td class="max-lg:hidden" title="{fmt(b.leaders)} of {fmt(data.partitions)} partitions ({data.partitions ? Math.round((b.leaders / data.partitions) * 100) : 0}%)">
              <span class="mt-[7px] block h-1.5 overflow-hidden rounded-[1px] bg-surface-2">
                <span class="block h-full bg-ink-2" style:width="{(b.leaders / maxLeaders) * 100}%"></span>
              </span>
            </td>
            <td class="num mono">{fmt(b.leaders)}</td>
            <td class="num mono">{fmt(b.replicas)}</td>
            <td class="faint text-xs" title="Guessed from supported API versions">{b.version ?? '—'}</td>
            <td>{#if b.controller}<span class="badge accent">controller</span>{/if}</td>
          </tr>
        {/each}
      </tbody>
    </table>
  </div>
{/if}

<style>
  /* Dotted underline marks a header that explains itself on hover. */
  .help { text-decoration: underline dotted var(--line-dash); text-underline-offset: 3px; cursor: help; }
</style>
