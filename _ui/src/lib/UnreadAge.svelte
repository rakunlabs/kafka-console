<script lang="ts">
  import { onDestroy, untrack } from 'svelte'
  import { api, isAbort, type Cluster, type PartitionLag } from './api'
  import { errorMessage } from './http'
  import { href } from './router.svelte'
  import { showTime } from './viewer'
  import { unreadRefs } from './consumers'
  import { Clock } from '@lucide/svelte'

  // How old the oldest message a group has not read yet is: lag in time
  // rather than in messages. Read again whenever the committed offsets move.
  let { cluster, topic, rows }: { cluster: Cluster; topic: string; rows: PartitionLag[] } = $props()

  // The partitions furthest behind are the ones that matter; the cap keeps
  // the request URL short enough for proxies in front of the console.
  const maxRefs = 200
  const refs = $derived(unreadRefs(rows).slice(0, maxRefs))
  const key = $derived(refs.map((r) => `${r.partition}:${r.offset}`).join(','))

  let oldest = $state<{ partition: number; offset: number; at: string }>()
  let error = $state('')
  let loading = $state(false)
  let seq = 0

  // Only a change in the offsets reads again, not every refresh of the rows.
  $effect(() => {
    const k = key
    const want = untrack(() => refs)
    const mine = ++seq
    if (!k) {
      oldest = undefined
      loading = false
      return
    }
    loading = true
    api.recordTimes(cluster.name, topic, want).then(
      (times) => {
        if (mine !== seq) return
        error = ''
        let best: typeof oldest
        let bestMs = Infinity
        for (const t of times) {
          const ms = t.timestamp ? Date.parse(t.timestamp) : NaN
          if (ms < bestMs) {
            bestMs = ms
            best = { partition: t.partition, offset: t.offset, at: t.timestamp! }
          }
        }
        oldest = best
        if (!best && times[0]?.error) error = times[0].error
        loading = false
      },
      (e) => {
        if (mine !== seq || isAbort(e)) return
        error = errorMessage(e)
        loading = false
      },
    )
  })

  let now = $state(Date.now())
  const tick = setInterval(() => (now = Date.now()), 15_000)
  onDestroy(() => {
    clearInterval(tick)
    seq++
  })
</script>

{#if key}
  <span class="age" aria-live="polite">
    <Clock size={13} />
    {#if oldest}
      Oldest unread written <a href={href(cluster.name, 'topics', topic, { p: String(oldest.partition), o: String(oldest.offset) })}
        title="Open it: partition {oldest.partition}, offset {oldest.offset}">{showTime(oldest.at, 'relative', now)}</a>
    {:else if error}
      <span class="faint" title={error}>Age of unread messages unknown</span>
    {:else if loading}
      <span class="faint">Reading message age…</span>
    {/if}
  </span>
{/if}

<style>
  .age { display: inline-flex; align-items: center; gap: 5px; font-size: 13px; color: var(--text-2); white-space: nowrap; }
  .age :global(svg) { color: var(--text-3); }
</style>
