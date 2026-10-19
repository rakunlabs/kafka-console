<script lang="ts">
  import { untrack } from 'svelte'
  import { api, type Cluster, type Partition, type PartitionMove, type ReassignResult } from './api'
  import Modal from './Modal.svelte'
  import { Scale, RotateCcw } from '@lucide/svelte'

  // Move partition replicas between brokers. The rows load fresh replica
  // lists when the dialog opens: the page's copy may be stale, and the
  // balance plan is made from the cluster's current state. Each row's list is
  // editable as "1, 2, 3"; the first broker becomes the preferred leader.
  // "Balance" fills the rows with a plan that evens out replicas and
  // leaders, moving as little as it can. Nothing changes before Apply, and
  // Apply shows the plan's diff first.
  let {
    cluster,
    topic,
    open = $bindable(false),
    ondone,
  }: { cluster: Cluster; topic: string; open: boolean; ondone?: () => void } = $props()

  let brokers = $state<number[]>([])
  let rows = $state<{ partition: number; from: number[]; text: string }[]>([])
  let moving = $state<PartitionMove[]>([])
  let error = $state('')
  let busy = $state(false)
  let done = $state<ReassignResult[]>()
  let opened = false

  $effect(() => {
    if (open && !opened) {
      untrack(reset)
    }
    opened = open
  })

  async function reset() {
    error = ''
    done = undefined
    rows = []
    busy = true
    try {
      const [t, o, m] = await Promise.all([api.topic(cluster.name, topic), api.overview(cluster.name), api.reassignments(cluster.name, topic)])
      rows = t.partitions.map((p: Partition) => ({ partition: p.id, from: p.replicas, text: p.replicas.join(', ') }))
      brokers = o.brokers.map((b) => b.id)
      moving = m
    } catch (e: any) {
      error = e.message
    } finally {
      busy = false
    }
  }

  const parse = (s: string) => s.split(',').map((x) => x.trim()).filter(Boolean).map(Number)
  const same = (a: number[], b: number[]) => a.length === b.length && a.every((x, i) => x === b[i])

  // Row problems, checked as typed; the server checks again.
  function problem(r: { from: number[]; text: string }): string {
    const to = parse(r.text)
    if (to.some((b) => !Number.isInteger(b))) return 'Broker IDs are numbers'
    if (to.length !== r.from.length) return `Needs ${r.from.length} ${r.from.length === 1 ? 'broker' : 'brokers'} (replication factor)`
    if (new Set(to).size !== to.length) return 'A broker is listed twice'
    const unknown = to.find((b) => brokers.length && !brokers.includes(b))
    if (unknown !== undefined) return `Broker ${unknown} does not exist`
    return ''
  }

  const changed = $derived(rows.filter((r) => !problem(r) && !same(parse(r.text), r.from)))
  const invalid = $derived(rows.some((r) => problem(r)))

  // Replicas and leaders per broker, before and after.
  const spread = $derived.by(() => {
    const out = new Map<number, { now: number; next: number; leadNow: number; leadNext: number }>()
    const at = (b: number) => out.get(b) ?? out.set(b, { now: 0, next: 0, leadNow: 0, leadNext: 0 }).get(b)!
    for (const b of brokers) at(b)
    for (const r of rows) {
      r.from.forEach((b, i) => { at(b).now++; if (i === 0) at(b).leadNow++ })
      const to = problem(r) ? r.from : parse(r.text)
      to.forEach((b, i) => { at(b).next++; if (i === 0) at(b).leadNext++ })
    }
    return [...out.entries()].sort((a, b) => a[0] - b[0])
  })

  async function plan() {
    busy = true
    error = ''
    try {
      const p = await api.balancePlan(cluster.name, topic)
      const by = new Map(p.map((x) => [x.partition, x.replicas]))
      rows = rows.map((r) => ({ ...r, text: (by.get(r.partition) ?? r.from).join(', ') }))
    } catch (e: any) {
      error = e.message
    } finally {
      busy = false
    }
  }

  async function apply() {
    busy = true
    error = ''
    try {
      done = await api.reassign(cluster.name, topic, { partitions: changed.map((r) => ({ partition: r.partition, replicas: parse(r.text) })) })
      ondone?.()
    } catch (e: any) {
      error = e.message
    } finally {
      busy = false
    }
  }

  async function cancelMoves() {
    busy = true
    error = ''
    try {
      await api.reassign(cluster.name, topic, { cancel: true, partitions: moving.map((m) => ({ partition: m.partition, replicas: [] })) })
      moving = await api.reassignments(cluster.name, topic)
      ondone?.()
    } catch (e: any) {
      error = e.message
    } finally {
      busy = false
    }
  }
</script>

<Modal title="Move replicas: {topic}" bind:open width={640}>
  {#if done}
    <div class="ok">Started moving {done.length} {done.length === 1 ? 'partition' : 'partitions'}. Brokers copy the data in the background; large partitions take a while.</div>
    {#each done.filter((d) => d.error) as d (d.partition)}<div class="error small-err">Partition {d.partition}: {d.error}</div>{/each}
  {:else}
    {#if moving.length}
      <div class="warnbox">
        {moving.length} {moving.length === 1 ? 'partition is' : 'partitions are'} still moving
        ({moving.map((m) => m.partition).join(', ')}). A new move can start once they finish.
        <button class="small danger" disabled={busy || cluster.read_only} onclick={cancelMoves}>Cancel moves</button>
      </div>
    {/if}
    <p class="faint small">Brokers per partition, comma separated; the first one becomes the preferred leader. The replication factor stays the same. Racks are not taken into account.</p>

    <div class="spread">
      {#each spread as [b, s] (b)}
        <span class="mono" title="Replicas (leaders): now → after">
          <b>#{b}</b> {s.now}{#if s.next !== s.now}→<b>{s.next}</b>{/if}
          <span class="faint">({s.leadNow}{#if s.leadNext !== s.leadNow}→{s.leadNext}{/if})</span>
        </span>
      {/each}
    </div>

    {#if !rows.length && busy}<div class="empty">Loading…</div>{/if}
    <div class="panel tbl">
      <table>
        <thead><tr><th class="num">P</th><th>Now</th><th>Replicas</th></tr></thead>
        <tbody>
          {#each rows as r (r.partition)}
            {@const why = problem(r)}
            {@const diff = !why && !same(parse(r.text), r.from)}
            <tr class:diff>
              <td class="num mono">{r.partition}</td>
              <td class="mono faint">{r.from.join(', ')}</td>
              <td>
                <input class="mono" bind:value={r.text} aria-label="Replicas of partition {r.partition}" aria-invalid={!!why} />
                {#if why}<span class="field-err">{why}</span>{/if}
              </td>
            </tr>
          {/each}
        </tbody>
      </table>
    </div>
  {/if}
  {#if error}<div class="error">{error}</div>{/if}

  {#snippet footer()}
    {#if done}
      <button class="primary" onclick={() => (open = false)}>Close</button>
    {:else}
      <button onclick={plan} disabled={busy || !!moving.length} title="Fill in a plan that spreads replicas and leaders evenly"><Scale size={14} />Balance</button>
      <button class="ghost" onclick={reset} disabled={busy}><RotateCcw size={14} />Reset</button>
      <span class="spacer"></span>
      <button onclick={() => (open = false)}>Cancel</button>
      <button class="primary" disabled={busy || invalid || !changed.length || !!moving.length || cluster.read_only} onclick={apply}>
        {busy ? 'Working…' : changed.length ? `Move ${changed.length} ${changed.length === 1 ? 'partition' : 'partitions'}` : 'Nothing to move'}
      </button>
    {/if}
  {/snippet}
</Modal>

<style>
  .spread { display: flex; flex-wrap: wrap; gap: 6px 14px; font-size: 12.5px; margin: 8px 0; }
  .tbl { max-height: 360px; overflow: auto; }
  .tbl input { width: 100%; }
  tr.diff td { background: var(--accent-soft, rgb(80 120 255 / 0.08)); }
  .field-err { display: block; color: var(--danger); font-size: 12px; }
  .warnbox { display: flex; flex-wrap: wrap; align-items: center; gap: 8px; padding: 8px 10px; border: 1px solid var(--warn); border-radius: var(--radius); margin-bottom: 8px; font-size: 13px; }
</style>
