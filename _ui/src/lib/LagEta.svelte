<script lang="ts">
  import { onDestroy } from 'svelte'
  import { errorMessage } from './http'
  import { fmt } from './format'
  import { Timer, Square, RotateCcw } from '@lucide/svelte'

  // Estimates when a group catches up, from two or more readings of its lag
  // and committed offsets taken in the browser. Nothing is stored server-side;
  // closing the page discards the samples. With per-partition readings it
  // also names the partitions that have lag but whose commit did not move.
  type Part = { topic: string; partition: number; committed: number; lag: number }
  type Sample = { at: number; lag: number; committed: number; high: number; parts?: Part[] }

  let {
    read,
    interval = 5000,
    link,
  }: {
    // Returns the current total lag, sum of committed offsets and sum of
    // high watermarks for the scope being measured, and optionally each
    // partition's committed offset and lag.
    read: () => Promise<{ lag: number; committed: number; high: number; parts?: Part[] }>
    interval?: number
    // Where a stuck partition's next message can be opened.
    link?: (topic: string, partition: number, offset: number) => string
  } = $props()

  let samples = $state<Sample[]>([])
  let running = $state(false)
  let error = $state('')
  let timer: ReturnType<typeof setTimeout> | undefined

  const window = 12 // keep about a minute of readings at 5s

  // Each start() is a new run; a reading that was still pending from an
  // earlier run is dropped and does not schedule a second loop.
  let run = 0

  async function tick(id: number) {
    try {
      const r = await read()
      if (id !== run) return
      samples = [...samples, { at: Date.now(), ...r }].slice(-window)
      error = ''
    } catch (e) {
      if (id !== run) return
      error = errorMessage(e)
    }
    if (running) timer = setTimeout(() => tick(id), interval)
  }

  function start() {
    clearTimeout(timer)
    samples = []
    running = true
    void tick(++run)
  }

  function stop() {
    run++
    running = false
    clearTimeout(timer)
  }

  onDestroy(stop)

  // Rates over the whole window, which smooths poll jitter.
  const est = $derived.by(() => {
    if (samples.length < 2) return undefined
    const a = samples[0]
    const b = samples[samples.length - 1]
    const secs = (b.at - a.at) / 1000
    if (secs <= 0) return undefined
    const consume = (b.committed - a.committed) / secs
    const produce = (b.high - a.high) / secs
    const net = consume - produce
    const lag = b.lag
    let eta: number | undefined
    let state: 'done' | 'catching' | 'falling' | 'stalled' | 'steady'
    if (lag <= 0) state = 'done'
    else if (consume <= 0 && produce <= 0) state = 'stalled'
    else if (net > 0) {
      state = 'catching'
      eta = lag / net
    } else if (net < 0) state = 'falling'
    else state = 'steady'
    return { consume, produce, net, lag, eta, state, secs }
  })

  // Partitions with lag at every reading whose committed offset never moved.
  // Needs a few readings so a consumer between commits is not flagged.
  const stuckAfter = 3
  const stuck = $derived.by(() => {
    if (samples.length < stuckAfter) return []
    const first = samples[0].parts
    const last = samples[samples.length - 1].parts
    if (!first || !last) return []
    const key = (p: Part) => `${p.topic}\u0000${p.partition}`
    const start = new Map(first.map((p) => [key(p), p]))
    return last
      .filter((p) => {
        const a = start.get(key(p))
        return a && p.lag > 0 && a.lag > 0 && p.committed >= 0 && p.committed === a.committed
      })
      .sort((a, b) => b.lag - a.lag)
  })
  const stuckSecs = $derived(samples.length ? (samples[samples.length - 1].at - samples[0].at) / 1000 : 0)
  let showAll = $state(false)

  function dur(s: number) {
    if (s < 60) return `${Math.max(1, Math.round(s))} s`
    if (s < 3600) return `${Math.round(s / 60)} min`
    if (s < 86400) {
      const h = Math.floor(s / 3600)
      const m = Math.round((s % 3600) / 60)
      return m ? `${h} h ${m} min` : `${h} h`
    }
    const d = Math.floor(s / 86400)
    const h = Math.round((s % 86400) / 3600)
    return h ? `${d} d ${h} h` : `${d} d`
  }
  const rate = (r: number) => `${r >= 100 ? fmt(Math.round(r)) : r.toFixed(1)}/s`
  const clock = (s: number) =>
    new Date(Date.now() + s * 1000).toLocaleTimeString([], s < 120 ? { hour: '2-digit', minute: '2-digit', second: '2-digit' } : { hour: '2-digit', minute: '2-digit' })

  // Sparkline of lag across the readings.
  const spark = $derived.by(() => {
    if (samples.length < 2) return ''
    const w = 120
    const h = 22
    const max = Math.max(...samples.map((s) => s.lag), 1)
    return samples.map((s, i) => `${(i / (samples.length - 1)) * w},${h - (s.lag / max) * (h - 2) - 1}`).join(' ')
  })
</script>

<div class="eta" class:live={running}>
  {#if !running && !samples.length}
    <button class="small" onclick={start} title="Measure for a few seconds and estimate when lag reaches zero">
      <Timer size={13} />Estimate catch-up
    </button>
  {:else}
    {#if running}
      <span class="sq ok live" aria-hidden="true"></span>
    {/if}
    <span class="txt" aria-live="polite">
      {#if !est}
        Measuring… <span class="faint">{samples.length}/2 readings</span>
      {:else if est.state === 'done'}
        <b>Caught up</b>
      {:else if est.state === 'catching'}
        Caught up in <b>~{dur(est.eta!)}</b> <span class="faint">around {clock(est.eta!)}</span>
      {:else if est.state === 'falling'}
        <b class="bad">Falling behind</b> <span class="faint">lag grows {rate(-est.net)}</span>
      {:else if est.state === 'stalled'}
        <b class="bad">Not consuming</b> <span class="faint">committed offsets did not move</span>
      {:else}
        <b>Holding steady</b> <span class="faint">lag not shrinking</span>
      {/if}
    </span>
    {#if est}
      <span class="faint rates" title="Averaged over the last {Math.round(est.secs)} s">consume {rate(est.consume)} · produce {rate(est.produce)}</span>
      {#if spark}<svg class="spark" width="120" height="22" viewBox="0 0 120 22" aria-hidden="true"><polyline points={spark} /></svg>{/if}
    {/if}
    {#if running}
      <button class="ghost small" onclick={stop} title="Stop measuring"><Square size={12} />Stop</button>
    {:else}
      <button class="ghost small" onclick={start} title="Measure again"><RotateCcw size={12} />Again</button>
    {/if}
  {/if}
  {#if error}<span class="bad small">{error}</span>{/if}
  {#if stuck.length}
    {@const many = new Set(stuck.map((p) => p.topic)).size > 1}
    <div class="stuck" role="status">
      <b class="bad">{stuck.length} {stuck.length === 1 ? 'partition' : 'partitions'} not moving</b>
      <span class="faint">lag but no commit for {dur(stuckSecs)}:</span>
      {#each showAll ? stuck : stuck.slice(0, 6) as p (p.topic + p.partition)}
        {@const label = `${many ? p.topic + ' ' : ''}P${p.partition}`}
        {#if link}
          <a class="chip mono" href={link(p.topic, p.partition, p.committed)} title="Open the message it is stuck on: offset {p.committed}, lag {fmt(p.lag)}">{label} @ {fmt(p.committed)}</a>
        {:else}
          <span class="chip mono" title="Lag {fmt(p.lag)}">{label} @ {fmt(p.committed)}</span>
        {/if}
      {/each}
      {#if stuck.length > 6}
        <button class="ghost small" onclick={() => (showAll = !showAll)}>{showAll ? 'Fewer' : `+${stuck.length - 6} more`}</button>
      {/if}
    </div>
  {/if}
</div>

<style>
  .eta { display: inline-flex; flex-wrap: wrap; align-items: center; gap: 4px 10px; font-size: 13px; }
  .eta.live, .eta:has(.txt) { padding: 3px 4px 3px 10px; background: var(--surface); border: 1px solid var(--border-strong); border-radius: var(--radius); }
  .txt b { font-weight: 600; }
  .bad { color: var(--danger); }
  .rates { font-variant-numeric: tabular-nums; }
  .spark polyline { fill: none; stroke: var(--text-2); stroke-width: 1.4; stroke-linejoin: round; }
  .stuck { flex-basis: 100%; display: flex; flex-wrap: wrap; align-items: center; gap: 4px 6px; padding: 4px 0 2px; border-top: 1px dashed var(--line-dash); }
  .stuck b { font-weight: 600; }
  .chip { font-size: 12px; padding: 0 6px; line-height: 20px; border: 1px solid var(--border-strong); border-radius: 2px; background: var(--surface-2); color: var(--text); }
  a.chip:hover { border-color: var(--danger); text-decoration: none; }
</style>
