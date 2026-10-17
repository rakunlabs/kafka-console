<script lang="ts">
  // Committed position between low and high watermark. The filled part has
  // been consumed; the remainder is lag.
  let { low, high, committed }: { low: number; high: number; committed: number } = $props()

  const span = $derived(Math.max(high - low, 0))
  const pos = $derived(committed < 0 || span === 0 ? (committed >= high ? 1 : 0) : Math.min(Math.max((committed - low) / span, 0), 1))
  const behind = $derived(committed >= 0 && high - committed > 0)
</script>

<div class="bar" title="low {low} · committed {committed} · high {high}">
  <div class="fill" class:behind style:width="{pos * 100}%"></div>
</div>

<style>
  .bar {
    width: 100%;
    min-width: 120px;
    height: 6px;
    border-radius: 3px;
    background: var(--surface-2);
    box-shadow: inset 0 0 0 1px var(--border);
    overflow: hidden;
    margin-top: 6px;
  }
  .fill { height: 100%; background: var(--ok); }
  .fill.behind { background: var(--accent); }
</style>
