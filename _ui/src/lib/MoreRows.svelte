<script lang="ts">
  import { fmt } from './format'

  // Footer for long tables: renders a page of rows at a time so a cluster
  // with thousands of topics or groups does not build thousands of rows.
  let { total, shown = $bindable(), step = 200 }: { total: number; shown: number; step?: number } = $props()
</script>

{#if total > shown}
  <div class="flex flex-wrap items-center gap-2 border-t border-line px-3.5 py-2 text-[12.5px] text-ink-3">
    <span>Showing {fmt(shown)} of {fmt(total)}</span>
    <span class="flex-1"></span>
    <button onclick={() => (shown += step)}>Show {fmt(Math.min(step, total - shown))} more</button>
    <button class="ghost" onclick={() => (shown = total)}>Show all</button>
  </div>
{/if}
