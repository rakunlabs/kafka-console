<script lang="ts">
  import { fmt } from './format'
  import { Loader } from '@lucide/svelte'

  // Footer for a server-paged table: how many are shown and a button that
  // fetches the next page.
  let { shown, total, step, busy, onmore }: { shown: number; total: number; step: number; busy: boolean; onmore: () => void } = $props()
</script>

{#if shown < total}
  <div class="flex flex-wrap items-center gap-2 border-t border-line px-3.5 py-2 text-[13px] text-ink-3">
    <span>Showing {fmt(shown)} of {fmt(total)}</span>
    <span class="flex-1"></span>
    <button onclick={onmore} disabled={busy}>
      {#if busy}<Loader size={14} class="animate-spin" />Loading…{:else}Show {fmt(Math.min(step, total - shown))} more{/if}
    </button>
  </div>
{/if}
