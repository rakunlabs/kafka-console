<script lang="ts">
  import { fmt } from './format'

  // One square per partition. `tone` sets the colour (state); `level`, 0–1,
  // sets how strongly it is filled (load or lag). Squares share the 8px
  // lattice used by the page backdrop.
  export type Cell = {
    label: string
    tone?: 'ok' | 'warn' | 'danger' | 'idle' | 'cta'
    level?: number
    title?: string
    href?: string
  }

  let { cells, size = 14, gap = 2, legend }: { cells: Cell[]; size?: number; gap?: number; legend?: [string, Cell['tone']][] } = $props()
</script>

<div class="map" style:--s="{size}px" style:--g="{gap}px" role="list">
  {#each cells as c, i (i)}
    {@const lvl = c.level === undefined ? 1 : Math.max(0.12, Math.min(1, c.level))}
    <svelte:element
      this={c.href ? 'a' : 'span'}
      href={c.href}
      role="listitem"
      class="c {c.tone ?? 'ok'}"
      style:--lvl={lvl}
      title={c.title ?? c.label}
      aria-label={c.title ?? c.label}
    ></svelte:element>
  {/each}
</div>
{#if legend}
  <div class="legend">
    {#each legend as [label, tone] (label)}<span><span class="c {tone}" style:--lvl="1"></span>{label}</span>{/each}
    <span class="faint">{fmt(cells.length)} {cells.length === 1 ? 'partition' : 'partitions'}</span>
  </div>
{/if}

<style>
  .map { display: flex; flex-wrap: wrap; gap: var(--g); }
  .c {
    display: block;
    width: var(--s);
    height: var(--s);
    background: color-mix(in srgb, var(--tone) calc(var(--lvl) * 100%), var(--surface-2));
    outline: 1px solid color-mix(in srgb, var(--tone) 35%, transparent);
    outline-offset: -1px;
  }
  a.c:hover, a.c:focus-visible { outline: 2px solid var(--focus); outline-offset: 0; z-index: 1; }
  .ok { --tone: var(--ok); }
  .warn { --tone: var(--warn); }
  .danger { --tone: var(--danger); }
  .idle { --tone: var(--text-3); }
  .cta { --tone: var(--cta-hover); }
  .legend { display: flex; flex-wrap: wrap; align-items: center; gap: 4px 14px; margin-top: 8px; font-size: 12px; color: var(--text-2); }
  .legend > span { display: inline-flex; align-items: center; gap: 6px; }
  .legend .c { width: 9px; height: 9px; }
</style>
