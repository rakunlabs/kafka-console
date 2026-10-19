<script lang="ts">
  import type { Snippet } from 'svelte'
  import { Ellipsis } from '@lucide/svelte'

  // A "⋯" button that holds secondary actions on narrow screens. The actions
  // are plain buttons; picking one, clicking outside or Esc closes the menu.
  let { label = 'More actions', children }: { label?: string; children: Snippet } = $props()

  let el = $state<HTMLDetailsElement>()

  function onclick(e: MouseEvent) {
    if (!el?.open) return
    const t = e.target as Element
    if (!el.contains(t) || t.closest('.menu button')) el.open = false
  }
</script>

<svelte:window {onclick} onkeydown={(e) => e.key === 'Escape' && el?.open && (el.open = false)} />

<details class="more" bind:this={el}>
  <summary class="btn" aria-label={label} title={label}><Ellipsis size={15} /></summary>
  <div class="menu">{@render children()}</div>
</details>

<style>
  .more { position: relative; }
  summary { list-style: none; padding: 5px 8px; }
  summary::-webkit-details-marker { display: none; }
  .more[open] summary { background: var(--surface-2); }
  .menu {
    position: absolute;
    right: 0;
    top: calc(100% + 4px);
    z-index: 20;
    display: grid;
    min-width: 190px;
    padding: 3px;
    background: var(--surface);
    border: 1px solid var(--border-strong);
    border-radius: var(--radius);
    box-shadow: 0 8px 24px -10px rgb(0 0 0 / .3);
  }
  .menu :global(button) { justify-content: flex-start; border-color: transparent; background: transparent; padding: 7px 10px; }
  .menu :global(button:hover:not(:disabled)) { background: var(--row-hover); }
</style>
