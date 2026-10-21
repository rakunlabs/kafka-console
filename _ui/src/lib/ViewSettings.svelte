<script lang="ts">
  import { prefs, type TimeFormat } from './prefs.svelte'
  import { Columns3, ChevronUp, ChevronDown, X, Plus, RotateCcw } from '@lucide/svelte'

  // The message list's view settings: how times read, and which value
  // fields get their own column. Both are kept in this browser only.
  let {
    columns,
    onadd,
    onremove,
    onmove,
    resized = false,
    onresetwidths,
  }: {
    columns: string[]
    onadd: (path: string) => void
    onremove: (path: string) => void
    onmove: (path: string, by: number) => void
    // Some column was resized by hand; offer to go back to automatic widths.
    resized?: boolean
    onresetwidths?: () => void
  } = $props()

  let el = $state<HTMLDetailsElement>()
  let path = $state('')

  const formats: [TimeFormat, string][] = [['local', 'Local'], ['utc', 'UTC'], ['relative', 'Relative'], ['unix', 'Unix ms']]

  function add(e: Event) {
    e.preventDefault()
    const p = path.trim()
    if (!p) return
    onadd(p)
    path = ''
  }

  function onclick(e: MouseEvent) {
    if (el?.open && !el.contains(e.target as Node)) el.open = false
  }
</script>

<svelte:window {onclick} onkeydown={(e) => e.key === 'Escape' && el?.open && (el.open = false)} />

<details class="more view" bind:this={el}>
  <summary class="btn ghost small" title="Time format and value columns"><Columns3 size={13} />View</summary>
  <div class="menu">
    <div class="sec">
      <span class="lbl">Time</span>
      <div class="seg" role="radiogroup" aria-label="Time format">
        {#each formats as [id, label] (id)}
          <button type="button" role="radio" aria-checked={prefs.time === id} class:on={prefs.time === id}
            onclick={() => (prefs.time = id)}>{label}</button>
        {/each}
      </div>
    </div>
    <div class="sec">
      <span class="lbl">Value columns</span>
      <p class="hint">Show a field of JSON values as a column, like <code>order.id</code> or <code>items[0].sku</code>. Kept for this topic.</p>
      {#each columns as c, i (c)}
        <div class="col">
          <span class="mono">{c}</span>
          <span class="spacer"></span>
          <button type="button" class="ghost icon" disabled={i === 0} onclick={() => onmove(c, -1)} aria-label="Move {c} left"><ChevronUp size={13} /></button>
          <button type="button" class="ghost icon" disabled={i === columns.length - 1} onclick={() => onmove(c, 1)} aria-label="Move {c} right"><ChevronDown size={13} /></button>
          <button type="button" class="ghost icon" onclick={() => onremove(c)} aria-label="Remove column {c}"><X size={13} /></button>
        </div>
      {/each}
      <form class="add" onsubmit={add}>
        <input class="mono" placeholder="field.path" bind:value={path} aria-label="Field path" />
        <button type="submit" class="small" disabled={!path.trim()}><Plus size={13} />Add</button>
      </form>
    </div>
    <div class="sec">
      <span class="lbl">Column widths</span>
      <p class="hint">Drag a header's right edge to resize; double-click it to fit the longest value. Kept for this topic.</p>
      <button type="button" class="small" disabled={!resized} onclick={() => onresetwidths?.()}><RotateCcw size={13} />Reset to automatic</button>
    </div>
  </div>
</details>

<style>
  .view { position: relative; }
  summary { list-style: none; display: inline-flex; align-items: center; gap: 5px; cursor: pointer; }
  summary::-webkit-details-marker { display: none; }
  .menu {
    position: absolute;
    right: 0;
    top: calc(100% + 4px);
    z-index: 20;
    width: 320px;
    padding: 14px;
    display: grid;
    gap: 16px;
    background: var(--surface);
    border: 1px solid var(--border-strong);
    border-radius: var(--radius);
    box-shadow: 0 8px 24px rgb(0 0 0 / 0.12);
    color: var(--text);
  }
  .sec { display: grid; gap: 8px; }
  .lbl { font-size: 12px; font-weight: 600; }
  .hint { margin: 0; font-size: 12px; color: var(--text-3); }
  .seg { display: grid; grid-template-columns: repeat(4, 1fr); border: 1px solid var(--border); border-radius: var(--radius); overflow: hidden; }
  .seg button { justify-content: center; border: 0; border-radius: 0; padding: 5px 8px; font-size: 12px; background: transparent; }
  .seg button + button { border-left: 1px solid var(--border); }
  .seg button.on { background: var(--surface-2); font-weight: 600; }
  .col { display: flex; align-items: center; gap: 2px; padding: 3px 4px 3px 10px; font-size: 13px; border: 1px solid var(--border); border-radius: var(--radius); }
  .col .mono { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .icon { padding: 3px 5px; }
  .add { display: flex; gap: 6px; }
  .add input { flex: 1; min-width: 0; padding-inline: 10px; }
  .sec > button { justify-self: start; }
</style>
