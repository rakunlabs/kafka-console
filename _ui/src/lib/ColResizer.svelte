<script lang="ts">
  import { drag } from './drag'

  // The grab handle on a table header's right edge: drag to resize the
  // column, double-click to fit its longest content, arrows from the keyboard.
  // The parent owns the width; this only reports where it should go.
  let {
    label,
    width,
    min = 48,
    max = 1200,
    onchange,
    ondone,
    onfit,
  }: {
    label: string
    width: number
    min?: number
    max?: number
    // While dragging; ondone follows once the width settles.
    onchange: (px: number) => void
    ondone: () => void
    onfit: () => void
  } = $props()

  const clamp = (px: number) => Math.round(Math.min(Math.max(px, min), Math.max(min, max)))

  function down(e: PointerEvent) {
    const th = (e.currentTarget as HTMLElement).parentElement
    if (!th || e.button !== 0) return
    const startW = th.getBoundingClientRect().width
    drag(e, { move: (dx) => onchange(clamp(startW + dx)), end: ondone, stopPropagation: true, cursor: true })
  }

  function key(e: KeyboardEvent) {
    const by = e.key === 'ArrowRight' ? 16 : e.key === 'ArrowLeft' ? -16 : 0
    if (e.key === 'Enter') {
      e.preventDefault()
      e.stopPropagation()
      onfit()
      return
    }
    if (!by) return
    e.preventDefault()
    e.stopPropagation()
    onchange(clamp(width + by))
    ondone()
  }
</script>

<!-- svelte-ignore a11y_no_noninteractive_tabindex, a11y_no_noninteractive_element_interactions, a11y_click_events_have_key_events -->
<span class="rz" role="separator" aria-orientation="vertical" aria-label="Resize {label} column"
  aria-valuenow={width} aria-valuemin={min} aria-valuemax={max} tabindex="0"
  title="Drag to resize · double-click to fit the longest"
  onpointerdown={down} ondblclick={(e) => { e.stopPropagation(); onfit() }} onclick={(e) => e.stopPropagation()} onkeydown={key}></span>

<style>
  .rz {
    position: absolute;
    top: 0;
    right: 0;
    bottom: 0;
    z-index: 3;
    width: 8px;
    cursor: col-resize;
    touch-action: none;
  }
  .rz::after {
    content: '';
    position: absolute;
    top: 6px;
    bottom: 6px;
    left: 5px;
    width: 2px;
    border-radius: 1px;
    background: transparent;
  }
  :global(th:hover) > .rz::after { background: var(--border-strong); }
  .rz:hover::after, .rz:focus-visible::after, :global(.col-resizing) .rz:active::after { background: var(--cta-border); }
  .rz:focus-visible { outline: none; }
  :global(html.col-resizing), :global(html.col-resizing *) { cursor: col-resize !important; user-select: none; }
</style>
