<script lang="ts">
  // Page backdrop: a fine grid under the content, visible near the top and
  // around the pointer, plus a scatter of filled cells that thins out from the
  // top-right corner. Purely decorative; panels and tables sit on solid
  // surfaces above it.
  let el: HTMLDivElement
  let raf = 0

  const cell = 8
  const cols = 64
  const rows = 34

  // Deterministic scatter: the same cells on every load, denser toward the
  // corner. A small hash keeps it stable without storing a list.
  function hash(x: number, y: number) {
    let h = (x * 374761393 + y * 668265263) ^ 0x5bf03635
    h = Math.imul(h ^ (h >>> 13), 1274126177)
    return ((h ^ (h >>> 16)) >>> 0) / 4294967295
  }

  const cells: { x: number; y: number; strong: boolean }[] = []
  for (let y = 0; y < rows; y++) {
    for (let x = 0; x < cols; x++) {
      // Distance from the top-right corner, 0 at the corner, ~1 at the edge.
      const d = Math.hypot((cols - 1 - x) / cols, y / rows * 0.9)
      const p = Math.max(0, 0.55 - d * 0.75)
      const r = hash(x, y)
      if (r < p) cells.push({ x: x * cell, y: y * cell, strong: r < p * 0.25 })
    }
  }

  function move(e: PointerEvent) {
    if (raf) return
    raf = requestAnimationFrame(() => {
      raf = 0
      const r = el.getBoundingClientRect()
      el.style.setProperty('--mx', `${e.clientX - r.left}px`)
      el.style.setProperty('--my', `${e.clientY - r.top}px`)
    })
  }

  const reduced = typeof matchMedia !== 'undefined' && matchMedia('(prefers-reduced-motion: reduce)').matches
</script>

<svelte:window onpointermove={reduced ? undefined : move} />

<div class="backdrop" bind:this={el} aria-hidden="true" style:--cell="{cell}px">
  <div class="grid"></div>
  <svg class="cells" width={cols * cell} height={rows * cell} viewBox="0 0 {cols * cell} {rows * cell}" shape-rendering="crispEdges">
    {#each cells as c (c.x + ',' + c.y)}
      <rect x={c.x} y={c.y} width={cell} height={cell} class:strong={c.strong} />
    {/each}
  </svg>
</div>

<style>
  .backdrop {
    --mx: 70%;
    --my: 0px;
    position: absolute;
    inset: 0;
    z-index: 0;
    pointer-events: none;
    overflow: hidden;
    /* Kept faint so it reads as texture, not as content. */
    opacity: .55;
  }
  .grid {
    position: absolute;
    inset: 0;
    background-image:
      linear-gradient(to right, var(--grid-line) 1px, transparent 1px),
      linear-gradient(to bottom, var(--grid-line) 1px, transparent 1px);
    background-size: var(--cell) var(--cell);
    /* Lines counted from the top-right, the same origin as the cells. */
    background-position: right -1px top -1px;
    /* Visible at the top and around the pointer, gone by mid-page. */
    mask-image:
      radial-gradient(circle 300px at var(--mx) var(--my), #000 0%, transparent 100%),
      linear-gradient(to bottom, rgb(0 0 0 / .85) 0, rgb(0 0 0 / .3) 200px, transparent 460px);
    mask-composite: add;
    -webkit-mask-composite: source-over;
  }
  /* Anchored to the top-right on the same 8px lattice as the grid. */
  /* Cells cover the whole square, grid line included, so they read as flat
     painted cells rather than raised tiles. */
  .cells {
    position: absolute;
    top: 0;
    right: 0;
    mask-image: linear-gradient(to bottom left, #000 20%, transparent 80%);
  }
  .cells rect { fill: var(--cell-ink); }
  .cells rect.strong { fill: var(--cell-ink-strong); }

  @media (max-width: 760px) {
    .cells { display: none; }
  }
</style>
