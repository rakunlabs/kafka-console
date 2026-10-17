<script lang="ts">
  // Mark: a scan frame over three log lines, the middle one highlighted the
  // way a selected record is. Corners echo the corner ticks used across the UI.
  let { size = 22, title }: { size?: number; title?: string } = $props()
</script>

<svg
  width={size}
  height={size}
  viewBox="0 0 24 24"
  fill="none"
  stroke-linecap="round"
  stroke-linejoin="round"
  role={title ? 'img' : undefined}
  aria-label={title}
  aria-hidden={title ? undefined : 'true'}
>
  <rect class="hl" x="5.2" y="10.1" width="13.6" height="3.8" rx="0.6" />
  <g class="ink" stroke-width="1.8">
    <path d="M3 7V5a2 2 0 0 1 2-2h2" />
    <path d="M17 3h2a2 2 0 0 1 2 2v2" />
    <path d="M21 17v2a2 2 0 0 1-2 2h-2" />
    <path d="M7 21H5a2 2 0 0 1-2-2v-2" />
  </g>
  <g class="ink" stroke-width="1.8">
    <path class="dim" d="M7 7.5h7" />
    <path d="M7 12h10" />
    <path class="dim" d="M7 16.5h5" />
  </g>
</svg>

<style>
  svg { flex: none; display: block; }
  .hl { fill: var(--cta); stroke: var(--cta-border); stroke-width: 0.6; }
  .ink { stroke: var(--text); }
  .dim { stroke: var(--text-2); }
</style>
