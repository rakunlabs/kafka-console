<script lang="ts">
  // Loading marker: three grid cells lighting up in turn, the same squares
  // the backdrop and status markers use, above a short label.
  let { label = 'Loading…' }: { label?: string } = $props()
</script>

<span class="pending" role="status">
  <span class="cells" aria-hidden="true"><i></i><i></i><i></i></span>
  {label}
</span>

<style>
  .pending { display: inline-flex; flex-direction: column; align-items: center; gap: 10px; }
  .cells { display: flex; gap: 3px; }
  i {
    width: 7px;
    height: 7px;
    background: var(--text-3);
    opacity: .2;
    animation: cell 1.2s infinite;
  }
  i:nth-child(2) { animation-delay: .2s; }
  i:nth-child(3) { animation-delay: .4s; }
  @keyframes cell {
    0%, 60%, 100% { opacity: .2; }
    20% { opacity: 1; background: var(--text); }
  }
  @media (prefers-reduced-motion: reduce) {
    i { animation: none; opacity: .5; }
  }
</style>
