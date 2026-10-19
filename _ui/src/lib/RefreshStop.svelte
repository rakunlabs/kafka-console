<script lang="ts">
  import { Loader, RefreshCw, Square } from '@lucide/svelte'

  // Refresh that turns into Stop while a load runs. Stopping aborts the
  // request on the server too and keeps the rows already shown.
  let { loading, onrefresh, onstop }: { loading: boolean; onrefresh: () => void; onstop: () => void } = $props()
</script>

{#if loading}
  <button onclick={onstop} title="Stop loading (Esc)"><Loader size={14} class="animate-spin" />Stop</button>
{:else}
  <button onclick={onrefresh}><RefreshCw size={14} />Refresh</button>
{/if}

<svelte:window onkeydown={(e) => {
  if (e.key !== 'Escape' || !loading) return
  // An open dialog or menu owns Esc.
  if (document.querySelector('dialog[open], details.more[open]')) return
  onstop()
}} />
