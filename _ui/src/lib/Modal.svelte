<script lang="ts">
  import type { Snippet } from 'svelte'

  let {
    title,
    open = $bindable(false),
    width = 480,
    children,
    footer,
  }: { title: string; open: boolean; width?: number; children: Snippet; footer?: Snippet } = $props()

  let dialog: HTMLDialogElement

  $effect(() => {
    if (open && !dialog.open) dialog.showModal()
    if (!open && dialog.open) dialog.close()
  })
</script>

<dialog bind:this={dialog} onclose={() => (open = false)} style:width="{width}px">
  {#if open}
    <header>
      <h2>{title}</h2>
      <button class="ghost" aria-label="Kapat" onclick={() => (open = false)}>✕</button>
    </header>
    <div class="body">{@render children()}</div>
    {#if footer}<footer>{@render footer()}</footer>{/if}
  {/if}
</dialog>

<style>
  dialog {
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 0;
    max-width: calc(100vw - 32px);
    color: var(--text);
    box-shadow: 0 12px 40px rgb(0 0 0 / .12);
  }
  dialog::backdrop { background: rgb(15 20 25 / .35); }
  header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 12px 16px;
    border-bottom: 1px solid var(--border);
  }
  .body { padding: 16px; display: grid; gap: 12px; }
  footer {
    display: flex;
    justify-content: flex-end;
    gap: 8px;
    padding: 12px 16px;
    border-top: 1px solid var(--border);
    background: var(--surface-2);
  }
</style>
