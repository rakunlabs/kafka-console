<script lang="ts">
  import type { Snippet } from 'svelte'
  import { X } from '@lucide/svelte'

  let {
    title,
    open = $bindable(false),
    width = 480,
    children,
    footer,
    locked = false,
    dismissable = false,
  }: {
    title: string; open: boolean; width?: number; children: Snippet; footer?: Snippet
    /** Work is running: Esc and the close button do nothing. */ locked?: boolean
    /** A click outside closes it. Only for dialogs with nothing to lose. */ dismissable?: boolean
  } = $props()

  let dialog: HTMLDialogElement

  // A click outside closes only when it also started outside: a press inside
  // (selecting text, dragging) released over the backdrop fires click on the
  // dialog too, and must not close it.
  let downOutside = false
  const outside = (e: MouseEvent) => {
    const r = dialog.getBoundingClientRect()
    return e.target === dialog && (e.clientX < r.left || e.clientX > r.right || e.clientY < r.top || e.clientY > r.bottom)
  }
  function down(e: PointerEvent) {
    downOutside = dismissable && e.button === 0 && outside(e)
  }
  function click(e: MouseEvent) {
    const close = downOutside && outside(e) && !locked
    downOutside = false
    if (close) open = false
  }

  $effect(() => {
    if (open && !dialog.open) dialog.showModal()
    if (!open && dialog.open) dialog.close()
  })
</script>

<dialog bind:this={dialog} class="ticks" oncancel={(e) => locked && e.preventDefault()} onclose={() => (open = false)} onpointerdown={down} onclick={click} style:width="{width}px">
  {#if open}
    <header>
      <h2>{title}</h2>
      <button class="ghost px-1.5" aria-label="Close" disabled={locked} onclick={() => (open = false)}><X size={16} /></button>
    </header>
    <div class="body">{@render children()}</div>
    {#if footer}<footer>{@render footer()}</footer>{/if}
  {/if}
</dialog>

<style>
  dialog {
    /* Anchored near the top and centred only horizontally, so content that
       appears after opening (a preview, a second step) grows the dialog
       downward instead of shifting it up. */
    margin: min(10vh, 96px) auto auto;
    max-height: calc(100vh - min(10vh, 96px) - 24px);
    overflow: hidden;
    border: 1px solid var(--border-strong);
    border-radius: var(--radius);
    padding: 0;
    max-width: calc(100vw - 32px);
    color: var(--text);
    background: var(--surface);
    box-shadow: 0 16px 48px -12px rgb(20 20 16 / .22);
  }
  /* Header and footer stay put; only the body scrolls when the dialog hits
     the bottom of the screen, so the action buttons never scroll away. */
  dialog[open] { display: flex; flex-direction: column; }
  dialog::backdrop { background: rgb(23 23 20 / .32); }
  header, footer { flex: none; }
  header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 12px 16px;
    border-bottom: 1px solid var(--border);
  }
  .body { padding: 16px; display: grid; gap: 12px; min-height: 0; overflow: auto; }

  @media (max-height: 640px) {
    dialog { margin-top: 12px; max-height: calc(100vh - 24px); }
  }
  footer {
    display: flex;
    justify-content: flex-end;
    gap: 8px;
    padding: 12px 16px;
    border-top: 1px solid var(--border);
    background: var(--surface-2);
  }
</style>
