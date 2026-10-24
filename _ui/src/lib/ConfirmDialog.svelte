<script lang="ts">
  import type { Snippet } from 'svelte'
  import { errorMessage } from './http'
  import Modal from './Modal.svelte'

  // Asks before a destructive change. onconfirm runs the change; the dialog
  // closes when it succeeds and shows the error, staying open, when it fails.
  // Like the other dialogs, mount it only while open, so each open starts
  // with an empty confirmation and no old error.
  let {
    title,
    open = $bindable(false),
    confirmLabel = 'Delete',
    busyLabel,
    requireText,
    requirePrompt = 'Type the name to confirm',
    danger = true,
    disabled = false,
    width,
    onconfirm,
    children,
  }: {
    title: string
    open: boolean
    confirmLabel?: string
    /** Shown on the button while onconfirm runs. */ busyLabel?: string
    /** Text the user must type before confirming. */ requireText?: string
    requirePrompt?: string
    danger?: boolean
    disabled?: boolean
    width?: number
    onconfirm: () => Promise<void>
    children: Snippet
  } = $props()

  let typed = $state('')
  let busy = $state(false)
  let error = $state('')

  async function confirm() {
    busy = true
    error = ''
    try {
      await onconfirm()
      open = false
    } catch (e) {
      error = errorMessage(e)
    } finally {
      busy = false
    }
  }
</script>

<Modal {title} bind:open {width}>
  {@render children()}
  {#if requireText !== undefined}
    <label>{requirePrompt} <input bind:value={typed} class="mono" autocomplete="off" /></label>
  {/if}
  {#if error}<div class="error" role="alert">{error}</div>{/if}
  {#snippet footer()}
    <button onclick={() => (open = false)}>Cancel</button>
    <button class={danger ? 'danger solid' : 'primary'} disabled={busy || disabled || (requireText !== undefined && typed !== requireText)}
      onclick={confirm}>{busy && busyLabel ? busyLabel : confirmLabel}</button>
  {/snippet}
</Modal>
