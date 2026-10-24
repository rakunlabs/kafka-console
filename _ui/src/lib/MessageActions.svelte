<script lang="ts">
  import type { Message } from './api'
  import { Pencil, Repeat } from '@lucide/svelte'

  // Edit & resend and Resend for one message; compact in an expanded row.
  let {
    message,
    editing,
    compact = false,
    onedit,
    onresend,
  }: {
    message: Message
    editing: boolean
    compact?: boolean
    onedit: (m: Message) => void
    onresend: (m: Message) => void
  } = $props()

  const size = $derived(compact ? 12 : 13)
</script>

<button type="button" class={compact ? 'ghost small' : 'small'} disabled={editing} title={compact ? undefined : 'Open this message in Produce'}
  onclick={() => onedit(message)}><Pencil {size} />{editing ? 'Loading…' : 'Edit & resend'}</button>
<button type="button" class={compact ? 'ghost small' : 'small'} title={compact ? undefined : 'Resend this message byte for byte'}
  onclick={() => onresend(message)}><Repeat {size} />Resend…</button>
