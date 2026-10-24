<script lang="ts">
  import type { Message } from './api'
  import { fmt } from './format'

  // A record value's encoding, schema, size and truncation, as badges.
  let { message: m }: { message: Message } = $props()
</script>

<span class="badge" class:accent={!!m.value_schema_id}>{m.value_format ?? 'null'}</span>
{#if m.value_schema_id}<span class="faint">schema #{m.value_schema_id}</span>{/if}
<span class="faint">{fmt(m.value_bytes)} bytes</span>
{#if m.truncated}<span class="badge warn">truncated</span>{/if}
