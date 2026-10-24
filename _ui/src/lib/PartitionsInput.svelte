<script lang="ts">
  import { partitionList } from './format'

  // A comma-separated list of partition numbers; empty means all of them.
  // The owner reads the list with partitionList(text), undefined while invalid.
  let { text = $bindable(''), disabled = false }: { text: string; disabled?: boolean } = $props()

  const valid = $derived(!!partitionList(text))
</script>

<label>Partitions (empty = all) <input class="mono" placeholder="0, 1, 2" bind:value={text} {disabled} aria-invalid={!valid} /></label>
{#if !valid}<div class="error small">Use partition numbers separated by commas, e.g. 0, 1, 2.</div>{/if}
