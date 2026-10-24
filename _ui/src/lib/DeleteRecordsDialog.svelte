<script lang="ts">
  import { api, type Cluster, type DeleteRecordsResult, type DeleteRecordsStrategy } from './api'
  import { errorMessage } from './http'
  import { fmt, inputMs, partitionList } from './format'
  import Modal from './Modal.svelte'
  import PartitionsInput from './PartitionsInput.svelte'

  // Deletes records from the start of some or all partitions of a topic.
  // Mounted only while open, so every open starts from a fresh form.
  let {
    cluster,
    topic,
    partitionCount,
    open = $bindable(false),
    ondone,
  }: { cluster: Cluster; topic: string; partitionCount: number; open: boolean; ondone?: () => void } = $props()

  let strategy = $state<DeleteRecordsStrategy>('all')
  let offset = $state(0)
  let timestamp = $state('')
  let partitionsText = $state('')
  let confirm = $state('')
  let busy = $state(false)
  let error = $state('')
  let result = $state<DeleteRecordsResult[]>()

  const parts = $derived(partitionList(partitionsText))
  const valid = $derived(
    strategy === 'all' ||
      (strategy === 'offset' && Number.isInteger(offset) && offset >= 0) ||
      (strategy === 'timestamp' && !!timestamp && !Number.isNaN(new Date(timestamp).getTime())),
  )
  // Says in words what the button is about to do.
  const summary = $derived.by(() => {
    const where = parts?.length
      ? `${parts.length === 1 ? 'partition' : 'partitions'} ${parts.join(', ')}`
      : `all ${partitionCount} partitions`
    if (strategy === 'all') return `Every record in ${where} will be deleted; the topic itself stays.`
    if (strategy === 'offset') return `Records before offset ${fmt(Number(offset) || 0)} in ${where} will be deleted.`
    return `Records written before ${timestamp ? new Date(timestamp).toLocaleString() : 'the chosen time'} in ${where} will be deleted.`
  })

  async function run() {
    busy = true
    error = ''
    try {
      result = await api.deleteRecords(cluster.name, topic, {
        strategy,
        offset: strategy === 'offset' ? Number(offset) : undefined,
        timestamp: strategy === 'timestamp' ? inputMs(timestamp) : undefined,
        partitions: parts ?? [],
      })
      ondone?.()
    } catch (e) {
      error = errorMessage(e)
    } finally {
      busy = false
    }
  }
</script>

<Modal title="Delete records" bind:open width={520}>
  {#if result}
    {#if result.some((r) => !r.error)}
      <div class="ok">Records deleted. New start offsets:</div>
    {:else}
      <div class="error">No records were deleted.</div>
    {/if}
    <div class="panel">
      <table>
        <thead><tr><th class="num" title="Partition"><abbr>P</abbr></th><th class="num">New low</th><th></th></tr></thead>
        <tbody>
          {#each result as r (r.partition)}
            <tr><td class="num mono">{r.partition}</td><td class="num mono">{fmt(r.low_watermark)}</td><td>{#if r.error}<span class="badge danger">{r.error}</span>{/if}</td></tr>
          {/each}
        </tbody>
      </table>
    </div>
  {:else}
    <div class="grid2">
      <label>Range to delete
        <select bind:value={strategy}>
          <option value="all">Everything (empty the topic)</option>
          <option value="offset">Records before an offset</option>
          <option value="timestamp">Records before a time</option>
        </select>
      </label>
      {#if strategy === 'offset'}
        <label>Offset <input type="number" min="0" step="1" bind:value={offset} aria-invalid={!valid} /></label>
      {:else if strategy === 'timestamp'}
        <label>Time <input type="datetime-local" step="1" bind:value={timestamp} aria-invalid={!valid} /></label>
      {:else}
        <div></div>
      {/if}
    </div>
    <PartitionsInput bind:text={partitionsText} />
    {#if parts}
      <p class="what">{summary} This cannot be undone.</p>
    {/if}
    <label>Type the topic name to confirm <input bind:value={confirm} class="mono" /></label>
  {/if}
  {#if error}<div class="error">{error}</div>{/if}
  {#snippet footer()}
    <button onclick={() => (open = false)}>{result ? 'Close' : 'Cancel'}</button>
    {#if !result}
      <button class="danger solid" disabled={busy || confirm !== topic || !parts || !valid} onclick={run}>{busy ? 'Deleting…' : strategy === 'all' ? 'Delete all records' : 'Delete records'}</button>
    {/if}
  {/snippet}
</Modal>

<style>
  .what { padding: 8px 10px; font-size: 13px; color: var(--danger); background: var(--danger-soft); border: 1px solid var(--danger-border); border-radius: var(--radius); }
</style>
