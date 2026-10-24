<script lang="ts">
  import { untrack } from 'svelte'
  import { errorMessage } from './http'
  import { topicNames } from './topicNames.svelte'
  import { api, type Cluster, type Message, type ResendResult } from './api'
  import { fmt } from './format'
  import { href } from './router.svelte'
  import Modal from './Modal.svelte'
  import TopicPicker from './TopicPicker.svelte'
  import { Send, Check, TriangleAlert, Repeat } from '@lucide/svelte'

  // Copies selected messages, byte for byte, to this topic or another one.
  // Mounted only while open, so every open starts from a fresh form.
  let {
    cluster,
    topic,
    messages,
    open = $bindable(false),
  }: { cluster: Cluster; topic: string; messages: Message[]; open: boolean } = $props()

  let target = $state('')
  let elsewhere = $state(false)
  let keepPartition = $state(false)
  let mark = $state(true)
  const names = topicNames(untrack(() => cluster.name))
  let results = $state<ResendResult[]>()
  let error = $state('')
  let busy = $state(false)
  let confirm = $state('')

  const dest = $derived(elsewhere ? target : topic)
  const same = $derived(dest === topic)
  const destInfo = $derived(names.topics.find((t) => t.name === dest))
  const partitionsUsed = $derived([...new Set(messages.map((m) => m.partition))].sort((a, b) => a - b))
  const missing = $derived(keepPartition && destInfo ? partitionsUsed.filter((p) => p >= destInfo.partitions) : [])
  const ok = $derived(results?.filter((r) => !r.error) ?? [])
  const failed = $derived(results?.filter((r) => r.error) ?? [])
  // Re-sending into the topic a consumer already read means it reads it again;
  // clusters with confirm_writes ask for the topic name first.
  const needsConfirm = $derived(cluster.confirm_writes)


  async function send() {
    busy = true
    error = ''
    try {
      results = await api.resend(cluster.name, topic, {
        records: messages.map((m) => ({ partition: m.partition, offset: m.offset })),
        target: dest,
        keep_partition: keepPartition,
        mark_source: mark,
      })
    } catch (e) {
      error = errorMessage(e)
    } finally {
      busy = false
    }
  }
</script>

<Modal title="Resend {messages.length === 1 ? 'message' : `${messages.length} messages`}" bind:open width={620}>
  {#if results}
    {#if ok.length}
      <div class="donemsg"><Check size={15} /><span><b>{ok.length}</b> {ok.length === 1 ? 'message' : 'messages'} written to <a class="mono" href={href(cluster.name, 'topics', dest)}>{dest}</a>.</span></div>
    {/if}
    <div class="panel tbl">
      <table>
        <thead><tr><th>From</th><th>To</th><th>Result</th></tr></thead>
        <tbody>
          {#each results as r, i (i)}
            <tr>
              <td class="mono">p{r.source.partition} · {fmt(r.source.offset)}</td>
              <td class="mono">{r.error ? '—' : `p${r.partition} · ${fmt(r.offset)}`}</td>
              <td>{#if r.error}<span class="err">{r.error}</span>{:else}<span class="badge ok">sent</span>{/if}</td>
            </tr>
          {/each}
        </tbody>
      </table>
    </div>
    {#if failed.length}<p class="err small">{failed.length} could not be sent.</p>{/if}
  {:else}
    <p class="lead">
      Key, value and headers are copied exactly as stored, including schema-encoded and binary payloads.
      Order is kept as listed: {messages.slice(0, 4).map((m) => `p${m.partition}·${m.offset}`).join(', ')}{messages.length > 4 ? ` and ${messages.length - 4} more` : ''}.
    </p>

    <fieldset class="modes" aria-label="Destination">
      <label class="mode" class:on={!elsewhere}><input type="radio" name="dest" checked={!elsewhere} onchange={() => (elsewhere = false)} /><Repeat size={14} />Same topic</label>
      <label class="mode" class:on={elsewhere}><input type="radio" name="dest" checked={elsewhere} onchange={() => (elsewhere = true)} /><Send size={14} />Another topic</label>
    </fieldset>

    {#if elsewhere}
      <TopicPicker topics={names.topics} bind:value={target} id="resend-target" />
      {#if names.error}<p class="err small">{names.error}</p>{/if}
    {:else}
      <div class="warnbox"><TriangleAlert size={15} /><span>Consumers of <span class="mono">{topic}</span> will receive these messages again, as new records at the end.</span></div>
    {/if}

    <label class="check opt"><input type="checkbox" bind:checked={keepPartition} /> Keep partition numbers
      <span class="hint">Off: the key decides the partition, as for any producer.</span></label>
    {#if missing.length}<p class="err small">{dest} has {destInfo?.partitions} partitions; partition {missing.join(', ')} does not exist there.</p>{/if}
    <label class="check opt"><input type="checkbox" bind:checked={mark} /> Add an <code>x-resent-from</code> header
      <span class="hint">e.g. <code>{topic}/{messages[0]?.partition ?? 0}/{messages[0]?.offset ?? 0}</code>, so a replay can be told apart from the original.</span></label>

    {#if needsConfirm}
      <label>Type <b class="mono">{dest || 'the topic name'}</b> to confirm
        <input class="mono" bind:value={confirm} />
      </label>
    {/if}
  {/if}

  {#if error}<div class="error">{error}</div>{/if}

  {#snippet footer()}
    {#if results}
      <button class="primary" onclick={() => (open = false)}>Done</button>
    {:else}
      <button onclick={() => (open = false)}>Cancel</button>
      <button class="primary" onclick={send}
        disabled={busy || !dest || missing.length > 0 || (needsConfirm && confirm !== dest) || cluster.read_only}>
        <Send size={14} />{busy ? 'Sending…' : `Send to ${dest || '…'}`}
      </button>
    {/if}
  {/snippet}
</Modal>

<style>
  .tbl { max-height: 300px; overflow: auto; }
</style>
