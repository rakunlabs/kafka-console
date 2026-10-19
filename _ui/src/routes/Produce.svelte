<script lang="ts">
  import { untrack } from 'svelte'
  import { api, type Cluster, type SubjectSummary, type SchemaVersion, type Message, type ProduceBody, type TopicName } from '../lib/api'
  import { time, jsonProblem } from '../lib/format'
  import { messageDraft, produceChanges, formatPayload } from '../lib/produce'
  import { names } from '../lib/names'
  import { href } from '../lib/router.svelte'
  import TopicPicker from '../lib/TopicPicker.svelte'
  import Modal from '../lib/Modal.svelte'
  import { Plus, X, Send, WandSparkles, AlignLeft } from '@lucide/svelte'

  let { cluster, topic, partitions, source, onclear }: { cluster: Cluster; topic: string; partitions: number[]; source?: Message; onclear: () => void } = $props()
  const initial = untrack(() => source ? messageDraft(source) : undefined)
  let target = $state(untrack(() => topic))
  let topics = $state<TopicName[]>([])
  let choosingTarget = $state(false)
  let topicsError = $state('')
  const targetPartitions = $derived(target === topic ? partitions : Array.from({ length: topics.find((t) => t.name === target)?.partitions ?? 0 }, (_, i) => i))

  async function chooseTarget() {
    topicsError = ''
    try {
      topics = (await names(cluster.name)).topics
      choosingTarget = true
    } catch (e: any) {
      topicsError = `Could not load topics: ${e.message}. Try changing the target again.`
    }
  }

  let valueEncoding = $state<'text' | 'base64' | 'schema'>(initial?.value_schema ? 'schema' : initial?.value_encoding === 'base64' ? 'base64' : 'text')
  let keyEncoding = $state<'text' | 'base64' | 'schema'>(initial?.key_schema ? 'schema' : initial?.key_encoding === 'base64' ? 'base64' : 'text')
  let compression = $state('')
  let subjects = $state<SubjectSummary[]>([])
  let subject = $state('')
  let schema = $state<SchemaVersion>()
  let message = $state('')

  function loadSubjects() {
    if (!cluster.schema_registry) return
    api
      .registry(cluster.name)
      .then((r) => {
        subjects = r.subjects
        // TopicNameStrategy is the common default.
        if (!source && r.subjects.some((s) => s.name === `${topic}-value`)) {
          subject = `${topic}-value`
          valueEncoding = 'schema'
          loadSchema()
        }
      })
      .catch(() => {})
  }
  loadSubjects()

  async function loadSchema() {
    schema = undefined
    message = ''
    if (!subject) return
    try {
      const d = await api.subject(cluster.name, subject)
      schema = d.versions.find((v) => !v.deleted)
      message = schema?.messages?.[0] ?? ''
    } catch {}
  }

  // A skeleton built from the Avro record so the user starts from a valid shape.
  function template() {
    if (!schema) return
    try {
      if (schema.type === 'AVRO') value = JSON.stringify(avroSample(JSON.parse(schema.schema)), null, 2)
      else if (schema.type === 'JSON') value = JSON.stringify(jsonSample(JSON.parse(schema.schema)), null, 2)
      else value = '{}'
    } catch {}
  }

  function avroSample(s: any, depth = 0): any {
    if (depth > 8) return null
    if (typeof s === 'string') {
      return ({ null: null, boolean: false, int: 0, long: 0, float: 0, double: 0, string: '', bytes: '' } as Record<string, any>)[s] ?? {}
    }
    if (Array.isArray(s)) return avroSample(s.find((t) => t !== 'null') ?? 'null', depth + 1)
    if (s.logicalType === 'timestamp-millis' || s.logicalType === 'timestamp-micros') return new Date().toISOString()
    if (s.logicalType === 'date') return new Date().toISOString().slice(0, 10)
    if (s.logicalType === 'decimal') return '0.00'
    switch (s.type) {
      case 'record':
        return Object.fromEntries((s.fields ?? []).map((f: any) => [f.name, 'default' in f ? f.default : avroSample(f.type, depth + 1)]))
      case 'enum':
        return s.symbols?.[0] ?? ''
      case 'array':
        return []
      case 'map':
        return {}
      case 'fixed':
        return ''
      default:
        return avroSample(s.type, depth + 1)
    }
  }

  function jsonSample(s: any, depth = 0): any {
    if (!s || depth > 8) return null
    if (s.default !== undefined) return s.default
    if (s.enum) return s.enum[0]
    const t = Array.isArray(s.type) ? s.type.find((x: string) => x !== 'null') : s.type
    switch (t) {
      case 'object':
        return Object.fromEntries(Object.entries(s.properties ?? {}).map(([k, v]) => [k, jsonSample(v, depth + 1)]))
      case 'array':
        return []
      case 'string':
        return ''
      case 'integer':
      case 'number':
        return 0
      case 'boolean':
        return false
    }
    return null
  }

  let key = $state(initial?.key ?? '')
  let nullKey = $state(initial ? initial.key === null : true)
  let value = $state(initial?.value ?? '')
  let nullValue = $state(initial ? initial.value === null : false)
  let partition = $state(initial?.partition === undefined || initial.partition === null ? '' : String(initial.partition))
  let headers = $state<ProduceBody['headers']>(initial?.headers.map((h) => ({ ...h })) ?? [])

  let busy = $state(false)
  let error = $state('')
  let sent = $state<{ topic: string; partition: number; offset: number; timestamp: string }[]>([])
  let reviewing = $state(false)
  let confirmation = $state('')
  let pending = $state<{ target: string; body: ProduceBody }>()
  const changes = $derived(initial && pending ? produceChanges(initial, pending.body, topic, pending.target) : [])
  const valueSchema = $derived(valueEncoding === 'schema' ? (subject ? { subject, message: message || undefined } : initial?.value_schema) : undefined)
  const invalid = $derived.by(() => {
    if (!target || (target !== topic && !topics.some((t) => t.name === target))) return 'Select an existing target topic.'
    if (partition !== '' && !targetPartitions.includes(Number(partition))) return `Partition ${partition} does not exist in ${target}. Choose Automatic or another partition.`
    if (!nullValue && valueEncoding === 'schema' && !valueSchema) return 'Select a schema subject.'
    if (!nullValue && valueEncoding === 'schema') {
      try { JSON.parse(value) } catch { return 'Value must be valid JSON to encode with a schema.' }
    }
    if (!nullKey && keyEncoding === 'schema') {
      try { JSON.parse(key) } catch { return 'Key must be valid JSON to encode with its schema.' }
    }
    for (const [label, text] of [
      ...(!nullKey && keyEncoding === 'base64' ? [['Key', key]] : []),
      ...(!nullValue && valueEncoding === 'base64' ? [['Value', value]] : []),
      ...headers.filter((h) => h.key && !h.null && h.encoding === 'base64').map((h) => [`Header ${h.key}`, h.value]),
    ]) {
      // Match the server's padded RFC 4648 encoding, allowing CR/LF.
      if (!/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(text.trim().replace(/[\r\n]/g, ''))) return `${label} must be valid base64.`
    }
    return ''
  })

  // Values that look like JSON (or must be JSON for schema encoding) are checked as you type.
  const jsonError = $derived.by(() => {
    const v = value.trim()
    if (!v || nullValue) return undefined
    if (valueEncoding !== 'schema' && !(v.startsWith('{') || v.startsWith('['))) return undefined
    if (valueEncoding === 'base64') return undefined
    return jsonProblem(value)
  })

  function format() {
    try {
      value = formatPayload(value)
    } catch {}
  }

  function body(): ProduceBody {
    return {
      key: nullKey ? null : key,
      value: nullValue ? null : value,
      headers: (source ? headers : headers.filter((h) => h.key.trim())).map((h) => ({ ...h, null: h.null || undefined })),
      partition: partition === '' ? null : Number(partition),
      key_encoding: keyEncoding === 'base64' ? 'base64' : 'utf8',
      value_encoding: valueEncoding === 'base64' ? 'base64' : 'utf8',
      key_schema: keyEncoding === 'schema' ? initial?.key_schema : undefined,
      value_schema: valueSchema,
      compression: compression || undefined,
    }
  }

  function submit() {
    if (busy || cluster.read_only || invalid) return
    if (source) {
      pending = { target, body: JSON.parse(JSON.stringify(body())) }
      confirmation = ''
      error = ''
      reviewing = true
    } else {
      send(target, body())
    }
  }

  async function send(destination: string, request: ProduceBody) {
    if (busy || cluster.read_only || (source && (!reviewing || (cluster.production && confirmation !== destination)))) return
    busy = true
    error = ''
    try {
      const res = await api.produce(cluster.name, destination, request)
      sent = [{ ...res, topic: destination }, ...sent].slice(0, 10)
      reviewing = false
    } catch (e: any) {
      error = e.message
    } finally {
      busy = false
    }
  }
</script>

{#if cluster.read_only}
  <div class="error">This cluster is configured as read-only; messages cannot be produced.</div>
{:else}
  {#if source}
    <div class="panel source-note">
      <div><b>Edit & resend</b> <span class="mono">{topic} / p{source.partition} / {source.offset}</span></div>
      <p class="faint">This creates a new record. The original message is not changed. The draft stays in this tab only; reloading or leaving the topic discards it.</p>
      {#if initial?.key_schema || initial?.value_schema}<p class="faint">Schema payloads are re-encoded using the original schema ID and message type. For an exact byte-for-byte copy, use Resend instead.</p>{/if}
      <div class="row"><span>Target <b class="mono">{target}</b></span><span class="spacer"></span>
        <button type="button" onclick={chooseTarget}>Change target</button>
        <button type="button" onclick={onclear}>Discard draft</button>
      </div>
      {#if choosingTarget}<TopicPicker {topics} bind:value={target} id="edit-target" /><button type="button" onclick={() => (choosingTarget = false)}>Use {target}</button>{/if}
      {#if topicsError}<div class="error" role="alert">{topicsError}</div>{/if}
    </div>
  {/if}
  <div class="layout">
    <form class="panel form" onsubmit={(e) => { e.preventDefault(); submit() }}>
      <div class="grid">
        <div class="field">
          <label class="lbl" for="produce-key">Key</label>
          <!-- Typing a key switches null off; ticking null keeps the typed text for later. -->
          <div class="keybox" class:isnull={nullKey}>
            <input id="produce-key" bind:value={key} class="mono" placeholder={nullKey ? 'null' : '(empty string)'}
              oninput={() => (nullKey = false)} />
            <label class="nullsel" title="Send the record without a key (null). The partitioner then spreads records across partitions.">
              <input type="checkbox" bind:checked={nullKey} />No key (null)
            </label>
          </div>
        </div>
        <label>Partition
          <select bind:value={partition}>
            <option value="">Automatic (by key)</option>
             {#if partition !== '' && !targetPartitions.includes(Number(partition))}<option value={partition}>{partition} (unavailable)</option>{/if}
             {#each targetPartitions as p (p)}<option value={String(p)}>{p}</option>{/each}
          </select>
        </label>
      </div>

      {#if source}
        <label>Key format
          <select bind:value={keyEncoding} disabled={nullKey}>
            <option value="text">Text / JSON</option>
            <option value="base64">Binary (base64)</option>
            {#if initial?.key_schema}<option value="schema">Original schema #{initial.key_schema.id}{initial.key_schema.message ? ` · ${initial.key_schema.message}` : ''}</option>{/if}
          </select>
        </label>
      {/if}

      <div class="grid">
        <label>Value format
          <select bind:value={valueEncoding}>
            <option value="text">Text / JSON</option>
            <option value="base64">Binary (base64)</option>
            {#if cluster.schema_registry}<option value="schema">Encode with Schema Registry</option>{/if}
          </select>
        </label>
        <label>Compression
          <select bind:value={compression}>
            <option value="">None</option>
            <option value="gzip">gzip</option>
            <option value="snappy">snappy</option>
            <option value="lz4">lz4</option>
            <option value="zstd">zstd</option>
          </select>
        </label>
      </div>

      {#if valueEncoding === 'schema'}
        <div class="grid">
          <label>Subject
            <select bind:value={subject} onchange={loadSchema}>
              <option value="">{initial?.value_schema ? `Original schema #${initial.value_schema.id}` : 'Select…'}</option>
              {#each subjects as s (s.name)}<option value={s.name}>{s.name} ({s.type})</option>{/each}
            </select>
          </label>
          {#if schema?.messages?.length}
            <label>Protobuf message
              <select bind:value={message}>
                {#each schema.messages as m (m)}<option value={m}>{m}</option>{/each}
              </select>
            </label>
          {:else if !subject && initial?.value_schema}
            <div class="schemainfo faint">Original schema #{initial.value_schema.id}{initial.value_schema.message ? ` · ${initial.value_schema.message}` : ''}</div>
          {:else if schema}
            <div class="schemainfo faint">v{schema.version} · id {schema.id} · {schema.type}</div>
          {/if}
        </div>
        <p class="faint small-hint">Write the value as JSON; the server validates and encodes it in Confluent format. {initial?.value_schema && !subject ? 'The original schema ID is kept.' : 'The latest version of the selected subject is used.'} For Avro unions, both a plain value and the <code>{'{"string": "x"}'}</code> form are accepted.</p>
      {/if}

      <div class="field">
        <div class="row">
           <span class="lbl">Value</span>
           <span class="spacer"></span>
           <label class="check"><input type="checkbox" bind:checked={nullValue} />Null value (tombstone)</label>
          {#if valueEncoding === 'schema' && schema}
            <button type="button" class="ghost small" onclick={template}><WandSparkles size={13} />Generate template</button>
          {/if}
          <button type="button" class="ghost small" onclick={format} disabled={!!jsonError || !value.trim()}><AlignLeft size={13} />Format JSON</button>
        </div>
        {#if nullValue}
          <p class="faint small-hint">A null payload will be sent, not the text “null” or an empty string. On compacted topics this is a deletion marker for the key.</p>
        {:else}
        {#await import('../lib/JsonEditor.svelte')}
          <div class="ed-loading"></div>
        {:then { default: JsonEditor }}
          <JsonEditor id="produce-value" rows={12} bind:value error={jsonError} mode={valueEncoding === 'base64' ? 'plain' : 'json'}
            onrun={submit}
            placeholder={valueEncoding === 'base64' ? 'SGVsbG8sIEthZmthIQ==' : '{"id": 1, "status": "NEW"}'} />
        {/await}
        {/if}
      </div>

      <div>
        <div class="row hhead"><span class="lbl">Headers</span><span class="spacer"></span>
          <button type="button" class="ghost small" onclick={() => (headers = [...headers, { key: '', value: '', encoding: 'utf8' }])}><Plus size={13} />Add header</button>
        </div>
        {#each headers as h, i (i)}
          <div class="row hrow">
            <input placeholder="key" aria-label="Header {i + 1} key" class="mono" bind:value={h.key} />
            <input placeholder={h.null ? 'null' : 'value'} disabled={h.null} aria-label="Header {i + 1} value" class="mono" bind:value={h.value} />
            {#if source}
              <select aria-label="Header {i + 1} encoding" disabled={h.null} bind:value={h.encoding}><option value="utf8">Text</option><option value="base64">Base64</option></select>
              <label class="check"><input type="checkbox" bind:checked={h.null} />Null</label>
            {/if}
            <button type="button" class="ghost" aria-label="Remove header" onclick={() => (headers = headers.filter((_, j) => j !== i))}><X size={14} /></button>
          </div>
        {/each}
      </div>

      {#if error}<div class="error">{error}</div>{/if}
      {#if invalid}<p class="error" role="alert">{invalid}</p>{/if}

      <div class="row submit">
        {#if cluster.production}<span class="prodnote">You are producing to a production cluster.</span>{/if}
        <span class="spacer"></span>
        <button class="primary" type="submit" disabled={busy || !!invalid}><Send size={14} />{busy ? 'Sending…' : source ? 'Review & send' : 'Send'}</button>
      </div>
    </form>

    <div class="panel">
      <table>
        <thead><tr><th colspan="3">Sent this session</th></tr></thead>
        <tbody>
          {#each sent as s (`${s.topic}:${s.partition}:${s.offset}`)}
            <tr><td class="mono"><a href={href(cluster.name, 'topics', s.topic)}>{s.topic}</a><br />p{s.partition}</td><td class="mono num">{s.offset}</td><td class="mono faint">{time(s.timestamp)}</td></tr>
          {:else}
            <tr><td colspan="3" class="faint">No messages sent yet.</td></tr>
          {/each}
        </tbody>
      </table>
    </div>
  </div>
{/if}

<Modal title="Review edited message" bind:open={reviewing} width={760}>
  {#if pending}
    <p>Write one new record to <b class="mono">{pending.target}</b> in <b>{cluster.name}</b>, partition <b>{pending.body.partition ?? 'Automatic (by key)'}</b>.</p>
    <p class="faint">The original record stays unchanged. Consumers of the target topic may process the new record again.</p>
    {#if changes.length}
      <div class="review-changes">
        {#each changes as change (change.field)}
          <section class="change">
            <h3>{change.field}</h3>
            <div class="comparison">
              <div><span class="faint">Original</span><pre>{change.before}</pre></div>
              <div><span class="faint">To send</span><pre>{change.after}</pre></div>
            </div>
          </section>
        {/each}
      </div>
    {:else}<p>No fields changed. Sending will create another record with the same content.</p>{/if}
    {#if cluster.production}
      <label>Production cluster: type <b class="mono">{pending.target}</b> to confirm
        <input class="mono" bind:value={confirmation} autocomplete="off" />
      </label>
    {/if}
    {#if error}<div class="error" role="alert">{error}</div>{/if}
  {/if}
  {#snippet footer()}
    <button disabled={busy} onclick={() => (reviewing = false)}>Back to editing</button>
    <button class="primary" disabled={busy || cluster.read_only || !pending || (cluster.production && confirmation !== pending.target)} onclick={() => pending && send(pending.target, pending.body)}><Send size={14} />{busy ? 'Sending…' : 'Send new record'}</button>
  {/snippet}
</Modal>

<style>
  .source-note { padding: 14px; display: grid; gap: 10px; margin-bottom: 12px; font-size: 13px; }
  .source-note .row { flex-wrap: wrap; }
  .source-note .mono { overflow-wrap: anywhere; }
  .review-changes { display: grid; gap: 16px; }
  .change h3 { font-size: 13px; margin-bottom: 6px; }
  .comparison { display: grid; grid-template-columns: minmax(0, 1fr) minmax(0, 1fr); gap: 12px; font-size: 12px; }
  .comparison pre { white-space: pre-wrap; overflow-wrap: anywhere; max-height: 240px; overflow: auto; margin-top: 4px; padding: 8px; background: var(--surface-2); border: 1px solid var(--border); font: 12px var(--mono); }
  .layout { display: grid; grid-template-columns: minmax(0, 2fr) minmax(0, 1fr); gap: 12px; align-items: start; }
  .form { padding: 14px; display: grid; gap: 12px; }
  .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
  .lbl { font-size: 12px; font-weight: 500; color: var(--text-2); }
  .field { display: grid; gap: 4px; }
  .keybox { display: flex; border: 1px solid var(--border-strong); border-radius: var(--radius); background: var(--surface); }
  .keybox:focus-within { outline: 2px solid var(--focus); outline-offset: -1px; }
  .keybox > input { flex: 1; border: 0; background: transparent; }
  .keybox > input:focus { outline: none; }
  .keybox.isnull > input { color: var(--text-3); }
  .nullsel {
    display: flex; align-items: center; gap: 7px; flex: none;
    padding: 0 10px; border-left: 1px solid var(--border);
    font-size: 12px; font-weight: 400; color: var(--text-2); white-space: nowrap; cursor: pointer;
  }
  .nullsel:hover { color: var(--text); background: var(--row-hover); }
  .keybox.isnull .nullsel { color: var(--text); }
  .hhead { margin-bottom: 6px; }
  .hrow { margin-bottom: 6px; }
  .hrow input { flex: 1; min-width: 0; }
  .hrow select { width: 88px; }
  .prodnote { font-size: 12px; color: var(--danger); }
  .schemainfo { align-self: end; padding-bottom: 7px; font-size: 12px; }
  .small-hint { font-size: 12px; margin-top: -4px; }
  .ed-loading { height: 252px; border: 1px solid var(--border-strong); border-radius: var(--radius); background: var(--surface-2); }
  @media (max-width: 1000px) { .layout { grid-template-columns: minmax(0, 1fr); } }
  @media (max-width: 560px) { .grid, .comparison { grid-template-columns: minmax(0, 1fr); } .hrow { flex-wrap: wrap; } .keybox > input { min-width: 0; } }
  @media (max-width: 639.98px) {
    .form { padding: 12px; }
    .field > .row { flex-wrap: wrap; row-gap: 2px; }
    .hrow input { flex-basis: calc(50% - 30px); }
    /* The send bar stays in reach while the value editor scrolls past. */
    .submit {
      position: sticky;
      bottom: 0;
      z-index: 2;
      flex-wrap: wrap;
      margin: 0 -12px -12px;
      padding: 10px 12px calc(10px + env(safe-area-inset-bottom));
      background: var(--surface);
      border-top: 1px solid var(--border);
    }
    .submit .spacer { display: none; }
    .submit button { flex: 1 1 100%; justify-content: center; padding-block: 9px; }
  }
</style>
