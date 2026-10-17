<script lang="ts">
  import { api, type Cluster, type SubjectSummary, type SchemaVersion } from '../lib/api'
  import { time, jsonProblem } from '../lib/format'
  import { Plus, X, Send, WandSparkles, AlignLeft } from '@lucide/svelte'

  let { cluster, topic, partitions }: { cluster: Cluster; topic: string; partitions: number[] } = $props()

  let valueEncoding = $state<'text' | 'base64' | 'schema'>('text')
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
        if (r.subjects.some((s) => s.name === `${topic}-value`)) {
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

  let key = $state('')
  let nullKey = $state(true)
  let value = $state('')
  let partition = $state('')
  let headers = $state<{ key: string; value: string }[]>([])

  let busy = $state(false)
  let error = $state('')
  let sent = $state<{ partition: number; offset: number; timestamp: string }[]>([])

  // Values that look like JSON (or must be JSON for schema encoding) are checked as you type.
  const jsonError = $derived.by(() => {
    const v = value.trim()
    if (!v) return undefined
    if (valueEncoding !== 'schema' && !(v.startsWith('{') || v.startsWith('['))) return undefined
    if (valueEncoding === 'base64') return undefined
    return jsonProblem(value)
  })

  function format() {
    try {
      value = JSON.stringify(JSON.parse(value), null, 2)
    } catch {}
  }

  async function send() {
    busy = true
    error = ''
    try {
      const res = await api.produce(cluster.name, topic, {
        key: nullKey ? null : key,
        value,
        headers: headers.filter((h) => h.key.trim()),
        partition: partition === '' ? null : Number(partition),
        value_encoding: valueEncoding === 'base64' ? 'base64' : undefined,
        value_schema: valueEncoding === 'schema' && subject ? { subject, message: message || undefined } : null,
        compression: compression || undefined,
      })
      sent = [res, ...sent].slice(0, 10)
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
  <div class="layout">
    <form class="panel form" onsubmit={(e) => { e.preventDefault(); send() }}>
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
            {#each partitions as p (p)}<option value={String(p)}>{p}</option>{/each}
          </select>
        </label>
      </div>

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
              <option value="">Select…</option>
              {#each subjects as s (s.name)}<option value={s.name}>{s.name} ({s.type})</option>{/each}
            </select>
          </label>
          {#if schema?.messages?.length}
            <label>Protobuf message
              <select bind:value={message}>
                {#each schema.messages as m (m)}<option value={m}>{m}</option>{/each}
              </select>
            </label>
          {:else if schema}
            <div class="schemainfo faint">v{schema.version} · id {schema.id} · {schema.type}</div>
          {/if}
        </div>
        <p class="faint small-hint">Write the value as JSON; the server validates it against the latest schema version and encodes it in Confluent format. For Avro unions, both a plain value and the <code>{'{"string": "x"}'}</code> form are accepted.</p>
      {/if}

      <div class="field">
        <div class="row">
          <span class="lbl">Value</span>
          <span class="spacer"></span>
          {#if valueEncoding === 'schema' && schema}
            <button type="button" class="ghost small" onclick={template}><WandSparkles size={13} />Generate template</button>
          {/if}
          <button type="button" class="ghost small" onclick={format} disabled={!!jsonError || !value.trim()}><AlignLeft size={13} />Format JSON</button>
        </div>
        {#await import('../lib/JsonEditor.svelte')}
          <div class="ed-loading"></div>
        {:then { default: JsonEditor }}
          <JsonEditor id="produce-value" rows={12} bind:value error={jsonError} mode={valueEncoding === 'base64' ? 'plain' : 'json'}
            onrun={() => { if (!busy && !(valueEncoding === 'schema' && (!subject || jsonError))) send() }}
            placeholder={valueEncoding === 'base64' ? 'SGVsbG8sIEthZmthIQ==' : '{"id": 1, "status": "NEW"}'} />
        {/await}
      </div>

      <div>
        <div class="row hhead"><span class="lbl">Headers</span><span class="spacer"></span>
          <button type="button" class="ghost small" onclick={() => (headers = [...headers, { key: '', value: '' }])}><Plus size={13} />Add header</button>
        </div>
        {#each headers as h, i (i)}
          <div class="row hrow">
            <input placeholder="key" class="mono" bind:value={h.key} />
            <input placeholder="value" class="mono" bind:value={h.value} />
            <button type="button" class="ghost" aria-label="Remove header" onclick={() => (headers = headers.filter((_, j) => j !== i))}><X size={14} /></button>
          </div>
        {/each}
      </div>

      {#if error}<div class="error">{error}</div>{/if}

      <div class="row">
        {#if cluster.production}<span class="prodnote">You are producing to a production cluster.</span>{/if}
        <span class="spacer"></span>
        <button class="primary" type="submit" disabled={busy || (valueEncoding === 'schema' && (!subject || !!jsonError))}
          title={valueEncoding === 'schema' && jsonError ? 'Fix the JSON before encoding with the schema' : ''}><Send size={14} />{busy ? 'Sending…' : 'Send'}</button>
      </div>
    </form>

    <div class="panel">
      <table>
        <thead><tr><th colspan="3">Sent this session</th></tr></thead>
        <tbody>
          {#each sent as s (`${s.partition}:${s.offset}`)}
            <tr><td class="mono">p{s.partition}</td><td class="mono num">{s.offset}</td><td class="mono faint">{time(s.timestamp)}</td></tr>
          {:else}
            <tr><td colspan="3" class="faint">No messages sent yet.</td></tr>
          {/each}
        </tbody>
      </table>
    </div>
  </div>
{/if}

<style>
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
  .hrow input { flex: 1; }
  .prodnote { font-size: 12px; color: var(--danger); }
  .schemainfo { align-self: end; padding-bottom: 7px; font-size: 12px; }
  .small-hint { font-size: 12px; margin-top: -4px; }
  .ed-loading { height: 252px; border: 1px solid var(--border-strong); border-radius: var(--radius); background: var(--surface-2); }
  @media (max-width: 1000px) { .layout { grid-template-columns: 1fr; } }
  @media (max-width: 560px) { .grid { grid-template-columns: 1fr; } }
</style>
