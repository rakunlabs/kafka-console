<script lang="ts">
  import { untrack } from 'svelte'
  import { errorMessage } from './http'
  import { X, Plus, Sparkles, ShieldCheck } from '@lucide/svelte'
  import { api, type Cluster, type SchemaReference } from './api'
  import Modal from './Modal.svelte'
  import { jsonProblem } from './format'

  let {
    cluster,
    open = $bindable(false),
    types,
    subject: fixedSubject = '',
    initial,
    onsaved,
  }: {
    cluster: Cluster
    open: boolean
    types: string[]
    subject?: string
    initial?: { type: string; schema: string; references: SchemaReference[] }
    onsaved: (subject: string) => void
  } = $props()

  // Mounted only while open, so every open starts from `initial`.
  const start = untrack(() => ({ subject: fixedSubject, initial }))
  let subject = $state(start.subject)
  let type = $state(start.initial?.type ?? 'AVRO')
  let schema = $state(start.initial ? pretty(start.initial.schema, start.initial.type) : '')
  let refs = $state<SchemaReference[]>(start.initial?.references.map((r) => ({ ...r })) ?? [])
  let normalize = $state(false)
  let error = $state('')
  let busy = $state(false)
  let check = $state<{ compatible: boolean; messages: string[] }>()

  function pretty(s: string, t: string) {
    if (t === 'PROTOBUF') return s
    try {
      return JSON.stringify(JSON.parse(s), null, 2)
    } catch {
      return s
    }
  }

  // Avro and JSON Schema are JSON documents and are checked as you type;
  // Protobuf is validated by the registry on save or compatibility check.
  const syntaxError = $derived(type === 'PROTOBUF' || !schema.trim() ? undefined : jsonProblem(schema))

  // A compatibility result describes the text it was run on; any edit voids it.
  let checkedFor = ''
  $effect(() => {
    if (check && schema !== checkedFor) check = undefined
  })

  const body = () => ({ subject: subject.trim(), type, schema, references: refs.filter((r) => r.name && r.subject), normalize })

  async function validate() {
    busy = true
    error = ''
    try {
      checkedFor = schema
      check = await api.checkSchema(cluster.name, body())
    } catch (e) {
      error = errorMessage(e)
    } finally {
      busy = false
    }
  }

  async function save() {
    busy = true
    error = ''
    try {
      await api.createSchema(cluster.name, body())
      open = false
      onsaved(subject.trim())
    } catch (e) {
      error = errorMessage(e)
    } finally {
      busy = false
    }
  }

  const samples: Record<string, string> = {
    AVRO: '{\n  "type": "record",\n  "name": "Event",\n  "namespace": "com.example",\n  "fields": [\n    { "name": "id", "type": "string" }\n  ]\n}',
    PROTOBUF: 'syntax = "proto3";\npackage com.example;\n\nmessage Event {\n  string id = 1;\n}',
    JSON: '{\n  "$schema": "http://json-schema.org/draft-07/schema#",\n  "type": "object",\n  "properties": {\n    "id": { "type": "string" }\n  },\n  "required": ["id"]\n}',
  }
</script>

<Modal title={fixedSubject ? `New version: ${fixedSubject}` : 'Create schema'} bind:open width={720}>
  <div class="grid">
    <label>Subject
      <input class="mono" bind:value={subject} disabled={!!fixedSubject} placeholder="orders-value" />
    </label>
    <label>Type
      <select bind:value={type} disabled={!!fixedSubject} onchange={() => (check = undefined)}>
        {#each types.length ? types : ['AVRO', 'PROTOBUF', 'JSON'] as t (t)}<option>{t}</option>{/each}
      </select>
    </label>
  </div>
  {#if !fixedSubject}
    <p class="faint small">Topic naming strategy: <code>&lt;topic&gt;-value</code> for values, <code>&lt;topic&gt;-key</code> for keys.</p>
  {/if}
  <div class="field">
    <div class="row">
      <span class="lbl">Schema</span>
      <span class="faint small">{type === 'PROTOBUF' ? 'Protobuf' : type === 'JSON' ? 'JSON Schema' : 'Avro'}</span>
      <span class="spacer"></span>
      {#if !schema.trim()}<button type="button" class="ghost small" onclick={() => (schema = samples[type] ?? '')}><Sparkles size={13} />Insert example</button>{/if}
    </div>
    {#await import('./JsonEditor.svelte')}
      <div class="ed-loading"></div>
    {:then { default: JsonEditor }}
      {#key type}
        <JsonEditor id="schema-text" rows={16} maxHeight={420} bind:value={schema} error={syntaxError}
          mode={type === 'PROTOBUF' ? 'protobuf' : 'json'} label="Schema"
          placeholder={samples[type]?.split('\n')[0] ?? ''} />
      {/key}
    {/await}
  </div>

  <div>
    <div class="row"><span class="lbl">References</span><span class="spacer"></span>
      <button type="button" class="ghost small" onclick={() => (refs = [...refs, { name: '', subject: '', version: 1 }])}><Plus size={13} />Reference</button>
    </div>
    {#each refs as r, i (i)}
      <div class="row ref">
        <input class="mono" placeholder="name (e.g. com.example.Address)" bind:value={r.name} />
        <input class="mono" placeholder="subject" bind:value={r.subject} />
        <input type="number" min="1" class="ver" bind:value={r.version} />
        <button type="button" class="ghost" aria-label="Remove reference" onclick={() => (refs = refs.filter((_, j) => j !== i))}><X size={14} /></button>
      </div>
    {/each}
  </div>

  <label class="check"><input type="checkbox" bind:checked={normalize} /> Normalize before saving</label>

  {#if check}
    {#if check.compatible}
      <div class="ok">Compatible with existing versions.</div>
    {:else}
      <div class="error">
        Incompatible.
        {#each check.messages.filter((m) => m.includes('errorType') || !m.startsWith('{')) as m, i (i)}<div class="mono msg">{m}</div>{/each}
      </div>
    {/if}
  {/if}
  {#if error}<div class="error">{error}</div>{/if}

  {#snippet footer()}
    <button onclick={() => (open = false)}>Cancel</button>
    <button disabled={busy || !subject.trim() || !schema.trim() || !!syntaxError} onclick={validate}><ShieldCheck size={14} />Check compatibility</button>
    <button class="primary" disabled={busy || !subject.trim() || !schema.trim() || !!syntaxError} onclick={save}>{busy ? 'Saving…' : 'Save'}</button>
  {/snippet}
</Modal>

<style>
  .grid { display: grid; grid-template-columns: 2fr 1fr; gap: 12px; }
  @media (max-width: 640px) { .grid { grid-template-columns: minmax(0, 1fr); } }
  .lbl { font-size: 12px; font-weight: 500; color: var(--text-2); }
  .ref { margin-top: 6px; }
  .field { display: grid; gap: 4px; }
  .ref input { flex: 1; }
  .ref .ver { flex: 0 0 70px; }
  .ed-loading { height: 332px; border: 1px solid var(--border-strong); border-radius: var(--radius); background: var(--surface-2); }
  .msg { font-size: 12px; margin-top: 4px; }
</style>
