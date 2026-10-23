<script lang="ts">
  import { api, type Cluster, type ImportRecord } from './api'
  import { fmt, bytes } from './format'
  import { href } from './router.svelte'
  import { detectFormat, RecordReader, FatalError, type FileFormat, type ItemShape, type Item } from './importFile'
  import { checkFile, sendFile, type SendResult } from './importer'
  import Modal from './Modal.svelte'
  import ValueView from './ValueView.svelte'
  import { FileUp, Check, Square, ListChecks, Send, StepForward, Plus, Minus } from '@lucide/svelte'

  // Writes records from a file to this topic: a console export (JSON or CSV),
  // JSON lines, or one value per line. The file is read in a stream, twice:
  // once to check every record, once to send it. It is never held in memory
  // whole, so its size is not limited.
  let {
    cluster,
    topic,
    partitions,
    open = $bindable(false),
  }: { cluster: Cluster; topic: string; partitions: number[]; open: boolean } = $props()

  // The server takes up to 1000 records per request; 500 keeps requests
  // small and progress moving.
  const batchSize = 500
  const headBytes = 1 << 20
  const keepProblems = 20

  let file = $state<File>()
  let head = $state('')
  let format = $state<FileFormat>('json')
  let shape = $state<ItemShape>('record')
  let keepPartition = $state(false)
  let keepTimestamp = $state(false)
  let keepSchema = $state(true)
  let compression = $state('')
  let confirm = $state('')
  let dragging = $state(false)

  type Phase = 'pick' | 'checking' | 'checked' | 'sending' | 'done'
  let phase = $state<Phase>('pick')
  let readBytes = $state(0)
  // Check results.
  let total = $state(0)
  let problems = $state<{ item: number; message: string }[]>([])
  let problemCount = $state(0)
  let fatal = $state('')
  // Send results. resumeAt is 0 when every item is written, else the first
  // item not known to be written.
  let written = $state(0)
  let skip = $state(0)
  let resumeAt = $state(0)
  let rewritten = $state(0)
  let uncertain = $state<SendResult['uncertain']>()
  let acceptDuplicates = $state(false)
  let byPartition = $state<Record<number, number>>({})
  let error = $state('')
  let stop = false

  const busy = $derived(phase === 'checking' || phase === 'sending')

  // Preview of the first items, from the start of the file only.
  const preview = $derived.by(() => {
    if (!head) return { items: [] as Item[], fatal: '', partitions: false, timestamps: false, schemaIds: false }
    const reader = new RecordReader(format, shape)
    let items: Item[] = []
    let fatal = ''
    try {
      items = reader.feed(head)
      // A file read whole may be closed here; a cut one is not.
      if (file && file.size <= headBytes) items.push(...reader.end())
    } catch (e: any) {
      if (!(e instanceof FatalError)) throw e
      fatal = e.message
    }
    const recs = items.flatMap((i) => (i.record ? [i.record] : []))
    return {
      items: items.slice(0, 5),
      fatal,
      partitions: recs.some((r) => r.partition !== null && r.partition !== undefined),
      timestamps: recs.some((r) => r.timestamp !== null && r.timestamp !== undefined),
      schemaIds: recs.some((r) => r.key_schema || r.value_schema),
    }
  })

  let wasOpen = false
  $effect(() => {
    if (open && !wasOpen) reset()
    if (!open && wasOpen) stop = true
    wasOpen = open
  })

  function reset() {
    file = undefined
    head = error = confirm = compression = fatal = ''
    format = 'json'
    shape = 'record'
    keepPartition = keepTimestamp = false
    keepSchema = true
    phase = 'pick'
    clearResults()
  }

  // A changed option invalidates the last check.
  function recheck() {
    clearResults()
    phase = 'pick'
  }

  function clearResults() {
    total = problemCount = written = skip = resumeAt = rewritten = readBytes = 0
    problems = []
    byPartition = {}
    fatal = error = ''
    uncertain = undefined
    acceptDuplicates = false
  }

  async function load(f: File | undefined) {
    if (!f) return
    clearResults()
    file = f
    expanded = new Set()
    head = await f.slice(0, headBytes).text()
    format = detectFormat(f.name, head)
    shape = format === 'lines' ? 'value' : 'record'
    phase = 'pick'
    keepPartition = preview.partitions && preview.items.every((i) => !i.record || (i.record.partition ?? -1) >= 0)
    keepTimestamp = false
    keepSchema = true
  }

  function prepare(r: ImportRecord): ImportRecord {
    const out = { ...r }
    if (!keepSchema) {
      delete out.key_schema
      delete out.value_schema
    }
    if (!keepPartition) out.partition = null
    if (!keepTimestamp) out.timestamp = null
    return out
  }

  // A record the topic cannot take with the chosen options.
  function unfit(r: ImportRecord) {
    if (!keepPartition) return ''
    if (r.partition === null || r.partition === undefined) return 'no partition; turn off “Write each record to its partition”'
    if (r.partition >= partitions.length) return `partition ${r.partition} does not exist; ${topic} has ${partitions.length}`
    return ''
  }

  function options() {
    return {
      format, shape, batchSize, unfit, prepare,
      send: (records: ImportRecord[], dry: boolean) =>
        api.importRecords(cluster.name, topic, { records, keep_partition: keepPartition, keep_timestamp: keepTimestamp, compression, dry_run: dry }),
      stopped: () => stop,
      onBytes: (n: number) => (readBytes = n),
    }
  }

  async function check() {
    if (!file) return
    stop = false
    clearResults()
    phase = 'checking'
    const r = await checkFile(file, options(), keepProblems)
    total = r.total
    problemCount = r.problemCount
    problems = r.problems
    fatal = r.fatal
    phase = r.stopped ? 'pick' : 'checked'
  }

  // Sends every item after `after`; a resume passes the last written item.
  async function run(after: number) {
    if (!file) return
    stop = false
    error = ''
    skip = after
    readBytes = 0
    if (!after) {
      written = 0
      byPartition = {}
    }
    const base = written
    const counts = { ...byPartition }
    phase = 'sending'
    const merge = (r: SendResult) => {
      written = base + r.written
      const next = { ...counts }
      for (const [p, n] of Object.entries(r.byPartition)) next[Number(p)] = (next[Number(p)] ?? 0) + n
      byPartition = next
    }
    const r = await sendFile(file, options(), after, merge)
    merge(r)
    resumeAt = r.resumeAt
    rewritten = r.rewritten
    uncertain = r.uncertain
    acceptDuplicates = false
    error = r.error
    phase = 'done'
  }

  const pct = $derived(file?.size ? Math.min(100, Math.round((readBytes / file.size) * 100)) : 0)

  // Preview rows opened to show the whole value, by item number.
  let expanded = $state(new Set<number>())
  function toggle(item: number) {
    const next = new Set(expanded)
    if (!next.delete(item)) next.add(item)
    expanded = next
  }
  const short = (v: string | null) => (v === null ? 'null' : v.length > 80 ? v.slice(0, 80) + '…' : v)
  const valid = $derived(total - problemCount)
  const finished = $derived(phase === 'done' && !error && !resumeAt)
</script>

<Modal title="Import messages into {topic}" bind:open width={720} locked={phase === 'sending'}>
  {#if phase === 'pick' || phase === 'checking' || (phase === 'checked' && (problemCount || fatal))}
    <label class="drop" class:on={dragging}
      ondragover={(e) => { e.preventDefault(); dragging = true }}
      ondragleave={() => (dragging = false)}
      ondrop={(e) => { e.preventDefault(); dragging = false; if (!busy) load(e.dataTransfer?.files[0]) }}>
      <input type="file" accept=".json,.jsonl,.ndjson,.csv,.txt,application/json,text/csv,text/plain" disabled={busy}
        onchange={(e) => load((e.currentTarget as HTMLInputElement).files?.[0])} />
      <FileUp size={18} />
      {#if file}<span><b class="mono">{file.name}</b> · {bytes(file.size)} · choose another file or drop it here</span>
      {:else}<span>Choose a file or drop it here</span>{/if}
      <span class="faint small">A JSON or CSV export from this console or kafka-mcp, JSON lines, or one value per line. Any size: the file is read in a stream.</span>
    </label>

    {#if file}
      <div class="grid2">
        <label>Format
          <select bind:value={format} disabled={busy} onchange={recheck}>
            <option value="json">JSON array</option>
            <option value="jsonl">JSON lines</option>
            <option value="csv">CSV with a header row</option>
            <option value="lines">Text, one value per line</option>
          </select>
        </label>
        {#if format === 'json' || format === 'jsonl'}
          <label>Each item is
            <select bind:value={shape} disabled={busy} onchange={recheck}>
              <option value="record">A record (key, value, headers…)</option>
              <option value="value">The value</option>
            </select>
          </label>
        {/if}
      </div>

      {#if preview.fatal}<div class="error" role="alert">{preview.fatal}</div>{/if}

      {#if preview.items.length}
        <p class="faint small">First items of the file:</p>
        <div class="panel tbl">
          <table class="manual">
            <colgroup>
              <col style:width="32px" /><col style:width="48px" />{#if keepPartition}<col style:width="48px" />{/if}<col style:width="28%" /><col /><col style:width="76px" />
            </colgroup>
            <thead><tr><th class="tog"></th><th class="num">#</th>{#if keepPartition}<th class="num" title="Partition"><abbr>P</abbr></th>{/if}<th>Key</th><th>Value</th><th class="num">Headers</th></tr></thead>
            <tbody>
              {#each preview.items as it, i (it.item)}
                {@const isOpen = !!it.record && expanded.has(it.item)}
                <tr class:alt={i % 2 === 1} class:opened={isOpen}>
                  <td class="tog">
                    {#if it.record}
                      <button type="button" class="xbtn" class:on={isOpen} aria-expanded={isOpen}
                        aria-label={isOpen ? `Collapse item ${it.item}` : `Show item ${it.item}`} onclick={() => toggle(it.item)}>
                        {#if isOpen}<Minus size={12} strokeWidth={2.2} />{:else}<Plus size={12} strokeWidth={2.2} />{/if}
                      </button>
                    {/if}
                  </td>
                  <td class="num mono">{it.item}</td>
                  {#if it.record}
                    {#if keepPartition}<td class="num mono">{it.record.partition ?? '—'}</td>{/if}
                    <td class="mono cut">{it.record.key ?? '—'}</td>
                    <td class="mono cut">{short(it.record.value)}</td>
                    <td class="num mono">{it.record.headers.length}</td>
                  {:else}
                    <td colspan={keepPartition ? 4 : 3} class="err cut">{it.problem}</td>
                  {/if}
                </tr>
                {#if isOpen && it.record}
                  {@const r = it.record}
                  <tr class="xrow" class:alt={i % 2 === 1}>
                    <td></td>
                    <td colspan={keepPartition ? 5 : 4}>
                      <div class="inl">
                        <div class="inl-meta">
                          {#if r.key !== null}<span class="faint">key</span><code>{r.key}</code>{/if}
                          {#if r.key_encoding === 'base64' || r.value_encoding === 'base64'}<span class="badge">base64</span>{/if}
                          {#if keepSchema && (r.key_schema || r.value_schema)}<span class="badge accent">schema</span>{/if}
                          {#if keepTimestamp && r.timestamp != null}<span class="faint">{new Date(r.timestamp).toISOString()}</span>{/if}
                          {#each r.headers as h, j (j)}<span class="hdr"><span class="faint">{h.key}</span>{h.value}</span>{/each}
                        </div>
                        <ValueView text={r.value} maxHeight={260} />
                      </div>
                    </td>
                  </tr>
                {/if}
              {/each}
            </tbody>
          </table>
        </div>

        <label class="check"><input type="checkbox" bind:checked={keepPartition} disabled={busy} onchange={recheck} /> Write each record to its partition
          <span class="hint">{preview.partitions ? 'Off: the key decides the partition.' : 'The first records have no partition numbers; the key decides.'}</span></label>
        <label class="check"><input type="checkbox" bind:checked={keepTimestamp} disabled={busy} onchange={recheck} /> Keep timestamps from the file
          <span class="hint">Off: records get the time they are written.</span></label>
        {#if preview.schemaIds}
          <label class="check"><input type="checkbox" bind:checked={keepSchema} disabled={busy} onchange={recheck} /> Encode with the schema IDs in the file
            <span class="hint">Values exported decoded from Avro, Protobuf or JSON Schema are encoded again with the same schema. Off: they are written as plain JSON text.</span></label>
        {/if}
        <label>Compression
          <select bind:value={compression} disabled={busy}>
            <option value="">None</option><option value="gzip">gzip</option><option value="snappy">snappy</option><option value="lz4">lz4</option><option value="zstd">zstd</option>
          </select>
        </label>
      {/if}
    {/if}

    {#if phase === 'checking'}
      <div class="progress" role="status"><div class="bar"><span style:width="{pct}%"></span></div>
        <span class="faint">Checking… {fmt(total)} items · {bytes(readBytes)} of {bytes(file?.size ?? 0)}{problemCount ? ` · ${fmt(problemCount)} problems` : ''}</span></div>
    {/if}

    {#if phase === 'checked' && (problemCount || fatal)}
      <div class="error" role="alert">
        {#if fatal}{fatal}{#if total} (after {fmt(total)} items){/if}{:else}
          {fmt(problemCount)} of {fmt(total)} items cannot be imported. Fix the file and check it again.{/if}
        {#if problems.length}
          <ul>
            {#each problems as p (p.item)}<li>Item {p.item}: {p.message}</li>{/each}
            {#if problemCount > problems.length}<li>and {fmt(problemCount - problems.length)} more</li>{/if}
          </ul>
        {/if}
      </div>
    {/if}
  {:else if phase === 'checked'}
    <p class="sum"><ListChecks size={15} /><span>All <b>{fmt(valid)}</b> records are valid and will be written to <b class="mono">{topic}</b> in <b>{cluster.name}</b>{keepPartition ? ', each to its partition' : ''}, in file order.</span></p>
    <p class="faint small">Consumers of the topic will receive them as new records. They are sent in batches of {batchSize}; a failed batch stops the import, and it can be resumed after the last written record.</p>
    {#if total === 0}<p class="err">The file has no records.</p>{/if}
    {#if cluster.confirm_writes}
      <label>Type <b class="mono">{topic}</b> to confirm <input class="mono" bind:value={confirm} autocomplete="off" /></label>
    {/if}
  {:else}
    <div class="progress" role="status">
      <div class="bar" class:fail={!!error}><span style:width="{finished ? 100 : pct}%"></span></div>
      <span class="faint">{fmt(written)} of {fmt(valid)} written{skip ? ` (resumed after item ${fmt(skip)})` : ''}</span>
    </div>
    {#if finished}
      <div class="donemsg"><Check size={15} /><span><b>{fmt(written)}</b> {written === 1 ? 'record' : 'records'} written to <a class="mono" href={href(cluster.name, 'topics', topic)}>{topic}</a>.</span></div>
    {:else if phase === 'done'}
      <p class="small">
        {error ? 'The import stopped' : 'Stopped'}. Items up to {fmt(resumeAt - 1)} are written.
        {#if rewritten}{fmt(rewritten)} later {rewritten === 1 ? 'record' : 'records'} of the failed batch were written too and will be written again on resume.{/if}
      </p>
      {#if uncertain}
        <div class="error" role="alert">The result of items {fmt(uncertain.first)}–{fmt(uncertain.last)} is unknown.
          Some or all may already be written. Resuming resends this batch and can create duplicates.</div>
        <label class="check"><input type="checkbox" bind:checked={acceptDuplicates} /> I understand that resuming may duplicate these records.</label>
      {/if}
    {/if}
    {#if Object.keys(byPartition).length}
      <p class="faint small mono">{Object.entries(byPartition).map(([p, n]) => `p${p}: ${fmt(n)}`).join(' · ')}</p>
    {/if}
  {/if}

  {#if error}<div class="error" role="alert">{error}</div>{/if}

  {#snippet footer()}
    {#if phase === 'pick' || phase === 'checking' || (phase === 'checked' && (problemCount || fatal))}
      <button onclick={() => (open = false)}>Cancel</button>
      {#if phase === 'checking'}<button onclick={() => (stop = true)}><Square size={13} />Stop</button>{/if}
      <button class="primary" disabled={!file || busy || !!preview.fatal || cluster.read_only} onclick={check}><ListChecks size={14} />{phase === 'checking' ? 'Checking…' : 'Check'}</button>
    {:else if phase === 'checked'}
      <button onclick={() => (phase = 'pick')}>Back</button>
      <button class="primary" disabled={!valid || cluster.read_only || (cluster.confirm_writes && confirm !== topic)} onclick={() => run(0)}><Send size={14} />Import {fmt(valid)}</button>
    {:else if phase === 'sending'}
      <button class="danger" onclick={() => (stop = true)}><Square size={13} />Stop after this batch</button>
    {:else}
      {#if !finished && resumeAt}
        <button disabled={cluster.read_only || (cluster.confirm_writes && confirm !== topic) || (!!uncertain && !acceptDuplicates)} onclick={() => run(resumeAt - 1)}><StepForward size={14} />Resume from item {fmt(resumeAt)}</button>
      {/if}
      <button class="primary" onclick={() => (open = false)}>Done</button>
    {/if}
  {/snippet}
</Modal>

<style>
  .drop {
    display: grid; justify-items: center; gap: 4px; padding: 18px 12px; text-align: center;
    font-size: 13px; font-weight: 400; color: var(--text-2);
    border: 1px dashed var(--border-strong); border-radius: var(--radius); background: var(--surface-2); cursor: pointer;
  }
  .drop:hover, .drop.on { color: var(--text); border-color: var(--cta-text); background: var(--row-hover); }
  .drop:has(input:focus-visible) { outline: 2px solid var(--focus); outline-offset: 1px; }
  .drop input { position: absolute; opacity: 0; width: 1px; height: 1px; pointer-events: none; }
  .hint { display: block; margin-left: 22px; font-size: 12px; color: var(--text-3); }
  .check { align-items: flex-start; flex-wrap: wrap; }
  .err { color: var(--danger); font-size: 13px; }
  .error ul { margin: 6px 0 0 18px; padding: 0; }
  .sum { display: flex; align-items: center; gap: 6px; font-size: 14px; }
  .sum b { font-weight: 600; }
  .tbl { max-height: 420px; overflow: auto; }
  .tbl table { table-layout: fixed; }
  .cut { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .tog { padding-right: 0; }
  td.tog { padding-top: calc(var(--cell-y) - 1px); }
  .xbtn { width: 20px; height: 20px; padding: 0; justify-content: center; border-radius: 2px; color: var(--text-2); }
  .xbtn.on { background: var(--cta); border-color: var(--cta-border); color: var(--cta-text); }
  .xbtn.on:hover:not(:disabled) { background: var(--cta-hover); }
  tr.opened td { border-bottom-color: transparent; }
  tr.xrow td { padding-top: 0; }
  tbody tr.xrow:hover td { background: transparent; }
  tbody tr.xrow.alt:hover td { background: var(--stripe); }
  .inl { display: grid; gap: 6px; padding-bottom: 6px; min-width: 0; }
  .inl-meta { display: flex; flex-wrap: wrap; align-items: center; gap: 6px 8px; font-size: 12px; }
  .inl-meta code { font-size: 12px; overflow-wrap: anywhere; }
  .hdr { display: inline-flex; gap: 4px; font: 12px var(--mono); padding: 0 6px; border: 1px solid var(--border); border-radius: 2px; }
  .donemsg { display: flex; align-items: center; gap: 6px; font-size: 14px; color: var(--ok); }
  .donemsg b, .donemsg a { color: var(--text); }
  .progress { display: grid; gap: 6px; font-size: 13px; }
  .bar { height: 6px; background: var(--surface-2); border: 1px solid var(--border); overflow: hidden; }
  .bar span { display: block; height: 100%; background: var(--cta-text); transition: width .3s ease-out; }
  .bar.fail span { background: var(--danger); }
</style>
