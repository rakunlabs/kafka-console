<script lang="ts" module>
  import { timeStarts, type MessageStart } from './messageStart'

  // What the Messages form asks for: where to start and what to show.
  export type MessageForm = {
    start: MessageStart
    partition: string
    offset: number
    // For "last": how many of the newest records of each partition.
    lastN: number
    limit: number
    // Custom time bounds are only active when selected in Start.
    since: string
    until: string
    // Exact time checks later offsets for out-of-order producer timestamps.
    // The default uses Kafka's time index and stays fast on large topics.
    exactTime: boolean
    filter: string
    search: string
    showFilter: boolean
    raw: boolean
    // How keys and values are read when the automatic choice is wrong, e.g.
    // MessagePack values that would otherwise show as base64.
    keyAs: string
    valueAs: string
  }

  export function messageForm(at?: { partition: number; offset: number }): MessageForm {
    return {
      start: at ? 'offset' : 'newest', partition: at ? String(at.partition) : '', offset: at?.offset ?? 0,
      lastN: 100, limit: 50, since: '', until: '', exactTime: false,
      filter: '', search: '', showFilter: false, raw: false, keyAs: '', valueAs: '',
    }
  }

  export const startsAtTime = (start: MessageStart) => start === 'custom' || timeStarts.some((preset) => preset.value === start)

  const decoders: [string, string][] = [
    ['', 'Auto'], ['json', 'JSON'], ['text', 'Text'], ['msgpack', 'MessagePack'], ['cbor', 'CBOR'],
    ['hex', 'Hex'], ['base64', 'Base64'], ['uint', 'Unsigned int'],
  ]

  const examples = [
    'return value.status === "FAILED"',
    'return key === "user-42"',
    'return headers["trace-id"] !== undefined',
    'return value.amount > 500 && timestamp > new Date("2026-10-01")',
  ]
</script>

<script lang="ts">
  import type { Cluster } from './api'
  import { app } from './features.svelte'
  import { Play, Pause, Square, Radio, Code, Binary, Search, X, Loader } from '@lucide/svelte'

  let {
    cluster,
    partitions,
    form = $bindable(),
    rangeError,
    live,
    paused,
    loading,
    onload,
    onlive,
    onpause,
    onstoplive,
    onstop,
  }: {
    cluster: Cluster
    partitions: number[]
    form: MessageForm
    rangeError: string
    live: boolean
    paused: boolean
    loading: boolean
    onload: () => void
    onlive: () => void
    onpause: (paused: boolean) => void
    onstoplive: () => void
    onstop: () => void
  } = $props()
</script>

<form class="controls" onsubmit={(e) => { e.preventDefault(); onload() }}>
  <!-- One strip of joined cells: label inside on the left, control on the
       right, matching the topic stat strip. Actions sit at the far end. -->
  <div class="fields">
    <label class="f search" title="Keep messages whose key, value or headers contain this text (case-insensitive). Schema-encoded values are searched in their decoded form.">
      <Search size={14} class="text-ink-3" />
      <input type="search" placeholder="Search messages…" bind:value={form.search} aria-label="Search messages"
        onkeydown={(e) => { if (e.key === 'Enter') { e.preventDefault(); onload() } }} />
      {#if form.search}
        <button type="button" class="ghost clear" aria-label="Clear search" onclick={() => { form.search = ''; onload() }}><X size={13} /></button>
      {/if}
    </label>
    <label class="f start" title="Choose where reading begins. Time ranges show the newest matching messages first; Load older continues the same range.">
      <span class="fl">Start</span>
      <select bind:value={form.start} aria-label="Start">
        <optgroup label="Position">
          <option value="newest">Newest</option>
          <option value="oldest">Oldest</option>
          <option value="offset">From offset</option>
          <option value="last">Last N per partition</option>
        </optgroup>
        <optgroup label="Time">
          {#each timeStarts as preset (preset.value)}<option value={preset.value}>{preset.label}</option>{/each}
          <option value="custom">Custom time range…</option>
        </optgroup>
      </select>
    </label>
    {#if form.start === 'offset'}
      <label class="f"><span class="fl">Offset</span><input type="number" min="0" class="w-num" bind:value={form.offset} /></label>
    {:else if form.start === 'last'}
      <label class="f" title="Read the newest N records of each partition, oldest first"><span class="fl">N</span><input type="number" min="1" class="w-sm" bind:value={form.lastN} /></label>
    {:else if form.start === 'custom'}
      <label class="f time-bound" class:bad={!!rangeError} title="Leave empty to start at the oldest message">
        <span class="fl">From</span><input type="datetime-local" step="1" bind:value={form.since} aria-invalid={!!rangeError} />
      </label>
      <label class="f time-bound" class:bad={!!rangeError} title={rangeError || 'Leave empty to read up to the newest message'}>
        <span class="fl">To</span><input type="datetime-local" step="1" bind:value={form.until} aria-invalid={!!rangeError} />
      </label>
    {/if}
    {#if startsAtTime(form.start)}
      <label class="f switch" class:on={form.exactTime} title="Check every later offset for out-of-order producer timestamps. Slower on large topics.">
        <input type="checkbox" bind:checked={form.exactTime} />Exact time
      </label>
    {/if}
    <label class="f">
      <span class="fl">Partition</span>
      <select bind:value={form.partition}>
        <option value="">All</option>
        {#each partitions as p (p)}<option value={String(p)}>{p}</option>{/each}
      </select>
    </label>
    <label class="f" title="Messages to return"><span class="fl">Limit</span><input type="number" min="1" max={app.maxLimit} class="w-sm" bind:value={form.limit} /></label>
    <label class="f switch" class:on={form.showFilter} title="Filter with a JavaScript expression">
      <input type="checkbox" bind:checked={form.showFilter} /><Code size={13} />JS filter
    </label>
    <label class="f" title="How keys are read. Auto: Schema Registry, then JSON, text, or base64 for binary. Any other choice reads the bytes in that format and skips the registry.">
      <span class="fl">Key</span>
      <select bind:value={form.keyAs}>{#each decoders as [id, label] (id)}<option value={id}>{label}</option>{/each}</select>
    </label>
    <label class="f" title="How values are read. Auto: Schema Registry, then JSON, text, or base64 for binary. Any other choice reads the bytes in that format and skips the registry.">
      <span class="fl">Value</span>
      <select bind:value={form.valueAs}>{#each decoders as [id, label] (id)}<option value={id}>{label}</option>{/each}</select>
    </label>
    {#if cluster.schema_registry}
      <label class="f switch" class:on={form.raw}
        title="Skip Schema Registry decoding. Avro, Protobuf and JSON Schema messages are shown as their stored bytes (base64, starting with the schema ID) instead of decoded JSON. Use it when decoding fails or to check which schema ID a producer wrote. Plain JSON and text messages look the same either way.">
        <input type="checkbox" bind:checked={form.raw} /><Binary size={13} />Raw
      </label>
    {/if}
  </div>
  <span class="spacer"></span>
  <div class="actions">
    {#if live}
      <button type="button" onclick={() => onpause(!paused)}>{#if paused}<Play size={14} />Resume{:else}<Pause size={14} />Pause{/if}</button>
      <button type="button" class="danger" onclick={onstoplive}><Square size={13} />Stop</button>
    {:else if loading}
      <button type="button" class="danger" onclick={onstop} title="Stop reading; the server stops scanning too (Esc)"><Square size={13} />Stop</button>
      <button class="primary" type="submit" disabled><Loader size={14} class="animate-spin" />Reading…</button>
    {:else}
      <button type="button" onclick={onlive} title="Watch messages that arrive from now on. Search, partition, filter and decoding still apply."><Radio size={14} />Live tail</button>
      <button class="primary" type="submit"><Play size={14} />Fetch</button>
    {/if}
  </div>

  {#if form.showFilter}
    <div class="filter">
      {#await import('./FilterEditor.svelte')}
        <div class="cm-loading"></div>
      {:then { default: FilterEditor }}
        <FilterEditor bind:value={form.filter} onrun={onload} placeholder={'return value.status === "FAILED"'} />
      {/await}
      <div class="hint faint">
        <span>Return <code>true</code> to keep a message · <code>value</code> <code>key</code> <code>headers</code> <code>partition</code> <code>offset</code> <code>timestamp</code> <code>raw</code> · <kbd>Ctrl/⌘ Enter</kbd> runs</span>
        <span class="spacer"></span>
        <span class="examples">
          {#each examples as ex (ex)}<button type="button" class="ghost ex mono" onclick={() => (form.filter = ex)}>{ex}</button>{/each}
        </span>
      </div>
    </div>
  {/if}
</form>

<style>
  .controls {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 8px 12px;
    margin-bottom: 14px;
  }
  .fields {
    display: flex;
    flex-wrap: wrap;
    background: var(--surface);
    border: 1px solid var(--border-strong);
  }
  .f {
    display: flex;
    flex-direction: row;
    align-items: center;
    gap: 8px;
    height: 32px;
    padding: 0 4px 0 10px;
    margin: 0 -1px -1px 0;
    border-right: 1px solid var(--border);
    border-bottom: 1px solid var(--border);
    font-size: 13px;
    font-weight: 400;
    color: var(--text);
    white-space: nowrap;
  }
  .f:focus-within { background: var(--row-hover); }
  .fl { color: var(--text-3); font-size: 12px; }
  /* Controls sit flush inside their cell; the cell is the visible box. */
  .f select, .f input:not([type='checkbox']) {
    height: 30px;
    padding: 0 6px;
    border: 0;
    background: transparent;
    font-size: 13px;
    font-weight: 500;
  }
  .f select { padding-right: 2px; cursor: pointer; }
  .f input:not([type='checkbox']):focus, .f select:focus { outline: none; }
  .f:has(input:focus-visible, select:focus-visible) { box-shadow: inset 0 -2px 0 var(--focus); }
  .w-sm { width: 56px; }
  .f.bad { box-shadow: inset 0 -2px 0 var(--danger); }
  .search { gap: 6px; padding-right: 2px; min-width: 240px; flex: 1 1 240px; }
  .search input[type='search'] { flex: 1; min-width: 0; font-weight: 400; }
  .search input::-webkit-search-cancel-button { display: none; }
  .clear { padding: 3px 5px; color: var(--text-3); }
  .clear:hover:not(:disabled) { color: var(--text); background: transparent; }
  .fields { flex: 1 1 auto; }
  .w-num { width: 84px; }
  /* Toggle cells: the cell is the switch, filled in the highlight when on. */
  .switch { position: relative; gap: 6px; padding: 0 11px; color: var(--text-2); cursor: pointer; }
  .switch:hover { color: var(--text); background: var(--row-hover); }
  .switch.on { color: var(--cta-text); background: var(--cta); }
  .switch input { position: absolute; opacity: 0; pointer-events: none; }
  .switch:has(input:focus-visible) { box-shadow: inset 0 -2px 0 var(--focus); }
  .actions { display: flex; gap: 8px; }
  .filter {
    flex-basis: 100%;
    display: grid;
    gap: 6px;
  }
  .filter :global(.cm) { margin-top: 0; }
  kbd { font: 11px var(--mono); padding: 0 4px; border: 1px solid var(--border-strong); border-radius: 2px; background: var(--surface); }
  .hint { display: flex; flex-wrap: wrap; align-items: center; gap: 4px 10px; font-size: 12px; }
  .hint code { font-size: 12px; }
  .examples { display: flex; flex-wrap: wrap; gap: 4px; }
  .ex { font-size: 12px; padding: 1px 7px; border: 1px solid var(--border-strong); background: var(--surface); color: var(--text-2); }

  /* Phones. The field strip becomes a two-column grid with search on its own
     row and the actions fill the width. */
  @media (max-width: 639.98px) {
    .controls { gap: 8px; }
    .fields { display: grid; grid-template-columns: 1fr 1fr; width: 100%; }
    .f { height: 40px; min-width: 0; }
    .f select, .f input:not([type='checkbox']) { height: 38px; min-width: 0; flex: 1; font-size: 16px; }
    .search, .start, .time-bound { grid-column: 1 / -1; min-width: 0; }
    .switch { justify-content: center; }
    .w-sm, .w-num { width: auto; }
    .controls > .spacer { display: none; }
    .actions { width: 100%; }
    .actions > button { flex: 1; justify-content: center; padding-block: 8px; }
    .hint .examples { display: none; }
  }
</style>
