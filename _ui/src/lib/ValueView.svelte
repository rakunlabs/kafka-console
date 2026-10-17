<script lang="ts">
  import { setContext } from 'svelte'
  import { ChevronsUpDown, ChevronsDownUp, Braces, FileText } from '@lucide/svelte'
  import JsonNode from './JsonNode.svelte'
  import { parseJson, isContainer, type TreeControl } from './jsonTree'

  // A message key or value: a foldable, highlighted tree when it is a JSON
  // object or array, plain text otherwise.
  let {
    text,
    format,
    depth = 3,
    maxHeight,
  }: { text: string | null; format?: string; depth?: number; maxHeight?: number } = $props()

  const structured = (f?: string) => f === 'json' || f === 'avro' || f === 'protobuf' || f === 'json-schema'

  const parsed = $derived.by(() => {
    if (text === null) return undefined
    if (!structured(format) && !/^\s*[[{]/.test(text)) return undefined
    const r = parseJson(text)
    return r.ok && isContainer(r.value) ? r.value : undefined
  })

  const ctl = $state<TreeControl>({ depth: 3, gen: 0, all: true })
  $effect.pre(() => {
    ctl.depth = depth
  })
  setContext('json-tree', ctl)

  let raw = $state(false)

  function setAll(all: boolean) {
    ctl.all = all
    ctl.gen++
  }

  const plain = $derived.by(() => {
    if (text === null) return 'null'
    if (parsed !== undefined) {
      try {
        return JSON.stringify(JSON.parse(text), null, 2)
      } catch {}
    }
    return text
  })
</script>

<div class="vv" style:max-height={maxHeight ? `${maxHeight}px` : undefined}>
  {#if parsed !== undefined}
    <div class="vbar">
      {#if !raw}
        <button type="button" class="ghost" title="Expand all" aria-label="Expand all" onclick={() => setAll(true)}><ChevronsUpDown size={13} /></button>
        <button type="button" class="ghost" title="Collapse all" aria-label="Collapse all" onclick={() => setAll(false)}><ChevronsDownUp size={13} /></button>
      {/if}
      <button type="button" class="ghost" title={raw ? 'Show as tree' : 'Show raw text'} aria-pressed={raw} onclick={() => (raw = !raw)}>
        {#if raw}<Braces size={13} />Tree{:else}<FileText size={13} />Raw{/if}
      </button>
    </div>
  {/if}
  {#if parsed !== undefined && !raw}
    <div class="tree"><JsonNode value={parsed} /></div>
  {:else}
    <pre>{plain}</pre>
  {/if}
</div>

<style>
  .vv {
    position: relative;
    overflow: auto;
    font: 12.5px/1.55 var(--mono);
    background: var(--bg);
    border: 1px solid var(--border);
    border-radius: var(--radius);
  }
  .vbar {
    position: sticky;
    top: 0;
    z-index: 1;
    float: right;
    display: flex;
    gap: 2px;
    padding: 4px;
    margin: 0 0 -4px 8px;
    background: color-mix(in srgb, var(--bg) 88%, transparent);
    border-bottom-left-radius: var(--radius);
  }
  .vbar button { padding: 2px 6px; font: 500 11.5px var(--font); color: var(--text-2); gap: 4px; }
  .vbar button:hover:not(:disabled) { color: var(--text); background: var(--surface-2); }
  .tree { padding: 8px 10px 8px 6px; }
  pre { margin: 0; padding: 10px 12px; font: inherit; white-space: pre-wrap; overflow-wrap: anywhere; }
</style>
