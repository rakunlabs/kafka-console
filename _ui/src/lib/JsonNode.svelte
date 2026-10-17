<script lang="ts">
  import { getContext, untrack } from 'svelte'
  import { Plus, Minus } from '@lucide/svelte'
  import JsonNode from './JsonNode.svelte'
  import { LosslessNumber, isContainer, type TreeControl } from './jsonTree'

  let {
    value,
    name,
    last = true,
    depth = 0,
  }: { value: unknown; name?: string | number; last?: boolean; depth?: number } = $props()

  const ctl = getContext<TreeControl>('json-tree')
  const page = 200

  const container = $derived(isContainer(value))
  const array = $derived(Array.isArray(value))
  const entries = $derived(
    container ? (array ? (value as unknown[]).map((v, i) => [i, v] as const) : Object.entries(value as object)) : [],
  )
  const [openCh, closeCh] = $derived(array ? ['[', ']'] : ['{', '}'])

  let open = $state(untrack(() => depth < ctl.depth))
  let limit = $state(page)

  // Follow expand/collapse-all; nodes mounted later pick up the last choice.
  $effect(() => {
    if (ctl.gen > 0) open = ctl.all
  })

  function display(v: unknown) {
    if (v instanceof LosslessNumber) return { cls: 'num', text: v.raw }
    switch (typeof v) {
      case 'string':
        return { cls: 'str', text: JSON.stringify(v) }
      case 'number':
        return { cls: 'num', text: String(v) }
      case 'boolean':
        return { cls: 'kw', text: String(v) }
    }
    return { cls: 'kw', text: 'null' }
  }
</script>

{#snippet label()}
  {#if name !== undefined}
    {#if typeof name === 'number'}<span class="idx">{name}</span>{:else}<span class="key">{JSON.stringify(name)}</span>{/if}<span class="p">{': '}</span>
  {/if}
{/snippet}

{#if container}
  <div class="ln">
    <button
      type="button"
      class="tg"
      aria-expanded={open}
      aria-label="{open ? 'Collapse' : 'Expand'} {name ?? 'root'}"
      onclick={() => (open = !open)}
    >
      {#if open}<Minus size={9} strokeWidth={3} />{:else}<Plus size={9} strokeWidth={3} />{/if}
    </button>
    <span class="body">
      {@render label()}<span class="p">{openCh}</span>
      {#if !entries.length}
        <span class="p">{closeCh}</span>{#if !last}<span class="p">,</span>{/if}
      {:else if !open}
        <button type="button" class="sum" onclick={() => (open = true)}>…</button><span class="p">{closeCh}</span>{#if !last}<span class="p">,</span>{/if}
        <span class="cnt">{entries.length} {array ? (entries.length === 1 ? 'item' : 'items') : entries.length === 1 ? 'key' : 'keys'}</span>
      {/if}
    </span>
  </div>
  {#if open && entries.length}
    <div class="kids">
      {#each entries.slice(0, limit) as [k, v], i (k)}
        <JsonNode value={v} name={k} last={i === entries.length - 1} depth={depth + 1} />
      {/each}
      {#if entries.length > limit}
        <button type="button" class="more" onclick={() => (limit += page)}>
          Show {Math.min(page, entries.length - limit)} more ({entries.length - limit} hidden)
        </button>
      {/if}
    </div>
    <div class="ln"><span class="gap"></span><span class="body"><span class="p">{closeCh}</span>{#if !last}<span class="p">,</span>{/if}</span></div>
  {/if}
{:else}
  {@const d = display(value)}
  <div class="ln">
    <span class="gap"></span>
    <span class="body">{@render label()}<span class={d.cls}>{d.text}</span>{#if !last}<span class="p">,</span>{/if}</span>
  </div>
{/if}

<style>
  .ln { display: flex; align-items: flex-start; min-height: 19.5px; }
  .ln:hover { background: var(--row-hover); }
  .body { flex: 1; min-width: 0; white-space: pre-wrap; overflow-wrap: anywhere; }
  .gap, .tg { flex: none; width: 14px; height: 14px; margin: 3px 4px 0 0; }
  .tg {
    padding: 0;
    justify-content: center;
    border-radius: 2px;
    border-color: var(--border-strong);
    background: var(--surface);
    color: var(--text-2);
  }
  .tg[aria-expanded='true'] { background: var(--surface-2); }
  .tg:hover:not(:disabled) { background: var(--cta); border-color: var(--cta-border); color: var(--cta-text); }
  .kids { margin-left: 6px; padding-left: 11px; border-left: 1px solid var(--border); }
  .kids:hover { border-left-color: var(--border-strong); }

  .key { color: var(--syn-key); }
  .idx { color: var(--text-3); }
  .str { color: var(--syn-str); }
  .num { color: var(--syn-num); }
  .kw { color: var(--syn-kw); }
  .p { color: var(--text-3); }
  .cnt { margin-left: 8px; font: 11px var(--font); color: var(--text-3); }
  .sum {
    padding: 0 4px;
    font: inherit;
    line-height: 1.2;
    color: var(--text-2);
    background: var(--surface-2);
    border-color: var(--border);
    border-radius: 2px;
  }
  .sum:hover:not(:disabled) { background: var(--sel); border-color: var(--cta-border); }
  .more { margin: 2px 0 2px 18px; padding: 0 6px; font: 11.5px var(--font); color: var(--text-2); background: transparent; border-style: dashed; }
</style>
