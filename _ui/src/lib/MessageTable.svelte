<script lang="ts">
  import type { Cluster, Message } from './api'
  import type { MessageColumns } from './messageColumns.svelte'
  import { COLS, hit, preview } from './messageLayout'
  import { pick as pickField, cell } from './viewer'
  import { narrow } from './media.svelte'
  import { toggled } from './sets'
  import ColResizer from './ColResizer.svelte'
  import Pending from './Pending.svelte'
  import ValueView from './ValueView.svelte'
  import ValueMeta from './ValueMeta.svelte'
  import MessageActions from './MessageActions.svelte'
  import { Copy, Plus, Minus, ChevronsUpDown, ChevronsDownUp, Square, Loader, ChevronsDown } from '@lucide/svelte'

  // The list of messages read: one row each, expandable in place on wide
  // screens, a two-line card on phones.
  let {
    cluster,
    messages,
    cols,
    searched,
    selected = $bindable(),
    picked = $bindable(),
    expanded = $bindable(),
    live,
    loading,
    loadingMore,
    hasMore,
    moreLabel,
    editing,
    fresh,
    when,
    other,
    onedit,
    onresend,
    oncopy,
    onmore,
    onstop,
  }: {
    cluster: Cluster
    messages: Message[]
    cols: MessageColumns
    searched: string
    selected: Message | undefined
    picked: Set<string>
    expanded: Set<string>
    live: boolean
    loading: boolean
    loadingMore: boolean
    hasMore: boolean
    moreLabel: string
    editing: boolean
    // Keys of rows that just arrived over live tail; each lights up once.
    fresh: Set<string>
    when: (iso: string) => string
    other: (iso: string) => string
    onedit: (m: Message) => void
    onresend: (m: Message) => void
    oncopy: (text: string, what: 'key' | 'value') => void
    onmore: () => void
    onstop: () => void
  } = $props()

  const mkey = (m: Message) => `${m.partition}:${m.offset}`

  const allPicked = $derived(messages.length > 0 && messages.every((m) => picked.has(mkey(m))))
  const pickAll = () => (picked = allPicked ? new Set() : new Set(messages.map(mkey)))

  // Each open row renders a full value tree; opening a thousand of them at
  // once would freeze the tab, so Expand all opens the first expandCap.
  const expandCap = 50
  const allOpen = $derived(messages.length > 0 && expanded.size >= Math.min(messages.length, expandCap))
  const toggleAll = () => (expanded = allOpen ? new Set() : new Set(messages.slice(0, expandCap).map(mkey)))

  // Each JSON value is parsed once per result, and only with columns shown.
  const parsed = $derived.by(() => {
    const out = new Map<Message, unknown>()
    if (!cols.shown.length) return out
    for (const m of messages) {
      if (m.value === null || m.value_format !== 'json' && !/^\s*[{[]/.test(m.value)) continue
      try {
        out.set(m, JSON.parse(m.value))
      } catch {}
    }
    return out
  })
  const field = (m: Message, path: string) => cell(pickField(parsed.get(m), path))

  function arrived(node: HTMLElement, key: string) {
    if (!fresh.delete(key)) return
    node.classList.add('arrived')
    node.addEventListener('animationend', () => node.classList.remove('arrived'), { once: true })
  }

  let listEl = $state<HTMLElement>()
</script>

<!-- Text with the search term marked. -->
{#snippet marked(text: string)}
  {@const [before, match, after] = hit(text, searched)}
  {before}{#if match}<mark>{match}</mark>{/if}{after}
{/snippet}

{#snippet rz(id: string, label: string, w: number)}
  <ColResizer {label} width={w} onchange={(px) => cols.setWidth(id, px)} ondone={cols.keep} onfit={() => listEl && cols.fit(listEl, id)} />
{/snippet}

<div class="panel list" bind:this={listEl}>
  <table class="manual">
    <colgroup>
      {#if !cluster.read_only}<col style:width="{COLS.pick}px" />{/if}
      {#if narrow.matches}
        <col />
      {:else}
        <col style:width="{COLS.toggle}px" /><col style:width="{COLS.partition}px" /><col style:width="{cols.offset}px" /><col style:width="{cols.time}px" />
        <col style:width="{cols.key}px" />
        {#each cols.shown as c (c)}<col style:width="{cols.field(c)}px" />{/each}
        <col />
      {/if}
    </colgroup>
    <thead>
      <tr>
        {#if !cluster.read_only}
          <th class="pk"><input type="checkbox" checked={allPicked} indeterminate={picked.size > 0 && !allPicked}
            disabled={!messages.length} onchange={pickAll} aria-label="Select all messages" /></th>
        {/if}
        {#if narrow.matches}
          <th>Message</th>
        {:else}
          <th class="tog">
            {#if messages.length}
              <button type="button" class="ghost xbtn" onclick={toggleAll} title={allOpen ? 'Collapse all' : messages.length > expandCap ? `Expand the first ${expandCap}` : 'Expand all'} aria-label={allOpen ? 'Collapse all' : 'Expand all'}>
                {#if allOpen}<ChevronsDownUp size={13} />{:else}<ChevronsUpDown size={13} />{/if}
              </button>
            {/if}
          </th>
          <th class="num" title="Partition"><abbr>P</abbr></th>
          <th class="num rzh" data-col="offset">Offset{@render rz('offset', 'Offset', cols.offset)}</th>
          <th class="rzh" data-col="time">Time{@render rz('time', 'Time', cols.time)}</th>
          <th class="rzh" data-col="key">Key{@render rz('key', 'Key', cols.key)}</th>
          {#each cols.shown as c (c)}<th class="mono vcol rzh" data-col="v:{c}" title="value.{c}">{c}{@render rz(`v:${c}`, c, cols.field(c))}</th>{/each}
          <th>Value</th>
        {/if}
      </tr>
    </thead>
    <tbody>
      {#each messages as m, i (mkey(m))}
        {@const open = !narrow.matches && expanded.has(mkey(m))}
        <tr id="m-{m.partition}-{m.offset}" class:alt={i % 2 === 1} class:sel={selected === m} class:bad={!!m.decode_error} class:opened={open} class:picked={picked.has(mkey(m))} onclick={() => (selected = m)}
          tabindex="0" aria-selected={selected === m} onkeydown={(e) => { if ((e.key === 'Enter' || e.key === ' ') && e.target === e.currentTarget) { e.preventDefault(); selected = m } }} use:arrived={mkey(m)}>
          {#if !cluster.read_only}
            <td class="pk"><input type="checkbox" checked={picked.has(mkey(m))} onclick={(e) => e.stopPropagation()}
              onchange={() => (picked = toggled(picked, mkey(m)))} aria-label="Select message {m.partition}:{m.offset}" /></td>
          {/if}
          {#if narrow.matches}
            <td class="card">
              <div class="card-meta mono">
                <span title="Partition {m.partition}">P{m.partition}</span><span title="Offset {m.offset}">#{m.offset}</span>
                <span class="faint">{when(m.timestamp)}</span>
              </div>
              <div class="mono key">{@render marked(m.key ?? '—')}</div>
              <div class="mono val">{@render marked(preview(m.value, searched))}</div>
            </td>
          {:else}
            <td class="tog">
              <button type="button" class="xbtn" class:on={open} aria-expanded={open}
                aria-label={open ? 'Collapse message' : 'Expand message'}
                onclick={(e) => { e.stopPropagation(); expanded = toggled(expanded, mkey(m)) }}>
                {#if open}<Minus size={12} strokeWidth={2.2} />{:else}<Plus size={12} strokeWidth={2.2} />{/if}
              </button>
            </td>
            <td class="num mono">{m.partition}</td>
            <td class="num mono" data-col="offset">{m.offset}</td>
            <td class="mono nowrap time" data-col="time" title={other(m.timestamp)}>{when(m.timestamp)}</td>
            <td class="mono key" data-col="key" title={m.key ?? undefined}>{@render marked(m.key ?? '—')}</td>
            {#each cols.shown as c (c)}{@const f = field(m, c)}<td class="mono vcol" data-col="v:{c}" title={f}>{f}</td>{/each}
            <td class="mono val">{@render marked(preview(m.value, searched))}</td>
          {/if}
        </tr>
        {#if open}
          <tr class="xrow" class:alt={i % 2 === 1} class:sel={selected === m}>
            {#if !cluster.read_only}<td></td>{/if}
            <td></td>
            <td colspan={cols.count - (cluster.read_only ? 1 : 2)}>
              <div class="inl">
                <div class="inl-meta">
                  <ValueMeta message={m} />
                  {#if m.key !== null}<span class="faint">key</span><code>{m.key}</code>{/if}
                  {#each m.headers as h, i (i)}<span class="hdr"><span class="faint">{h.key}</span>{h.value}</span>{/each}
                  <span class="spacer"></span>
                  {#if !cluster.read_only}<MessageActions message={m} {editing} compact {onedit} {onresend} />{/if}
                  {#if m.key !== null}<button type="button" class="ghost small" onclick={() => oncopy(m.key ?? '', 'key')}><Copy size={12} />Copy key</button>{/if}
                  <button type="button" class="ghost small" onclick={() => oncopy(m.value ?? '', 'value')}><Copy size={12} />Copy value</button>
                </div>
                {#if m.decode_error}<div class="error small-err">Schema decoding failed: {m.decode_error}</div>{/if}
                <ValueView text={m.value} format={m.value_format} maxHeight={420} />
              </div>
            </td>
          </tr>
        {/if}
      {:else}
        <tr><td colspan={cols.count} class="empty">{#if live}<Pending label="Waiting for new messages…" />{:else if loading}<Pending label="Reading…" />{:else}No messages found.{/if}</td></tr>
      {/each}
    </tbody>
  </table>
  {#if hasMore}
    <div class="more-rows">
      {#if loadingMore}
        <button type="button" class="danger" onclick={onstop}><Square size={12} />Stop</button>
        <span class="faint"><Loader size={13} class="animate-spin inline" /> Reading…</span>
      {:else}
        <button type="button" onclick={onmore} disabled={loading}
          title="Continue the same read from where it stopped"><ChevronsDown size={14} />{moreLabel}</button>
      {/if}
    </div>
  {/if}
</div>

<style>
  .list mark { background: var(--cta); color: var(--cta-text); padding: 0 1px; border-radius: 1px; }
  .list > table { min-width: 0; }
  /* The list grows with its rows and the page scrolls; the header row sticks
     under the top edge (under the app header below lg) while reading. */
  .list { --stick: 0px; }
  .list thead th { top: var(--stick); z-index: 2; }
  .list tbody tr { scroll-margin-top: calc(var(--stick) + 40px); scroll-margin-bottom: 12px; }
  @media (max-width: 1023.98px) { .list { --stick: 48px; } }
  /* Between phone and lg the panel scrolls sideways, so it is the header's
     sticky container and the offset would push the header onto the rows. */
  @media (min-width: 640px) and (max-width: 1023.98px) { .list thead th { position: static; } .list thead th.rzh { position: relative; } }
  .list table { table-layout: fixed; }
  .list tr { cursor: pointer; }
  .list tr:focus-visible { outline: 2px solid var(--focus); outline-offset: -2px; }
  .list tr.sel td { background: var(--sel); }
  .list tr.bad td:first-child { box-shadow: inset 2px 0 0 var(--danger); }
  /* A message that just arrived over live tail lights up in the highlight
     colour and settles into its row, so a burst reads as a burst. With
     reduced motion it is marked for a moment and then simply unmarked. */
  .list :global(tr.arrived > td) { animation: arrive 1.6s cubic-bezier(.16, 1, .3, 1) both; }
  .list :global(tr.arrived > td:first-child) { animation-name: arrive, arrive-edge; }
  @keyframes arrive { from { background-color: var(--cta); } }
  @keyframes arrive-edge { from { box-shadow: inset 3px 0 0 var(--cta-text); } to { box-shadow: inset 3px 0 0 transparent; } }
  @media (prefers-reduced-motion: reduce) {
    .list :global(tr.arrived > td) { animation-timing-function: steps(1, end); }
  }
  .small-err { font-size: 12px; padding: 6px 8px; }
  .nowrap { white-space: nowrap; }
  .time { overflow: hidden; text-overflow: ellipsis; }
  .key, .val { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .val { color: var(--text-2); }

  .pk { padding-right: 0; }
  td.pk { padding-top: calc(var(--cell-y) + 1px); }
  .pk input { display: block; margin: 0 0 0 2px; }
  .list tr.picked td { background: var(--sel); }
  .tog { width: 1%; padding-right: 0; }
  td.tog { padding-top: calc(var(--cell-y) - 1px); }
  .xbtn {
    width: 20px;
    height: 20px;
    padding: 0;
    justify-content: center;
    border-radius: 2px;
    color: var(--text-2);
  }
  .xbtn.on { background: var(--cta); border-color: var(--cta-border); color: var(--cta-text); }
  .xbtn.on:hover:not(:disabled) { background: var(--cta-hover); }
  th .xbtn { border: 0; color: var(--text-3); }
  tr.opened td { border-bottom-color: transparent; }
  tr.xrow td { padding-top: 0; cursor: default; }
  .vcol { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .more-rows { display: flex; align-items: center; justify-content: center; gap: 10px; padding: 10px; border-top: 1px solid var(--border); }
  tbody tr.xrow:hover td { background: transparent; }
  tbody tr.xrow.alt:hover td { background: var(--stripe); }
  .inl { display: grid; gap: 6px; padding-bottom: 4px; }
  .inl-meta { display: flex; flex-wrap: wrap; align-items: center; gap: 6px 8px; font-size: 12px; }
  .inl-meta code { font-size: 12px; }
  .hdr { display: inline-flex; gap: 4px; font: 12px var(--mono); padding: 0 6px; border: 1px solid var(--border); border-radius: 2px; }

  /* Phones: rows are two-line cards. */
  @media (max-width: 639.98px) {
    .list { overflow: visible; }
    .card { display: grid; gap: 2px; min-width: 0; padding-block: 9px; }
    .card-meta { display: flex; gap: 10px; font-size: 12px; color: var(--text-2); }
    .card-meta .faint { margin-left: auto; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .card .key { font-size: 12px; color: var(--text); }
    .card .val { font-size: 12px; }
    td.pk { padding-top: 12px; }
  }
</style>
