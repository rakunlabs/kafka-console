<script lang="ts">
  import type { Cluster, PartitionLag } from './api'
  import { fmt } from './format'
  import LagBar from './LagBar.svelte'
  import OffsetDialog from './OffsetDialog.svelte'
  import MoreRows from './MoreRows.svelte'
  import { PencilLine, X } from '@lucide/svelte'

  // One group's partitions of one topic, with per-row and multi-select offset
  // changes. Used by the group page and the topic's Consumers tab.
  let {
    cluster,
    group,
    topic,
    rows,
    activeMembers = 0,
    showLow = false,
    onchanged,
  }: {
    cluster: Cluster
    group: string
    topic: string
    rows: PartitionLag[]
    activeMembers?: number
    showLow?: boolean
    onchanged?: () => void
  } = $props()

  // A topic with thousands of partitions renders a page of rows at a time;
  // selecting all still covers every partition.
  let shown = $state(100)
  let selected = $state(new Set<number>())
  let target = $state<number[]>([])
  let editing = $state(false)

  const editable = $derived(!cluster.read_only)
  const allOn = $derived(rows.length > 0 && rows.every((r) => selected.has(r.partition)))
  const someOn = $derived(selected.size > 0 && !allOn)

  function toggle(p: number) {
    const next = new Set(selected)
    if (next.has(p)) next.delete(p)
    else next.add(p)
    selected = next
  }
  const toggleAll = () => (selected = allOn ? new Set() : new Set(rows.map((r) => r.partition)))

  function edit(parts: number[]) {
    target = parts
    editing = true
  }

  const why = $derived(cluster.read_only ? 'This cluster is read-only' : activeMembers > 0 ? 'Stop the consumers to change offsets (you can still preview)' : '')
</script>

{#if editable && selected.size > 0}
  <div class="selbar" role="region" aria-label="Selected partitions">
    <b>{selected.size}</b> {selected.size === 1 ? 'partition' : 'partitions'} selected
    <span class="faint mono">{[...selected].sort((a, b) => a - b).join(', ')}</span>
    <span class="spacer"></span>
    <button class="ghost small" onclick={() => (selected = new Set())}><X size={13} />Clear</button>
    <button class="primary small" onclick={() => edit([...selected].sort((a, b) => a - b))} title={why}>
      <PencilLine size={13} />Change offsets…
    </button>
  </div>
{/if}

<div class="panel overflow-x-auto">
  <table class="pt">
    <colgroup>
      {#if editable}<col style:width="36px" />{/if}
      <col style:width="48px" /><col style:width="26%" />
      {#if showLow}<col style:width="90px" />{/if}
      <col style:width="110px" /><col style:width="100px" /><col style:width="90px" /><col /><col style:width="140px" />
      {#if editable}<col style:width="52px" />{/if}
    </colgroup>
    <thead>
      <tr>
        {#if editable}
          <th class="ck">
            <input type="checkbox" checked={allOn} indeterminate={someOn} onchange={toggleAll} aria-label="Select all partitions" />
          </th>
        {/if}
        <th class="num">P</th><th>Progress</th>
        {#if showLow}<th class="num">Low</th>{/if}
        <th class="num">Committed</th><th class="num">High</th><th class="num">Lag</th><th>Client</th><th>Host</th>
        {#if editable}<th><span class="sr-only">Change offset</span></th>{/if}
      </tr>
    </thead>
    <tbody>
      {#each rows.slice(0, shown) as p (p.partition)}
        <tr class:picked={selected.has(p.partition)}>
          {#if editable}
            <td class="ck">
              <input type="checkbox" checked={selected.has(p.partition)} onchange={() => toggle(p.partition)} aria-label="Select partition {p.partition}" />
            </td>
          {/if}
          <td class="num mono">{p.partition}</td>
          <td><LagBar low={p.low} high={p.high} committed={p.committed} /></td>
          {#if showLow}<td class="num mono faint">{fmt(p.low)}</td>{/if}
          <td class="num mono">{fmt(p.committed)}</td>
          <td class="num mono">{fmt(p.high)}</td>
          <td class="num mono">{#if p.lag > 0}<span class="lagpill">{fmt(p.lag)}</span>{:else}<span class="faint">{fmt(p.lag)}</span>{/if}</td>
          <td class="mono small">
            {#if p.client_id}{p.client_id}{:else}<span class="faint">unassigned</span>{/if}
            {#if p.error}<span class="badge danger" title={p.error}>error</span>{/if}
          </td>
          <td class="mono small faint">{p.host ?? '—'}</td>
          {#if editable}
            <td class="act">
              <button class="ghost edit" onclick={() => edit([p.partition])}
                title={why || `Change partition ${p.partition}'s offset`} aria-label="Change partition {p.partition}'s offset">
                <PencilLine size={14} />
              </button>
            </td>
          {/if}
        </tr>
      {/each}
    </tbody>
  </table>
  <MoreRows total={rows.length} bind:shown step={100} />
</div>

<OffsetDialog {cluster} {group} {topic} partitions={target} {activeMembers} bind:open={editing}
  ondone={() => { selected = new Set(); onchanged?.() }} />

<style>
  .pt { table-layout: fixed; min-width: 760px; }
  .pt td { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .ck { padding-right: 0; }
  .ck input { display: block; margin: 2px 0 0 2px; }
  td.ck { padding-top: 9px; }
  .act { padding: 4px 8px; text-align: right; }
  .edit { padding: 4px 6px; color: var(--text-3); opacity: .55; }
  tr:hover .edit, .edit:focus-visible { opacity: 1; color: var(--text); }
  .edit:hover:not(:disabled) { background: var(--cta); border-color: var(--cta-border); color: var(--cta-text); opacity: 1; }
  tbody tr.picked td { background: var(--sel); }
  .lagpill { display: inline-block; padding: 0 6px; background: var(--sel); border: 1px solid var(--cta-border); border-radius: 2px; font-weight: 600; }
  .selbar {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 6px 10px;
    padding: 6px 8px 6px 12px;
    margin-bottom: 8px;
    font-size: 13px;
    background: var(--sel);
    border: 1px solid var(--cta-border);
    border-radius: var(--radius);
  }
  .selbar b { font-weight: 600; }
</style>
