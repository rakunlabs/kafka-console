<script lang="ts">
  import type { TopicName } from './api'
  import { Search, Check } from '@lucide/svelte'

  // Searchable topic picker. Typing filters, arrows move, Enter picks.
  let {
    topics,
    value = $bindable(''),
    exclude = [],
    id = 'topic-picker',
  }: { topics: TopicName[]; value: string; exclude?: string[]; id?: string } = $props()

  let query = $state('')
  let active = $state(0)
  let showInternal = $state(false)
  let list: HTMLUListElement

  // Matches are capped; on a large cluster typing narrows the list faster
  // than scrolling thousands of rows would.
  const cap = 100
  const matches = $derived.by(() => {
    const q = query.trim().toLowerCase()
    const hide = new Set(exclude)
    return topics
      .filter((t) => (showInternal || !t.internal) && !hide.has(t.name) && (!q || t.name.toLowerCase().includes(q)))
      .sort((a, b) => {
        // Prefix matches first, then alphabetical.
        if (q) {
          const ap = a.name.toLowerCase().startsWith(q) ? 0 : 1
          const bp = b.name.toLowerCase().startsWith(q) ? 0 : 1
          if (ap !== bp) return ap - bp
        }
        return a.name.localeCompare(b.name)
      })
  })
  const rows = $derived(matches.slice(0, cap))
  const hidden = $derived(exclude.filter((e) => topics.some((t) => t.name === e)).length)

  $effect(() => {
    void query
    active = 0
  })

  function pick(name: string) {
    value = name
  }

  function onkey(e: KeyboardEvent) {
    if (e.key === 'ArrowDown') {
      e.preventDefault()
      active = Math.min(active + 1, rows.length - 1)
    } else if (e.key === 'ArrowUp') {
      e.preventDefault()
      active = Math.max(active - 1, 0)
    } else if (e.key === 'Enter') {
      e.preventDefault()
      if (rows[active]) pick(rows[active].name)
    } else return
    list?.querySelector(`[data-i="${active}"]`)?.scrollIntoView({ block: 'nearest' })
  }

  function mark(name: string) {
    const q = query.trim()
    const i = q ? name.toLowerCase().indexOf(q.toLowerCase()) : -1
    return i < 0 ? [name, '', ''] : [name.slice(0, i), name.slice(i, i + q.length), name.slice(i + q.length)]
  }
</script>

<div class="picker">
  <div class="search">
    <Search size={14} />
    <!-- svelte-ignore a11y_autofocus -->
    <input
      type="search"
      placeholder="Search topics…"
      bind:value={query}
      onkeydown={onkey}
      autofocus
      role="combobox"
      aria-expanded="true"
      aria-controls="{id}-list"
      aria-activedescendant={rows[active] ? `${id}-${active}` : undefined}
      aria-label="Search topics"
    />
    <label class="check"><input type="checkbox" bind:checked={showInternal} /> internal</label>
  </div>
  <ul bind:this={list} id="{id}-list" role="listbox" aria-label="Topics">
    {#each rows as t, i (t.name)}
      {@const [a, b, c] = mark(t.name)}
      <li
        id="{id}-{i}"
        data-i={i}
        role="option"
        aria-selected={value === t.name}
        class:active={i === active}
        class:chosen={value === t.name}
        onclick={() => pick(t.name)}
        onmouseenter={() => (active = i)}
        onkeydown={() => {}}
      >
        <span class="name mono">{a}<mark>{b}</mark>{c}</span>
        {#if t.internal}<span class="badge">internal</span>{/if}
        <span class="meta">{t.partitions} {t.partitions === 1 ? 'partition' : 'partitions'}</span>
        <span class="tick">{#if value === t.name}<Check size={14} />{/if}</span>
      </li>
    {:else}
      <li class="none" role="presentation">{query ? `No topic matches “${query}”.` : 'No topics.'}</li>
    {/each}
  </ul>
  {#if matches.length > cap}<p class="foot faint">Showing {cap} of {matches.length} topics; type to narrow the list.</p>{/if}
  {#if hidden}<p class="foot faint">{hidden} {hidden === 1 ? 'topic' : 'topics'} this group already reads {hidden === 1 ? 'is' : 'are'} hidden.</p>{/if}
</div>

<style>
  .picker { border: 1px solid var(--border-strong); border-radius: var(--radius); background: var(--surface); overflow: hidden; }
  .search { display: flex; align-items: center; gap: 8px; padding: 0 10px; border-bottom: 1px solid var(--border); color: var(--text-3); }
  .search input[type='search'] { flex: 1; border: 0; padding: 8px 0; background: transparent; font-size: 13.5px; }
  .search input[type='search']:focus { outline: none; }
  .search:focus-within { box-shadow: inset 0 -2px 0 var(--accent); }
  .search .check { font-size: 12px; flex: none; }
  ul { list-style: none; margin: 0; padding: 4px; max-height: 236px; overflow: auto; }
  li {
    display: grid;
    grid-template-columns: minmax(0, 1fr) auto auto 16px;
    align-items: center;
    gap: 10px;
    padding: 6px 8px;
    font-size: 13px;
    border-radius: 2px;
    cursor: pointer;
  }
  li.active { background: var(--row-hover); }
  li.chosen { background: var(--sel); }
  .name { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  mark { background: var(--cta); color: var(--cta-text); border-radius: 1px; padding: 0 1px; }
  .meta { font-size: 12px; color: var(--text-3); white-space: nowrap; }
  .tick { color: var(--text); display: flex; }
  li.none { display: block; padding: 18px 8px; text-align: center; color: var(--text-3); cursor: default; }
  .foot { padding: 6px 10px; font-size: 12px; border-top: 1px solid var(--border); }
</style>
