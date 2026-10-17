<script lang="ts">
  import type { Cluster } from './api'
  import { names } from './names'
  import { go } from './router.svelte'
  import { Search, Rows3, Users, Braces, LayoutDashboard, ShieldCheck, Server, CornerDownLeft, Loader } from '@lucide/svelte'

  // ⌘K / Ctrl+K: jump to any page, topic, group or subject in the current
  // cluster, or switch cluster. Lists load on first open and are kept for the
  // session; reopening refreshes them in the background.
  let { cluster, clusters }: { cluster: Cluster; clusters: Cluster[] } = $props()

  type Item = { kind: string; label: string; detail?: string; run: () => void }

  let open = $state(false)
  let query = $state('')
  let active = $state(0)
  let loading = $state(false)
  let topics = $state<string[]>([])
  let groups = $state<string[]>([])
  let subjects = $state<string[]>([])
  let loadedFor = ''
  let dialog: HTMLDialogElement
  let input = $state<HTMLInputElement>()
  let list = $state<HTMLUListElement>()

  const icons: Record<string, typeof Search> = {
    Page: LayoutDashboard,
    Topic: Rows3,
    Group: Users,
    Subject: Braces,
    Cluster: Server,
    Security: ShieldCheck,
  }

  async function load() {
    const c = cluster.name
    if (loadedFor !== c) {
      topics = []
      groups = []
      subjects = []
    }
    loading = true
    try {
      // Names only: listing lag and sizes here would take seconds on a
      // cluster with thousands of topics and groups.
      const n = await names(c)
      if (c !== cluster.name) return
      topics = n.topics.filter((x) => !x.internal).map((x) => x.name)
      groups = n.groups
      subjects = n.subjects
      loadedFor = c
    } catch {}
    loading = false
  }

  export function show() {
    query = ''
    active = 0
    open = true
    dialog.showModal()
    queueMicrotask(() => input?.focus())
    void load()
  }

  function close() {
    open = false
    dialog.close()
  }

  const pages = $derived<Item[]>([
    { kind: 'Page', label: 'Overview', run: () => go(cluster.name, 'overview') },
    { kind: 'Page', label: 'Topics', run: () => go(cluster.name, 'topics') },
    { kind: 'Page', label: 'Consumer groups', run: () => go(cluster.name, 'groups') },
    ...(cluster.schema_registry ? [{ kind: 'Page', label: 'Schema Registry', run: () => go(cluster.name, 'schemas') }] : []),
    { kind: 'Security', label: 'Security', run: () => go(cluster.name, 'security') },
  ])

  // Fuzzy-ish: every query character must appear in order; contiguous and
  // early matches rank higher. Good enough for names like orders.created.
  function score(text: string, q: string): number {
    if (!q) return 1
    const t = text.toLowerCase()
    const i = t.indexOf(q)
    if (i === 0) return 1000 - t.length
    if (i > 0) return 800 - i - t.length / 10
    let at = 0
    let gaps = 0
    for (const ch of q) {
      const j = t.indexOf(ch, at)
      if (j < 0) return -1
      gaps += j - at
      at = j + 1
    }
    return 400 - gaps - t.length / 10
  }

  const items = $derived.by(() => {
    const q = query.trim().toLowerCase()
    const all: Item[] = [
      ...pages,
      ...topics.map((n) => ({ kind: 'Topic', label: n, run: () => go(cluster.name, 'topics', n) })),
      ...groups.map((n) => ({ kind: 'Group', label: n, run: () => go(cluster.name, 'groups', n) })),
      ...subjects.map((n) => ({ kind: 'Subject', label: n, run: () => go(cluster.name, 'schemas', n) })),
      ...clusters
        .filter((c) => c.name !== cluster.name)
        .map((c) => ({ kind: 'Cluster', label: c.name, detail: c.production ? 'production' : undefined, run: () => go(c.name, 'overview') })),
    ]
    if (!q) return [...pages, ...all.filter((x) => x.kind === 'Cluster')].slice(0, 50)
    return all
      .map((x) => ({ x, s: score(x.label, q) }))
      .filter((r) => r.s >= 0)
      .sort((a, b) => b.s - a.s)
      .slice(0, 50)
      .map((r) => r.x)
  })

  $effect(() => {
    void query
    active = 0
  })

  function run(item: Item | undefined) {
    if (!item) return
    close()
    item.run()
  }

  function onkey(e: KeyboardEvent) {
    if (e.key === 'ArrowDown') {
      e.preventDefault()
      active = Math.min(active + 1, items.length - 1)
    } else if (e.key === 'ArrowUp') {
      e.preventDefault()
      active = Math.max(active - 1, 0)
    } else if (e.key === 'Enter') {
      e.preventDefault()
      run(items[active])
    } else return
    list?.querySelector(`[data-i="${active}"]`)?.scrollIntoView({ block: 'nearest' })
  }

  function global(e: KeyboardEvent) {
    if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 'k') {
      e.preventDefault()
      if (open) close()
      else show()
    }
  }

  function mark(label: string) {
    const q = query.trim()
    const i = q ? label.toLowerCase().indexOf(q.toLowerCase()) : -1
    return i < 0 ? [label, '', ''] : [label.slice(0, i), label.slice(i, i + q.length), label.slice(i + q.length)]
  }
</script>

<svelte:window onkeydown={global} />

<dialog bind:this={dialog} class="palette" onclose={() => (open = false)} onclick={(e) => e.target === dialog && close()} aria-label="Go to">
  {#if open}
    <div class="search">
      <Search size={16} />
      <input
        bind:this={input}
        bind:value={query}
        onkeydown={onkey}
        placeholder="Go to topic, group, subject or page…"
        role="combobox"
        aria-expanded="true"
        aria-controls="palette-list"
        aria-activedescendant={items[active] ? `palette-${active}` : undefined}
        aria-label="Search"
        spellcheck="false"
        autocomplete="off"
      />
      {#if loading}<Loader size={14} class="spin" />{/if}
      <kbd>esc</kbd>
    </div>
    <ul bind:this={list} id="palette-list" role="listbox">
      {#each items as it, i (it.kind + it.label)}
        {@const Icon = icons[it.kind] ?? Search}
        {@const [a, b, c] = mark(it.label)}
        <li id="palette-{i}" data-i={i} role="option" aria-selected={i === active} class:active={i === active}
          onclick={() => run(it)} onmouseenter={() => (active = i)} onkeydown={() => {}}>
          <Icon size={15} />
          <span class="label" class:mono={it.kind !== 'Page' && it.kind !== 'Security'}>{a}<mark>{b}</mark>{c}</span>
          {#if it.detail}<span class="badge danger">{it.detail}</span>{/if}
          <span class="kind">{it.kind === 'Security' ? 'Page' : it.kind}</span>
          {#if i === active}<CornerDownLeft size={13} class="enter" />{/if}
        </li>
      {:else}
        <li class="none" role="presentation">{loading ? 'Loading…' : `Nothing matches “${query}”.`}</li>
      {/each}
    </ul>
    <div class="foot">
      <span><kbd>↑</kbd><kbd>↓</kbd> move</span>
      <span><kbd>↵</kbd> open</span>
      <span class="spacer"></span>
      <span class="faint">{topics.length} topics · {groups.length} groups{cluster.schema_registry ? ` · ${subjects.length} subjects` : ''} in <b>{cluster.name}</b></span>
    </div>
  {/if}
</dialog>

<style>
  .palette {
    margin: 12vh auto auto;
    width: min(640px, calc(100vw - 32px));
    max-height: 70vh;
    padding: 0;
    border: 1px solid var(--border-strong);
    border-radius: var(--radius);
    background: var(--surface);
    color: var(--text);
    box-shadow: 0 24px 64px -16px rgb(20 20 16 / .35);
    overflow: hidden;
  }
  .palette[open] { display: flex; flex-direction: column; }
  .palette::backdrop { background: rgb(23 23 20 / .32); }
  .search { display: flex; align-items: center; gap: 10px; padding: 0 14px; border-bottom: 1px solid var(--border-strong); color: var(--text-3); }
  .search input { flex: 1; border: 0; padding: 14px 0; font-size: 15px; background: transparent; }
  .search input:focus { outline: none; }
  .search :global(.spin) { animation: spin 1s linear infinite; }
  @keyframes spin { to { transform: rotate(360deg); } }
  ul { list-style: none; margin: 0; padding: 6px; overflow: auto; flex: 1; }
  li {
    display: grid;
    grid-template-columns: 16px minmax(0, 1fr) auto auto 14px;
    align-items: center;
    gap: 10px;
    padding: 7px 10px;
    border-radius: 2px;
    font-size: 13.5px;
    color: var(--text-2);
    cursor: pointer;
  }
  li.active { background: var(--sel); color: var(--text); }
  .label { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; color: var(--text); }
  .label.mono { font-size: 13px; }
  mark { background: var(--cta); color: var(--cta-text); border-radius: 1px; padding: 0 1px; }
  .kind { font-size: 11.5px; color: var(--text-3); }
  li :global(.enter) { color: var(--text-2); }
  li.none { display: block; padding: 22px 10px; text-align: center; color: var(--text-3); cursor: default; }
  .foot { display: flex; align-items: center; gap: 14px; padding: 7px 14px; font-size: 12px; color: var(--text-3); border-top: 1px solid var(--border); background: var(--surface-2); }
  .foot b { color: var(--text-2); font-weight: 500; }
  kbd { display: inline-block; min-width: 18px; padding: 0 4px; margin-right: 2px; font: 11px/1.5 var(--mono); text-align: center; color: var(--text-2); background: var(--surface); border: 1px solid var(--border-strong); border-radius: 2px; }
  @media (prefers-reduced-motion: reduce) { .search :global(.spin) { animation: none; } }
</style>
