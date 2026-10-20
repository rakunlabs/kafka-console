<script lang="ts">
  import { api, type Cluster } from './lib/api'
  import { app } from './lib/features.svelte'
  import { router, href, go } from './lib/router.svelte'
  import { wide } from './lib/media.svelte'
  import Overview from './routes/Overview.svelte'
  import Topics from './routes/Topics.svelte'
  import Topic from './routes/Topic.svelte'
  import Groups from './routes/Groups.svelte'
  import Group from './routes/Group.svelte'
  import Broker from './routes/Broker.svelte'
  import Security from './routes/Security.svelte'
  import Schemas from './routes/Schemas.svelte'
  import Subject from './routes/Subject.svelte'
  import CommandPalette from './lib/CommandPalette.svelte'
  import Logo from './lib/Logo.svelte'
  import Backdrop from './lib/Backdrop.svelte'
  import {
    LayoutDashboard,
    Rows3,
    Users,
    Braces,
    ShieldCheck,
    ChevronsUpDown,
    Lock,
    TriangleAlert,
    Menu,
    Sun,
    Moon,
    Monitor,
    Search,
  } from '@lucide/svelte'

  let clusters = $state<Cluster[]>([])
  let error = $state('')

  // The last cluster opened, so a bare URL returns to it.
  const lastKey = 'kc.cluster'
  const store = {
    get: () => {
      try {
        return localStorage.getItem(lastKey)
      } catch {
        return null
      }
    },
    set: (v: string) => {
      try {
        localStorage.setItem(lastKey, v)
      } catch {
        // Storage blocked or full: only the convenience is lost.
      }
    },
  }

  api
    .clusters()
    .then((c) => {
      clusters = c
      if (!router.route.cluster && c.length) {
        const last = store.get()
        go(c.some((x) => x.name === last) ? last! : c[0].name)
      }
    })
    .catch((e) => (error = e.message))

  const route = $derived(router.route)
  const current = $derived(clusters.find((c) => c.name === route.cluster))
  $effect(() => {
    if (current) store.set(current.name)
  })

  const nav = $derived([
    { section: 'overview', label: 'Overview', icon: LayoutDashboard },
    { section: 'topics', label: 'Topics', icon: Rows3 },
    { section: 'groups', label: 'Consumer groups', icon: Users },
    ...(current?.schema_registry ? [{ section: 'schemas', label: 'Schema Registry', icon: Braces }] : []),
    ...(current?.features.security ? [{ section: 'security', label: 'Security', icon: ShieldCheck }] : []),
  ])
  const activeSection = $derived(route.section === 'brokers' ? 'overview' : route.section)

  // Theme: follows the OS unless the user picked one.
  let theme = $state(localStorage.getItem('theme') ?? 'auto')
  $effect(() => {
    if (theme === 'auto') {
      localStorage.removeItem('theme')
      delete document.documentElement.dataset.theme
    } else {
      localStorage.setItem('theme', theme)
      document.documentElement.dataset.theme = theme
    }
  })
  const themes: Record<string, string> = { auto: 'System', light: 'Light', dark: 'Dark' }
  const ThemeIcon = $derived(theme === 'dark' ? Moon : theme === 'light' ? Sun : Monitor)
  let palette = $state<ReturnType<typeof CommandPalette>>()
  const mac = /Mac|iPhone|iPad/.test(navigator.platform)

  const nextTheme = () => (theme = theme === 'auto' ? 'dark' : theme === 'dark' ? 'light' : 'auto')

  // Below lg the sidebar is a drawer behind the header's menu button.
  let menuOpen = $state(false)
  $effect(() => {
    void route.cluster, route.section, route.name
    menuOpen = false
  })
  $effect(() => {
    if (wide.matches) menuOpen = false
    document.documentElement.style.overflow = menuOpen ? 'hidden' : ''
  })
</script>

<svelte:window onkeydown={(e) => e.key === 'Escape' && (menuOpen = false)} />

<div class="grid min-h-screen grid-cols-[minmax(0,1fr)] lg:grid-cols-[240px_minmax(0,1fr)]">
  {#if menuOpen}
    <button class="scrim lg:hidden" aria-label="Close menu" onclick={() => (menuOpen = false)}></button>
  {/if}
  <aside
    id="sidebar"
    class="theme-dark fixed inset-y-0 left-0 z-40 flex w-[min(280px,85vw)] flex-col overflow-y-auto border-r border-line-strong bg-sidebar transition-transform duration-200 lg:sticky lg:top-0 lg:z-auto lg:h-screen lg:w-auto lg:translate-x-0 lg:transition-none {menuOpen
      ? 'translate-x-0 shadow-2xl'
      : '-translate-x-full'}"
    class:prod-edge={current?.production}
    inert={!wide.matches && !menuOpen}
  >
    <div class="flex h-14 items-center gap-2 border-b border-line-strong px-3.5">
      <a
        href={route.cluster ? href(route.cluster, 'overview') : '#/'}
        class="flex min-w-0 items-center gap-2.5 text-[15px] font-semibold tracking-tight text-ink hover:no-underline"
        title="Overview"
      >
        <Logo size={22} />
        <span title={app.version ? `kafka-console ${app.version}` : undefined}>kafka-console</span>
      </a>
      {#if app.info}
        <span class="ml-auto truncate bg-[#fbff81] px-1.5 py-0.5 text-[11.5px] leading-tight font-semibold text-[#222220]" title={app.info}>{app.info}</span>
      {/if}
    </div>

    <div class="border-b border-line-strong px-3.5 py-3">
      <label class="gap-1.5">
        <span class="text-xs font-medium text-ink-3">Cluster</span>
        <span class="relative flex items-center text-ink-3">
          <span class="sq pointer-events-none absolute left-2.5 {current?.production ? 'danger' : 'ok'}"></span>
          <select
            class="noicon w-full cursor-pointer py-1.5 pr-7 pl-6.5 text-sm font-medium"
            value={route.cluster}
            onchange={(e) => go(e.currentTarget.value, route.section === 'brokers' ? 'overview' : route.section)}
          >
            {#each clusters as c (c.name)}
              <option value={c.name}>{c.name}</option>
            {/each}
          </select>
          <ChevronsUpDown class="pointer-events-none absolute right-2" size={14} />
        </span>
      </label>
      {#if current && (current.production || current.read_only)}
        <div class="mt-2 flex gap-1.5">
          {#if current.production}<span class="badge danger">production</span>{/if}
          {#if current.read_only}<span class="badge gap-1"><Lock size={11} />read-only</span>{/if}
        </div>
      {/if}
    </div>

    <div class="border-b border-line-strong px-3.5 py-2.5">
      <button
        class="goto w-full justify-start gap-2 px-2.5 text-ink-3"
        onclick={() => palette?.show()}
        disabled={!current}
        aria-keyshortcuts="Meta+K Control+K"
      >
        <Search size={14} />
        <span class="flex-1 text-left">Go to…</span>
        <kbd>{mac ? '⌘' : 'Ctrl'} K</kbd>
      </button>
    </div>

    <nav class="grid gap-0.5 px-2 py-2.5">
      {#each nav as item (item.section)}
        {@const active = activeSection === item.section}
        <a
          href={href(route.cluster, item.section)}
          aria-current={active ? 'page' : undefined}
          class="flex items-center gap-2.5 rounded-[1px] px-2 py-1.5 hover:no-underline {active
            ? 'corners hatched font-semibold text-ink'
            : 'text-ink-2 hover:bg-surface-2 hover:text-ink'}"
        >
          <item.icon size={16} strokeWidth={1.6} class={active ? 'text-ink' : 'text-ink-3'} />
          {item.label}
        </a>
      {/each}
    </nav>

    <div class="mt-auto border-t border-line-strong">
      <div class="p-2">
        <button class="ghost w-full justify-start px-1.5 text-xs text-ink-2" onclick={nextTheme} title="Switch theme">
          <ThemeIcon size={14} />
          Theme: {themes[theme]}
        </button>
      </div>
    </div>
  </aside>

  <main class="relative min-w-0">
    <Backdrop />
    <header class="sticky top-0 z-30 flex h-12 items-center gap-1.5 border-b border-line-strong bg-sidebar px-2 lg:hidden">
      <button
        class="ghost size-10 justify-center px-0"
        aria-label="Open menu"
        aria-controls="sidebar"
        aria-expanded={menuOpen}
        onclick={() => (menuOpen = true)}
      >
        <Menu size={18} />
      </button>
      <a
        href={route.cluster ? href(route.cluster, 'overview') : '#/'}
        class="flex min-w-0 items-center gap-2 text-ink hover:no-underline"
        title="Overview"
      >
        <Logo size={20} />
        {#if current}
          <span class="sq {current.production ? 'danger' : 'ok'}"></span>
          <span class="truncate text-sm font-semibold">{current.name}</span>
        {:else}
          <span class="text-sm font-semibold" title={app.version ? `kafka-console ${app.version}` : undefined}>kafka-console</span>
        {/if}
      </a>
      {#if app.info}
        <span class="truncate bg-[#fbff81] px-1.5 py-0.5 text-[11px] leading-tight font-semibold text-[#222220]" title={app.info}>{app.info}</span>
      {/if}
      <span class="flex-1"></span>
      <button class="ghost size-10 justify-center px-0" aria-label="Go to…" onclick={() => palette?.show()} disabled={!current}>
        <Search size={18} />
      </button>
    </header>
    {#if current?.production}
      <div class="relative z-[1] flex min-h-[34px] items-center gap-2 border-b border-danger-line bg-danger-soft px-4 py-1.5 text-[12.5px] text-danger sm:px-6 lg:px-8">
        <TriangleAlert size={14} />
        <span>Production cluster <b>{current.name}</b>. Writes affect real data.</span>
      </div>
    {/if}

    <div class="relative z-[1] max-w-[1440px] px-4 pt-5 pb-14 sm:px-6 lg:px-10 lg:pt-7">
      {#if error}
        <div class="error">{error}</div>
      {:else if !current}
        <div class="empty">{clusters.length ? 'Cluster not found.' : 'Loading…'}</div>
      {:else}
        {#key `${route.cluster}/${route.section}/${route.name}`}
          {#if route.section === 'topics' && route.name}
            <Topic cluster={current} name={route.name} />
          {:else if route.section === 'topics'}
            <Topics cluster={current} />
          {:else if route.section === 'groups' && route.name}
            <Group cluster={current} name={route.name} />
          {:else if route.section === 'groups'}
            <Groups cluster={current} />
          {:else if route.section === 'brokers' && route.name}
            <Broker cluster={current} id={Number(route.name)} />
          {:else if route.section === 'schemas' && route.name}
            <Subject cluster={current} name={route.name} />
          {:else if route.section === 'schemas'}
            <Schemas cluster={current} />
          {:else if route.section === 'security' && current.features.security}
            <Security cluster={current} />
          {:else}
            <Overview cluster={current} />
          {/if}
        {/key}
      {/if}
    </div>
  </main>
</div>

{#if current}<CommandPalette bind:this={palette} cluster={current} {clusters} security={current.features.security} />{/if}

<style>
  .goto { font-weight: 400; background: var(--surface); }
  .goto:hover:not(:disabled) { color: var(--text); }
  .goto kbd { font: 11px/1.5 var(--mono); padding: 0 5px; color: var(--text-2); background: var(--surface-2); border: 1px solid var(--border-strong); border-radius: 2px; }
  .prod-edge { box-shadow: inset 0 3px 0 var(--danger); }
  .scrim { position: fixed; inset: 0; z-index: 35; padding: 0; border: 0; border-radius: 0; background: rgb(0 0 0 / .35); cursor: default; }
  .scrim:hover { background: rgb(0 0 0 / .35) !important; }
  :global(html.sheet-open) header { display: none; }
  nav a.corners { --c: var(--text-2); --k: 6px; }
</style>
