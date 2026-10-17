<script lang="ts">
  import { api, type Cluster } from './lib/api'
  import { router, href, go } from './lib/router.svelte'
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
    Sun,
    Moon,
    Monitor,
    Search,
  } from '@lucide/svelte'

  let clusters = $state<Cluster[]>([])
  let error = $state('')
  let info = $state('')
  api.info().then((r) => (info = r.info)).catch(() => {})

  api
    .clusters()
    .then((c) => {
      clusters = c
      if (!router.route.cluster && c.length) go(c[0].name)
    })
    .catch((e) => (error = e.message))

  const route = $derived(router.route)
  const current = $derived(clusters.find((c) => c.name === route.cluster))

  const nav = $derived([
    { section: 'overview', label: 'Overview', icon: LayoutDashboard },
    { section: 'topics', label: 'Topics', icon: Rows3 },
    { section: 'groups', label: 'Consumer groups', icon: Users },
    ...(current?.schema_registry ? [{ section: 'schemas', label: 'Schema Registry', icon: Braces }] : []),
    { section: 'security', label: 'Security', icon: ShieldCheck },
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
</script>

<div class="grid min-h-screen grid-cols-[240px_minmax(0,1fr)] max-md:grid-cols-[minmax(0,1fr)]">
  <aside
    class="sticky top-0 flex h-screen flex-col overflow-y-auto border-r border-line-strong bg-sidebar max-md:static max-md:h-auto max-md:border-r-0 max-md:border-b"
    class:prod-edge={current?.production}
  >
    <div class="flex h-14 items-center gap-2 border-b border-line-strong px-3.5">
      <a
        href={route.cluster ? href(route.cluster, 'overview') : '#/'}
        class="flex min-w-0 items-center gap-2.5 text-[15px] font-semibold tracking-tight text-ink hover:no-underline"
        title="Overview"
      >
        <Logo size={22} />
        kafka-console
      </a>
      {#if info}
        <span class="ml-auto truncate bg-[#fbff81] px-1.5 py-0.5 text-[11.5px] leading-tight font-semibold text-[#222220]" title={info}>{info}</span>
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

    <nav class="grid gap-0.5 px-2 py-2.5 max-md:grid-cols-2">
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

    <div class="mt-auto border-t border-line-strong max-md:hidden">
      {#if current}
        <div class="grid gap-0.5 border-b border-line-strong px-3.5 py-3">
          <span class="text-xs font-medium text-ink-3">Bootstrap servers</span>
          {#each current.brokers as b (b)}<div class="font-mono text-[11px] break-all text-ink-3">{b}</div>{/each}
        </div>
      {/if}
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
    {#if current?.production}
      <div class="relative z-[1] flex h-[34px] items-center gap-2 border-b border-danger-line bg-danger-soft px-8 text-[12.5px] text-danger max-md:px-4">
        <TriangleAlert size={14} />
        <span>Production cluster <b>{current.name}</b>. Writes affect real data.</span>
      </div>
    {/if}

    <div class="relative z-[1] max-w-[1440px] px-10 pt-7 pb-14 max-md:px-4 max-md:pt-5">
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
          {:else if route.section === 'security'}
            <Security cluster={current} />
          {:else}
            <Overview cluster={current} />
          {/if}
        {/key}
      {/if}
    </div>
  </main>
</div>

{#if current}<CommandPalette bind:this={palette} cluster={current} {clusters} />{/if}

<style>
  .goto { font-weight: 400; background: var(--surface); }
  .goto:hover:not(:disabled) { color: var(--text); }
  .goto kbd { font: 11px/1.5 var(--mono); padding: 0 5px; color: var(--text-2); background: var(--surface-2); border: 1px solid var(--border-strong); border-radius: 2px; }
  .prod-edge { box-shadow: inset 0 3px 0 var(--danger); }
  nav a.corners { --c: var(--text-2); --k: 6px; }
</style>
