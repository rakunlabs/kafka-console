<script lang="ts">
  import { api, type Cluster } from './lib/api'
  import { router, href, go } from './lib/router.svelte'
  import Overview from './routes/Overview.svelte'
  import Topics from './routes/Topics.svelte'
  import Topic from './routes/Topic.svelte'
  import Groups from './routes/Groups.svelte'
  import Group from './routes/Group.svelte'

  let clusters = $state<Cluster[]>([])
  let error = $state('')

  api
    .clusters()
    .then((c) => {
      clusters = c
      if (!router.route.cluster && c.length) go(c[0].name)
    })
    .catch((e) => (error = e.message))

  const route = $derived(router.route)
  const current = $derived(clusters.find((c) => c.name === route.cluster))

  const nav = [
    { section: 'overview', label: 'Overview' },
    { section: 'topics', label: 'Topics' },
    { section: 'groups', label: 'Consumer groups' },
  ]
</script>

<div class="app">
  <aside>
    <div class="brand">kafka-console</div>

    <label class="cluster">
      Cluster
      <select value={route.cluster} onchange={(e) => go(e.currentTarget.value, route.section === 'overview' ? 'overview' : route.section)}>
        {#each clusters as c (c.name)}
          <option value={c.name}>{c.name}</option>
        {/each}
      </select>
    </label>

    {#if current}
      <div class="tags">
        {#if current.production}<span class="badge danger">production</span>{/if}
        {#if current.read_only}<span class="badge">read-only</span>{/if}
      </div>
    {/if}

    <nav>
      {#each nav as item (item.section)}
        <a href={href(route.cluster, item.section)} class:active={route.section === item.section}>{item.label}</a>
      {/each}
    </nav>

    {#if current}
      <div class="brokers faint mono">
        {#each current.brokers as b (b)}<div>{b}</div>{/each}
      </div>
    {/if}
  </aside>

  <main>
    {#if current?.production}
      <div class="prod">Production cluster: <b>{current.name}</b>. Yazma işlemleri gerçek veriyi etkiler.</div>
    {/if}

    <div class="content">
      {#if error}
        <div class="error">{error}</div>
      {:else if !current}
        <div class="empty">{clusters.length ? 'Cluster bulunamadı.' : 'Yükleniyor…'}</div>
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
          {:else}
            <Overview cluster={current} />
          {/if}
        {/key}
      {/if}
    </div>
  </main>
</div>

<style>
  .app {
    display: grid;
    grid-template-columns: 220px 1fr;
    min-height: 100vh;
  }
  aside {
    background: var(--surface);
    border-right: 1px solid var(--border);
    padding: 16px 12px;
    display: flex;
    flex-direction: column;
    gap: 12px;
    position: sticky;
    top: 0;
    height: 100vh;
  }
  .brand { font-weight: 700; padding: 0 4px 4px; }
  .cluster select { width: 100%; }
  .tags { display: flex; gap: 6px; }
  nav { display: grid; gap: 2px; margin-top: 4px; }
  nav a {
    color: var(--text-2);
    padding: 6px 8px;
    border-radius: var(--radius);
    font-weight: 500;
  }
  nav a:hover { background: var(--surface-2); text-decoration: none; }
  nav a.active { background: var(--accent-soft); color: var(--accent); }
  .brokers { margin-top: auto; font-size: 11px; padding: 0 4px; overflow-wrap: anywhere; }
  main { min-width: 0; }
  .prod {
    background: var(--danger-soft);
    color: var(--danger);
    border-bottom: 1px solid #eab8b6;
    padding: 6px 24px;
    font-size: 12px;
  }
  .content { padding: 20px 24px 40px; max-width: 1400px; }

  @media (max-width: 760px) {
    .app { grid-template-columns: 1fr; }
    aside { position: static; height: auto; }
    .brokers { display: none; }
    .content { padding: 16px; }
  }
</style>
