<script lang="ts">
  import { api, type Cluster, type SubjectDetail } from '../lib/api'
  import { href, go, router } from '../lib/router.svelte'
  import Modal from '../lib/Modal.svelte'
  import SchemaEditor from '../lib/SchemaEditor.svelte'
  import { Plus, Trash2 } from '@lucide/svelte'

  let { cluster, name }: { cluster: Cluster; name: string } = $props()

  let data = $state<SubjectDetail>()
  let error = $state('')

  async function load() {
    error = ''
    try {
      data = await api.subject(cluster.name, name)
    } catch (e: any) {
      error = e.message
    }
  }
  load()

  const selectedVersion = $derived(Number(router.route.query.get('v') ?? 0))
  const current = $derived(data?.versions.find((v) => v.version === selectedVersion) ?? data?.versions[0])
  let compareTo = $state(0)
  const other = $derived(compareTo ? data?.versions.find((v) => v.version === compareTo) : undefined)

  function pretty(s: string, type: string) {
    if (type === 'PROTOBUF') return s
    try {
      return JSON.stringify(JSON.parse(s), null, 2)
    } catch {
      return s
    }
  }

  // Line diff by longest common subsequence; schemas are small.
  function diff(a: string, b: string) {
    const x = a.split('\n')
    const y = b.split('\n')
    const n = x.length
    const m = y.length
    const dp = Array.from({ length: n + 1 }, () => new Array<number>(m + 1).fill(0))
    for (let i = n - 1; i >= 0; i--) for (let j = m - 1; j >= 0; j--) dp[i][j] = x[i] === y[j] ? dp[i + 1][j + 1] + 1 : Math.max(dp[i + 1][j], dp[i][j + 1])
    const out: { kind: ' ' | '+' | '-'; text: string }[] = []
    let i = 0
    let j = 0
    while (i < n && j < m) {
      if (x[i] === y[j]) {
        out.push({ kind: ' ', text: x[i] })
        i++
        j++
      } else if (dp[i + 1][j] >= dp[i][j + 1]) out.push({ kind: '-', text: x[i++] })
      else out.push({ kind: '+', text: y[j++] })
    }
    while (i < n) out.push({ kind: '-', text: x[i++] })
    while (j < m) out.push({ kind: '+', text: y[j++] })
    return out
  }

  const lines = $derived(current && other ? diff(pretty(other.schema, other.type), pretty(current.schema, current.type)) : [])

  const levels = ['DEFAULT', 'NONE', 'BACKWARD', 'BACKWARD_TRANSITIVE', 'FORWARD', 'FORWARD_TRANSITIVE', 'FULL', 'FULL_TRANSITIVE']
  let busy = $state(false)
  let actionError = $state('')

  async function setCompat(level: string) {
    busy = true
    actionError = ''
    try {
      await api.setCompatibility(cluster.name, name, level)
      await load()
    } catch (e: any) {
      actionError = e.message
    } finally {
      busy = false
    }
  }

  let adding = $state(false)

  // Delete subject or version
  let deleting = $state(false)
  let deleteTarget = $state<{ version?: number }>({})
  let permanent = $state(false)
  let confirm = $state('')

  function askDelete(version?: number) {
    deleteTarget = { version }
    permanent = false
    confirm = ''
    actionError = ''
    deleting = true
  }

  const alreadySoft = $derived(
    deleteTarget.version
      ? !!data?.versions.find((v) => v.version === deleteTarget.version)?.deleted
      : !!data && data.versions.every((v) => v.deleted),
  )

  async function remove() {
    busy = true
    actionError = ''
    try {
      if (deleteTarget.version) await api.deleteSchemaVersion(cluster.name, name, deleteTarget.version, permanent || alreadySoft)
      else await api.deleteSubject(cluster.name, name, permanent || alreadySoft)
      deleting = false
      if (!deleteTarget.version && (permanent || alreadySoft)) go(cluster.name, 'schemas')
      else load()
    } catch (e: any) {
      actionError = e.message
    } finally {
      busy = false
    }
  }

  function copy(text: string) {
    navigator.clipboard?.writeText(text)
  }

  // A subject named <topic>-value or <topic>-key likely belongs to that topic.
  const topic = $derived(name.match(/^(.+)-(value|key)$/)?.[1])
</script>

<div class="head">
  <h1 class="crumbed"><a class="parent" href={href(cluster.name, 'schemas')}>Schema Registry</a><span class="sep" aria-hidden="true">/</span><span class="mono">{name}</span></h1>
  {#if current}<span class="badge">{current.type}</span>{/if}
  <span class="spacer"></span>
  <button class="primary" disabled={cluster.read_only || !current} onclick={() => (adding = true)}><Plus size={14} />New version</button>
  <button class="danger" disabled={cluster.read_only || !data} onclick={() => askDelete()}><Trash2 size={14} />Delete subject</button>
</div>

{#if error && !data}
  <div class="error">{error}</div>
{:else if !data || !current}
  <div class="empty">Loading…</div>
{:else}
  {#if error}<div class="error mb-3">{error}</div>{/if}
  <div class="summary">
    <span>Compatibility
      <select class="compat" value={data.override ? data.compatibility : 'DEFAULT'} disabled={cluster.read_only || busy}
        onchange={(e) => setCompat(e.currentTarget.value)}>
        {#each levels as l (l)}<option value={l}>{l === 'DEFAULT' ? `Global (${data.override ? 'default' : data.compatibility})` : l}</option>{/each}
      </select>
    </span>
    <span>Mode <b>{data.mode || '—'}</b></span>
    {#if topic}<span>Topic <a class="mono" href={href(cluster.name, 'topics', topic)}>{topic}</a></span>{/if}
    {#if data.referred_by.length}<span>Referenced by: <b class="mono">{data.referred_by.join(', ')}</b></span>{/if}
    {#if data.topics?.length}
      <span title="Topics this subject is named after (topic-key, topic-value or topic-Record); producers may still use it elsewhere">Topic
        {#each data.topics as t, i (t.topic)}{#if i}, {/if}<a class="mono" href={href(cluster.name, 'topics', t.topic)}>{t.topic}</a> <span class="faint">({t.role})</span>{/each}
      </span>
    {/if}
  </div>
  {#if actionError}<div class="error">{actionError}</div>{/if}

  <div class="layout">
    <div class="panel versions">
      <table>
        <thead><tr><th>Version</th><th class="num">ID</th></tr></thead>
        <tbody>
          {#each data.versions as v (v.version)}
            <tr class:sel={v.version === current.version} class:deleted={v.deleted}>
              <td>
                <a href={href(cluster.name, 'schemas', name, { v: String(v.version) })}>v{v.version}</a>
                {#if v.deleted}<span class="badge danger">deleted</span>{/if}
                {#if v === data.versions[0] && !v.deleted}<span class="badge ok">latest</span>{/if}
              </td>
              <td class="num mono faint">{v.id}</td>
            </tr>
          {/each}
        </tbody>
      </table>
    </div>

    <div class="panel detail">
      <div class="dhead">
        <b>v{current.version}</b>
        <span class="faint">ID {current.id}</span>
        <span class="spacer"></span>
        <label class="cmp">Compare with
          <select bind:value={compareTo}>
            <option value={0}>—</option>
            {#each data.versions.filter((v) => v.version !== current.version) as v (v.version)}<option value={v.version}>v{v.version}</option>{/each}
          </select>
        </label>
        <button class="ghost small" onclick={() => copy(pretty(current.schema, current.type))}>Copy</button>
        <button class="ghost small danger" disabled={cluster.read_only} onclick={() => askDelete(current.version)}>Delete version</button>
      </div>

      {#if current.references.length}
        <div class="refs">
          <span class="faint">References:</span>
          {#each current.references as r (r.name)}
            <a class="mono" href={href(cluster.name, 'schemas', r.subject, { v: String(r.version) })}>{r.name} → {r.subject} v{r.version}</a>
          {/each}
        </div>
      {/if}
      {#if current.messages?.length}
        <div class="refs"><span class="faint">Messages:</span> {#each current.messages as m (m)}<span class="mono">{m}</span>{/each}</div>
      {/if}

      {#if other}
        <pre class="code diff">{#each lines as l, i (i)}<span class="line {l.kind === '+' ? 'add' : l.kind === '-' ? 'del' : ''}">{l.kind} {l.text}
</span>{/each}</pre>
      {:else}
        <pre class="code">{pretty(current.schema, current.type)}</pre>
      {/if}
    </div>
  </div>
{/if}

{#if current}
  <SchemaEditor {cluster} bind:open={adding} types={[current.type]} subject={name}
    initial={{ type: current.type, schema: current.schema, references: current.references }}
    onsaved={() => { load(); go(cluster.name, 'schemas', name) }} />
{/if}

<Modal title={deleteTarget.version ? `Delete version v${deleteTarget.version}` : 'Delete subject'} bind:open={deleting}>
  {#if alreadySoft}
    <p>This {deleteTarget.version ? 'version' : 'subject'} is already soft-deleted. Continuing will delete it <b>permanently</b>, and messages that use its schema ID can no longer be decoded.</p>
  {:else}
    <p>
      {#if deleteTarget.version}Version <b>v{deleteTarget.version}</b> of subject <b class="mono">{name}</b> will be deleted.
      {:else}All versions of subject <b class="mono">{name}</b> will be deleted.{/if}
      Soft delete can be undone; schema IDs remain readable.
    </p>
    <label class="check"><input type="checkbox" bind:checked={permanent} /> Delete permanently (hard delete)</label>
  {/if}
  {#if permanent || alreadySoft}
    <label>Type the subject name to confirm <input class="mono" bind:value={confirm} /></label>
  {/if}
  {#if actionError}<div class="error">{actionError}</div>{/if}
  {#snippet footer()}
    <button onclick={() => (deleting = false)}>Cancel</button>
    <button class="danger solid" disabled={busy || ((permanent || alreadySoft) && confirm !== name)} onclick={remove}>
      {permanent || alreadySoft ? 'Delete permanently' : 'Delete'}
    </button>
  {/snippet}
</Modal>

<style>
  .compat { padding: 2px 6px; margin-left: 4px; }
  .error { margin-bottom: 12px; }
  .layout { display: grid; grid-template-columns: 200px minmax(0, 1fr); gap: 12px; align-items: start; }
  .versions { max-height: calc(100vh - 220px); overflow: auto; }
  .versions tr.sel td { background: var(--sel); }
  .versions tr.deleted a { color: var(--text-3); }
  .versions .badge { margin-left: 4px; }
  .dhead { display: flex; flex-wrap: wrap; align-items: center; gap: 6px 10px; padding: 8px 12px; border-bottom: 1px solid var(--border); white-space: nowrap; }
  .cmp { display: flex; flex-direction: row; align-items: center; gap: 6px; font-weight: 400; }
  .cmp select { padding: 2px 6px; min-width: 52px; }
  .refs { display: flex; flex-wrap: wrap; gap: 4px 12px; padding: 8px 12px; border-bottom: 1px solid var(--border); font-size: 12px; }
  .code { margin: 0; padding: 12px; font: 12.5px/1.5 var(--mono); white-space: pre-wrap; overflow-wrap: anywhere; max-height: calc(100vh - 280px); overflow: auto; }
  .diff .line { display: block; }
  .diff .add { background: var(--ok-soft); color: var(--ok); }
  .diff .del { background: var(--danger-soft); color: var(--danger); }
  @media (max-width: 900px) { .layout { grid-template-columns: minmax(0, 1fr); } }
</style>
