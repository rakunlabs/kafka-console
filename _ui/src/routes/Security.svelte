<script lang="ts">
  import { resizable } from '../lib/resizable'
  import Pending from '../lib/Pending.svelte'
  import { api, isAbort, type ACL, type Cluster, type Quota, type User } from '../lib/api'
  import { fmt } from '../lib/format'
  import { href, router } from '../lib/router.svelte'
  import Modal from '../lib/Modal.svelte'
  import MoreRows from '../lib/MoreRows.svelte'
  import { quotaKeys, quotaDraft, quotaChanges } from '../lib/quota'
  import { RefreshCw, Plus } from '@lucide/svelte'

  let { cluster }: { cluster: Cluster } = $props()

  const tabs = [
    { id: 'acls', label: 'ACL' },
    { id: 'users', label: 'Users (SCRAM)' },
    { id: 'quotas', label: 'Quota' },
  ]
  const tab = $derived(router.route.query.get('tab') ?? 'acls')

  // ─── ACLs ───
  // Raw: a cluster can hold tens of thousands of ACLs; they are replaced on
  // load, never edited in place.
  let acls = $state.raw<ACL[]>([])
  let aclError = $state('')
  let aclLoading = $state(false)
  let aclSearch = $state('')
  // The filter follows the search box after a pause, not on every key.
  let aclQuery = $state('')
  $effect(() => {
    const q = aclSearch
    const t = setTimeout(() => (aclQuery = q), q ? 150 : 0)
    return () => clearTimeout(t)
  })

  // Rows render a page at a time; a new search starts from the top.
  const page = 200
  let aclShown = $state(page)
  let userShown = $state(page)
  let quotaShown = $state(page)
  $effect(() => {
    void aclQuery
    aclShown = page
  })

  async function loadACLs() {
    aclLoading = true
    aclError = ''
    try {
      acls = await api.acls(cluster.name)
    } catch (e: any) {
      if (!isAbort(e)) aclError = e.message
    } finally {
      aclLoading = false
    }
  }

  const aclRows = $derived.by(() => {
    const q = aclQuery.trim().toLowerCase()
    if (!q) return acls
    return acls.filter((a) => [a.principal, a.resource_name, a.resource_type, a.operation].some((v) => v.toLowerCase().includes(q)))
  })

  const resourceTypes = ['TOPIC', 'GROUP', 'CLUSTER', 'TRANSACTIONAL_ID']
  const operations = ['ALL', 'READ', 'WRITE', 'CREATE', 'DELETE', 'ALTER', 'DESCRIBE', 'CLUSTER_ACTION', 'DESCRIBE_CONFIGS', 'ALTER_CONFIGS', 'IDEMPOTENT_WRITE']

  let aclOpen = $state(false)
  let aclForm = $state({ principal: '', host: '*', resource_type: 'TOPIC', resource_name: '', pattern_type: 'LITERAL', permission: 'ALLOW', operations: ['READ'] as string[] })
  let aclFormError = $state('')
  let busy = $state(false)

  // Common bundles so the operator does not have to know every operation a
  // client needs.
  const presets: { label: string; type: string; ops: string[] }[] = [
    { label: 'Consumer', type: 'TOPIC', ops: ['READ', 'DESCRIBE'] },
    { label: 'Producer', type: 'TOPIC', ops: ['WRITE', 'DESCRIBE'] },
    { label: 'Group', type: 'GROUP', ops: ['READ'] },
  ]

  function openACL() {
    aclForm = { principal: '', host: '*', resource_type: 'TOPIC', resource_name: '', pattern_type: 'LITERAL', permission: 'ALLOW', operations: ['READ'] }
    aclFormError = ''
    aclOpen = true
  }

  function toggleOp(op: string) {
    aclForm.operations = aclForm.operations.includes(op) ? aclForm.operations.filter((o) => o !== op) : [...aclForm.operations, op]
  }

  async function createACL() {
    busy = true
    aclFormError = ''
    try {
      const { operations: ops, ...base } = aclForm
      await api.createACLs(cluster.name, ops.map((operation) => ({ ...base, operation })))
      aclOpen = false
      loadACLs()
    } catch (e: any) {
      aclFormError = e.message
    } finally {
      busy = false
    }
  }

  let aclDelete = $state<ACL>()
  let aclDeleting = $state(false)

  async function removeACL() {
    if (!aclDelete) return
    busy = true
    aclError = ''
    try {
      await api.deleteACL(cluster.name, aclDelete)
      aclDeleting = false
      loadACLs()
    } catch (e: any) {
      aclError = e.message
      aclDeleting = false
    } finally {
      busy = false
    }
  }

  // ─── Users ───
  let users = $state.raw<User[]>([])
  let userError = $state('')
  let userLoading = $state(false)

  async function loadUsers() {
    userLoading = true
    userError = ''
    try {
      users = await api.users(cluster.name)
    } catch (e: any) {
      if (!isAbort(e)) userError = e.message
    } finally {
      userLoading = false
    }
  }

  let userOpen = $state(false)
  let userForm = $state({ name: '', password: '', mechanism: 'SCRAM-SHA-256' })
  let userFormError = $state('')
  let userExisting = $state(false)

  function openUser(u?: User) {
    userForm = { name: u?.name ?? '', password: '', mechanism: u?.mechanisms[0] ?? 'SCRAM-SHA-256' }
    userExisting = !!u
    userFormError = ''
    userOpen = true
  }

  function generate() {
    const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz23456789'
    const buf = crypto.getRandomValues(new Uint32Array(24))
    userForm.password = Array.from(buf, (n) => chars[n % chars.length]).join('')
  }

  async function saveUser() {
    busy = true
    userFormError = ''
    try {
      await api.upsertUser(cluster.name, userForm)
      userOpen = false
      loadUsers()
    } catch (e: any) {
      userFormError = e.message
    } finally {
      busy = false
    }
  }

  let userDelete = $state('')
  let userDeleting = $state(false)

  async function removeUser() {
    busy = true
    userError = ''
    try {
      await api.deleteUser(cluster.name, userDelete)
      userDeleting = false
      loadUsers()
    } catch (e: any) {
      userError = e.message
      userDeleting = false
    } finally {
      busy = false
    }
  }

  // ─── Quotas ───
  let quotas = $state.raw<Quota[]>([])
  let quotaError = $state('')
  let quotaLoading = $state(false)

  async function loadQuotas() {
    quotaLoading = true
    quotaError = ''
    try {
      quotas = await api.quotas(cluster.name)
    } catch (e: any) {
      if (!isAbort(e)) quotaError = e.message
    } finally {
      quotaLoading = false
    }
  }

  let quotaOpen = $state(false)
  let quotaForm = $state(quotaDraft())
  let quotaFormError = $state('')

  function openQuota(q?: Quota) {
    quotaForm = quotaDraft(q)
    quotaFormError = ''
    quotaOpen = true
  }

  async function saveQuota() {
    busy = true
    quotaFormError = ''
    try {
      await api.alterQuota(cluster.name, quotaChanges(quotaForm))
      quotaOpen = false
      loadQuotas()
    } catch (e: any) {
      quotaFormError = e.message
    } finally {
      busy = false
    }
  }

  let quotaDelete = $state<Quota>()
  let quotaDeleting = $state(false)

  async function removeQuota(q: Quota) {
    busy = true
    quotaError = ''
    try {
      await api.alterQuota(cluster.name, { entity: q.entity, remove: Object.keys(q.values) })
      quotaDeleting = false
      loadQuotas()
    } catch (e: any) {
      quotaError = e.message
    } finally {
      busy = false
    }
  }

  const entityLabel = (q: Quota) => q.entity.map((e) => `${e.type}=${e.name ?? '<default>'}`).join(', ')

  $effect(() => {
    if (tab === 'users') loadUsers()
    else if (tab === 'quotas') loadQuotas()
    else loadACLs()
  })
</script>

<div class="head">
  <h1>Security</h1>
</div>

<div class="tabs" role="tablist">
  {#each tabs as t (t.id)}
    <a role="tab" aria-selected={tab === t.id} class:active={tab === t.id} href={href(cluster.name, 'security', '', { tab: t.id })}>{t.label}</a>
  {/each}
</div>

{#if tab === 'acls'}
  <div class="filters">
    <input type="search" placeholder="Search principal, resource or operation…" bind:value={aclSearch} />
    <span class="faint">{aclRows.length}</span>
    <span class="spacer"></span>
    <button onclick={loadACLs} disabled={aclLoading}><RefreshCw size={14} class={aclLoading ? 'animate-spin' : ''} />Refresh</button>
    {#if !cluster.read_only}<button class="primary" onclick={openACL}><Plus size={14} />Add ACL</button>{/if}
  </div>
  {#if aclError}
    <div class="error">{aclError}</div>
  {:else}
    <div class="panel">
      <table use:resizable={'security.acls'}>
        <thead><tr><th>Principal</th><th>Host</th><th>Resource</th><th>Pattern</th><th>Operation</th><th>Permission</th><th></th></tr></thead>
        <tbody>
          {#each aclRows.slice(0, aclShown) as a, i (i)}
            <tr>
              <td class="mono">{a.principal}</td>
              <td class="mono faint">{a.host}</td>
              <td class="grow">
                <span class="cell">
                  <span class="faint small">{a.resource_type}</span>
                  {#if a.resource_type === 'TOPIC' && a.pattern_type === 'LITERAL' && a.resource_name !== '*'}
                    <a class="mono" href={href(cluster.name, 'topics', a.resource_name)} title={a.resource_name}>{a.resource_name}</a>
                  {:else}
                    <span class="mono" title={a.resource_name}>{a.resource_name}</span>
                  {/if}
                </span>
              </td>
              <td class="faint small">{a.pattern_type}</td>
              <td class="mono small">{a.operation}</td>
              <td><span class="badge {a.permission === 'ALLOW' ? 'ok' : 'danger'}">{a.permission}</span></td>
              <td class="actions">
                {#if !cluster.read_only}<button class="ghost small danger" onclick={() => { aclDelete = a; aclDeleting = true }}>Delete</button>{/if}
              </td>
            </tr>
          {:else}
            <tr><td colspan="7" class="empty">{#if aclLoading}<Pending />{:else}No ACLs defined.{/if}</td></tr>
          {/each}
        </tbody>
      </table>
      <MoreRows total={aclRows.length} bind:shown={aclShown} step={page} />
    </div>
  {/if}
{:else if tab === 'users'}
  <div class="filters">
    <span class="faint">{users.length} users</span>
    <span class="spacer"></span>
    <button onclick={loadUsers} disabled={userLoading}><RefreshCw size={14} class={userLoading ? 'animate-spin' : ''} />Refresh</button>
    {#if !cluster.read_only}<button class="primary" onclick={() => openUser()}><Plus size={14} />Create user</button>{/if}
  </div>
  {#if userError}
    <div class="error">{userError}</div>
  {:else}
    <div class="panel">
      <table use:resizable={'security.users'}>
        <thead><tr><th>User</th><th>Mechanism</th><th></th></tr></thead>
        <tbody>
          {#each users.slice(0, userShown) as u (u.name)}
            <tr>
              <td class="mono grow" title={u.name}>{u.name}</td>
              <td>{#each u.mechanisms as m (m)}<span class="badge">{m}</span> {/each}</td>
              <td class="actions">
                {#if !cluster.read_only}
                  <button class="ghost small" onclick={() => openUser(u)}>Change password</button>
                  <button class="ghost small danger" onclick={() => { userDelete = u.name; userDeleting = true }}>Delete</button>
                {/if}
              </td>
            </tr>
          {:else}
            <tr><td colspan="3" class="empty">{#if userLoading}<Pending />{:else}No SCRAM users.{/if}</td></tr>
          {/each}
        </tbody>
      </table>
      <MoreRows total={users.length} bind:shown={userShown} step={page} />
    </div>
  {/if}
{:else}
  <div class="filters">
    <span class="faint">{quotas.length} entries</span>
    <span class="spacer"></span>
    <button onclick={loadQuotas} disabled={quotaLoading}><RefreshCw size={14} class={quotaLoading ? 'animate-spin' : ''} />Refresh</button>
    {#if !cluster.read_only}<button class="primary" onclick={() => openQuota()}><Plus size={14} />Add quota</button>{/if}
  </div>
  {#if quotaError}
    <div class="error">{quotaError}</div>
  {:else}
    <div class="panel">
      <table use:resizable={'security.quotas'}>
        <thead><tr><th>Entity</th>{#each quotaKeys as k (k.key)}<th class="num">{k.label}</th>{/each}<th></th></tr></thead>
        <tbody>
          {#each quotas.slice(0, quotaShown) as q, i (i)}
            <tr>
              <td class="mono grow" title={entityLabel(q)}>{entityLabel(q)}</td>
              {#each quotaKeys as k (k.key)}<td class="num mono">{q.values[k.key] !== undefined ? fmt(q.values[k.key]) : '—'}</td>{/each}
              <td class="actions">
                {#if !cluster.read_only}
                  <button class="ghost small" onclick={() => openQuota(q)}>Edit</button>
                  <button class="ghost small danger" disabled={busy} onclick={() => { quotaDelete = q; quotaDeleting = true }}>Delete</button>
                {/if}
              </td>
            </tr>
          {:else}
            <tr><td colspan={quotaKeys.length + 2} class="empty">{#if quotaLoading}<Pending />{:else}No quotas defined.{/if}</td></tr>
          {/each}
        </tbody>
      </table>
      <MoreRows total={quotas.length} bind:shown={quotaShown} step={page} />
    </div>
  {/if}
{/if}

<Modal title="Add ACL" bind:open={aclOpen} width={560}>
  <div class="row presets">
    <span class="faint small">Presets:</span>
    {#each presets as p (p.label)}
      <button type="button" class="ghost small chip" onclick={() => { aclForm.resource_type = p.type; aclForm.operations = [...p.ops] }}>{p.label}</button>
    {/each}
  </div>
  <div class="grid2">
    <label>Principal <input class="mono" bind:value={aclForm.principal} placeholder="User:alice" /></label>
    <label>Host <input class="mono" bind:value={aclForm.host} /></label>
  </div>
  <div class="grid3">
    <label>Resource type
      <select bind:value={aclForm.resource_type}>{#each resourceTypes as t (t)}<option>{t}</option>{/each}</select>
    </label>
    <label>Resource name
      <input class="mono" bind:value={aclForm.resource_name} disabled={aclForm.resource_type === 'CLUSTER'}
        placeholder={aclForm.resource_type === 'CLUSTER' ? 'kafka-cluster' : '* or name'} />
    </label>
    <label>Pattern
      <select bind:value={aclForm.pattern_type}><option>LITERAL</option><option>PREFIXED</option></select>
    </label>
  </div>
  <label>Permission
    <select bind:value={aclForm.permission}><option>ALLOW</option><option>DENY</option></select>
  </label>
  <div>
    <div class="lbl">Operations</div>
    <div class="ops">
      {#each operations as op (op)}
        <label class="check"><input type="checkbox" checked={aclForm.operations.includes(op)} onchange={() => toggleOp(op)} /> <span class="mono small">{op}</span></label>
      {/each}
    </div>
  </div>
  {#if aclFormError}<div class="error">{aclFormError}</div>{/if}
  {#snippet footer()}
    <button onclick={() => (aclOpen = false)}>Cancel</button>
    <button class="primary" disabled={busy || !aclForm.principal.trim() || !aclForm.operations.length || (aclForm.resource_type !== 'CLUSTER' && !aclForm.resource_name.trim())}
      onclick={createACL}>{aclForm.operations.length > 1 ? `Create ${aclForm.operations.length} ACLs` : 'Create'}</button>
  {/snippet}
</Modal>

<Modal title="Delete ACL" bind:open={aclDeleting}>
  {#if aclDelete}
    <p>The <b>{aclDelete.permission} {aclDelete.operation}</b> rule for <b class="mono">{aclDelete.principal}</b> on <b class="mono">{aclDelete.resource_type}:{aclDelete.resource_name}</b> will be deleted. Clients relying on this rule may lose access.</p>
  {/if}
  {#snippet footer()}
    <button onclick={() => (aclDeleting = false)}>Cancel</button>
    <button class="danger solid" disabled={busy} onclick={removeACL}>Delete</button>
  {/snippet}
</Modal>

<Modal title={userExisting ? `Change password: ${userForm.name}` : 'Create user'} bind:open={userOpen}>
  {#if !userExisting}
    <label>Username <input class="mono" bind:value={userForm.name} /></label>
  {/if}
  <div class="field">
    <div class="row"><label class="lbl" for="user-password">Password</label><span class="spacer"></span><button type="button" class="ghost small" onclick={generate}>Generate</button></div>
    <input id="user-password" class="mono" type="text" autocomplete="new-password" bind:value={userForm.password} />
  </div>
  <label>Mechanism
    <select bind:value={userForm.mechanism}><option>SCRAM-SHA-256</option><option>SCRAM-SHA-512</option></select>
  </label>
  <p class="faint small">The password is not shown again after saving; copy it now.</p>
  {#if userFormError}<div class="error">{userFormError}</div>{/if}
  {#snippet footer()}
    <button onclick={() => (userOpen = false)}>Cancel</button>
    <button class="primary" disabled={busy || !userForm.name.trim() || !userForm.password} onclick={saveUser}>Save</button>
  {/snippet}
</Modal>

<Modal title="Delete quota" bind:open={quotaDeleting}>
  {#if quotaDelete}
    <p>Every limit set for <b class="mono">{entityLabel(quotaDelete)}</b> will be removed. Its clients fall back to the default quota, or to no limit.</p>
  {/if}
  {#if quotaError}<div class="error">{quotaError}</div>{/if}
  {#snippet footer()}
    <button onclick={() => (quotaDeleting = false)}>Cancel</button>
    <button class="danger solid" disabled={busy} onclick={() => quotaDelete && removeQuota(quotaDelete)}>Delete</button>
  {/snippet}
</Modal>

<Modal title="Delete user" bind:open={userDeleting}>
  <p>All SCRAM credentials for user <b class="mono">{userDelete}</b> will be deleted. Clients connected as this user will not be able to reconnect. ACLs are not deleted.</p>
  {#snippet footer()}
    <button onclick={() => (userDeleting = false)}>Cancel</button>
    <button class="danger solid" disabled={busy} onclick={removeUser}>Delete</button>
  {/snippet}
</Modal>

<Modal title="Quota" bind:open={quotaOpen} width={520}>
  {#if quotaForm.entity}
    <p><span class="faint">Entity</span> <span class="mono">{entityLabel({ entity: quotaForm.entity, values: {} })}</span></p>
    <p class="faint small">Editing limits for this exact entity. To use a different entity, create a new quota.</p>
  {:else}
  <div class="grid2">
    <label>Entity type
      <select bind:value={quotaForm.type}><option value="client-id">client-id</option><option value="user">user</option><option value="ip">ip</option></select>
    </label>
    <label>Name <input class="mono" bind:value={quotaForm.name} disabled={quotaForm.isDefault} /></label>
  </div>
  <label class="check"><input type="checkbox" bind:checked={quotaForm.isDefault} /> Default quota for this type</label>
  {/if}
  <div class="grid2">
    {#each quotaKeys as k (k.key)}
      <label>{k.label} <input type="number" min="0" bind:value={quotaForm.values[k.key]} placeholder="unlimited" /></label>
    {/each}
  </div>
  <p class="faint small">Fields left empty are removed.</p>
  {#if quotaFormError}<div class="error">{quotaFormError}</div>{/if}
  {#snippet footer()}
    <button onclick={() => (quotaOpen = false)}>Cancel</button>
    <button class="primary" disabled={busy || (!quotaForm.entity && !quotaForm.isDefault && !quotaForm.name.trim())} onclick={saveQuota}>Save</button>
  {/snippet}
</Modal>

<style>
  .actions { white-space: nowrap; text-align: right; width: 1%; }
  button.small { padding: 2px 8px; font-size: 12px; }
  .grid3 { display: grid; grid-template-columns: 1fr 1.4fr 1fr; gap: 12px; }
  .ops { display: grid; grid-template-columns: repeat(3, 1fr); gap: 4px 12px; }
  .lbl { font-size: 12px; font-weight: 500; color: var(--text-2); margin-bottom: 6px; }
  .presets { gap: 6px; }
  .field { display: grid; gap: 4px; }
  .chip { background: var(--surface-2); }
  @media (max-width: 640px) {
    .grid3 { grid-template-columns: minmax(0, 1fr); }
    .ops { grid-template-columns: repeat(2, 1fr); }
  }
</style>
