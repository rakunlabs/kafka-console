<script lang="ts">
  import { resizable } from '../../lib/resizable'
  import { errorMessage } from '../../lib/http'
  import Pending from '../../lib/Pending.svelte'
  import { api, type ACL, type ACLResourceType, type Cluster, type NewACL } from '../../lib/api'
  import { resource } from '../../lib/resource.svelte'
  import { href } from '../../lib/router.svelte'
  import Modal from '../../lib/Modal.svelte'
  import ConfirmDialog from '../../lib/ConfirmDialog.svelte'
  import MoreRows from '../../lib/MoreRows.svelte'
  import { RefreshCw, Plus } from '@lucide/svelte'

  let { cluster, search = $bindable('') }: { cluster: Cluster; search?: string } = $props()

  // Raw: a cluster can hold tens of thousands of ACLs; they are replaced on
  // load, never edited in place.
  const list = resource(() => api.acls(cluster.name))
  list.load()
  const acls = $derived(list.data ?? [])

  // The filter follows the search box after a pause, not on every key.
  let query = $state('')
  $effect(() => {
    const q = search
    const t = setTimeout(() => (query = q), q ? 150 : 0)
    return () => clearTimeout(t)
  })

  // Rows render a page at a time; a new search starts from the top.
  const page = 200
  let shown = $derived.by(() => {
    void query
    return page
  })

  const rows = $derived.by(() => {
    const q = query.trim().toLowerCase()
    if (!q) return acls
    return acls.filter((a) => [a.principal, a.resource_name, a.resource_type, a.operation].some((v) => v.toLowerCase().includes(q)))
  })

  const resourceTypes: ACLResourceType[] = ['TOPIC', 'GROUP', 'CLUSTER', 'TRANSACTIONAL_ID']
  const operations = ['ALL', 'READ', 'WRITE', 'CREATE', 'DELETE', 'ALTER', 'DESCRIBE', 'CLUSTER_ACTION', 'DESCRIBE_CONFIGS', 'ALTER_CONFIGS', 'IDEMPOTENT_WRITE']

  let formOpen = $state(false)
  type Form = Omit<NewACL, 'operation'> & { operations: string[] }
  const blank = (): Form => ({ principal: '', host: '*', resource_type: 'TOPIC', resource_name: '', pattern_type: 'LITERAL', permission: 'ALLOW', operations: ['READ'] })
  let form = $state(blank())
  let formError = $state('')
  let busy = $state(false)

  // Common bundles so the operator does not have to know every operation a
  // client needs.
  const presets: { label: string; type: ACLResourceType; ops: string[] }[] = [
    { label: 'Consumer', type: 'TOPIC', ops: ['READ', 'DESCRIBE'] },
    { label: 'Producer', type: 'TOPIC', ops: ['WRITE', 'DESCRIBE'] },
    { label: 'Group', type: 'GROUP', ops: ['READ'] },
  ]

  function openForm() {
    form = blank()
    formError = ''
    formOpen = true
  }

  function toggleOp(op: string) {
    form.operations = form.operations.includes(op) ? form.operations.filter((o) => o !== op) : [...form.operations, op]
  }

  async function create() {
    busy = true
    formError = ''
    try {
      const { operations: ops, ...base } = form
      await api.createACLs(cluster.name, ops.map((operation) => ({ ...base, operation })))
      formOpen = false
      list.load()
    } catch (e) {
      formError = errorMessage(e)
    } finally {
      busy = false
    }
  }

  let toDelete = $state<ACL>()
  let deleting = $state(false)

  async function remove() {
    if (!toDelete) return
    await api.deleteACL(cluster.name, toDelete)
    list.load()
  }
</script>

<div class="filters">
  <input type="search" placeholder="Search principal, resource or operation…" bind:value={search} />
  <span class="faint">{rows.length}</span>
  <span class="spacer"></span>
  <button onclick={list.load} disabled={list.loading}><RefreshCw size={14} class={list.loading ? 'animate-spin' : ''} />Refresh</button>
  {#if !cluster.read_only}<button class="primary" onclick={openForm}><Plus size={14} />Add ACL</button>{/if}
</div>
{#if list.error && !list.data}
  <div class="error">{list.error}</div>
{:else}
  {#if list.error}<div class="error mb-3">{list.error}</div>{/if}
  <div class="panel">
    <table use:resizable={'security.acls'}>
      <thead><tr><th>Principal</th><th>Host</th><th>Resource</th><th>Pattern</th><th>Operation</th><th>Permission</th><th></th></tr></thead>
      <tbody>
        {#each rows.slice(0, shown) as a, i (i)}
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
              {#if !cluster.read_only}<button class="ghost small danger" onclick={() => { toDelete = a; deleting = true }}>Delete</button>{/if}
            </td>
          </tr>
        {:else}
          <tr><td colspan="7" class="empty">{#if list.loading}<Pending />{:else}No ACLs defined.{/if}</td></tr>
        {/each}
      </tbody>
    </table>
    <MoreRows total={rows.length} bind:shown step={page} />
  </div>
{/if}

<Modal title="Add ACL" bind:open={formOpen} width={560}>
  <div class="row presets">
    <span class="faint small">Presets:</span>
    {#each presets as p (p.label)}
      <button type="button" class="ghost small chip" onclick={() => { form.resource_type = p.type; form.operations = [...p.ops] }}>{p.label}</button>
    {/each}
  </div>
  <div class="grid2">
    <label>Principal <input class="mono" bind:value={form.principal} placeholder="User:alice" /></label>
    <label>Host <input class="mono" bind:value={form.host} /></label>
  </div>
  <div class="grid3">
    <label>Resource type
      <select bind:value={form.resource_type}>{#each resourceTypes as t (t)}<option>{t}</option>{/each}</select>
    </label>
    <label>Resource name
      <input class="mono" bind:value={form.resource_name} disabled={form.resource_type === 'CLUSTER'}
        placeholder={form.resource_type === 'CLUSTER' ? 'kafka-cluster' : '* or name'} />
    </label>
    <label>Pattern
      <select bind:value={form.pattern_type}><option>LITERAL</option><option>PREFIXED</option></select>
    </label>
  </div>
  <label>Permission
    <select bind:value={form.permission}><option>ALLOW</option><option>DENY</option></select>
  </label>
  <div>
    <div class="lbl">Operations</div>
    <div class="ops">
      {#each operations as op (op)}
        <label class="check"><input type="checkbox" checked={form.operations.includes(op)} onchange={() => toggleOp(op)} /> <span class="mono small">{op}</span></label>
      {/each}
    </div>
  </div>
  {#if formError}<div class="error">{formError}</div>{/if}
  {#snippet footer()}
    <button onclick={() => (formOpen = false)}>Cancel</button>
    <button class="primary" disabled={busy || !form.principal.trim() || !form.operations.length || (form.resource_type !== 'CLUSTER' && !form.resource_name.trim())}
      onclick={create}>{form.operations.length > 1 ? `Create ${form.operations.length} ACLs` : 'Create'}</button>
  {/snippet}
</Modal>

{#if deleting && toDelete}
  <ConfirmDialog title="Delete ACL" bind:open={deleting} onconfirm={remove}>
    <p>The <b>{toDelete.permission} {toDelete.operation}</b> rule for <b class="mono">{toDelete.principal}</b> on <b class="mono">{toDelete.resource_type}:{toDelete.resource_name}</b> will be deleted. Clients relying on this rule may lose access.</p>
  </ConfirmDialog>
{/if}

<style>
  .actions { white-space: nowrap; text-align: right; width: 1%; }
  button.small { padding: 2px 8px; font-size: 12px; }
  .grid3 { display: grid; grid-template-columns: 1fr 1.4fr 1fr; gap: 12px; }
  .ops { display: grid; grid-template-columns: repeat(3, 1fr); gap: 4px 12px; }
  .lbl { font-size: 12px; font-weight: 500; color: var(--text-2); margin-bottom: 6px; }
  .presets { gap: 6px; }
  .chip { background: var(--surface-2); }
  @media (max-width: 640px) {
    .grid3 { grid-template-columns: minmax(0, 1fr); }
    .ops { grid-template-columns: repeat(2, 1fr); }
  }
</style>
