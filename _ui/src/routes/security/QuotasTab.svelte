<script lang="ts">
  import { resizable } from '../../lib/resizable'
  import { errorMessage } from '../../lib/http'
  import Pending from '../../lib/Pending.svelte'
  import { api, type Cluster, type Quota } from '../../lib/api'
  import { resource } from '../../lib/resource.svelte'
  import { fmt } from '../../lib/format'
  import Modal from '../../lib/Modal.svelte'
  import ConfirmDialog from '../../lib/ConfirmDialog.svelte'
  import MoreRows from '../../lib/MoreRows.svelte'
  import { quotaKeys, quotaDraft, quotaChanges } from '../../lib/quota'
  import { RefreshCw, Plus } from '@lucide/svelte'

  let { cluster }: { cluster: Cluster } = $props()

  const list = resource(() => api.quotas(cluster.name))
  list.load()
  const quotas = $derived(list.data ?? [])

  const page = 200
  let shown = $state(page)

  let formOpen = $state(false)
  let form = $state(quotaDraft())
  let formError = $state('')
  let busy = $state(false)

  function openForm(q?: Quota) {
    form = quotaDraft(q)
    formError = ''
    formOpen = true
  }

  async function save() {
    busy = true
    formError = ''
    try {
      await api.alterQuota(cluster.name, quotaChanges(form))
      formOpen = false
      list.load()
    } catch (e) {
      formError = errorMessage(e)
    } finally {
      busy = false
    }
  }

  let toDelete = $state<Quota>()
  let deleting = $state(false)

  async function remove() {
    if (!toDelete) return
    await api.alterQuota(cluster.name, { entity: toDelete.entity, remove: Object.keys(toDelete.values) })
    list.load()
  }

  const entityLabel = (q: Quota) => q.entity.map((e) => `${e.type}=${e.name ?? '<default>'}`).join(', ')
</script>

<div class="filters">
  <span class="faint">{quotas.length} entries</span>
  <span class="spacer"></span>
  <button onclick={list.load} disabled={list.loading}><RefreshCw size={14} class={list.loading ? 'animate-spin' : ''} />Refresh</button>
  {#if !cluster.read_only}<button class="primary" onclick={() => openForm()}><Plus size={14} />Add quota</button>{/if}
</div>
{#if list.error && !list.data}
  <div class="error">{list.error}</div>
{:else}
  {#if list.error}<div class="error mb-3">{list.error}</div>{/if}
  <div class="panel">
    <table use:resizable={'security.quotas'}>
      <thead><tr><th>Entity</th>{#each quotaKeys as k (k.key)}<th class="num">{k.label}</th>{/each}<th></th></tr></thead>
      <tbody>
        {#each quotas.slice(0, shown) as q, i (i)}
          <tr>
            <td class="mono grow" title={entityLabel(q)}>{entityLabel(q)}</td>
            {#each quotaKeys as k (k.key)}<td class="num mono">{q.values[k.key] !== undefined ? fmt(q.values[k.key]) : '—'}</td>{/each}
            <td class="actions">
              {#if !cluster.read_only}
                <button class="ghost small" onclick={() => openForm(q)}>Edit</button>
                <button class="ghost small danger" onclick={() => { toDelete = q; deleting = true }}>Delete</button>
              {/if}
            </td>
          </tr>
        {:else}
          <tr><td colspan={quotaKeys.length + 2} class="empty">{#if list.loading}<Pending />{:else}No quotas defined.{/if}</td></tr>
        {/each}
      </tbody>
    </table>
    <MoreRows total={quotas.length} bind:shown step={page} />
  </div>
{/if}

{#if deleting && toDelete}
  <ConfirmDialog title="Delete quota" bind:open={deleting} onconfirm={remove}>
    <p>Every limit set for <b class="mono">{entityLabel(toDelete)}</b> will be removed. Its clients fall back to the default quota, or to no limit.</p>
  </ConfirmDialog>
{/if}

<Modal title="Quota" bind:open={formOpen} width={520}>
  {#if form.entity}
    <p><span class="faint">Entity</span> <span class="mono">{entityLabel({ entity: form.entity, values: {} })}</span></p>
    <p class="faint small">Editing limits for this exact entity. To use a different entity, create a new quota.</p>
  {:else}
  <div class="grid2">
    <label>Entity type
      <select bind:value={form.type}><option value="client-id">client-id</option><option value="user">user</option><option value="ip">ip</option></select>
    </label>
    <label>Name <input class="mono" bind:value={form.name} disabled={form.isDefault} /></label>
  </div>
  <label class="check"><input type="checkbox" bind:checked={form.isDefault} /> Default quota for this type</label>
  {/if}
  <div class="grid2">
    {#each quotaKeys as k (k.key)}
      <label>{k.label} <input type="number" min="0" bind:value={form.values[k.key]} placeholder="unlimited" /></label>
    {/each}
  </div>
  <p class="faint small">Fields left empty are removed.</p>
  {#if formError}<div class="error">{formError}</div>{/if}
  {#snippet footer()}
    <button onclick={() => (formOpen = false)}>Cancel</button>
    <button class="primary" disabled={busy || (!form.entity && !form.isDefault && !form.name.trim())} onclick={save}>Save</button>
  {/snippet}
</Modal>

<style>
  .actions { white-space: nowrap; text-align: right; width: 1%; }
  button.small { padding: 2px 8px; font-size: 12px; }
</style>
