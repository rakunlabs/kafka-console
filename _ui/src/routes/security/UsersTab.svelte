<script lang="ts">
  import { resizable } from '../../lib/resizable'
  import { errorMessage } from '../../lib/http'
  import Pending from '../../lib/Pending.svelte'
  import { api, type Cluster, type User } from '../../lib/api'
  import { resource } from '../../lib/resource.svelte'
  import Modal from '../../lib/Modal.svelte'
  import ConfirmDialog from '../../lib/ConfirmDialog.svelte'
  import MoreRows from '../../lib/MoreRows.svelte'
  import { RefreshCw, Plus } from '@lucide/svelte'

  let { cluster }: { cluster: Cluster } = $props()

  const list = resource(() => api.users(cluster.name))
  list.load()
  const users = $derived(list.data ?? [])

  const page = 200
  let shown = $state(page)

  let formOpen = $state(false)
  let form = $state({ name: '', password: '', mechanism: 'SCRAM-SHA-256' })
  let formError = $state('')
  let existing = $state(false)
  let busy = $state(false)

  function openForm(u?: User) {
    form = { name: u?.name ?? '', password: '', mechanism: u?.mechanisms[0] ?? 'SCRAM-SHA-256' }
    existing = !!u
    formError = ''
    formOpen = true
  }

  function generate() {
    const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz23456789'
    const buf = crypto.getRandomValues(new Uint32Array(24))
    form.password = Array.from(buf, (n) => chars[n % chars.length]).join('')
  }

  async function save() {
    busy = true
    formError = ''
    try {
      await api.upsertUser(cluster.name, form)
      formOpen = false
      list.load()
    } catch (e) {
      formError = errorMessage(e)
    } finally {
      busy = false
    }
  }

  let toDelete = $state('')
  let deleting = $state(false)

  async function remove() {
    await api.deleteUser(cluster.name, toDelete)
    list.load()
  }
</script>

<div class="filters">
  <span class="faint">{users.length} users</span>
  <span class="spacer"></span>
  <button onclick={list.load} disabled={list.loading}><RefreshCw size={14} class={list.loading ? 'animate-spin' : ''} />Refresh</button>
  {#if !cluster.read_only}<button class="primary" onclick={() => openForm()}><Plus size={14} />Create user</button>{/if}
</div>
{#if list.error && !list.data}
  <div class="error">{list.error}</div>
{:else}
  {#if list.error}<div class="error mb-3">{list.error}</div>{/if}
  <div class="panel">
    <table use:resizable={'security.users'}>
      <thead><tr><th>User</th><th>Mechanism</th><th></th></tr></thead>
      <tbody>
        {#each users.slice(0, shown) as u (u.name)}
          <tr>
            <td class="mono grow" title={u.name}>{u.name}</td>
            <td>{#each u.mechanisms as m (m)}<span class="badge">{m}</span> {/each}</td>
            <td class="actions">
              {#if !cluster.read_only}
                <button class="ghost small" onclick={() => openForm(u)}>Change password</button>
                <button class="ghost small danger" onclick={() => { toDelete = u.name; deleting = true }}>Delete</button>
              {/if}
            </td>
          </tr>
        {:else}
          <tr><td colspan="3" class="empty">{#if list.loading}<Pending />{:else}No SCRAM users.{/if}</td></tr>
        {/each}
      </tbody>
    </table>
    <MoreRows total={users.length} bind:shown step={page} />
  </div>
{/if}

<Modal title={existing ? `Change password: ${form.name}` : 'Create user'} bind:open={formOpen}>
  {#if !existing}
    <label>Username <input class="mono" bind:value={form.name} /></label>
  {/if}
  <div class="field">
    <div class="row"><label class="lbl" for="user-password">Password</label><span class="spacer"></span><button type="button" class="ghost small" onclick={generate}>Generate</button></div>
    <input id="user-password" class="mono" type="text" autocomplete="new-password" bind:value={form.password} />
  </div>
  <label>Mechanism
    <select bind:value={form.mechanism}><option>SCRAM-SHA-256</option><option>SCRAM-SHA-512</option></select>
  </label>
  <p class="faint small">The password is not shown again after saving; copy it now.</p>
  {#if formError}<div class="error">{formError}</div>{/if}
  {#snippet footer()}
    <button onclick={() => (formOpen = false)}>Cancel</button>
    <button class="primary" disabled={busy || !form.name.trim() || !form.password} onclick={save}>Save</button>
  {/snippet}
</Modal>

{#if deleting}
  <ConfirmDialog title="Delete user" bind:open={deleting} onconfirm={remove}>
    <p>All SCRAM credentials for user <b class="mono">{toDelete}</b> will be deleted. Clients connected as this user will not be able to reconnect. ACLs are not deleted.</p>
  </ConfirmDialog>
{/if}

<style>
  .actions { white-space: nowrap; text-align: right; width: 1%; }
  button.small { padding: 2px 8px; font-size: 12px; }
  .lbl { font-size: 12px; font-weight: 500; color: var(--text-2); margin-bottom: 6px; }
  .field { display: grid; gap: 4px; }
</style>
