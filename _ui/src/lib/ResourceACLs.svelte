<script lang="ts">
  import { resizable } from './resizable'
  import Pending from './Pending.svelte'
  import { untrack } from 'svelte'
  import { api, isAbort, type ACL, type Cluster } from './api'
  import { href } from './router.svelte'

  // The ACLs that apply to one topic or group, read-only. Editing stays on
  // the Security page, which this links to.
  let { cluster, type, name }: { cluster: Cluster; type: 'TOPIC' | 'GROUP'; name: string } = $props()

  let acls = $state.raw<ACL[]>()
  let error = $state('')

  // The page remounts per resource, so the first values are the ones.
  untrack(() => api.resourceACLs(cluster.name, type, name))
    .then((r) => (acls = r))
    .catch((e) => { if (!isAbort(e)) error = e.message })

  // Why an ACL covers this resource, for rows that are not on its exact name.
  function via(a: ACL) {
    if (a.resource_name === '*') return 'all'
    if (a.pattern_type === 'PREFIXED') return `prefix ${a.resource_name}`
    return ''
  }
</script>

{#if error}
  <div class="error">{error}</div>
{:else if !acls}
  <div class="empty"><Pending /></div>
{:else}
  <div class="panel overflow-x-auto">
    <table use:resizable={'resource.acls'}>
      <thead><tr><th>Principal</th><th>Host</th><th>Operation</th><th>Permission</th><th>Matches</th></tr></thead>
      <tbody>
        {#each acls as a, i (i)}
          <tr>
            <td class="mono">{a.principal}</td>
            <td class="mono faint">{a.host}</td>
            <td class="mono small">{a.operation}</td>
            <td><span class="badge {a.permission === 'ALLOW' ? 'ok' : 'danger'}">{a.permission}</span></td>
            <td class="faint small">{via(a) || 'this name'}</td>
          </tr>
        {:else}
          <tr><td colspan="5" class="empty">No ACL applies to this {type === 'TOPIC' ? 'topic' : 'group'}.</td></tr>
        {/each}
      </tbody>
    </table>
  </div>
  <p class="faint small mt-2">ACLs on this name, on prefixes that cover it and on <code>*</code>. Manage them on the <a href={href(cluster.name, 'security')}>Security</a> page.</p>
{/if}
