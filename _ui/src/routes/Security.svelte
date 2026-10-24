<script lang="ts">
  import type { Cluster } from '../lib/api'
  import { href, router } from '../lib/router.svelte'
  import AclsTab from './security/AclsTab.svelte'
  import UsersTab from './security/UsersTab.svelte'
  import QuotasTab from './security/QuotasTab.svelte'

  let { cluster }: { cluster: Cluster } = $props()

  const tabs = [
    { id: 'acls', label: 'ACL' },
    { id: 'users', label: 'Users (SCRAM)' },
    { id: 'quotas', label: 'Quota' },
  ]
  const tab = $derived(router.route.query.get('tab') ?? 'acls')
  // Kept across tab switches, as it was when the tabs shared one page.
  let aclSearch = $state('')
</script>

<div class="head">
  <h1>Security</h1>
</div>

<div class="tabs" role="tablist">
  {#each tabs as t (t.id)}
    <a role="tab" aria-selected={tab === t.id} class:active={tab === t.id} href={href(cluster.name, 'security', '', { tab: t.id })}>{t.label}</a>
  {/each}
</div>

<!-- Each tab loads when shown, as switching tabs always reloaded. -->
{#if tab === 'users'}
  <UsersTab {cluster} />
{:else if tab === 'quotas'}
  <QuotasTab {cluster} />
{:else}
  <AclsTab {cluster} bind:search={aclSearch} />
{/if}
