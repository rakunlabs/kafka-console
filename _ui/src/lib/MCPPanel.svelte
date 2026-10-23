<script lang="ts">
  import type { Cluster, MCPEndpoint } from './api'
  import { mcpURL, mcpServerName, mcpSnippet, tokenVariable, type MCPClient } from './mcp'
  import { copyText } from './resource.svelte'
  import Modal from './Modal.svelte'
  import { Bot, Check, Copy, Lock } from '@lucide/svelte'

  // Where an AI agent connects to this cluster, and how to set it up.
  let { cluster }: { cluster: Cluster } = $props()

  const endpoints = $derived(cluster.mcp ?? [])
  const origin = location.origin

  let copiedKey = $state('')
  async function copy(key: string, text: string) {
    if (!(await copyText(text))) return
    copiedKey = key
    setTimeout(() => copiedKey === key && (copiedKey = ''), 1200)
  }

  let connecting = $state<MCPEndpoint>()
  let open = $state(false)
  let client = $state<MCPClient>('opencode')
  const clients: { id: MCPClient; label: string; where: string; check: string }[] = [
    { id: 'opencode', label: 'OpenCode', where: 'Add to opencode.json', check: 'opencode mcp list' },
    { id: 'claude', label: 'Claude Code', where: 'Run in a terminal', check: 'claude mcp get <name>, or /mcp in Claude Code' },
    { id: 'codex', label: 'Codex', where: 'Add to ~/.codex/config.toml', check: '/mcp in Codex' },
  ]
  const current = $derived(clients.find((c) => c.id === client)!)
  const name = $derived(connecting ? mcpServerName(cluster.name, connecting) : '')
  const snippet = $derived(connecting ? mcpSnippet(client, name, mcpURL(origin, connecting), connecting.auth_token) : '')

  function connect(e: MCPEndpoint) {
    connecting = e
    open = true
  }
</script>

{#if endpoints.length}
  <section class="mcp panel mb-6" aria-label="MCP for AI agents">
    {#each endpoints as e (e.name)}
      {@const url = mcpURL(origin, e)}
      <div class="row">
        <span class="lead"><Bot size={15} class="text-ink-3" /><b>MCP</b>{#if endpoints.length > 1}<span class="faint">{e.name}</span>{/if}</span>
        <code class="url" title={url}>{url}</code>
        <button class="ghost small" onclick={() => copy(e.name, url)} aria-label="Copy MCP address" title="Copy address">
          {#if copiedKey === e.name}<Check size={13} />{:else}<Copy size={13} />{/if}
        </button>
        <span class="badge {e.read_only ? '' : 'warn'}">{e.read_only ? 'read-only' : 'can write'}</span>
        {#if e.auth_token}<span class="faint tok" title="Clients send the console's MCP token as a bearer token"><Lock size={12} />token</span>{/if}
        <span class="spacer"></span>
        <button class="small" onclick={() => connect(e)}>Connect an agent…</button>
      </div>
    {/each}
  </section>
{/if}

<Modal title="Connect an AI agent" bind:open width={620}>
  {#if connecting}
    <p class="faint small">Gives an MCP client access to <b class="mono">{cluster.name}</b>
      {connecting.read_only ? 'without write access.' : 'including writes. MCP tools still ask for confirmation before changing data.'}</p>
    <div class="tabs" role="tablist">
      {#each clients as c (c.id)}
        <button role="tab" aria-selected={client === c.id} class:active={client === c.id} onclick={() => (client = c.id)}>{c.label}</button>
      {/each}
    </div>
    <div class="snip">
      <div class="snip-head">
        <span class="faint small">{current.where}</span>
        <button class="ghost small" onclick={() => copy('snippet', snippet)}>
          {#if copiedKey === 'snippet'}<Check size={13} />Copied{:else}<Copy size={13} />Copy{/if}
        </button>
      </div>
      <pre class="mono">{snippet}</pre>
    </div>
    {#if connecting.auth_token}
      <p class="small">Set <code>{tokenVariable}</code> to the console's <code>mcp.auth_token</code> in the shell the agent starts from. The token is never shown here.</p>
    {/if}
    <p class="faint small">Check the connection with {current.check}, then ask the agent to call <code>server_config</code>.</p>
  {/if}
  {#snippet footer()}
    <button class="primary" onclick={() => (open = false)}>Done</button>
  {/snippet}
</Modal>

<style>
  .mcp { display: grid; }
  .row { display: flex; flex-wrap: wrap; align-items: center; gap: 6px 10px; padding: 8px 14px; font-size: 13px; }
  .row + .row { border-top: 1px dashed var(--line-dash); }
  .lead { display: inline-flex; align-items: center; gap: 6px; }
  .lead b { font-weight: 600; }
  .url { min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; color: var(--text-2); }
  .tok { display: inline-flex; align-items: center; gap: 4px; font-size: 12px; }
  .tabs { display: flex; gap: 4px; border-bottom: 1px solid var(--border); }
  .tabs button { border: 0; border-bottom: 2px solid transparent; border-radius: 0; background: transparent; padding: 6px 10px; color: var(--text-2); }
  .tabs button.active { border-bottom-color: var(--text); color: var(--text); font-weight: 600; }
  .snip { border: 1px solid var(--border-strong); background: var(--surface-2); }
  .snip-head { display: flex; align-items: center; justify-content: space-between; padding: 4px 6px 4px 10px; border-bottom: 1px solid var(--border); }
  pre { margin: 0; padding: 10px 12px; font-size: 12px; overflow-x: auto; white-space: pre; }
  @media (max-width: 639.98px) { .url { flex-basis: 100%; order: 1; } }
</style>
