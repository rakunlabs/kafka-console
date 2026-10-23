import type { MCPEndpoint } from './api'

export const tokenVariable = 'KAFKA_CONSOLE_MCP_AUTH_TOKEN'

// The address the agent dials: the origin the browser reached this console
// at, so a proxy's public host and scheme are used, plus the served path.
export function mcpURL(origin: string, endpoint: MCPEndpoint): string {
  return origin.replace(/\/+$/, '') + endpoint.path
}

// A name every client accepts: letters, digits, hyphens and underscores.
export function mcpServerName(cluster: string, endpoint: MCPEndpoint): string {
  const base = endpoint.name === cluster ? `kafka-${cluster}` : `kafka-${cluster}-${endpoint.name}`
  return base.replace(/[^A-Za-z0-9_-]+/g, '-').replace(/-+/g, '-').replace(/^-|-$/g, '') || 'kafka'
}

export type MCPClient = 'opencode' | 'claude' | 'codex'

// Config snippets never contain the token: each client reads it from the
// environment at connect time.
export function mcpSnippet(client: MCPClient, name: string, url: string, token: boolean): string {
  const shell = (s: string) => `'${s.replaceAll("'", `'\\''`)}'`
  if (client === 'opencode') {
    const server: Record<string, unknown> = { type: 'remote', url, enabled: true }
    if (token) {
      server.oauth = false
      server.headers = { Authorization: `Bearer {env:${tokenVariable}}` }
    }
    return JSON.stringify({ $schema: 'https://opencode.ai/config.json', mcp: { [name]: server } }, null, 2)
  }
  if (client === 'claude') {
    const lines = [`claude mcp add --transport http ${name} ${shell(url)}`]
    if (token) lines.push(`  --header "Authorization: Bearer $${tokenVariable}"`)
    return lines.join(' \\\n')
  }
  const lines = [`[mcp_servers.${name}]`, `url = ${JSON.stringify(url)}`]
  if (token) lines.push(`bearer_token_env_var = "${tokenVariable}"`)
  return lines.join('\n')
}
