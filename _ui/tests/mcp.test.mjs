import assert from 'node:assert/strict'
import { test } from 'node:test'
import { mcpURL, mcpServerName, mcpSnippet, tokenVariable } from '../src/lib/mcp.ts'

const endpoint = { name: 'prod', path: '/kafka/mcp/prod', read_only: true, auth_token: true }

test('address uses the public origin the browser reached plus the served path', () => {
  assert.equal(mcpURL('https://kafka.example.com', endpoint), 'https://kafka.example.com/kafka/mcp/prod')
  assert.equal(mcpURL('https://kafka.example.com/', endpoint), 'https://kafka.example.com/kafka/mcp/prod')
  assert.equal(mcpURL('http://localhost:8080', { ...endpoint, path: '/mcp/team%20space' }), 'http://localhost:8080/mcp/team%20space')
})

test('server names are accepted by every client', () => {
  assert.equal(mcpServerName('prod', endpoint), 'kafka-prod')
  assert.equal(mcpServerName('team space', { ...endpoint, name: 'team space' }), 'kafka-team-space')
  assert.equal(mcpServerName('prod', { ...endpoint, name: 'read.only' }), 'kafka-prod-read-only')
})

test('snippets reference the token variable and never contain a token', () => {
  const url = mcpURL('https://kafka.example.com', endpoint)
  const opencode = JSON.parse(mcpSnippet('opencode', 'kafka-prod', url, true))
  assert.deepEqual(opencode.mcp['kafka-prod'], {
    type: 'remote', url, enabled: true, oauth: false, headers: { Authorization: `Bearer {env:${tokenVariable}}` },
  })
  assert.equal(mcpSnippet('claude', 'kafka-prod', url, true),
    `claude mcp add --transport http kafka-prod '${url}' \\\n  --header "Authorization: Bearer $${tokenVariable}"`)
  assert.equal(mcpSnippet('codex', 'kafka-prod', url, true),
    `[mcp_servers.kafka-prod]\nurl = "${url}"\nbearer_token_env_var = "${tokenVariable}"`)
})

test('without a token, snippets send no authorization', () => {
  const url = 'http://localhost:8080/mcp/local'
  assert.deepEqual(JSON.parse(mcpSnippet('opencode', 'kafka-local', url, false)).mcp['kafka-local'], { type: 'remote', url, enabled: true })
  for (const client of ['claude', 'codex']) assert.doesNotMatch(mcpSnippet(client, 'kafka-local', url, false), /Authorization|bearer/i)
})

test('shell quoting keeps odd URLs as one argument', () => {
  assert.match(mcpSnippet('claude', 'kafka-x', "http://h/mcp/it's", false), /'http:\/\/h\/mcp\/it'\\''s'/)
})
