import type { Message } from './api'

// Keep payload JSON as text: parsing it would round large integers.
function rawJSON(m: Message) {
  if (m.value === null || m.truncated || !m.value_format || m.value_format === 'text' || m.value_format === 'binary') return undefined
  try {
    JSON.parse(m.value)
    // Import treats JSON null as a tombstone and unquotes strings. Keep
    // these JSON documents inside a string field to preserve their bytes.
    if (m.value.trim() === 'null' || m.value.trimStart().startsWith('"')) return undefined
    return m.value.trim()
  } catch {
    return undefined
  }
}

export function exportJSON(messages: Message[]): string {
  return '[\n' + messages.map((m) => {
    const raw = rawJSON(m)
    const rest = JSON.stringify({ ...m, value: raw === undefined ? m.value : null }, null, 2)
    return '  ' + (raw === undefined ? rest : rest.replace(/"value": null/, () => `"value": ${raw}`)).replaceAll('\n', '\n  ')
  }).join(',\n') + '\n]\n'
}

// Explicit nulls and safety/schema metadata make new CSV exports round-trip.
// Import still accepts older, less expressive CSV files.
const columns = [
  'partition', 'offset', 'timestamp', 'key', 'value', 'headers', 'key_encoding', 'value_encoding',
  'key_null', 'value_null', 'truncated', 'key_format', 'value_format', 'key_schema_id', 'value_schema_id',
  'key_schema_message', 'value_schema_message', 'decode_error',
] as const

export function exportCSV(messages: Message[]): string {
  const esc = (v: unknown) => {
    const s = v === null || v === undefined ? '' : String(v)
    return /[",\r\n]/.test(s) ? `"${s.replaceAll('"', '""')}"` : s
  }
  const rows: unknown[][] = [columns.slice()]
  for (const m of messages) {
    const fields = { ...m, headers: JSON.stringify(m.headers), key_null: m.key === null, value_null: m.value === null }
    rows.push(columns.map((name) => fields[name]))
  }
  return rows.map((r) => r.map(esc).join(',')).join('\n')
}
