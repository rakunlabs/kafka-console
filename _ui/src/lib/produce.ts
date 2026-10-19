import type { Message, ProduceBody } from './api'

// Only full, decoded records may seed an editable form. In particular a
// protobuf schema ID alone is not enough to identify its message type.
export function messageDraft(message: Message): ProduceBody {
  if (message.truncated) throw new Error('The full message could not be loaded. Use Resend to copy the original bytes instead.')
  if (message.decode_error) throw new Error(`Cannot safely edit this message: ${message.decode_error}. Use Resend to copy the original bytes instead.`)
  for (const side of ['key', 'value'] as const) {
    const format = message[`${side}_format`]
    // Decoded without the registry (local .proto, MessagePack, CBOR, uint):
    // the form would send the JSON view as the bytes, not the original
    // encoding.
    if (format && !message[`${side}_schema_id`] && !['json', 'text', 'binary'].includes(format)) {
      throw new Error(`The ${side} is ${format} decoded for display, and Produce cannot encode it back. Use Resend to copy the original bytes instead.`)
    }
    if (format === 'protobuf' && !message[`${side}_schema_message`]) {
      throw new Error(`The ${side}'s Protobuf message type is missing. Reload the message before editing.`)
    }
  }
  return {
    key: message.key,
    value: message.value,
    key_encoding: message.key_encoding,
    value_encoding: message.value_encoding,
    key_schema: message.key_schema_id ? { id: message.key_schema_id, message: message.key_schema_message } : undefined,
    value_schema: message.value_schema_id ? { id: message.value_schema_id, message: message.value_schema_message } : undefined,
    headers: message.headers.map((h) => ({ ...h })),
    partition: message.partition,
  }
}

export type ProduceChange = { field: string; before: string; after: string }

// Change whitespace only. JSON.parse/stringify would round large numbers,
// normalize exponent/decimal spelling, collapse duplicate keys and re-escape strings.
export function formatPayload(text: string): string {
  JSON.parse(text) // Syntax check only; never use the parsed values.
  const tokens = text.match(/"(?:\\.|[^"\\])*"|-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?|true|false|null|[{}\[\],:]/g) ?? []
  let depth = 0
  let result = ''
  const indent = () => '  '.repeat(depth)
  for (let i = 0; i < tokens.length; i++) {
    const token = tokens[i]
    if (token === '{' || token === '[') {
      result += token
      depth++
      if (tokens[i + 1] !== (token === '{' ? '}' : ']')) result += '\n' + indent()
    } else if (token === '}' || token === ']') {
      depth--
      if (tokens[i - 1] !== (token === '}' ? '{' : '[')) result += '\n' + indent()
      result += token
    } else if (token === ',') result += ',\n' + indent()
    else if (token === ':') result += ': '
    else result += token
  }
  return result
}

// Keep payload text verbatim: parsing/pretty-printing JSON here would round
// large integer fields and make the preview differ from the submitted bytes.
export function produceChanges(before: ProduceBody, after: ProduceBody, source: string, target: string): ProduceChange[] {
  const headers = (body: ProduceBody) => body.headers.map((h) => ({
    key: h.key, value: h.value, encoding: h.encoding || 'utf8', ...(h.null ? { null: true } : {}),
  }))
  const fields: [string, unknown, unknown][] = [
    ['Topic', source, target],
    ['Partition', before.partition ?? 'Automatic (by key)', after.partition ?? 'Automatic (by key)'],
    ['Key', before.key, after.key],
    ['Key encoding', before.key_encoding || 'utf8', after.key_encoding || 'utf8'],
    ['Key schema', before.key_schema ?? null, after.key_schema ?? null],
    ['Value', before.value, after.value],
    ['Value encoding', before.value_encoding || 'utf8', after.value_encoding || 'utf8'],
    ['Value schema', before.value_schema ?? null, after.value_schema ?? null],
    ['Headers', headers(before), headers(after)],
    ['Compression', before.compression || 'none', after.compression || 'none'],
  ]
  const text = (v: unknown, field: string) => {
    if (field === 'Key' || field === 'Value') {
      if (v === null) return '(null payload)'
      if (v === '') return '(empty string)'
      return `(text)\n${v}`
    }
    return typeof v === 'string' ? v : JSON.stringify(v, null, 2)
  }
  return fields.filter(([, a, b]) => JSON.stringify(a) !== JSON.stringify(b))
    .map(([field, a, b]) => ({ field, before: text(a, field), after: text(b, field) }))
}
