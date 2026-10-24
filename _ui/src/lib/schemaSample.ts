// Skeleton values built from a schema, so a message can be written starting
// from a valid shape. Defaults are used where the schema gives one; nesting
// stops at maxDepth, which also ends recursive types.

const maxDepth = 8

const avroPrimitives: Record<string, unknown> = { null: null, boolean: false, int: 0, long: 0, float: 0, double: 0, string: '', bytes: '' }

export function avroSample(s: any, depth = 0, now = new Date()): unknown {
  if (depth > maxDepth) return null
  if (typeof s === 'string') return s in avroPrimitives ? avroPrimitives[s] : {}
  if (Array.isArray(s)) return avroSample(s.find((t) => t !== 'null') ?? 'null', depth + 1, now)
  if (s.logicalType === 'timestamp-millis' || s.logicalType === 'timestamp-micros') return now.toISOString()
  if (s.logicalType === 'date') return now.toISOString().slice(0, 10)
  if (s.logicalType === 'decimal') return '0.00'
  switch (s.type) {
    case 'record':
      return Object.fromEntries((s.fields ?? []).map((f: any) => [f.name, 'default' in f ? f.default : avroSample(f.type, depth + 1, now)]))
    case 'enum':
      return s.symbols?.[0] ?? ''
    case 'array':
      return []
    case 'map':
      return {}
    case 'fixed':
      return ''
    default:
      return avroSample(s.type, depth + 1, now)
  }
}

export function jsonSample(s: any, depth = 0): unknown {
  if (!s || depth > maxDepth) return null
  if (s.default !== undefined) return s.default
  if (s.enum) return s.enum[0]
  const t = Array.isArray(s.type) ? s.type.find((x: string) => x !== 'null') : s.type
  switch (t) {
    case 'object':
      return Object.fromEntries(Object.entries(s.properties ?? {}).map(([k, v]) => [k, jsonSample(v, depth + 1)]))
    case 'array':
      return []
    case 'string':
      return ''
    case 'integer':
    case 'number':
      return 0
    case 'boolean':
      return false
  }
  return null
}

/** A pretty-printed sample for a registry schema of the given type. */
export function schemaSample(type: string, schema: string): string {
  if (type === 'AVRO') return JSON.stringify(avroSample(JSON.parse(schema)), null, 2)
  if (type === 'JSON') return JSON.stringify(jsonSample(JSON.parse(schema)), null, 2)
  return '{}'
}
