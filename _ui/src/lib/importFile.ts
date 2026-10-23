import type { Header, ImportRecord } from './api'

export type FileFormat = 'json' | 'jsonl' | 'csv' | 'lines'
// record: items carry key, value, headers…; value: each item is the value.
export type ItemShape = 'record' | 'value'

// One item of a file: a record, or why it cannot be one. Items are numbered
// from 1 in file order.
export type Item = { item: number; record?: ImportRecord; problem?: string }

// A file that cannot be read further: not an array, a CSV without a value
// column, an unterminated quote.
export class FatalError extends Error {}

export type Parsed = {
  records: ImportRecord[]
  // Items that could not be turned into a record, with their 1-based number;
  // item 0 is the whole file.
  problems: { item: number; message: string }[]
  // Whether any record carries a partition, timestamp or schema ID, which
  // the dialog offers to keep.
  partitions: boolean
  timestamps: boolean
  schemaIds: boolean
}

export function detectFormat(name: string, text: string): FileFormat {
  const ext = name.toLowerCase().split('.').pop()
  if (ext === 'csv') return 'csv'
  if (ext === 'jsonl' || ext === 'ndjson') return 'jsonl'
  if (ext === 'txt' || ext === 'log') return 'lines'
  const head = text.trimStart()
  if (head.startsWith('[')) return 'json'
  if (head.startsWith('{')) return 'jsonl'
  return 'lines'
}

const ws = (c: string) => c === ' ' || c === '\n' || c === '\r' || c === '\t'

// ─── Splitters ───────────────────────────────────────────
// Each takes the file in chunks of any size and returns the complete items
// seen so far. Chunk boundaries may fall anywhere, including inside a string,
// an escape or a CRLF.

class Lines {
  private buf = ''

  feed(chunk: string): string[] {
    const out: string[] = []
    let start = 0
    let i
    while ((i = chunk.indexOf('\n', start)) >= 0) {
      out.push(stripCR(this.buf + chunk.slice(start, i)))
      this.buf = ''
      start = i + 1
    }
    this.buf += chunk.slice(start)
    return out
  }

  end(): string[] {
    const rest = this.buf
    this.buf = ''
    return rest ? [stripCR(rest)] : []
  }
}

const stripCR = (s: string) => (s.endsWith('\r') ? s.slice(0, -1) : s)

class Csv {
  private row: string[] = []
  private cell = ''
  private quoted = false
  // A quote inside a quoted field: either the first of "" or the closing one,
  // decided by the next character, which may be in the next chunk.
  private quote = false
  private skipLF = false

  feed(chunk: string): string[][] {
    const out: string[][] = []
    for (let i = 0; i < chunk.length; i++) {
      let c = chunk[i]
      if (this.skipLF) {
        this.skipLF = false
        if (c === '\n') continue
      }
      if (this.quote) {
        this.quote = false
        if (c === '"') {
          this.cell += '"'
          continue
        }
        this.quoted = false
      }
      if (this.quoted) {
        // Copy up to the next quote in one step.
        const q = chunk.indexOf('"', i)
        if (q < 0) {
          this.cell += chunk.slice(i)
          break
        }
        this.cell += chunk.slice(i, q)
        i = q
        this.quote = true
        continue
      }
      if (c === '"' && this.cell === '') this.quoted = true
      else if (c === ',') {
        this.row.push(this.cell)
        this.cell = ''
      } else if (c === '\n' || c === '\r') {
        this.skipLF = c === '\r'
        this.row.push(this.cell)
        out.push(this.row)
        this.row = []
        this.cell = ''
      } else {
        // Copy the rest of an unquoted cell in one step.
        let j = i + 1
        while (j < chunk.length && (c = chunk[j]) !== ',' && c !== '\n' && c !== '\r') j++
        this.cell += chunk.slice(i, j)
        i = j - 1
      }
    }
    return out.filter(notBlank)
  }

  end(): string[][] {
    if (this.quote) {
      this.quote = false
      this.quoted = false
    }
    if (this.quoted) throw new FatalError('CSV: unterminated quoted field')
    const out: string[][] = []
    if (this.cell !== '' || this.row.length) {
      this.row.push(this.cell)
      out.push(this.row)
    }
    this.row = []
    this.cell = ''
    return out.filter(notBlank)
  }
}

const notBlank = (r: string[]) => !(r.length === 1 && r[0] === '')

// Raw text of each element of a top-level JSON array. Structure is checked
// here; each element is validated by JSON.parse when it is used, so one bad
// element is reported alone instead of failing the file.
class JsonArray {
  private state: 'start' | 'first' | 'value' | 'in' | 'after' | 'done' = 'start'
  private cur = ''
  private depth = 0
  private inStr = false
  private esc = false
  private scalar = false

  feed(chunk: string): string[] {
    const out: string[] = []
    let from = this.state === 'in' ? 0 : -1
    const emit = (to: number) => {
      out.push(this.cur + chunk.slice(from, to))
      this.cur = ''
      from = -1
      this.state = 'after'
    }

    for (let i = 0; i < chunk.length; i++) {
      const c = chunk[i]
      if (this.state === 'in') {
        if (this.inStr) {
          if (this.esc) this.esc = false
          else if (c === '\\') this.esc = true
          else if (c === '"') {
            this.inStr = false
            if (this.depth === 0) emit(i + 1)
          }
        } else if (this.scalar) {
          if (ws(c) || c === ',' || c === ']') {
            emit(i)
            i--
          }
        } else if (c === '"') this.inStr = true
        else if (c === '{' || c === '[') this.depth++
        else if ((c === '}' || c === ']') && --this.depth === 0) emit(i + 1)
        continue
      }

      if (ws(c)) continue

      if (this.state === 'start') {
        if (c !== '[') throw new FatalError('Not a JSON array: expected a JSON array')
        this.state = 'first'
      } else if (this.state === 'after') {
        if (c === ',') this.state = 'value'
        else if (c === ']') this.state = 'done'
        else throw new FatalError(`Not a JSON array: expected , or ] but found ${JSON.stringify(c)}`)
      } else if (this.state === 'done') {
        throw new FatalError('Not a JSON array: unexpected data after the array')
      } else if (c === ']' && this.state === 'first') {
        this.state = 'done'
      } else if (c === ']' || c === ',' || c === '}' || c === ':') {
        throw new FatalError(`Not a JSON array: unexpected ${JSON.stringify(c)}`)
      } else {
        from = i
        this.state = 'in'
        this.depth = c === '{' || c === '[' ? 1 : 0
        this.inStr = c === '"'
        this.esc = false
        this.scalar = !this.inStr && this.depth === 0
      }
    }

    if (this.state === 'in') this.cur += chunk.slice(from)
    return out
  }

  end(): string[] {
    if (this.state === 'start') throw new FatalError('Not a JSON array: the file is empty')
    if (this.state !== 'done') throw new FatalError('Not a JSON array: the array is not closed')
    return []
  }
}

// ─── Raw JSON fields ─────────────────────────────────────
// Values are kept as the exact text in the file. Parsing and re-serializing
// would round large integers and respell numbers and escapes.

function skip(t: string, i: number) {
  while (i < t.length && ws(t[i])) i++
  return i
}

// End index (exclusive) of the JSON value starting at i. Assumes the text is
// valid JSON, which callers check with JSON.parse first.
function valueEnd(t: string, i: number): number {
  const c = t[i]
  if (c === '"') {
    for (i++; i < t.length; i++) {
      if (t[i] === '\\') i++
      else if (t[i] === '"') return i + 1
    }
    throw new Error('unterminated string')
  }
  if (c === '{' || c === '[') {
    let depth = 0
    for (; i < t.length; i++) {
      const ch = t[i]
      if (ch === '"') i = valueEnd(t, i) - 1
      else if (ch === '{' || ch === '[') depth++
      else if (ch === '}' || ch === ']') {
        if (--depth === 0) return i + 1
      }
    }
    throw new Error('unterminated ' + (c === '{' ? 'object' : 'array'))
  }
  while (i < t.length && !ws(t[i]) && t[i] !== ',' && t[i] !== '}' && t[i] !== ']') i++
  return i
}

// Raw text of each field of a JSON object, by name.
export function rawFields(text: string): Map<string, string> {
  const out = new Map<string, string>()
  let i = skip(text, 0)
  if (text[i] !== '{') throw new Error('expected a JSON object')
  i = skip(text, i + 1)
  if (text[i] === '}') return out
  for (;;) {
    const kEnd = valueEnd(text, i)
    const name = JSON.parse(text.slice(i, kEnd)) as string
    i = skip(text, kEnd)
    i = skip(text, i + 1) // ':'
    const vEnd = valueEnd(text, i)
    out.set(name, text.slice(i, vEnd))
    i = skip(text, vEnd)
    if (text[i] === '}') return out
    i = skip(text, i + 1)
  }
}

export function rawElements(text: string): string[] {
  const s = new JsonArray()
  return [...s.feed(text), ...s.end()]
}

export function csvRows(text: string): string[][] {
  const s = new Csv()
  return [...s.feed(text), ...s.end()]
}

// ─── Records ─────────────────────────────────────────────

// Text of a JSON value as a payload: strings unquoted, null as a null
// payload, anything else verbatim.
function payload(raw: string | undefined): string | null | undefined {
  if (raw === undefined) return undefined
  if (raw === 'null') return null
  if (raw.startsWith('"')) return JSON.parse(raw) as string
  return raw
}

function timestampOf(v: unknown): number | null {
  if (typeof v === 'number' && Number.isFinite(v)) return v
  if (typeof v === 'string' && v.trim()) {
    const n = Number(v)
    if (Number.isFinite(n)) return n
    const d = Date.parse(v)
    if (!Number.isNaN(d)) return d
    throw new Error(`timestamp ${JSON.stringify(v)} is not a date or unix milliseconds`)
  }
  return null
}

function headersOf(v: unknown): Header[] {
  if (v === undefined || v === null || v === '') return []
  if (typeof v === 'string') return headersOf(JSON.parse(v))
  if (Array.isArray(v)) {
    return v.map((h, i) => {
      if (!h || typeof h !== 'object' || typeof h.key !== 'string') throw new Error(`header ${i + 1} has no key`)
      if (h.null) return { key: h.key, value: '', encoding: 'utf8', null: true }
      if (typeof h.value !== 'string') throw new Error(`header ${h.key}: value must be a string`)
      return { key: h.key, value: h.value, encoding: h.encoding === 'base64' ? 'base64' : 'utf8' }
    })
  }
  if (typeof v === 'object') {
    return Object.entries(v as Record<string, unknown>).map(([key, value]) =>
      value === null ? { key, value: '', encoding: 'utf8', null: true } : { key, value: typeof value === 'string' ? value : JSON.stringify(value), encoding: 'utf8' })
  }
  throw new Error('headers must be a list or an object')
}

function partitionOf(v: unknown): number | null {
  if (v === undefined || v === null || v === '') return null
  const n = Number(v)
  if (!Number.isInteger(n) || n < 0) throw new Error(`partition ${JSON.stringify(v)} is not a non-negative integer`)
  return n
}

const schemaFormats = new Set(['avro', 'protobuf', 'json-schema'])

// A record object as written by Export (JSON) or by hand.
function fromObject(raw: string): ImportRecord {
  const f = rawFields(raw)
  const get = (k: string) => (f.has(k) ? JSON.parse(f.get(k)!) : undefined)
  if (get('truncated') === true) throw new Error('the value was truncated when exported; export it again from the message detail')
  if (get('decode_error')) throw new Error('the record could not be decoded when exported; export the raw record instead')
  if (!f.has('value') && !f.has('key')) throw new Error('no "key" or "value" field; switch “Each item is” to Value')

  const rec: ImportRecord = {
    key: payload(f.get('key')) ?? null,
    value: payload(f.get('value')) ?? null,
    headers: headersOf(get('headers')),
    partition: partitionOf(get('partition')),
    timestamp: timestampOf(get('timestamp')),
  }
  // kafka-mcp files have one "encoding": a transform_messages item (it names a
  // topic) uses it for key and value; an older search_messages line, for the
  // value only.
  const shared = get('encoding') === 'base64'
  if (rec.key !== null && (get('key_encoding') === 'base64' || (shared && f.has('topic') && !f.has('key_encoding')))) rec.key_encoding = 'base64'
  if (rec.value !== null && (get('value_encoding') === 'base64' || (shared && !f.has('value_encoding')))) rec.value_encoding = 'base64'
  for (const side of ['key', 'value'] as const) {
    const id = get(`${side}_schema_id`)
    if (typeof id === 'number' && id > 0 && (get(`${side}_format`) === undefined || schemaFormats.has(get(`${side}_format`)))) {
      if (get(`${side}_format`) === 'protobuf' && !get(`${side}_schema_message`)) throw new Error(`${side} is Protobuf but has no message type`)
      rec[`${side}_schema`] = { id, message: get(`${side}_schema_message`) || undefined }
    }
  }
  return rec
}

const valueOnly = (value: string): ImportRecord => ({ key: null, value, headers: [] })

function fromJSON(item: string, shape: ItemShape): ImportRecord {
  const t = item.trim()
  JSON.parse(t)
  if (shape === 'value') return valueOnly(t.startsWith('"') ? (JSON.parse(t) as string) : t)
  if (!t.startsWith('{')) throw new Error('not a JSON object; switch “Each item is” to Value')
  return fromObject(t)
}

// RecordReader turns a file, fed in chunks, into numbered items.
export class RecordReader {
  private n = 0
  private lines?: Lines
  private csv?: Csv
  private json?: JsonArray
  private cols?: number[]
  private format: FileFormat
  private shape: ItemShape

  constructor(format: FileFormat, shape: ItemShape) {
    this.format = format
    this.shape = shape
    if (format === 'csv') this.csv = new Csv()
    else if (format === 'json') this.json = new JsonArray()
    else this.lines = new Lines()
  }

  feed(chunk: string): Item[] {
    if (this.json) return this.fromJSON(this.json.feed(chunk))
    if (this.csv) return this.fromRows(this.csv.feed(chunk))
    return this.fromLines(this.lines!.feed(chunk))
  }

  end(): Item[] {
    if (this.json) return this.fromJSON(this.json.end())
    if (this.csv) {
      const items = this.fromRows(this.csv.end())
      if (!this.cols) throw new FatalError('The file is empty.')
      return items
    }
    return this.fromLines(this.lines!.end())
  }

  private make(fn: () => ImportRecord): Item {
    const item = ++this.n
    try {
      return { item, record: fn() }
    } catch (e: any) {
      return { item, problem: e.message }
    }
  }

  private fromJSON(items: string[]) {
    return items.map((t) => this.make(() => fromJSON(t, this.shape)))
  }

  private fromLines(lines: string[]) {
    const out: Item[] = []
    for (const line of lines) {
      if (this.format === 'jsonl' ? !line.trim() : !line) continue
      out.push(this.make(() => (this.format === 'jsonl' ? fromJSON(line, this.shape) : valueOnly(line))))
    }
    return out
  }

  private fromRows(rows: string[][]) {
    const out: Item[] = []
    for (const row of rows) {
      if (!this.cols) {
        const head = row.map((h) => h.trim().toLowerCase())
        this.cols = ['key', 'value', 'headers', 'partition', 'timestamp', 'key_encoding', 'value_encoding',
          'key_null', 'value_null', 'truncated', 'key_format', 'value_format', 'key_schema_id', 'value_schema_id',
          'key_schema_message', 'value_schema_message', 'decode_error'].map((name) => head.indexOf(name))
        if (this.cols[1] < 0) {
          throw new FatalError('The CSV has no "value" column. The first row must name the columns: key, value, headers, partition, timestamp.')
        }
        continue
      }
      const [k, v, h, p, ts, ke, ve, kn, vn, cut, kf, vf, ks, vs, km, vm, de] = this.cols
      out.push(this.make(() => {
        const flag = (index: number, name: string) => {
          // Spreadsheets save booleans as TRUE/FALSE.
          const value = index < 0 ? '' : row[index]?.trim().toLowerCase() ?? ''
          if (value && value !== 'true' && value !== 'false') throw new Error(`${name} must be true or false`)
          return value === 'true'
        }
        const schemaID = (index: number, name: string) => {
          const value = index < 0 ? '' : row[index]?.trim() ?? ''
          if (!value) return undefined
          const id = Number(value)
          if (!Number.isSafeInteger(id) || id <= 0) throw new Error(`${name} must be a positive integer`)
          return id
        }
        return fromObject(JSON.stringify({
          // Legacy exports have no null flags: keep their old interpretation.
          key: flag(kn, 'key_null') || (kn < 0 && !row[k]) ? null : row[k] ?? '',
          value: flag(vn, 'value_null') ? null : row[v] ?? '',
          headers: h >= 0 ? headersOf(row[h]) : [],
          partition: p >= 0 ? partitionOf(row[p]) : null,
          timestamp: ts >= 0 ? timestampOf(row[ts]) : null,
          key_encoding: row[ke]?.trim(), value_encoding: row[ve]?.trim(),
          truncated: flag(cut, 'truncated'), decode_error: row[de],
          key_format: row[kf] || undefined, value_format: row[vf] || undefined,
          key_schema_id: schemaID(ks, 'key_schema_id'),
          value_schema_id: schemaID(vs, 'value_schema_id'),
          key_schema_message: row[km], value_schema_message: row[vm],
        }))
      }))
    }
    return out
  }
}

// Collects items into a Parsed summary.
export class Summary implements Parsed {
  records: ImportRecord[] = []
  problems: { item: number; message: string }[] = []
  partitions = false
  timestamps = false
  schemaIds = false

  add(it: Item) {
    if (it.problem !== undefined) {
      this.problems.push({ item: it.item, message: it.problem })
      return
    }
    const r = it.record!
    this.records.push(r)
    if (r.partition !== null && r.partition !== undefined) this.partitions = true
    if (r.timestamp !== null && r.timestamp !== undefined) this.timestamps = true
    if (r.key_schema || r.value_schema) this.schemaIds = true
  }
}

export function parseFile(text: string, format: FileFormat, shape: ItemShape): Parsed {
  const out = new Summary()
  const reader = new RecordReader(format, shape)
  try {
    for (const it of reader.feed(text)) out.add(it)
    for (const it of reader.end()) out.add(it)
  } catch (e: any) {
    if (!(e instanceof FatalError)) throw e
    out.problems.push({ item: 0, message: e.message })
  }
  return out
}

// readFile streams a file as items without holding it in memory. onBytes
// reports how many bytes have been read so far.
export async function* readFile(file: Blob, format: FileFormat, shape: ItemShape, onBytes?: (n: number) => void): AsyncGenerator<Item> {
  const reader = new RecordReader(format, shape)
  let read = 0
  const counted = file.stream().pipeThrough(
    new TransformStream<Uint8Array, Uint8Array>({
      transform(chunk, ctl) {
        read += chunk.byteLength
        ctl.enqueue(chunk)
      },
    }),
  )
  const text = counted.pipeThrough(new TextDecoderStream() as unknown as ReadableWritablePair<string, Uint8Array>).getReader()
  try {
    for (;;) {
      const { value, done } = await text.read()
      if (done) break
      onBytes?.(read)
      yield* reader.feed(value)
    }
    yield* reader.end()
  } finally {
    text.cancel().catch(() => {})
  }
}
