import { responseError } from './http.ts'

export type Cluster = {
  name: string
  brokers: string[]
  read_only: boolean
  // Writes ask for the target topic's name before sending.
  confirm_writes: boolean
  // "#rrggbb" or a CSS color name, tinting the UI while the cluster is open.
  color?: string
  schema_registry: boolean
  // Topic documentation (Markdown files) is configured.
  docs?: boolean
  // HTML rendered server-side from the configured Markdown description.
  description?: string
  // The cluster's features: its own settings over the global ones.
  features: Features
  // MCP endpoints for agents; empty when MCP is off. Paths include base_path.
  mcp?: MCPEndpoint[]
}

export type MCPEndpoint = { name: string; path: string; read_only: boolean; auth_token: boolean }

export type TopicDoc = { html: string; source: string; file: string; modified: string }

export type Features = {
  security: boolean
  create_topic: boolean
  delete_topic: boolean
  alter_topic: boolean
  delete_records: boolean
  delete_group: boolean
  alter_broker: boolean
}

export type AppInfo = {
  info: string
  version: string
  features: Features
  /** The most messages one read returns. */
  max_limit?: number
}

export type Broker = {
  id: number
  host: string
  port: number
  rack?: string
  controller: boolean
  leaders: number
  replicas: number
  version?: string
}

export type LogDir = {
  broker: number
  dir: string
  size: number
  total_bytes: number
  usable_bytes: number
  partitions: number
  error?: string
}

export type BrokerDetail = Broker & { configs: ConfigEntry[]; log_dirs: LogDir[] }

export type Overview = {
  cluster: string
  cluster_id: string
  brokers: Broker[]
  controller: number
  topics: number
  internal_topics: number
  partitions: number
  under_replicated: number
  offline_partitions: number
  groups: number
  has_registry: boolean
  // Only topics with an unhealthy partition; one char per partition, o/u/x.
  problems: { topic: string; partitions: string }[]
  at: string
}

export type DiskUsage = { size: number; at: string }

export type TopicName = { name: string; internal: boolean; partitions: number }

export type Names = { topics: TopicName[]; groups: string[]; subjects: string[] }

export type TopicSummary = {
  name: string
  internal: boolean
  partitions: number
  replication_factor: number
  under_replicated: number
  messages: number
  size: number
  replicated_size: number
  cleanup_policy?: string
}

export type TopicPage = { topics: TopicSummary[]; total: number }

export type TopicQuery = {
  q?: string
  internal?: boolean
  sort?: 'name' | 'partitions' | 'messages' | 'size' | 'under_replicated'
  desc?: boolean
  offset?: number
  limit?: number
}

export type Partition = {
  id: number
  leader: number
  replicas: number[]
  isr: number[]
  offline_replicas: number[]
  low: number
  high: number
  size: number
  error?: string
}

export type Reassignment = { partition: number; replicas: number[] }
export type ReassignResult = { partition: number; from: number[]; to: number[]; error?: string }
export type PartitionMove = { partition: number; replicas: number[]; adding: number[]; removing: number[] }

export type ConfigEntry = { key: string; value: string | null; sensitive: boolean; source: string }

export type TopicDetail = {
  name: string
  id: string
  internal: boolean
  partitions: Partition[]
  configs: ConfigEntry[] | null
  groups: string[] | null
  size: number
  replicated_size: number
}

export type Header = { key: string; value: string; encoding: string; null?: boolean }

export type Message = {
  partition: number
  offset: number
  timestamp: string
  key: string | null
  key_encoding?: string
  key_format?: string
  key_schema_id?: number
  key_schema_message?: string
  value: string | null
  value_encoding?: string
  value_format?: string
  value_schema_id?: number
  value_schema_message?: string
  value_bytes: number
  key_bytes: number
  truncated: boolean
  headers: Header[]
  decode_error?: string
}

export type TailEvent = {
  message?: Message
  scanned: number
  matched: number
  error?: string
  script_errors?: number
  first_error?: string
}

export type ConfigChange = { key: string; value?: string | null; op?: 'set' | 'delete' }

export type ACLResourceType = 'TOPIC' | 'GROUP' | 'CLUSTER' | 'TRANSACTIONAL_ID'
export type ACLPatternType = 'LITERAL' | 'PREFIXED'
export type ACLPermission = 'ALLOW' | 'DENY'

// Listed ACLs carry Kafka's own names, which include kinds this console
// never creates (USER and DELEGATION_TOKEN resources, MATCH patterns).
export type ACL = {
  principal: string
  host: string
  resource_type: ACLResourceType | (string & {})
  resource_name: string
  pattern_type: ACLPatternType | (string & {})
  operation: string
  permission: ACLPermission | (string & {})
}

export type NewACL = {
  principal: string
  host: string
  resource_type: ACLResourceType
  resource_name: string
  pattern_type: ACLPatternType
  operation: string
  permission: ACLPermission
}

export type User = { name: string; mechanisms: string[] }

export type QuotaEntity = { type: string; name: string | null }
export type Quota = { entity: QuotaEntity[]; values: Record<string, number> }

export type SubjectSummary = {
  name: string
  type: string
  latest_version: number
  latest_id: number
  versions: number
  compatibility?: string
  deleted: boolean
}

export type RegistryOverview = { mode: string; compatibility: string; types: string[]; contexts?: string[]; subjects: SubjectSummary[]; total: number }

export type RegistryQuery = { q?: string; type?: string; context?: string; deleted?: boolean; offset?: number; limit?: number }

export type SchemaReference = { name: string; subject: string; version: number }

export type SchemaVersion = {
  subject: string
  version: number
  id: number
  type: string
  schema: string
  references: SchemaReference[]
  deleted: boolean
  messages?: string[]
}

export type SubjectDetail = {
  name: string
  compatibility: string
  override: boolean
  mode: string
  versions: SchemaVersion[]
  referred_by: string[]
  // Existing topics this subject is named after (key, value or record).
  topics: { topic: string; role: string }[]
}

export type EncodeSchema = { subject?: string; version?: number; id?: number; message?: string }

export type ProduceBody = {
  key: string | null
  value: string | null
  headers: { key: string; value: string; encoding?: string; null?: boolean }[]
  partition: number | null
  key_schema?: EncodeSchema | null
  value_schema?: EncodeSchema | null
  key_encoding?: string
  value_encoding?: string
  compression?: string
}

export type ConsumeResult = {
  messages: Message[]
  scanned: number
  script_errors: number
  first_error?: string
  stopped_by: string
  elapsed_ms: number
  // Where to continue, per partition; empty once everything was read.
  next?: Record<string, number>
}

export type GroupSummary = {
  name: string
  state: string
  protocol_type: string
  members: number
  topics: string[]
  lag: number
  error?: string
  unreadable?: string[]
}

export type GroupPage = { groups: GroupSummary[]; total: number }

export type GroupQuery = { q?: string; sort?: 'name' | 'lag' | 'members'; desc?: boolean; offset?: number; limit?: number }

export type Member = { id: string; instance_id?: string; client_id: string; host: string }

export type PartitionLag = {
  topic: string
  partition: number
  low: number
  high: number
  committed: number
  lag: number
  member_id?: string
  client_id?: string
  host?: string
  error?: string
  no_access?: boolean
}

export type GroupDetail = {
  name: string
  state: string
  protocol_type: string
  protocol: string
  coordinator: number
  members: Member[]
  partitions: PartitionLag[]
  lag: number
}

export type TopicConsumer = {
  group: string
  state: string
  members: number
  lag: number
  partitions: PartitionLag[]
  error?: string
}

export type ResetPlan = { partition: number; from: number; to: number; low: number; high: number }

// offsets commits the listed offsets as they are (restore, copy a group);
// a body with plan commits a reviewed preview.
export type ResetStrategy = 'earliest' | 'latest' | 'offset' | 'offsets' | 'timestamp' | 'shift'
export type PartitionOffset = { partition: number; offset: number }
export type ResetOffsetsBody = {
  topic: string
  partitions?: number[]
  strategy?: ResetStrategy
  plan?: ResetPlan[]
  offset?: number
  timestamp?: number
  shift?: number
  offsets?: PartitionOffset[]
  dry_run: boolean
}
export type ResetOffsetsResult = { dry_run: boolean; partitions: ResetPlan[] }

export type DeleteRecordsStrategy = 'all' | 'offset' | 'timestamp'
export type DeleteRecordsBody = { partitions?: number[]; strategy: DeleteRecordsStrategy; offset?: number; timestamp?: number }
export type DeleteRecordsResult = { partition: number; low_watermark: number; error?: string }

export type CreateTopicBody = { name: string; partitions: number; replication_factor: number; configs?: Record<string, string> }
export type ProduceResult = { partition: number; offset: number; timestamp: string }
export type UserBody = { name: string; password: string; mechanism: string }
export type QuotaChange = { entity: QuotaEntity[]; set?: Record<string, number>; remove?: string[] }
export type SchemaBody = { subject: string; type: string; schema: string; references?: SchemaReference[]; normalize?: boolean }
export type SchemaCheck = { compatible: boolean; messages: string[] }
export type CreateGroupBody = { group: string; topic: string; offsets: PartitionOffset[] }
export type AddGroupTopicBody = { topic: string; offsets: PartitionOffset[] }
export type ReassignBody = { partitions: Reassignment[]; cancel?: boolean; dry_run?: boolean }

// Every read is tied to the page that started it. Navigating away aborts the
// page's scope, so a slow listing or search the user left behind stops on
// the server too (handlers pass the request context into Kafka).
let pageScope = new AbortController()

/** Abort every request of the current page and start a new scope. */
export function leavePage() {
  pageScope.abort(new DOMException('Left the page', 'AbortError'))
  pageScope = new AbortController()
}

/** True for the error a fetch throws when its request was aborted. */
export function isAbort(e: unknown): boolean {
  return e instanceof DOMException && e.name === 'AbortError'
}

// Extra signal for the api.* calls started synchronously inside withSignal.
// A module variable rather than a parameter so every api.* method takes it
// without growing an options argument. JavaScript runs one call stack at a
// time, so it is only ever set while fn runs synchronously and is restored
// before withSignal returns; nothing started after an await sees it.
let scoped: AbortSignal | undefined

/**
 * Bind the api.* calls that fn starts to signal, on top of the page scope,
 * so one part of a page (a search, a list reload) can be cancelled on its
 * own. Only calls made before fn's first await pick the signal up, so keep
 * fn to a direct api call.
 */
export function withSignal<T>(signal: AbortSignal, fn: () => Promise<T>): Promise<T> {
  const prev = scoped
  scoped = signal
  try {
    return fn()
  } finally {
    scoped = prev
  }
}

async function request<T>(method: string, path: string, body?: unknown, opts: { app?: boolean } = {}): Promise<T> {
  // Reads belong to the page and stop with it. Writes are never aborted on
  // navigation: the server may already be applying them, and the user would
  // lose the result. App-wide reads (info, cluster list, names) outlive pages.
  const page = method === 'GET' && !opts.app
  const scope = pageScope.signal
  const signals = [page ? scope : undefined, scoped].filter((s): s is AbortSignal => !!s)
  const signal = signals.length > 1 ? AbortSignal.any(signals) : signals[0]
  // Cancellation always settles. Callers ignore AbortError and guard stale
  // state; keeping a request pending forever would skip their cleanup.
  const res = await fetch(`${BASE}${path}`, {
    method,
    headers: body === undefined ? undefined : { 'content-type': 'application/json' },
    body: body === undefined ? undefined : JSON.stringify(body),
    signal,
  })
  signal?.throwIfAborted()

  if (!res.ok) {
    const err = await responseError(res)
    signal?.throwIfAborted()
    throw err
  }
  if (res.status === 204) return undefined as T

  const text = await res.text()
  signal?.throwIfAborted()
  try {
    return (text ? JSON.parse(text) : undefined) as T
  } catch {
    return text as T
  }
}

const enc = encodeURIComponent
// Relative: resolves against the page URL, so it follows the server base_path.
const BASE = 'api/v1'
const cl = (c: string) => `/clusters/${enc(c)}`
const tp = (c: string, t: string) => `${cl(c)}/topics/${enc(t)}`

/** Link that downloads a record's key or value bytes exactly as stored. */
export function rawURL(c: string, t: string, partition: number, offset: number, part: 'key' | 'value') {
  return `${BASE}${tp(c, t)}/messages/${partition}/${offset}/raw?part=${part}`
}

function query(params: Record<string, string | number | boolean | undefined>) {
  const q = new URLSearchParams()
  for (const [k, v] of Object.entries(params)) if (v !== undefined && v !== '' && v !== false) q.set(k, String(v))
  return q.toString()
}

// tail opens a server-sent event stream; the returned function closes it.
export function tail(
  c: string,
  t: string,
  params: Record<string, string | number | boolean | undefined>,
  on: { event: (e: TailEvent) => void; error: (msg: string) => void; end: () => void },
) {
  const url = `${BASE}${tp(c, t)}/tail?${query(params)}`
  const es = new EventSource(url)
  let closed = false
  // Set by the caller's close: nothing of this stream reaches it after.
  let stopped = false
  const probe = new AbortController()
  const close = () => {
    closed = true
    es.close()
  }
  es.addEventListener('message', (e) => {
    if (stopped) return
    let event: TailEvent
    try {
      event = JSON.parse((e as MessageEvent).data)
    } catch {
      // A garbled event ends the stream as an error instead of throwing
      // out of the listener, where nothing would report it.
      close()
      on.error('The live tail sent an unreadable event')
      return
    }
    on.event(event)
  })
  es.addEventListener('end', () => {
    close()
    if (!stopped) on.end()
  })
  es.addEventListener('error', (e) => {
    if (closed) return
    const data = (e as MessageEvent).data
    close()
    if (data) {
      try {
        if (!stopped) on.error(JSON.parse(data).error)
        return
      } catch {}
    }
    // Validation failures come back as plain HTTP errors, which EventSource
    // cannot read; fetch once to surface the message. A 200 means the stream
    // itself works (the error was the connection), so it is closed at once
    // instead of being left open as a second, unread tail.
    fetch(url, { signal: probe.signal })
      .then((r) => (r.ok ? '' : responseError(r).then((e) => e.message)))
      .catch(() => '')
      .then((msg) => {
        probe.abort()
        if (!stopped) on.error(msg || 'Connection lost')
      })
  })
  return () => {
    stopped = true
    probe.abort()
    close()
  }
}

export type ResendBody = { records: PartitionOffset[]; target: string; keep_partition: boolean; mark_source: boolean }

export type ResendResult = {
  source: { partition: number; offset: number }
  partition: number
  offset: number
  error?: string
}

export type FilterProblem = { line: number; column: number; message: string }

export type RangeBound = { type: string; offset?: number; timestamp?: number }

export type ReplayBody = {
  plan?: ReplayRange[]
  partitions?: number[]
  from: RangeBound
  to: RangeBound
  search?: string
  filter?: string
  target: string
  keep_partition: boolean
  keep_timestamp: boolean
  mark_source: boolean
  limit?: number
  exact?: boolean
  dry_run: boolean
}

export type ReplayRange = { partition: number; start: number; end: number }

export type ReplayEvent = {
  type: 'plan' | 'progress' | 'done'
  plan?: ReplayRange[]
  total: number
  scanned: number
  matched: number
  estimated?: boolean
  written: number
  failed: number
  script_errors: number
  first_error?: string
  stopped_by?: string
  elapsed_ms: number
}

export type ImportRecord = {
  key: string | null
  value: string | null
  headers: Header[]
  partition?: number | null
  key_schema?: EncodeSchema | null
  value_schema?: EncodeSchema | null
  key_encoding?: string
  value_encoding?: string
  timestamp?: number | null
}

export type ImportBody = {
  records: ImportRecord[]
  keep_partition: boolean
  keep_timestamp: boolean
  compression?: string
  dry_run: boolean
}

export type ImportResult = { partition: number; offset: number; error?: string }

// replay streams newline-delimited events until the replay ends or signal
// aborts, which also stops it on the server.
export async function replay(c: string, t: string, body: ReplayBody, onevent: (e: ReplayEvent) => void, signal?: AbortSignal) {
  const res = await fetch(`${BASE}${tp(c, t)}/replay`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
    signal,
  })
  await readLines(res, onevent)
}

export type MessageStreamEvent =
  | { type: 'message'; message: Message; scanned: number }
  | { type: 'progress'; scanned: number; next?: Record<string, number> }
  | { type: 'done'; scanned: number; result: ConsumeResult }
  | { type: 'error'; scanned: number; error: string }

// messageStream reads messages from the messages endpoint, handing each match over
// as soon as the server finds it. Aborting signal stops the read on the
// server too. Like other reads, it stops when its page is left.
export async function messageStream(
  c: string,
  t: string,
  params: Record<string, string | number | boolean | undefined>,
  onevent: (e: MessageStreamEvent) => void,
  signal: AbortSignal,
) {
  const res = await fetch(`${BASE}${tp(c, t)}/messages?${query({ ...params, stream: true })}`, {
    signal: AbortSignal.any([signal, pageScope.signal]),
  })
  await readLines(res, onevent)
}

// readLines parses a newline-delimited JSON response, or throws its error.
async function readLines<T>(res: Response, onevent: (e: T) => void) {
  if (!res.ok || !res.body) throw await responseError(res)
  const reader = res.body.pipeThrough(new TextDecoderStream()).getReader()
  let buf = ''
  for (;;) {
    const { value, done } = await reader.read()
    if (done) break
    buf += value
    let nl
    while ((nl = buf.indexOf('\n')) >= 0) {
      const line = buf.slice(0, nl).trim()
      buf = buf.slice(nl + 1)
      if (line) onevent(JSON.parse(line))
    }
  }
}

export const api = {
  resend: (c: string, t: string, body: ResendBody) => request<ResendResult[]>('POST', `${tp(c, t)}/resend`, body),
  importRecords: (c: string, t: string, body: ImportBody) =>
    request<ImportResult[]>('POST', `${tp(c, t)}/import`, body),
  checkFilter: (source: string) => request<{ problems: FilterProblem[] }>('POST', '/filter/check', { source }),
  info: () => request<AppInfo>('GET', '/info', undefined, { app: true }),
  clusters: () => request<Cluster[]>('GET', '/clusters', undefined, { app: true }),
  // Shared and cached across pages (⌘K, pickers), so not tied to one page.
  names: (c: string) => request<Names>('GET', `${cl(c)}/names`, undefined, { app: true }),
  overview: (c: string, fresh = false) => request<Overview>('GET', `${cl(c)}/overview${fresh ? '?fresh=true' : ''}`),
  diskUsage: (c: string, fresh = false) => request<DiskUsage>('GET', `${cl(c)}/disk-usage${fresh ? '?fresh=true' : ''}`),
  topicDoc: (c: string, t: string) => request<TopicDoc>('GET', `${tp(c, t)}/docs`),
  balancePlan: (c: string, t: string) => request<Reassignment[]>('GET', `${tp(c, t)}/balance-plan`),
  reassignments: (c: string, t: string) => request<PartitionMove[]>('GET', `${tp(c, t)}/reassignments`),
  reassign: (c: string, t: string, body: ReassignBody) =>
    request<ReassignResult[]>('POST', `${tp(c, t)}/reassign`, body),
  electLeaders: (c: string, t: string, partitions: number[] = []) =>
    request<ReassignResult[]>('POST', `${tp(c, t)}/elect-leaders`, { partitions }),
  topics: (c: string, q: TopicQuery = {}) => request<TopicPage>('GET', `${cl(c)}/topics?${query(q)}`),
  topic: (c: string, t: string) => request<TopicDetail>('GET', `${tp(c, t)}`),
  topicConsumers: (c: string, t: string) => request<TopicConsumer[]>('GET', `${tp(c, t)}/consumers`),
  createTopic: (c: string, body: CreateTopicBody) =>
    request<{ name: string }>('POST', `${cl(c)}/topics`, body),
  deleteTopic: (c: string, t: string) => request<void>('DELETE', `${tp(c, t)}`),
  alterTopicConfigs: (c: string, t: string, changes: ConfigChange[]) =>
    request<void>('PATCH', `${tp(c, t)}/configs`, changes),
  addPartitions: (c: string, t: string, total: number) =>
    request<void>('POST', `${tp(c, t)}/partitions`, { total }),
  deleteRecords: (c: string, t: string, body: DeleteRecordsBody) =>
    request<DeleteRecordsResult[]>('POST', `${tp(c, t)}/delete-records`, body),
  message: (c: string, t: string, partition: number, offset: number) =>
    request<Message>('GET', `${tp(c, t)}/messages/${partition}/${offset}`),
  produce: (c: string, t: string, body: ProduceBody) =>
    request<ProduceResult>('POST', `${tp(c, t)}/messages`, body),
  broker: (c: string, id: number) => request<BrokerDetail>('GET', `${cl(c)}/brokers/${id}`),
  alterBrokerConfigs: (c: string, id: number, changes: ConfigChange[]) =>
    request<void>('PATCH', `${cl(c)}/brokers/${id}/configs`, changes),
  deleteGroupOffsets: (c: string, g: string, topic: string, partitions?: number[]) =>
    request<void>('POST', `${cl(c)}/groups/${enc(g)}/delete-offsets`, { topic, partitions }),

  acls: (c: string) => request<ACL[]>('GET', `${cl(c)}/acls`),
  // The ACLs Kafka checks for one resource: exact, covering prefixes and *.
  resourceACLs: (c: string, type: Extract<ACLResourceType, 'TOPIC' | 'GROUP'>, name: string) =>
    request<ACL[]>('GET', `${cl(c)}/acls?${query({ resource_type: type, resource_name: name })}`),
  createACLs: (c: string, acls: NewACL[]) => request<void>('POST', `${cl(c)}/acls`, acls),
  deleteACL: (c: string, acl: ACL) => request<{ deleted: number }>('POST', `${cl(c)}/acls/delete`, acl),
  users: (c: string) => request<User[]>('GET', `${cl(c)}/users`),
  upsertUser: (c: string, body: UserBody) =>
    request<void>('POST', `${cl(c)}/users`, body),
  deleteUser: (c: string, name: string) => request<void>('DELETE', `${cl(c)}/users/${enc(name)}`),
  quotas: (c: string) => request<Quota[]>('GET', `${cl(c)}/quotas`),
  alterQuota: (c: string, body: QuotaChange) =>
    request<void>('POST', `${cl(c)}/quotas`, body),

  registry: (c: string, q: RegistryQuery = {}) => request<RegistryOverview>('GET', `${cl(c)}/schemas?${query(q)}`),
  subject: (c: string, s: string) => request<SubjectDetail>('GET', `${cl(c)}/schemas/subjects/${enc(s)}`),
  createSchema: (c: string, body: SchemaBody) =>
    request<SchemaVersion>('POST', `${cl(c)}/schemas`, body),
  checkSchema: (c: string, body: SchemaBody) =>
    request<SchemaCheck>('POST', `${cl(c)}/schemas/check`, body),
  setGlobalCompatibility: (c: string, level: string) =>
    request<void>('PUT', `${cl(c)}/schemas/compatibility`, { level }),
  setCompatibility: (c: string, s: string, level: string) =>
    request<void>('PUT', `${cl(c)}/schemas/subjects/${enc(s)}/compatibility`, { level }),
  deleteSubject: (c: string, s: string, permanent: boolean) =>
    request<void>('DELETE', `${cl(c)}/schemas/subjects/${enc(s)}?${query({ permanent })}`),
  deleteSchemaVersion: (c: string, s: string, v: number, permanent: boolean) =>
    request<void>('DELETE', `${cl(c)}/schemas/subjects/${enc(s)}/versions/${v}?${query({ permanent })}`),
  groups: (c: string, q: GroupQuery = {}) => request<GroupPage>('GET', `${cl(c)}/groups?${query(q)}`),
  group: (c: string, g: string) => request<GroupDetail>('GET', `${cl(c)}/groups/${enc(g)}`),
  createGroup: (c: string, body: CreateGroupBody) =>
    request<ResetPlan[]>('POST', `${cl(c)}/groups`, body),
  addGroupTopic: (c: string, g: string, body: AddGroupTopicBody) =>
    request<ResetPlan[]>('POST', `${cl(c)}/groups/${enc(g)}/topics`, body),
  deleteGroup: (c: string, g: string) => request<void>('DELETE', `${cl(c)}/groups/${enc(g)}`),
  resetOffsets: (c: string, g: string, body: ResetOffsetsBody) => request<ResetOffsetsResult>('POST', `${cl(c)}/groups/${enc(g)}/reset`, body),
}
