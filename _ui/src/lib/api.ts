export type Cluster = {
  name: string
  brokers: string[]
  read_only: boolean
  production: boolean
  schema_registry: boolean
  // Topic documentation (Markdown files) is configured.
  docs?: boolean
}

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
  features: Features
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
  size: number
  has_registry: boolean
  // Only topics with an unhealthy partition; one char per partition, o/u/x.
  problems: { topic: string; partitions: string }[]
}

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

export type TailEvent = { message?: Message; scanned: number; matched: number; error?: string }

export type ConfigChange = { key: string; value?: string | null; op?: 'set' | 'delete' }

export type ACL = {
  principal: string
  host: string
  resource_type: string
  resource_name: string
  pattern_type: string
  operation: string
  permission: string
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

export type GroupQuery = { q?: string; sort?: 'name' | 'lag'; offset?: number; limit?: number }

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
  let res: Response
  try {
    // Relative: resolves against the page URL, so it follows the server base_path.
    res = await fetch(`api/v1${path}`, {
      method,
      headers: body === undefined ? undefined : { 'content-type': 'application/json' },
      body: body === undefined ? undefined : JSON.stringify(body),
      signal,
    })
  } catch (e) {
    // Aborted because its page was left: the page is gone, so settle never.
    // Its catch would only flash "aborted" over the next page, and its
    // finally has nothing left to update. Other aborts (a Stop button, a
    // newer search) reject normally so their own code can tell; see isAbort.
    if (page && scope.aborted && isAbort(e)) return new Promise<T>(() => {})
    throw e
  }

  if (res.status === 204) return undefined as T

  let text: string
  try {
    text = await res.text()
  } catch (e) {
    if (page && scope.aborted && isAbort(e)) return new Promise<T>(() => {})
    throw e
  }
  let data: any = undefined
  try {
    data = text ? JSON.parse(text) : undefined
  } catch {
    data = text
  }

  if (!res.ok) {
    const message = (data && typeof data === 'object' && data.message) || text || res.statusText
    throw new Error(message)
  }

  return data as T
}

const enc = encodeURIComponent

/** Link that downloads a record's key or value bytes exactly as stored. */
export function rawURL(c: string, t: string, partition: number, offset: number, part: 'key' | 'value') {
  return `api/v1/clusters/${enc(c)}/topics/${enc(t)}/messages/${partition}/${offset}/raw?part=${part}`
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
  const url = `api/v1/clusters/${enc(c)}/topics/${enc(t)}/tail?${query(params)}`
  const es = new EventSource(url)
  let closed = false
  const close = () => {
    closed = true
    es.close()
  }
  es.addEventListener('message', (e) => on.event(JSON.parse((e as MessageEvent).data)))
  es.addEventListener('end', () => {
    close()
    on.end()
  })
  es.addEventListener('error', (e) => {
    if (closed) return
    const data = (e as MessageEvent).data
    close()
    if (data) {
      try {
        on.error(JSON.parse(data).error)
        return
      } catch {}
    }
    // Validation failures come back as plain HTTP errors, which EventSource
    // cannot read; fetch once to surface the message. A 200 means the stream
    // itself works (the error was the connection), so it is closed at once
    // instead of being left open as a second, unread tail.
    const probe = new AbortController()
    fetch(url, { signal: probe.signal })
      .then((r) => (r.ok ? '' : r.json().then((j) => j.message ?? r.statusText)))
      .catch(() => '')
      .then((msg) => {
        probe.abort()
        on.error(msg || 'Connection lost')
      })
  })
  return close
}

export type ResendResult = {
  source: { partition: number; offset: number }
  partition: number
  offset: number
  error?: string
}

export type FilterProblem = { line: number; column: number; message: string }

export type RangeBound = { type: string; offset?: number; timestamp?: number }

export type ReplayBody = {
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
  dry_run: boolean
}

export type ReplayRange = { partition: number; start: number; end: number }

export type ReplayEvent = {
  type: 'plan' | 'progress' | 'done'
  plan?: ReplayRange[]
  total: number
  scanned: number
  matched: number
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
  const res = await fetch(`api/v1/clusters/${enc(c)}/topics/${enc(t)}/replay`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
    signal,
  })
  if (!res.ok || !res.body) {
    const text = await res.text()
    let message = text || res.statusText
    try {
      message = JSON.parse(text).message ?? message
    } catch {}
    throw new Error(message)
  }
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
  resend: (
    c: string,
    t: string,
    body: { records: { partition: number; offset: number }[]; target: string; keep_partition: boolean; mark_source: boolean },
  ) => request<ResendResult[]>('POST', `/clusters/${enc(c)}/topics/${enc(t)}/resend`, body),
  importRecords: (c: string, t: string, body: ImportBody) =>
    request<ImportResult[]>('POST', `/clusters/${enc(c)}/topics/${enc(t)}/import`, body),
  checkFilter: (source: string) => request<{ problems: FilterProblem[] }>('POST', '/filter/check', { source }),
  info: () => request<AppInfo>('GET', '/info', undefined, { app: true }),
  clusters: () => request<Cluster[]>('GET', '/clusters', undefined, { app: true }),
  // Shared and cached across pages (⌘K, pickers), so not tied to one page.
  names: (c: string) => request<Names>('GET', `/clusters/${enc(c)}/names`, undefined, { app: true }),
  overview: (c: string) => request<Overview>('GET', `/clusters/${enc(c)}/overview`),
  topicDoc: (c: string, t: string) => request<TopicDoc>('GET', `/clusters/${enc(c)}/topics/${enc(t)}/docs`),
  balancePlan: (c: string, t: string) => request<Reassignment[]>('GET', `/clusters/${enc(c)}/topics/${enc(t)}/balance-plan`),
  reassignments: (c: string, t: string) => request<PartitionMove[]>('GET', `/clusters/${enc(c)}/topics/${enc(t)}/reassignments`),
  reassign: (c: string, t: string, body: { partitions: Reassignment[]; cancel?: boolean; dry_run?: boolean }) =>
    request<ReassignResult[]>('POST', `/clusters/${enc(c)}/topics/${enc(t)}/reassign`, body),
  electLeaders: (c: string, t: string, partitions: number[] = []) =>
    request<ReassignResult[]>('POST', `/clusters/${enc(c)}/topics/${enc(t)}/elect-leaders`, { partitions }),
  topics: (c: string, q: TopicQuery = {}) => request<TopicPage>('GET', `/clusters/${enc(c)}/topics?${query(q)}`),
  topic: (c: string, t: string) => request<TopicDetail>('GET', `/clusters/${enc(c)}/topics/${enc(t)}`),
  topicConsumers: (c: string, t: string) => request<TopicConsumer[]>('GET', `/clusters/${enc(c)}/topics/${enc(t)}/consumers`),
  createTopic: (c: string, body: { name: string; partitions: number; replication_factor: number; configs?: Record<string, string> }) =>
    request<{ name: string }>('POST', `/clusters/${enc(c)}/topics`, body),
  deleteTopic: (c: string, t: string) => request<void>('DELETE', `/clusters/${enc(c)}/topics/${enc(t)}`),
  alterTopicConfigs: (c: string, t: string, changes: ConfigChange[]) =>
    request<void>('PATCH', `/clusters/${enc(c)}/topics/${enc(t)}/configs`, changes),
  addPartitions: (c: string, t: string, total: number) =>
    request<void>('POST', `/clusters/${enc(c)}/topics/${enc(t)}/partitions`, { total }),
  deleteRecords: (c: string, t: string, body: { partitions?: number[]; strategy: string; offset?: number; timestamp?: number }) =>
    request<{ partition: number; low_watermark: number; error?: string }[]>('POST', `/clusters/${enc(c)}/topics/${enc(t)}/delete-records`, body),
  messages: (c: string, t: string, params: Record<string, string | number | boolean | undefined>) =>
    request<ConsumeResult>('GET', `/clusters/${enc(c)}/topics/${enc(t)}/messages?${query(params)}`),
  message: (c: string, t: string, partition: number, offset: number) =>
    request<Message>('GET', `/clusters/${enc(c)}/topics/${enc(t)}/messages/${partition}/${offset}`),
  produce: (c: string, t: string, body: ProduceBody) =>
    request<{ partition: number; offset: number; timestamp: string }>('POST', `/clusters/${enc(c)}/topics/${enc(t)}/messages`, body),
  broker: (c: string, id: number) => request<BrokerDetail>('GET', `/clusters/${enc(c)}/brokers/${id}`),
  alterBrokerConfigs: (c: string, id: number, changes: ConfigChange[]) =>
    request<void>('PATCH', `/clusters/${enc(c)}/brokers/${id}/configs`, changes),
  deleteGroupOffsets: (c: string, g: string, topic: string, partitions?: number[]) =>
    request<void>('POST', `/clusters/${enc(c)}/groups/${enc(g)}/delete-offsets`, { topic, partitions }),

  acls: (c: string) => request<ACL[]>('GET', `/clusters/${enc(c)}/acls`),
  // The ACLs Kafka checks for one resource: exact, covering prefixes and *.
  resourceACLs: (c: string, type: 'TOPIC' | 'GROUP', name: string) =>
    request<ACL[]>('GET', `/clusters/${enc(c)}/acls?${query({ resource_type: type, resource_name: name })}`),
  createACLs: (c: string, acls: Partial<ACL>[]) => request<void>('POST', `/clusters/${enc(c)}/acls`, acls),
  deleteACL: (c: string, acl: ACL) => request<{ deleted: number }>('POST', `/clusters/${enc(c)}/acls/delete`, acl),
  users: (c: string) => request<User[]>('GET', `/clusters/${enc(c)}/users`),
  upsertUser: (c: string, body: { name: string; password: string; mechanism: string }) =>
    request<void>('POST', `/clusters/${enc(c)}/users`, body),
  deleteUser: (c: string, name: string) => request<void>('DELETE', `/clusters/${enc(c)}/users/${enc(name)}`),
  quotas: (c: string) => request<Quota[]>('GET', `/clusters/${enc(c)}/quotas`),
  alterQuota: (c: string, body: { entity: QuotaEntity[]; set?: Record<string, number>; remove?: string[] }) =>
    request<void>('POST', `/clusters/${enc(c)}/quotas`, body),

  registry: (c: string, q: RegistryQuery = {}) => request<RegistryOverview>('GET', `/clusters/${enc(c)}/schemas?${query(q)}`),
  subject: (c: string, s: string) => request<SubjectDetail>('GET', `/clusters/${enc(c)}/schemas/subjects/${enc(s)}`),
  createSchema: (c: string, body: { subject: string; type: string; schema: string; references?: SchemaReference[]; normalize?: boolean }) =>
    request<SchemaVersion>('POST', `/clusters/${enc(c)}/schemas`, body),
  checkSchema: (c: string, body: { subject: string; type: string; schema: string; references?: SchemaReference[] }) =>
    request<{ compatible: boolean; messages: string[] }>('POST', `/clusters/${enc(c)}/schemas/check`, body),
  setGlobalCompatibility: (c: string, level: string) =>
    request<void>('PUT', `/clusters/${enc(c)}/schemas/compatibility`, { level }),
  setCompatibility: (c: string, s: string, level: string) =>
    request<void>('PUT', `/clusters/${enc(c)}/schemas/subjects/${enc(s)}/compatibility`, { level }),
  deleteSubject: (c: string, s: string, permanent: boolean) =>
    request<void>('DELETE', `/clusters/${enc(c)}/schemas/subjects/${enc(s)}?${query({ permanent })}`),
  deleteSchemaVersion: (c: string, s: string, v: number, permanent: boolean) =>
    request<void>('DELETE', `/clusters/${enc(c)}/schemas/subjects/${enc(s)}/versions/${v}?${query({ permanent })}`),
  groups: (c: string, q: GroupQuery = {}) => request<GroupPage>('GET', `/clusters/${enc(c)}/groups?${query(q)}`),
  group: (c: string, g: string) => request<GroupDetail>('GET', `/clusters/${enc(c)}/groups/${enc(g)}`),
  createGroup: (c: string, body: { group: string; topic: string; offsets: { partition: number; offset: number }[] }) =>
    request<ResetPlan[]>('POST', `/clusters/${enc(c)}/groups`, body),
  addGroupTopic: (c: string, g: string, body: { topic: string; offsets: { partition: number; offset: number }[] }) =>
    request<ResetPlan[]>('POST', `/clusters/${enc(c)}/groups/${enc(g)}/topics`, body),
  deleteGroup: (c: string, g: string) => request<void>('DELETE', `/clusters/${enc(c)}/groups/${enc(g)}`),
  resetOffsets: (
    c: string,
    g: string,
    body: { topic: string; partitions?: number[]; strategy: string; offset?: number; timestamp?: number; shift?: number; dry_run: boolean },
  ) => request<{ dry_run: boolean; partitions: ResetPlan[] }>('POST', `/clusters/${enc(c)}/groups/${enc(g)}/reset`, body),
}
