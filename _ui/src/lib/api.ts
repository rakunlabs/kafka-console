export type Cluster = {
  name: string
  brokers: string[]
  read_only: boolean
  production: boolean
}

export type Broker = { id: number; host: string; port: number; rack?: string; controller: boolean }

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
}

export type TopicSummary = {
  name: string
  internal: boolean
  partitions: number
  replication_factor: number
  under_replicated: number
  messages: number
}

export type Partition = {
  id: number
  leader: number
  replicas: number[]
  isr: number[]
  offline_replicas: number[]
  low: number
  high: number
  error?: string
}

export type ConfigEntry = { key: string; value: string | null; sensitive: boolean; source: string }

export type TopicDetail = {
  name: string
  id: string
  internal: boolean
  partitions: Partition[]
  configs: ConfigEntry[] | null
  groups: string[] | null
}

export type Header = { key: string; value: string; encoding: string }

export type Message = {
  partition: number
  offset: number
  timestamp: string
  key: string | null
  key_encoding?: string
  value: string | null
  value_encoding?: string
  value_format?: string
  value_bytes: number
  truncated: boolean
  headers: Header[]
}

export type ConsumeResult = {
  messages: Message[]
  scanned: number
  script_errors: number
  first_error?: string
  stopped_by: string
  elapsed_ms: number
}

export type GroupSummary = {
  name: string
  state: string
  protocol_type: string
  members: number
  topics: string[]
  lag: number
  error?: string
}

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

export type ResetPlan = { partition: number; from: number; to: number }

async function request<T>(method: string, path: string, body?: unknown): Promise<T> {
  // Relative: resolves against the page URL, so it follows the server base_path.
  const res = await fetch(`api/v1${path}`, {
    method,
    headers: body === undefined ? undefined : { 'content-type': 'application/json' },
    body: body === undefined ? undefined : JSON.stringify(body),
  })

  if (res.status === 204) return undefined as T

  const text = await res.text()
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

export const api = {
  clusters: () => request<Cluster[]>('GET', '/clusters'),
  overview: (c: string) => request<Overview>('GET', `/clusters/${enc(c)}/overview`),
  topics: (c: string) => request<TopicSummary[]>('GET', `/clusters/${enc(c)}/topics`),
  topic: (c: string, t: string) => request<TopicDetail>('GET', `/clusters/${enc(c)}/topics/${enc(t)}`),
  createTopic: (c: string, body: { name: string; partitions: number; replication_factor: number; configs?: Record<string, string> }) =>
    request<{ name: string }>('POST', `/clusters/${enc(c)}/topics`, body),
  deleteTopic: (c: string, t: string) => request<void>('DELETE', `/clusters/${enc(c)}/topics/${enc(t)}`),
  messages: (c: string, t: string, params: Record<string, string | number | undefined>) => {
    const q = new URLSearchParams()
    for (const [k, v] of Object.entries(params)) if (v !== undefined && v !== '') q.set(k, String(v))
    return request<ConsumeResult>('GET', `/clusters/${enc(c)}/topics/${enc(t)}/messages?${q}`)
  },
  produce: (c: string, t: string, body: { key: string | null; value: string | null; headers: { key: string; value: string }[]; partition: number | null }) =>
    request<{ partition: number; offset: number; timestamp: string }>('POST', `/clusters/${enc(c)}/topics/${enc(t)}/messages`, body),
  groups: (c: string) => request<GroupSummary[]>('GET', `/clusters/${enc(c)}/groups`),
  group: (c: string, g: string) => request<GroupDetail>('GET', `/clusters/${enc(c)}/groups/${enc(g)}`),
  deleteGroup: (c: string, g: string) => request<void>('DELETE', `/clusters/${enc(c)}/groups/${enc(g)}`),
  resetOffsets: (
    c: string,
    g: string,
    body: { topic: string; partitions?: number[]; strategy: string; offset?: number; timestamp?: number; dry_run: boolean },
  ) => request<{ dry_run: boolean; partitions: ResetPlan[] }>('POST', `/clusters/${enc(c)}/groups/${enc(g)}/reset`, body),
}
