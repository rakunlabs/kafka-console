// Hash router: #/<cluster>/<section>/<name>?query
// Hash keeps deep links working without server-side route config.
import { leavePage } from './api'

export type Route = {
  cluster: string
  section: string
  name: string
  query: URLSearchParams
}

// A malformed escape (a stray %) keeps the raw text instead of throwing.
function decode(s: string) {
  try {
    return decodeURIComponent(s)
  } catch {
    return s
  }
}

function parse(): Route {
  const raw = location.hash.replace(/^#\/?/, '')
  const [path, qs = ''] = raw.split('?')
  const parts = path.split('/').filter(Boolean).map(decode)

  return {
    cluster: parts[0] ?? '',
    section: parts[1] ?? 'overview',
    name: parts.slice(2).join('/'),
    query: new URLSearchParams(qs),
  }
}

export const router = $state({ route: parse() })

// The page is cluster/section/name; the query (tabs, filters) stays within it.
const pageOf = (r: Route) => `${r.cluster}/${r.section}/${r.name}`

window.addEventListener('hashchange', () => {
  const next = parse()
  // Reads still running for the page being left are aborted, here and on
  // the server, before the next page starts its own.
  if (pageOf(next) !== pageOf(router.route)) leavePage()
  router.route = next
})

export function href(cluster: string, section = 'overview', name = '', query?: Record<string, string>) {
  let h = `#/${encodeURIComponent(cluster)}/${section}`
  if (name) h += `/${encodeURIComponent(name)}`
  if (query) {
    const q = new URLSearchParams(query).toString()
    if (q) h += `?${q}`
  }
  return h
}

export function go(cluster: string, section = 'overview', name = '', query?: Record<string, string>) {
  location.hash = href(cluster, section, name, query)
}

// Updates query values of the current page in place, without a history
// entry or a hashchange: for view state (open rows, filters) that a reload
// or a shared link should keep. Empty values are removed.
export function replaceQuery(values: Record<string, string | string[] | undefined>) {
  const raw = location.hash.replace(/^#/, '')
  const [path, qs = ''] = raw.split('?')
  const q = new URLSearchParams(qs)
  for (const [k, v] of Object.entries(values)) {
    q.delete(k)
    for (const one of Array.isArray(v) ? v : [v]) if (one) q.append(k, one)
  }
  const s = q.toString()
  history.replaceState(history.state, '', `#${path}${s ? `?${s}` : ''}`)
}
