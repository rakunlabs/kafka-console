// Hash router: #/<cluster>/<section>/<name>?query
// Hash keeps deep links working without server-side route config.
import { leavePage } from './api'

export type Route = {
  cluster: string
  section: string
  name: string
  query: URLSearchParams
}

function parse(): Route {
  const raw = location.hash.replace(/^#\/?/, '')
  const [path, qs = ''] = raw.split('?')
  const parts = path.split('/').filter(Boolean).map(decodeURIComponent)

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
