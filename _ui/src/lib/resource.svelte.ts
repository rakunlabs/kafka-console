import { isAbort, withSignal } from './api'

// A page's data from one read, reloadable. A load replaces the one still
// running, on the server too, so a slow earlier answer can never overwrite
// a newer one. With keep, a failed reload keeps the data shown and only
// sets error.
export function resource<T>(fetch: () => Promise<T>) {
  let data = $state.raw<T>()
  let error = $state('')
  let loading = $state(false)
  let ctl: AbortController | undefined

  async function load() {
    ctl?.abort()
    const c = (ctl = new AbortController())
    loading = true
    try {
      const value = await withSignal(c.signal, fetch)
      if (ctl !== c) return
      data = value
      error = ''
    } catch (e: any) {
      if (ctl === c && !isAbort(e)) error = e.message
    } finally {
      if (ctl === c) loading = false
    }
  }

  return {
    get data() {
      return data
    },
    set data(v: T | undefined) {
      data = v
    },
    get error() {
      return error
    },
    get loading() {
      return loading
    },
    load,
  }
}

// copyText writes text to the clipboard and resolves whether it did.
export async function copyText(text: string): Promise<boolean> {
  try {
    await navigator.clipboard.writeText(text)
    return true
  } catch {
    return false
  }
}
