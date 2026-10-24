import { isAbort, withSignal } from './api'
import { errorMessage } from './http'
import { onDestroy } from 'svelte'

// A page's data from one read, reloadable. A load replaces the one still
// running, on the server too, so a slow earlier answer can never overwrite
// a newer one. With keep, a failed reload keeps the data shown and only
// sets error.
export function resource<T>(fetch: () => Promise<T>) {
  let data = $state.raw<T>()
  let error = $state('')
  let loading = $state(false)
  let ctl: AbortController | undefined
  onDestroy(() => { ctl?.abort(); ctl = undefined })

  async function load() {
    started++
    ctl?.abort()
    const c = (ctl = new AbortController())
    loading = true
    try {
      const value = await withSignal(c.signal, fetch)
      if (ctl !== c || c.signal.aborted) return
      data = value
      error = ''
    } catch (e) {
      if (ctl === c && !isAbort(e)) error = errorMessage(e)
    } finally {
      if (ctl === c) loading = false
    }
  }

  // For data read elsewhere: call before the read starts, then pass the
  // value to the returned function. It is kept, replacing a load still
  // running, unless a load started after the read did.
  let started = 0
  function since() {
    const at = started
    return (value: T) => {
      if (started !== at) return
      ctl?.abort()
      ctl = undefined
      data = value
      error = ''
      loading = false
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
    since,
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

// copier shows which of several copy buttons copied last, for a moment:
// copied is its key until it fades.
export function copier(ms = 1200) {
  let copied = $state<string | null>(null)
  let timer: ReturnType<typeof setTimeout> | undefined
  onDestroy(() => clearTimeout(timer))
  return {
    get copied() {
      return copied
    },
    async copy(text: string, key = 'value') {
      if (!(await copyText(text))) return false
      copied = key
      clearTimeout(timer)
      timer = setTimeout(() => (copied = null), ms)
      return true
    },
  }
}
