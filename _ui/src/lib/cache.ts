// Last result per key, kept for the session. A page shows it immediately on
// return and refreshes in the background, so lists that take a few seconds
// on a large cluster do not open empty every time.
const store = new Map<string, unknown>()

export function cached<T>(key: string): T | undefined {
  return store.get(key) as T | undefined
}

export function remember<T>(key: string, value: T): T {
  store.set(key, value)
  return value
}
