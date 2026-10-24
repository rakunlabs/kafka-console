// Keep status information: a rejected request and a lost response have very
// different retry semantics for writes.
export class HTTPError extends Error {
  status: number

  constructor(status: number, message: string) {
    super(message)
    this.status = status
    this.name = 'HTTPError'
  }
}

export function importRefused(error: unknown): boolean {
  // A timeout may come from a proxy after the server started writing.
  return error instanceof HTTPError && error.status >= 400 && error.status < 500 && error.status !== 408
}

// errorMessage is the text to show for anything a failed call threw.
export function errorMessage(e: unknown): string {
  if (e instanceof Error) return e.message
  if (typeof e === 'string') return e
  return String(e)
}

// responseError reads a failed response's body into an HTTPError: the
// server's JSON {message}, else the body text, else the status text.
export async function responseError(res: Response): Promise<HTTPError> {
  const text = await res.text().catch(() => '')
  let message = text || res.statusText
  try {
    const data = JSON.parse(text)
    if (data && typeof data === 'object' && typeof data.message === 'string' && data.message) message = data.message
  } catch {}
  return new HTTPError(res.status, message)
}
