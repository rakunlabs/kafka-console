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
