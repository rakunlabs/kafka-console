import { inputMs } from './format.ts'

export const timeStarts = [
  { value: '15m', label: 'Last 15 min', milliseconds: 15 * 60_000 },
  { value: '1h', label: 'Last hour', milliseconds: 3_600_000 },
  { value: '6h', label: 'Last 6 hours', milliseconds: 6 * 3_600_000 },
  { value: '24h', label: 'Last 24 hours', milliseconds: 86_400_000 },
  { value: '7d', label: 'Last 7 days', milliseconds: 7 * 86_400_000 },
] as const

export type MessageStart = 'newest' | 'oldest' | 'offset' | 'last' | 'custom' | typeof timeStarts[number]['value']
type StartFields = { offset: number; lastN: number; since: string; until: string }

export function startProblem(start: MessageStart, since: string, until: string): string {
  if (start !== 'custom') return ''
  for (const value of [since, until]) {
    if (value && !Number.isFinite(new Date(value).getTime())) return 'Enter a valid start and end time'
  }
  return since && until && new Date(until) < new Date(since) ? 'The end of the time range is before its start' : ''
}

// One selector owns the whole start mode. Hidden offset/time inputs never
// leak into another mode. Time ranges read newest first, so a limit keeps
// the most recent matching records.
export function messageStart(start: MessageStart, fields: StartFields, now = Date.now()): {
  start: 'newest' | 'oldest' | 'offset' | 'last'; offset?: number; since?: number; until?: number
} {
  const problem = startProblem(start, fields.since, fields.until)
  if (problem) throw new Error(problem)
  if (start === 'custom') return {
    start: 'newest',
    since: inputMs(fields.since),
    until: inputMs(fields.until),
  }
  const preset = timeStarts.find((s) => s.value === start)
  if (preset) return { start: 'newest', since: now - preset.milliseconds, until: now }
  if (start === 'offset') return { start, offset: fields.offset }
  if (start === 'last') return { start, offset: fields.lastN }
  return { start: start === 'oldest' ? 'oldest' : 'newest' }
}
