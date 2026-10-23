import type { ReplayBody, ReplayEvent, ReplayRange } from './api'

// Capture the options as well as the offsets: editing a form during a slow
// review must not change what its Write button sends.
export function reviewedReplay(body: ReplayBody, plan: ReplayRange[], done: ReplayEvent): ReplayBody {
  if (done.type !== 'done' || !['end', 'limit'].includes(done.stopped_by ?? '')) {
    throw new Error(done.first_error || 'The review did not finish. Review the replay again.')
  }
  return {
    ...body,
    from: { ...body.from },
    to: { ...body.to },
    partitions: body.partitions?.slice(),
    plan: plan.map((r) => ({ ...r })),
    dry_run: false,
  }
}
