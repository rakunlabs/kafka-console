import type { ResetPlan } from './api'

// Never send the original latest/shift strategy on Apply. Its meaning may
// have changed while the operator reviewed the concrete offsets.
export function reviewedOffsets(topic: string, plan: ResetPlan[] | undefined) {
  if (!plan?.length) throw new Error('Preview the offsets before applying them.')
  return { topic, plan: plan.map((p) => ({ ...p })), dry_run: false }
}
