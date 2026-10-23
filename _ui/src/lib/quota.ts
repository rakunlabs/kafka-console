import type { Quota, QuotaEntity } from './api'

export const quotaKeys = [
  { key: 'producer_byte_rate', label: 'Producer bytes/s' },
  { key: 'consumer_byte_rate', label: 'Consumer bytes/s' },
  { key: 'request_percentage', label: 'Request %' },
  { key: 'controller_mutation_rate', label: 'Controller mutations/s' },
]

export function quotaDraft(quota?: Quota) {
  return {
    // An existing quota's identity is immutable, including compound/default
    // components. Changing the identity would edit a different quota.
    entity: quota?.entity.map((e) => ({ ...e })),
    type: 'client-id', name: '', isDefault: false,
    values: Object.fromEntries(quotaKeys.map(({ key }) => [key, quota?.values[key] === undefined ? '' : String(quota.values[key])])) as Record<string, string | number | null>,
    had: Object.keys(quota?.values ?? {}),
  }
}

export function quotaChanges(draft: ReturnType<typeof quotaDraft>) {
  const set: Record<string, number> = {}
  const remove: string[] = []
  for (const { key } of quotaKeys) {
    const text = String(draft.values[key] ?? '').trim()
    if (text) {
      const value = Number(text)
      if (!Number.isFinite(value) || value < 0) throw new Error(`${key} must be a non-negative number`)
      set[key] = value
    } else if (draft.had.includes(key)) remove.push(key)
  }
  const entity: QuotaEntity[] = draft.entity?.map((e) => ({ ...e })) ??
    [{ type: draft.type, name: draft.isDefault ? null : draft.name.trim() }]
  return { entity, set, remove }
}
