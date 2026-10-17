// Numbers that JSON.parse would round (int64 ids are common in Kafka payloads)
// keep their source text, so the viewer never shows a value that was not sent.
export class LosslessNumber {
  constructor(public raw: string) {}
}

// Tree-wide controls shared through context: `gen` bumps on every
// expand/collapse-all so each node can follow it.
export type TreeControl = { depth: number; gen: number; all: boolean }

export function parseJson(text: string): { ok: true; value: unknown } | { ok: false } {
  try {
    const value = JSON.parse(text, function (_key: string, v: unknown, ctx?: { source?: string }) {
      // ctx.source is only passed by newer engines; without it numbers parse as usual.
      if (typeof v === 'number' && ctx?.source !== undefined && String(v) !== ctx.source) {
        return new LosslessNumber(ctx.source)
      }
      return v
    } as (this: unknown, key: string, value: unknown) => unknown)
    return { ok: true, value }
  } catch {
    return { ok: false }
  }
}

export const isContainer = (v: unknown): v is object =>
  v !== null && typeof v === 'object' && !(v instanceof LosslessNumber)
