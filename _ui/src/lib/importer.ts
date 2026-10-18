import type { ImportRecord, ImportResult } from './api'
import { readFile, FatalError, type FileFormat, type ItemShape } from './importFile.ts'

export type ImportOptions = {
  format: FileFormat
  shape: ItemShape
  batchSize: number
  // Why a parsed record cannot go to the topic with the chosen options, or ''.
  unfit: (r: ImportRecord) => string
  // The record as it is sent.
  prepare: (r: ImportRecord) => ImportRecord
  // Writes (or with dry, checks) one batch; throws when the batch is refused.
  send: (records: ImportRecord[], dry: boolean) => Promise<ImportResult[]>
  stopped: () => boolean
  onBytes?: (n: number) => void
}

export type Problem = { item: number; message: string }

export type CheckResult = {
  total: number
  problemCount: number
  problems: Problem[]
  fatal: string
  stopped: boolean
}

// Server errors name a record by its 1-based place in the batch.
const recordRef = /^record (\d+): /

// check reads the whole file. Every record is checked locally; batches are
// also checked by the server, which encodes schema payloads, until the first
// problem. After that only the local check continues, to count the rest.
export async function checkFile(file: Blob, o: ImportOptions, keep = 20): Promise<CheckResult> {
  const out: CheckResult = { total: 0, problemCount: 0, problems: [], fatal: '', stopped: false }
  const problem = (item: number, message: string) => {
    out.problemCount++
    if (out.problems.length < keep) out.problems.push({ item, message })
  }

  let batch: ImportRecord[] = []
  let first = 0
  const flush = async () => {
    const b = batch
    batch = []
    if (!b.length || out.problemCount) return
    try {
      await o.send(b, true)
    } catch (e: any) {
      const m = recordRef.exec(e.message)
      problem(m ? first + Number(m[1]) - 1 : first, m ? e.message.slice(m[0].length) : e.message)
    }
  }

  try {
    for await (const it of readFile(file, o.format, o.shape, o.onBytes)) {
      if (o.stopped()) {
        out.stopped = true
        return out
      }
      out.total = it.item
      const why = it.problem ?? o.unfit(it.record!)
      if (why) {
        problem(it.item, why)
        continue
      }
      if (!batch.length) first = it.item
      batch.push(o.prepare(it.record!))
      if (batch.length >= o.batchSize) await flush()
    }
    await flush()
  } catch (e: any) {
    out.fatal = e instanceof FatalError ? e.message : `Could not read the file: ${e.message}`
  }
  return out
}

export type SendResult = {
  // Records written by this run.
  written: number
  byPartition: Record<number, number>
  // 0 when every item was written; otherwise the first item not known to be
  // written. Every item before it is written, in order.
  resumeAt: number
  // Records after resumeAt that were written anyway, from the failed batch,
  // and would be written again on resume.
  rewritten: number
  error: string
}

// sendFile writes every item after `after` in file order. A batch that fails
// stops the import.
export async function sendFile(file: Blob, o: ImportOptions, after = 0, onBatch?: (r: SendResult) => void): Promise<SendResult> {
  const out: SendResult = { written: 0, byPartition: {}, resumeAt: 0, rewritten: 0, error: '' }
  let batch: ImportRecord[] = []
  let first = 0

  const flush = async () => {
    const b = batch
    batch = []
    if (!b.length) return
    let res: ImportResult[]
    try {
      res = await o.send(b, false)
    } catch (e: any) {
      // Refused as a whole: nothing of it was written.
      out.resumeAt = first
      const m = recordRef.exec(e.message)
      throw new Error(m ? `item ${first + Number(m[1]) - 1}: ${e.message.slice(m[0].length)}` : e.message)
    }
    for (const r of res) {
      if (r.error) continue
      out.written++
      out.byPartition[r.partition] = (out.byPartition[r.partition] ?? 0) + 1
    }
    onBatch?.(out)
    const failed = res.findIndex((r) => r.error)
    if (failed >= 0) {
      out.resumeAt = first + failed
      out.rewritten = res.slice(failed).filter((r) => !r.error).length
      throw new Error(`item ${out.resumeAt}: ${res[failed].error}`)
    }
  }

  let last = after
  try {
    for await (const it of readFile(file, o.format, o.shape, o.onBytes)) {
      if (it.item <= after) continue
      if (o.stopped()) {
        // The batch being filled was not sent.
        out.resumeAt = batch.length ? first : it.item
        return out
      }
      const why = it.problem ?? o.unfit(it.record!)
      if (why) {
        await flush()
        out.resumeAt = it.item
        throw new Error(`item ${it.item}: ${why}. The file changed after it was checked.`)
      }
      if (!batch.length) first = it.item
      batch.push(o.prepare(it.record!))
      last = it.item
      if (batch.length >= o.batchSize) await flush()
    }
    await flush()
  } catch (e: any) {
    out.error = e.message
    if (!out.resumeAt) out.resumeAt = (batch.length ? first : last + 1) || 1
  }
  return out
}
