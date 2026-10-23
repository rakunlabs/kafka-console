import type { Message } from './api'

// Kafka defines order within a partition by offset, not by producer time.
// There is no total order across partitions; group them deterministically.
export function resendOrder(messages: Message[]): Message[] {
  return [...messages].sort((a, b) => a.partition - b.partition || a.offset - b.offset)
}
