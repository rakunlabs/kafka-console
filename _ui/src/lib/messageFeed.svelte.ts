import { messageStream, tail } from './api'
import { createMessageFeed, initialFeed } from './messageFeed'

export function messageFeed(
  source: () => { cluster: string; topic: string },
  hooks: { reset: () => void; trim: (kept: Set<string>) => void },
) {
  let state = $state.raw(initialFeed())
  const feed = createMessageFeed({ read: messageStream, tail }, source, (next) => (state = next), hooks)
  return { ...feed, get state() { return state } }
}
