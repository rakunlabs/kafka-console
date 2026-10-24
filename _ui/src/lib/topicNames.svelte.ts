import type { TopicName } from './api'
import { errorMessage } from './http'
import { names } from './names'

// The cluster's topic names for a dialog's picker, loaded once when the
// dialog mounts. A failure is kept in error for the dialog to show; the
// picker stays usable for typing a name.
export function topicNames(cluster: string) {
  let topics = $state.raw<TopicName[]>([])
  let error = $state('')
  names(cluster).then(
    (n) => (topics = n.topics),
    (e) => (error = `Could not load topics: ${errorMessage(e)}`),
  )
  return {
    get topics() {
      return topics
    },
    get error() {
      return error
    },
  }
}
