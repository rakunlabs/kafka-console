<script lang="ts">
  import { untrack } from 'svelte'
  import { api, type Cluster, type TopicDoc } from './api'
  import { showTime } from './viewer'

  // A topic's documentation page from <docs.path>/<topic>.md. The server
  // renders the Markdown without raw HTML and with safe links only, so the
  // result is inserted as is.
  let { cluster, topic }: { cluster: Cluster; topic: string } = $props()

  let doc = $state<TopicDoc>()
  let missing = $state(false)
  let error = $state('')

  untrack(() => api.topicDoc(cluster.name, topic))
    .then((d) => (doc = d))
    .catch((e) => {
      if (/not found/i.test(e.message)) missing = true
      else error = e.message
    })
</script>

{#if error}
  <div class="error">{error}</div>
{:else if missing}
  <div class="panel empty">
    No documentation for this topic yet. Add <code>{topic}.md</code> to the cluster's docs directory.
  </div>
{:else if !doc}
  <div class="empty">Loading…</div>
{:else}
  <article class="panel doc markdown">
    <!-- eslint-disable-next-line svelte/no-at-html-tags -- rendered server-side without raw HTML -->
    {@html doc.html}
  </article>
  <p class="faint small mt-2"><code>{doc.file}</code> · updated {showTime(doc.modified, 'relative')}</p>
{/if}

<style>
  .doc { padding: 16px 20px; }
</style>
