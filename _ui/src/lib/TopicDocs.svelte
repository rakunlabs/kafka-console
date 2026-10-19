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
  <article class="panel doc">
    <!-- eslint-disable-next-line svelte/no-at-html-tags -- rendered server-side without raw HTML -->
    {@html doc.html}
  </article>
  <p class="faint small mt-2"><code>{doc.file}</code> · updated {showTime(doc.modified, 'relative')}</p>
{/if}

<style>
  .doc { padding: 16px 20px; line-height: 1.6; overflow-x: auto; }
  .doc :global(h1) { font-size: 20px; margin: 0 0 10px; }
  .doc :global(h2) { font-size: 16px; margin: 18px 0 8px; }
  .doc :global(h3) { font-size: 14px; margin: 14px 0 6px; }
  .doc :global(p), .doc :global(ul), .doc :global(ol) { margin: 0 0 10px; }
  .doc :global(ul), .doc :global(ol) { padding-left: 22px; }
  .doc :global(ul) { list-style: disc; }
  .doc :global(ol) { list-style: decimal; }
  .doc :global(code) { font-family: var(--font-mono); font-size: 12.5px; background: var(--surface-2); padding: 1px 4px; border-radius: 3px; }
  .doc :global(pre) { background: var(--surface-2); padding: 10px 12px; border-radius: var(--radius); overflow-x: auto; margin: 0 0 10px; }
  .doc :global(pre code) { background: none; padding: 0; }
  .doc :global(table) { border-collapse: collapse; margin: 0 0 10px; }
  .doc :global(th), .doc :global(td) { border: 1px solid var(--border); padding: 4px 8px; }
  .doc :global(blockquote) { border-left: 3px solid var(--border-strong); padding-left: 12px; color: var(--text-2); margin: 0 0 10px; }
  .doc :global(a) { text-decoration: underline; }
</style>
