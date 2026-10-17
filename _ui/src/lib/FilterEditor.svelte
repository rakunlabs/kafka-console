<script lang="ts">
  import { javascript, javascriptLanguage } from '@codemirror/lang-javascript'
  import { linter, type Diagnostic } from '@codemirror/lint'
  import { autocompletion, completeFromList } from '@codemirror/autocomplete'
  import CodeEditor from './CodeEditor.svelte'
  import { api } from './api'

  let {
    value = $bindable(''),
    onrun,
    placeholder = '',
  }: { value: string; onrun?: () => void; placeholder?: string } = $props()

  let problems = $state(0)

  // The variables the server passes to the filter, with what each holds.
  const variables = [
    { label: 'value', type: 'variable', detail: 'any', info: 'Message value, parsed when it is JSON or decoded by the registry' },
    { label: 'key', type: 'variable', detail: 'string | null', info: 'Message key' },
    { label: 'headers', type: 'variable', detail: 'Record<string, string>', info: 'Headers by name' },
    { label: 'partition', type: 'variable', detail: 'number' },
    { label: 'offset', type: 'variable', detail: 'number' },
    { label: 'timestamp', type: 'variable', detail: 'Date', info: 'Record timestamp' },
    { label: 'raw', type: 'variable', detail: 'string', info: 'Value as text, before any parsing' },
    { label: 'return', type: 'keyword' },
  ]

  // Syntax is checked by the same engine that runs the filter, so the editor
  // never accepts what the server would refuse.
  const serverLint = linter(
    async (v) => {
      const doc = v.state.doc
      const source = doc.toString()
      if (!source.trim()) {
        problems = 0
        return []
      }
      try {
        const res = await api.checkFilter(source)
        const out: Diagnostic[] = res.problems.map((p) => {
          const line = doc.line(Math.min(Math.max(p.line, 1), doc.lines))
          const from = Math.min(line.from + Math.max(p.column - 1, 0), line.to)
          return {
            from,
            to: Math.min(Math.max(from + 1, line.to), doc.length),
            severity: 'error',
            message: p.message,
            source: 'filter',
          }
        })
        if (!/\breturn\b/.test(source)) {
          out.push({ from: 0, to: Math.min(doc.length, doc.line(1).to), severity: 'warning', message: 'The filter never returns, so no message will match. Return true for messages to keep.' })
        }
        problems = out.filter((d) => d.severity === 'error').length
        return out
      } catch {
        return []
      }
    },
    { delay: 350 },
  )

  const extensions = [
    javascriptLanguage.data.of({ autocomplete: completeFromList(variables) }),
    autocompletion({ icons: false }),
    serverLint,
  ]
</script>

<CodeEditor bind:value language={javascript()} {extensions} invalid={problems > 0} {placeholder} {onrun} label="JavaScript filter" />
