<script lang="ts">
  import { json } from '@codemirror/lang-json'
  import { StreamLanguage } from '@codemirror/language'
  import { protobuf } from '@codemirror/legacy-modes/mode/protobuf'
  import { setDiagnostics, type Diagnostic } from '@codemirror/lint'
  import type { EditorView } from '@codemirror/view'
  import CodeEditor from './CodeEditor.svelte'

  type Problem = { line?: number; column?: number; message: string }

  // JSON (or Protobuf / plain text) editor whose error comes from the caller,
  // so the page that decides whether it may submit is the single source of
  // truth for what is invalid.
  let {
    value = $bindable(''),
    error,
    mode = 'json',
    label = 'Message value',
    maxHeight = 560,
    id,
    placeholder = '',
    rows = 12,
    onrun,
  }: {
    value: string
    error?: Problem
    mode?: 'json' | 'protobuf' | 'plain'
    label?: string
    maxHeight?: number
    id?: string
    placeholder?: string
    rows?: number
    onrun?: () => void
  } = $props()

  let view = $state<EditorView>()
  const proto = StreamLanguage.define(protobuf)
  const language = $derived(mode === 'json' ? json() : mode === 'protobuf' ? proto : [])
  const lineHeight = 20

  $effect(() => {
    if (!view) return
    const doc = view.state.doc
    const diagnostics: Diagnostic[] = []
    if (error) {
      // Without a position (Safari's parser gives none) the first line carries it.
      const line = doc.line(Math.min(Math.max(error.line ?? 1, 1), doc.lines))
      const from = Math.min(line.from + Math.max((error.column ?? 1) - 1, 0), line.to)
      diagnostics.push({ from, to: Math.min(Math.max(from + 1, line.to), doc.length), severity: 'error', message: error.message })
    }
    view.dispatch(setDiagnostics(view.state, diagnostics))
  })
</script>

<CodeEditor
  bind:value
  bind:view
  {language}
  invalid={!!error}
  {id}
  {placeholder}
  {onrun}
  {label}
  minHeight={rows * lineHeight + 12}
  {maxHeight}
/>
{#if error}
  <div class="msg" id={id ? `${id}-error` : undefined} role="status">
    <span class="where">{error.line ? `Line ${error.line}${error.column ? `, col ${error.column}` : ''}` : 'Invalid JSON'}</span>
    {error.message}
  </div>
{/if}

<style>
  .msg { display: flex; align-items: baseline; gap: 8px; margin-top: 6px; font-size: 12px; color: var(--danger); }
  .where {
    flex: none;
    font: 500 11px/1.6 var(--mono);
    padding: 0 6px;
    border-radius: 2px;
    color: var(--surface);
    background: var(--danger);
  }
</style>
