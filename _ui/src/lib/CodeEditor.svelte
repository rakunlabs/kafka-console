<script lang="ts">
  import { onMount } from 'svelte'
  import { EditorState, Compartment, type Extension } from '@codemirror/state'
  import { EditorView, keymap, lineNumbers, highlightActiveLine, placeholder as cmPlaceholder, drawSelection } from '@codemirror/view'
  import { defaultKeymap, history, historyKeymap, indentWithTab } from '@codemirror/commands'
  import { lintGutter } from '@codemirror/lint'
  import { closeBrackets, closeBracketsKeymap } from '@codemirror/autocomplete'
  import { bracketMatching, syntaxHighlighting, HighlightStyle } from '@codemirror/language'
  import { tags as t } from '@lezer/highlight'

  // Shared CodeMirror shell: theme, keys and two-way binding. Callers add the
  // language, linting and completion they need through `extensions`.
  let {
    value = $bindable(''),
    view = $bindable(),
    language = [],
    extensions = [],
    invalid = false,
    placeholder = '',
    label,
    id,
    minHeight = 76,
    maxHeight = 240,
    onrun,
  }: {
    value: string
    view?: EditorView
    language?: Extension
    extensions?: Extension[]
    invalid?: boolean
    placeholder?: string
    label: string
    id?: string
    minHeight?: number
    maxHeight?: number
    onrun?: () => void
  } = $props()

  let host: HTMLDivElement
  const lang = new Compartment()

  const highlight = HighlightStyle.define([
    { tag: t.keyword, color: 'var(--cm-keyword)', fontWeight: '500' },
    { tag: [t.string, t.special(t.string)], color: 'var(--cm-string)' },
    { tag: [t.number, t.bool, t.null], color: 'var(--cm-number)' },
    { tag: t.comment, color: 'var(--text-3)', fontStyle: 'italic' },
    { tag: t.propertyName, color: 'var(--cm-prop)' },
    { tag: t.variableName, color: 'var(--cm-var)' },
    { tag: [t.operator, t.punctuation, t.bracket, t.separator], color: 'var(--text-2)' },
  ])

  onMount(() => {
    view = new EditorView({
      parent: host,
      state: EditorState.create({
        doc: value,
        extensions: [
          lineNumbers(),
          history(),
          drawSelection(),
          highlightActiveLine(),
          bracketMatching(),
          closeBrackets(),
          lang.of(language),
          syntaxHighlighting(highlight),
          lintGutter(),
          cmPlaceholder(placeholder),
          keymap.of([
            ...(onrun ? [{ key: 'Mod-Enter', run: () => (onrun(), true) }] : []),
            ...closeBracketsKeymap,
            ...defaultKeymap,
            ...historyKeymap,
            indentWithTab,
          ]),
          EditorView.updateListener.of((u) => {
            if (u.docChanged) value = u.state.doc.toString()
          }),
          EditorView.contentAttributes.of({ 'aria-label': label, ...(id ? { id } : {}) }),
          ...extensions,
        ],
      }),
    })
    return () => view?.destroy()
  })

  // Outside changes (a template, Format, an example chip) replace the text.
  $effect(() => {
    const v = value
    if (view && v !== view.state.doc.toString()) {
      view.dispatch({ changes: { from: 0, to: view.state.doc.length, insert: v } })
    }
  })

  $effect(() => {
    const l = language
    view?.dispatch({ effects: lang.reconfigure(l) })
  })
</script>

<div class="cm theme-dark" class:invalid bind:this={host} style:--min="{minHeight}px" style:--max="{maxHeight}px"></div>

<style>
  .cm {
    --cm-keyword: var(--syn-kw);
    --cm-string: var(--syn-str);
    --cm-number: var(--syn-num);
    --cm-var: var(--syn-var);
    --cm-prop: var(--syn-key);
    border: 1px solid var(--border-strong);
    border-radius: var(--radius);
    background: var(--surface);
    overflow: hidden;
  }
  .cm:focus-within { outline: 2px solid var(--focus); outline-offset: -1px; }
  .cm.invalid { border-color: var(--danger); }
  .cm.invalid:focus-within { outline-color: var(--danger); }

  .cm :global(.cm-editor) { font: 13px/1.6 var(--mono); min-height: var(--min); max-height: var(--max); }
  .cm :global(.cm-editor.cm-focused) { outline: none; }
  .cm :global(.cm-scroller) { font-family: var(--mono); min-height: var(--min); }
  .cm :global(.cm-content) { padding: 6px 0; caret-color: var(--text); }
  .cm :global(.cm-line) { padding: 0 10px; }
  .cm :global(.cm-gutters) { background: var(--surface-2); border-right: 1px solid var(--border); color: var(--text-3); }
  .cm :global(.cm-activeLine) { background: color-mix(in srgb, var(--sel) 55%, transparent); }
  .cm :global(.cm-activeLineGutter) { background: transparent; color: var(--text); }
  .cm :global(.cm-selectionBackground),
  .cm :global(.cm-focused .cm-selectionBackground) { background: var(--cta) !important; }
  .cm :global(.cm-cursor) { border-left-color: var(--text); }
  .cm :global(.cm-placeholder) { color: var(--text-3); }
  .cm :global(.cm-matchingBracket) { background: var(--sel); outline: 1px solid var(--cta-border); }

  .cm :global(.cm-lintRange-error) { background-image: none; text-decoration: underline wavy var(--danger); text-underline-offset: 3px; }
  .cm :global(.cm-lintRange-warning) { background-image: none; text-decoration: underline wavy var(--warn); text-underline-offset: 3px; }
  .cm :global(.cm-gutter-lint) { width: 14px; }
  /* Square lint markers instead of CodeMirror's round ones. */
  .cm :global(.cm-lint-marker) { width: 8px; height: 8px; margin: 5px 3px 0; background-image: none; content: none; }
  .cm :global(.cm-lint-marker-error) { background: var(--danger); }
  .cm :global(.cm-lint-marker-warning) { background: var(--warn); }
  .cm :global(.cm-lint-marker-info) { background: var(--text-3); }
  .cm :global(.cm-gutter-lint .cm-gutterElement) { padding: 0 2px; }

  :global(.cm-tooltip) {
    font: 12px/1.45 var(--font);
    background: var(--surface) !important;
    border: 1px solid var(--border-strong) !important;
    border-radius: var(--radius);
    box-shadow: 0 8px 24px -8px rgb(20 20 16 / .2);
    color: var(--text);
  }
  :global(.cm-diagnostic) { display: flex; align-items: baseline; gap: 7px; padding: 4px 9px; border-left: 0; }
  :global(.cm-diagnostic)::before { content: ''; flex: none; width: 7px; height: 7px; transform: translateY(-1px); }
  :global(.cm-diagnostic-error)::before { background: var(--danger); }
  :global(.cm-diagnostic-warning)::before { background: var(--warn); }
  :global(.cm-tooltip-autocomplete ul li[aria-selected]) { background: var(--sel) !important; color: var(--text) !important; }
  :global(.cm-completionLabel) { font-family: var(--mono); }
  :global(.cm-completionDetail) { color: var(--text-3); font-style: normal; margin-left: 8px; }
</style>
