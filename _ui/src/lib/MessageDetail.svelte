<script lang="ts">
  import { rawURL, type Cluster, type Message } from './api'
  import { fmt } from './format'
  import type { copier } from './resource.svelte'
  import { saveDetailWidth } from './prefs.svelte'
  import { DETAIL_MIN, clamp } from './messageLayout'
  import { drag } from './drag'
  import { narrow } from './media.svelte'
  import ValueView from './ValueView.svelte'
  import ValueMeta from './ValueMeta.svelte'
  import MessageActions from './MessageActions.svelte'
  import { Download, Copy, Check, ChevronUp, ChevronDown, X } from '@lucide/svelte'

  // One message in full, beside the list, over it as a drawer, or as a
  // full-screen sheet on phones.
  let {
    cluster,
    topic,
    message: m,
    clip,
    stacked,
    max,
    width = $bindable(),
    resizing = $bindable(false),
    editing,
    when,
    other,
    onedit,
    onresend,
    onstep,
    onclose,
  }: {
    cluster: Cluster
    topic: string
    message: Message
    // Shared with the list's copy buttons: the tick shows here either way.
    clip: ReturnType<typeof copier>
    // The list and detail do not fit side by side.
    stacked: boolean
    // The widest the detail may be beside the list.
    max: number
    // Dragged width, remembered; null for the default.
    width: number | null
    resizing?: boolean
    editing: boolean
    when: (iso: string) => string
    other: (iso: string) => string
    onedit: (m: Message) => void
    onresend: (m: Message) => void
    onstep: (by: number) => void
    onclose: () => void
  } = $props()

  const sheet = $derived(narrow.matches)
  // Too narrow for the side-by-side split but wider than a phone, the detail
  // slides over the list from the right instead of opening below it.
  const drawer = $derived(!narrow.matches && stacked)

  // On phones the detail is a full-screen sheet; the page behind must not
  // scroll with it, and the app header (a separate stacking layer) steps
  // aside so the sheet can cover the whole screen.
  $effect(() => {
    const html = document.documentElement
    html.style.overflow = sheet ? 'hidden' : ''
    html.classList.toggle('sheet-open', sheet)
    return () => {
      html.style.overflow = ''
      html.classList.remove('sheet-open')
    }
  })

  // Beside the list the detail can be dragged wider or narrower by its left
  // edge; the width is remembered. It never squeezes the list below its minimum.
  const RESIZE_KEY_STEP = 24
  let el = $state<HTMLElement>()
  const fit = (px: number) => clamp(px, DETAIL_MIN, max)
  const shown = $derived(width === null ? null : fit(width))
  function startResize(e: PointerEvent) {
    if (!el) return
    const startW = el.getBoundingClientRect().width
    resizing = drag(e, {
      move: (dx) => (width = fit(startW - dx)),
      end: () => {
        resizing = false
        saveDetailWidth(width)
      },
    })
  }
  function resizeKey(e: KeyboardEvent) {
    const by = e.key === 'ArrowLeft' ? RESIZE_KEY_STEP : e.key === 'ArrowRight' ? -RESIZE_KEY_STEP : 0
    if (!by || !el) return
    e.preventDefault()
    e.stopPropagation()
    width = fit((shown ?? el.getBoundingClientRect().width) + by)
    saveDetailWidth(width)
  }
  function resetWidth() {
    width = null
    saveDetailWidth(null)
  }
</script>

<div class="panel detail" class:sheet class:drawer bind:this={el} role={sheet || drawer ? 'dialog' : undefined} aria-modal={sheet ? 'true' : undefined}
  aria-label={sheet || drawer ? `Message ${m.partition}:${m.offset}` : undefined}>
  {#if !stacked}
    <!-- svelte-ignore a11y_no_noninteractive_tabindex, a11y_no_noninteractive_element_interactions -->
    <div class="resize" role="separator" aria-orientation="vertical" aria-label="Resize message panel"
      aria-valuemin={DETAIL_MIN} aria-valuemax={max} aria-valuenow={shown ?? undefined} tabindex="0"
      title="Drag to resize · double-click to reset" onpointerdown={startResize} onkeydown={resizeKey} ondblclick={resetWidth}></div>
  {/if}
  <div class="dhead">
    <div class="dtop">
      <div class="dtitle">
        <b><span class="faint">Partition</span> {m.partition} <span class="faint">·</span> <span class="faint">Offset</span> <span class="mono">{fmt(m.offset)}</span></b>
        <span class="faint mono time" title={other(m.timestamp)}>{when(m.timestamp)}</span>
      </div>
      <div class="seg" role="group" aria-label="Walk messages">
        <button title="Previous (k)" aria-label="Previous message" onclick={() => onstep(-1)}><ChevronUp size={15} /></button>
        <button title="Next (j)" aria-label="Next message" onclick={() => onstep(1)}><ChevronDown size={15} /></button>
      </div>
      <button class="ghost icon" title="Close (Esc)" aria-label="Close" onclick={onclose}><X size={16} /></button>
    </div>
    {#if !cluster.read_only}
      <div class="dacts"><MessageActions message={m} {editing} {onedit} {onresend} /></div>
    {/if}
  </div>

  <div class="dbody">
  {#if m.decode_error}
    <section><div class="error small-err">Schema decoding failed: {m.decode_error}</div></section>
  {/if}

  <section>
    <div class="label">
      Key
      {#if m.key_format}<span class="badge accent">{m.key_format}</span>{/if}
      {#if m.key_schema_id}<span class="faint">schema #{m.key_schema_id}</span>{/if}
      {#if m.key_encoding === 'base64'}<span class="badge">base64</span>{/if}
      {#if m.key !== null}<span class="faint">{fmt(m.key_bytes)} bytes</span>{/if}
      {#if m.key !== null}
        <span class="spacer"></span>
        <a class="btn ghost small" href={rawURL(cluster.name, topic, m.partition, m.offset, 'key')} download
          title="Download the key's bytes exactly as stored"><Download size={13} />Raw</a>
        <button class="ghost small" onclick={() => clip.copy(m.key ?? '', 'key')}>{#if clip.copied === 'key'}<Check size={13} />Copied{:else}<Copy size={13} />Copy{/if}</button>
      {/if}
    </div>
    <ValueView text={m.key} format={m.key_format} depth={2} />
  </section>

  <section>
    <div class="label">
      Value
      <ValueMeta message={m} />
      <span class="spacer"></span>
      {#if m.value !== null}
        <a class="btn ghost small" href={rawURL(cluster.name, topic, m.partition, m.offset, 'value')} download
          title={m.truncated ? 'Only the first 64 KiB is shown; download the whole value as stored' : "Download the value's bytes exactly as stored"}><Download size={13} />{m.truncated ? 'Download full' : 'Raw'}</a>
      {/if}
      <button class="ghost small" onclick={() => clip.copy(m.value ?? '', 'value')}>{#if clip.copied === 'value'}<Check size={13} />Copied{:else}<Copy size={13} />Copy{/if}</button>
    </div>
    {#key m}<ValueView text={m.value} format={m.value_format} />{/key}
  </section>

  <section>
    <div class="label">Headers <span class="faint">{m.headers.length}</span></div>
    {#if m.headers.length}
      <table class="headers">
        <tbody>
          {#each m.headers as h, i (i)}
            <tr><td class="mono">{h.key}</td><td class="mono">{h.value}</td></tr>
          {/each}
        </tbody>
      </table>
    {:else}
      <div class="faint">No headers.</div>
    {/if}
  </section>
  </div>
</div>

<style>
  .small-err { font-size: 12px; padding: 6px 8px; }
  /* Beside the list the detail grows with its content and the page scrolls;
     as a drawer or sheet it is fixed, so the header stays put and the body
     under it scrolls. */
  .detail { position: relative; display: flex; flex-direction: column; overflow: hidden; }
  /* The left edge is the grab handle for resizing beside the list. */
  .resize { position: absolute; top: 0; bottom: 0; left: 0; z-index: 3; width: 7px; cursor: col-resize; touch-action: none; }
  .resize:hover, .resize:focus-visible, :global(.resizing) .resize { background: var(--cta-border); outline: none; }
  .dbody { flex: 1 1 auto; min-height: 0; }
  .drawer .dbody, .sheet .dbody { overflow: auto; overscroll-behavior: contain; }
  /* Header: who the message is, how to walk and close, on one line that
     never wraps; the write actions sit on their own row under it. */
  .dhead { flex: none; display: grid; grid-template-columns: minmax(0, 1fr); border-bottom: 1px solid var(--border-strong); background: var(--surface-2); }
  .dtop { display: flex; align-items: center; gap: 8px; padding: 4px 4px 4px 12px; }
  /* Title and time on one baseline; the time gives way first when narrow. */
  .dtitle { display: flex; align-items: baseline; gap: 10px; flex: 1; min-width: 0; }
  .dtitle > * { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .dtitle b { flex: none; max-width: 100%; font-weight: 600; }
  .dtitle b .faint { font-weight: 400; }
  .dtitle b .mono { font-size: 14px; font-weight: 600; }
  .dtitle .time { flex: 0 1 auto; min-width: 0; font-size: 12px; }
  .seg { display: flex; flex: none; }
  .seg button { padding: 2px 5px; border-radius: 0; color: var(--text-2); background: var(--surface); }
  .seg button + button { margin-left: -1px; }
  .seg button:first-child { border-radius: var(--radius) 0 0 var(--radius); }
  .seg button:last-child { border-radius: 0 var(--radius) var(--radius) 0; }
  .seg button:hover:not(:disabled) { color: var(--text); }
  .icon { flex: none; padding: 3px; color: var(--text-2); }
  .icon:hover:not(:disabled) { color: var(--text); }
  .dacts { display: flex; flex-wrap: wrap; gap: 6px; padding: 4px 12px; border-top: 1px dashed var(--line-dash); }
  section { padding: 12px; border-bottom: 1px solid var(--border); }
  section:last-child { border-bottom: 0; }
  .label { display: flex; align-items: center; gap: 6px; font-size: 12px; font-weight: 500; color: var(--text-2); margin-bottom: 6px; }
  .headers td { padding: 4px 8px; font-size: 12px; }

  /* The drawer leaves the list's left side visible, so rows stay clickable
     and j/k keep walking it; under lg it starts below the app header. */
  .detail.drawer {
    position: fixed;
    top: 0;
    right: 0;
    bottom: 0;
    z-index: 32;
    width: min(560px, calc(100vw - 64px));
    max-height: none;
    border-width: 0 0 0 1px;
    border-radius: 0;
    box-shadow: -12px 0 32px rgb(0 0 0 / .18);
    animation: drawer-in .18s cubic-bezier(.16, 1, .3, 1);
  }
  @keyframes drawer-in { from { transform: translateX(24px); opacity: 0; } }
  @media (max-width: 1023.98px) { .detail.drawer { top: 48px; } }
  @media (prefers-reduced-motion: reduce) { .detail.drawer { animation: none; } }

  @media (max-width: 639.98px) {
    .detail.sheet {
      position: fixed;
      inset: 0;
      z-index: 50;
      max-height: none;
      border: 0;
      border-radius: 0;
      overscroll-behavior: contain;
      padding-bottom: env(safe-area-inset-bottom);
    }
    .sheet .dtop { padding: 10px 8px 10px 14px; }
    .sheet .dacts { padding-inline: 14px; }
    .sheet .seg button { padding: 6px 9px; }
    .sheet .icon { padding: 7px; }
  }
</style>
