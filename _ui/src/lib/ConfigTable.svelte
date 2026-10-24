<script lang="ts">
  import { resizable } from './resizable'
  import { errorMessage } from './http'
  import type { ConfigChange, ConfigEntry } from './api'

  let {
    configs,
    readOnly = false,
    save,
  }: {
    configs: ConfigEntry[]
    readOnly?: boolean
    save: (changes: ConfigChange[]) => Promise<void>
  } = $props()

  let filter = $state('')
  let onlyOverrides = $state(true)
  let editing = $state('')
  // The key whose Reset waits for a second click.
  let resetting = $state('')
  let draft = $state('')
  let busy = $state(false)
  let error = $state('')

  // Dynamic sources are what an operator set and can reset.
  const isOverride = (c: ConfigEntry) => c.source.startsWith('DYNAMIC_')

  const rows = $derived(
    configs.filter(
      (c) =>
        (!onlyOverrides || c.source !== 'DEFAULT_CONFIG') &&
        (!filter || c.key.toLowerCase().includes(filter.toLowerCase())),
    ),
  )

  function edit(c: ConfigEntry) {
    editing = c.key
    draft = c.value ?? ''
    error = ''
  }

  async function run(changes: ConfigChange[]) {
    busy = true
    error = ''
    try {
      await save(changes)
      editing = ''
    } catch (e) {
      error = errorMessage(e)
    } finally {
      busy = false
    }
  }

  // Durations in ms and sizes in bytes are hard to read raw.
  function hint(c: ConfigEntry) {
    if (c.value === null || c.sensitive) return ''
    const n = Number(c.value)
    if (!Number.isFinite(n) || n <= 0 || !/^\d+$/.test(c.value)) return ''
    if (c.key.endsWith('.ms')) {
      const units: [number, string][] = [[86400000, 'd'], [3600000, 'h'], [60000, 'min'], [1000, 's']]
      for (const [u, l] of units) if (n >= u && n % u === 0) return `${n / u} ${l}`
    }
    if (c.key.endsWith('.bytes') || c.key.endsWith('.size')) {
      const units: [number, string][] = [[1 << 30, 'GiB'], [1 << 20, 'MiB'], [1 << 10, 'KiB']]
      for (const [u, l] of units) if (n >= u) return `${+(n / u).toFixed(1)} ${l}`
    }
    return ''
  }
</script>

<div class="filters">
  <input type="search" placeholder="Search configs…" bind:value={filter} />
  <label class="check"><input type="checkbox" bind:checked={onlyOverrides} /> Only non-default values</label>
</div>

{#if error}<div class="error">{error}</div>{/if}

<div class="panel">
  <table use:resizable={'configs'}>
    <thead><tr><th>Key</th><th>Value</th><th>Source</th><th></th></tr></thead>
    <tbody>
      {#each rows as c (c.key)}
        <tr>
          <td class="mono">{c.key}</td>
          <td class="mono grow wrap">
            {#if editing === c.key}
              <form class="row" onsubmit={(e) => { e.preventDefault(); run([{ key: c.key, value: draft, op: 'set' }]) }}>
                <!-- svelte-ignore a11y_autofocus -->
                <input class="mono flex-1" bind:value={draft} autofocus onkeydown={(e) => e.key === 'Escape' && (editing = '')} />
                <button class="primary small" type="submit" disabled={busy}>Save</button>
                <button class="small" type="button" onclick={() => (editing = '')}>Cancel</button>
              </form>
            {:else}
              {c.sensitive ? '••••••' : (c.value ?? '—')}
              {#if hint(c)}<span class="faint hint">{hint(c)}</span>{/if}
            {/if}
          </td>
          <td class="faint">{c.source}</td>
          <td class="actions">
            {#if !readOnly && editing !== c.key}
              {#if resetting === c.key}
                <span class="confirm">
                  <span class="small">Remove this override?</span>
                  <button class="danger solid small" disabled={busy} onclick={() => run([{ key: c.key, op: 'delete' }]).then(() => (resetting = ''))}>Reset</button>
                  <button class="small" disabled={busy} onclick={() => (resetting = '')}>Keep</button>
                </span>
              {:else}
                <button class="ghost small" onclick={() => edit(c)}>Edit</button>
                {#if isOverride(c)}
                  <button class="ghost small" disabled={busy} title="Remove the dynamic value and revert to the default"
                    onclick={() => { resetting = c.key; error = '' }}>Reset</button>
                {/if}
              {/if}
            {/if}
          </td>
        </tr>
      {:else}
        <tr><td colspan="4" class="empty">No configs to show.</td></tr>
      {/each}
    </tbody>
  </table>
</div>

<style>
  .error { margin-bottom: 12px; }
  .hint { margin-left: 8px; font-family: var(--font); font-size: 12px; }
  .actions { white-space: nowrap; text-align: right; width: 1%; padding-block: 5px; }
  .confirm { display: inline-flex; align-items: center; gap: 6px; color: var(--danger); }
</style>
