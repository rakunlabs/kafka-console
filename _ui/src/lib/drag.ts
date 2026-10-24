// A horizontal pointer drag started from a pointerdown on a handle. The
// handle captures the pointer, so the drag keeps going over other elements
// and ends however the pointer is released. move gets the distance from
// where the drag started; returns false when the event does not start one.
export function drag(
  e: PointerEvent,
  on: { move: (dx: number) => void; end?: () => void; stopPropagation?: boolean; cursor?: boolean },
): boolean {
  if (e.button !== 0) return false
  e.preventDefault()
  if (on.stopPropagation) e.stopPropagation()
  const handle = e.currentTarget as HTMLElement
  handle.setPointerCapture(e.pointerId)
  const startX = e.clientX
  // Keeps the resize cursor everywhere and stops text selection meanwhile.
  const root = document.documentElement
  if (on.cursor) root.classList.add('col-resizing')
  const move = (ev: PointerEvent) => on.move(ev.clientX - startX)
  const end = () => {
    handle.removeEventListener('pointermove', move)
    handle.removeEventListener('pointerup', end)
    handle.removeEventListener('pointercancel', end)
    if (on.cursor) root.classList.remove('col-resizing')
    on.end?.()
  }
  handle.addEventListener('pointermove', move)
  handle.addEventListener('pointerup', end)
  handle.addEventListener('pointercancel', end)
  return true
}
