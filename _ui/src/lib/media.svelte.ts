// Reactive media queries. Breakpoints match Tailwind's sm and lg so CSS and
// script switch layouts at the same widths.
function query(q: string) {
  const mq = matchMedia(q)
  let matches = $state(mq.matches)
  mq.addEventListener('change', (e) => (matches = e.matches))
  return {
    get matches() {
      return matches
    },
  }
}

/** Phones: below 640px. */
export const narrow = query('(max-width: 639.98px)')
/** Desktop layout with the fixed sidebar: 1024px and up. */
export const wide = query('(min-width: 1024px)')
