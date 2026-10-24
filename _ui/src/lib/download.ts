/** Saves body as a file named name, as if downloaded. */
export function download(name: string, body: string, type: string) {
  const a = document.createElement('a')
  a.href = URL.createObjectURL(new Blob([body], { type }))
  a.download = name
  a.click()
  // Revoking at once can cancel the download before it starts.
  setTimeout(() => URL.revokeObjectURL(a.href), 1000)
}

/** A timestamp that is safe in a file name. */
export const fileStamp = (iso = new Date().toISOString()) => iso.replaceAll(':', '-')
