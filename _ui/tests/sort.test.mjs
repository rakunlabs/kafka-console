import { test } from 'node:test'
import assert from 'node:assert/strict'
import { nextSort } from '../src/lib/sort.ts'

test('a number column cycles largest first, smallest first, then clears', () => {
  let s = { key: 'name', desc: false }
  s = nextSort(s, 'lag')
  assert.deepEqual(s, { key: 'lag', desc: true })
  s = nextSort(s, 'lag')
  assert.deepEqual(s, { key: 'lag', desc: false })
  s = nextSort(s, 'lag')
  assert.deepEqual(s, { key: 'name', desc: false })
})

test('the name column flips A–Z and Z–A, and clears any other sort', () => {
  assert.deepEqual(nextSort({ key: 'name', desc: false }, 'name'), { key: 'name', desc: true })
  assert.deepEqual(nextSort({ key: 'name', desc: true }, 'name'), { key: 'name', desc: false })
  assert.deepEqual(nextSort({ key: 'size', desc: true }, 'name'), { key: 'name', desc: false })
})

test('switching columns starts the new one largest first', () => {
  assert.deepEqual(nextSort({ key: 'size', desc: false }, 'messages'), { key: 'messages', desc: true })
})
