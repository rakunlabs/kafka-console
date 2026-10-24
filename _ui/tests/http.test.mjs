import assert from 'node:assert/strict'
import { test } from 'node:test'
import { errorMessage, responseError, HTTPError } from '../src/lib/http.ts'
import { messageStream } from '../src/lib/api.ts'

test('errorMessage reads errors, strings and anything else', () => {
  assert.equal(errorMessage(new Error('boom')), 'boom')
  assert.equal(errorMessage('plain'), 'plain')
  assert.equal(errorMessage(42), '42')
  assert.equal(errorMessage(undefined), 'undefined')
})

test('responseError prefers the JSON message, then the body, then the status text', async () => {
  const json = await responseError(new Response('{"message":"no such topic"}', { status: 404, statusText: 'Not Found' }))
  assert.ok(json instanceof HTTPError)
  assert.equal(json.status, 404)
  assert.equal(json.message, 'no such topic')
  assert.equal((await responseError(new Response('plain text', { status: 500 }))).message, 'plain text')
  assert.equal((await responseError(new Response('', { status: 502, statusText: 'Bad Gateway' }))).message, 'Bad Gateway')
  assert.equal((await responseError(new Response('{"other":1}', { status: 400 }))).message, '{"other":1}')
})

test('stream errors carry their HTTP status', async (t) => {
  t.mock.method(globalThis, 'fetch', async () => new Response('{"message":"invalid filter"}', { status: 400 }))
  await assert.rejects(
    messageStream('c', 't', {}, () => {}, new AbortController().signal),
    (e) => e instanceof HTTPError && e.status === 400 && e.message === 'invalid filter',
  )
})
