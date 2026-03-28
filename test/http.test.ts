import test from 'node:test';
import assert from 'node:assert/strict';
import { postJson } from '../src/lib/http.js';

test('postJson returns parsed JSON payloads', async () => {
  const payload = await postJson({
    url: 'https://example.com',
    headers: { Authorization: 'Bearer x' },
    body: { hello: 'world' },
    timeoutMs: 10,
    fetchImpl: async () => ({
      ok: true,
      status: 200,
      headers: {
        get: () => 'application/json',
      },
      text: async () => '{"hello":"world"}',
    }),
  });

  assert.deepEqual(payload, { hello: 'world' });
});

test('postJson returns text payloads for non-json content', async () => {
  const payload = await postJson({
    url: 'https://example.com',
    headers: { Authorization: 'Bearer x' },
    body: { hello: 'world' },
    timeoutMs: 10,
    fetchImpl: async () => ({
      ok: true,
      status: 200,
      headers: {
        get: () => 'text/plain',
      },
      text: async () => 'ok',
    }),
  });

  assert.equal(payload, 'ok');
});

test('postJson returns text payloads when content type is missing', async () => {
  const payload = await postJson({
    url: 'https://example.com',
    headers: { Authorization: 'Bearer x' },
    body: { hello: 'world' },
    timeoutMs: 10,
    fetchImpl: async () => ({
      ok: true,
      status: 200,
      headers: {
        get: () => null,
      },
      text: async () => 'ok',
    }),
  });

  assert.equal(payload, 'ok');
});

test('postJson throws on http errors', async () => {
  await assert.rejects(
    () =>
      postJson({
        url: 'https://example.com',
        headers: { Authorization: 'Bearer x' },
        body: {},
        timeoutMs: 10,
        fetchImpl: async () => ({
          ok: false,
          status: 500,
          headers: {
            get: () => 'application/json',
          },
          text: async () => '',
        }),
      }),
    /HTTP 500/u,
  );
});

test('postJson throws on invalid json responses', async () => {
  await assert.rejects(
    () =>
      postJson({
        url: 'https://example.com',
        headers: { Authorization: 'Bearer x' },
        body: {},
        timeoutMs: 10,
        fetchImpl: async () => ({
          ok: true,
          status: 200,
          headers: {
            get: () => 'application/json',
          },
          text: async () => '{invalid-json',
        }),
      }),
    /not valid JSON/u,
  );
});
