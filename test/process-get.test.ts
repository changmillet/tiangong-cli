import test from 'node:test';
import assert from 'node:assert/strict';
import { CliError } from '../src/lib/errors.js';
import type { FetchLike } from '../src/lib/http.js';
import { runProcessGet } from '../src/lib/process-get.js';

function jsonFetch(responses: unknown[], observedUrls: string[] = []): FetchLike {
  let index = 0;
  return (async (input) => {
    observedUrls.push(String(input));
    const next = responses[Math.min(index, responses.length - 1)];
    index += 1;
    return {
      ok: true,
      status: 200,
      headers: {
        get: () => 'application/json',
      },
      text: async () => JSON.stringify(next),
    };
  }) as FetchLike;
}

test('runProcessGet resolves the exact requested process version', async () => {
  const observedUrls: string[] = [];
  const report = await runProcessGet({
    processId: ' proc-1 ',
    version: '00.00.001',
    timeoutMs: 99,
    now: new Date('2026-03-30T00:00:00.000Z'),
    env: {
      TIANGONG_LCA_API_BASE_URL: 'https://example.supabase.co/functions/v1',
      TIANGONG_LCA_API_KEY: 'secret-token',
    } as NodeJS.ProcessEnv,
    fetchImpl: jsonFetch(
      [
        [
          {
            id: 'proc-1',
            version: '00.00.001',
            json: { processDataSet: { id: 'proc-1' } },
            modified_at: '2026-03-29T00:00:00.000Z',
            state_code: 100,
          },
        ],
      ],
      observedUrls,
    ),
  });

  assert.deepEqual(report, {
    schema_version: 1,
    generated_at_utc: '2026-03-30T00:00:00.000Z',
    status: 'resolved_remote_process',
    process_id: 'proc-1',
    requested_version: '00.00.001',
    resolved_version: '00.00.001',
    resolution: 'remote_supabase_exact',
    source_url:
      'https://example.supabase.co/rest/v1/processes?select=id%2Cversion%2Cjson%2Cmodified_at%2Cstate_code&id=eq.proc-1&version=eq.00.00.001',
    modified_at: '2026-03-29T00:00:00.000Z',
    state_code: 100,
    process: { processDataSet: { id: 'proc-1' } },
  });
  assert.equal(observedUrls.length, 1);
});

test('runProcessGet can fall back to process.env and global fetch', async () => {
  const originalFetch = globalThis.fetch;
  const originalBaseUrl = process.env.TIANGONG_LCA_API_BASE_URL;
  const originalApiKey = process.env.TIANGONG_LCA_API_KEY;

  process.env.TIANGONG_LCA_API_BASE_URL = 'https://example.supabase.co';
  process.env.TIANGONG_LCA_API_KEY = 'secret-token';
  globalThis.fetch = (async () => ({
    ok: true,
    status: 200,
    headers: {
      get: () => 'application/json',
    },
    text: async () =>
      JSON.stringify([
        {
          id: '',
          version: '',
          json: { processDataSet: { id: 'proc-2', latest: true } },
          modified_at: null,
          state_code: null,
        },
      ]),
  })) as unknown as typeof fetch;

  try {
    const report = await runProcessGet({
      processId: 'proc-2',
    });

    assert.equal(report.process_id, 'proc-2');
    assert.equal(report.requested_version, null);
    assert.equal(report.resolution, 'remote_supabase_latest');
    assert.equal(report.resolved_version, '');
  } finally {
    globalThis.fetch = originalFetch;
    if (originalBaseUrl === undefined) {
      delete process.env.TIANGONG_LCA_API_BASE_URL;
    } else {
      process.env.TIANGONG_LCA_API_BASE_URL = originalBaseUrl;
    }
    if (originalApiKey === undefined) {
      delete process.env.TIANGONG_LCA_API_KEY;
    } else {
      process.env.TIANGONG_LCA_API_KEY = originalApiKey;
    }
  }
});

test('runProcessGet falls back to the latest reachable version when exact lookup misses', async () => {
  const observedUrls: string[] = [];
  const report = await runProcessGet({
    processId: 'proc-1',
    version: '00.00.001',
    env: {
      TIANGONG_LCA_API_BASE_URL: 'https://example.supabase.co/rest/v1',
      TIANGONG_LCA_API_KEY: 'secret-token',
    } as NodeJS.ProcessEnv,
    fetchImpl: jsonFetch(
      [
        [],
        [
          {
            id: 'proc-1',
            version: '00.00.003',
            json: '{"processDataSet":{"id":"proc-1","latest":true}}',
            modified_at: null,
            state_code: 50,
          },
        ],
      ],
      observedUrls,
    ),
  });

  assert.equal(report.resolution, 'remote_supabase_latest_fallback');
  assert.equal(report.resolved_version, '00.00.003');
  assert.deepEqual(report.process, {
    processDataSet: {
      id: 'proc-1',
      latest: true,
    },
  });
  assert.equal(observedUrls.length, 2);
  assert.match(observedUrls[0] as string, /version=eq\.00\.00\.001/u);
  assert.match(observedUrls[1] as string, /order=version.desc/u);
});

test('runProcessGet loads the latest row when no version is requested', async () => {
  const report = await runProcessGet({
    processId: 'proc-1',
    env: {
      TIANGONG_LCA_API_BASE_URL: 'https://example.supabase.co',
      TIANGONG_LCA_API_KEY: 'secret-token',
    } as NodeJS.ProcessEnv,
    fetchImpl: jsonFetch([
      [
        {
          id: 'proc-1',
          version: '00.00.004',
          json: { processDataSet: { id: 'proc-1', latest: true } },
          modified_at: null,
          state_code: null,
        },
      ],
    ]),
  });

  assert.equal(report.requested_version, null);
  assert.equal(report.resolution, 'remote_supabase_latest');
  assert.equal(report.resolved_version, '00.00.004');
});

test('runProcessGet rejects missing process identifiers', async () => {
  await assert.rejects(
    () =>
      runProcessGet({
        processId: '   ',
        env: {
          TIANGONG_LCA_API_BASE_URL: 'https://example.supabase.co',
          TIANGONG_LCA_API_KEY: 'secret-token',
        } as NodeJS.ProcessEnv,
        fetchImpl: jsonFetch([[]]),
      }),
    (error) => error instanceof CliError && error.code === 'PROCESS_ID_REQUIRED',
  );
});

test('runProcessGet rejects missing processes after fallback', async () => {
  await assert.rejects(
    () =>
      runProcessGet({
        processId: 'proc-missing',
        version: '00.00.001',
        env: {
          TIANGONG_LCA_API_BASE_URL: 'https://example.supabase.co',
          TIANGONG_LCA_API_KEY: 'secret-token',
        } as NodeJS.ProcessEnv,
        fetchImpl: jsonFetch([[], []]),
      }),
    (error) => error instanceof CliError && error.code === 'PROCESS_GET_NOT_FOUND',
  );
});

test('runProcessGet rejects missing processes when only the latest lookup is requested', async () => {
  await assert.rejects(
    () =>
      runProcessGet({
        processId: 'proc-missing',
        env: {
          TIANGONG_LCA_API_BASE_URL: 'https://example.supabase.co',
          TIANGONG_LCA_API_KEY: 'secret-token',
        } as NodeJS.ProcessEnv,
        fetchImpl: jsonFetch([[]]),
      }),
    (error) => error instanceof CliError && error.code === 'PROCESS_GET_NOT_FOUND',
  );
});
