import test from 'node:test';
import assert from 'node:assert/strict';
import { CliError } from '../src/lib/errors.js';
import { createDatasetCommandClient, __testInternals } from '../src/lib/dataset-command.js';

function makeResponse(options: {
  ok: boolean;
  status: number;
  contentType?: string;
  body?: string;
}) {
  return {
    ok: options.ok,
    status: options.status,
    headers: {
      get(name: string): string | null {
        return name.toLowerCase() === 'content-type'
          ? (options.contentType ?? 'application/json')
          : null;
      },
    },
    async text(): Promise<string> {
      return options.body ?? '';
    },
  };
}

const runtime = {
  apiBaseUrl: 'https://example.supabase.co/rest/v1',
  publishableKey: 'sb-publishable-key',
  getAccessToken: async () => 'access-token',
  refreshAccessToken: async () => 'refreshed-access-token',
};

test('dataset command helpers derive URLs, headers, bodies, and unwrap success envelopes', () => {
  assert.equal(__testInternals.command_endpoint('create'), 'app_dataset_create');
  assert.equal(__testInternals.command_endpoint('save_draft'), 'app_dataset_save_draft');
  assert.equal(
    __testInternals.buildDatasetCommandUrl('https://example.supabase.co', 'create'),
    'https://example.supabase.co/functions/v1/app_dataset_create',
  );
  assert.equal(
    __testInternals.buildDatasetCommandUrl('https://example.supabase.co/rest/v1', 'save_draft'),
    'https://example.supabase.co/functions/v1/app_dataset_save_draft',
  );
  assert.deepEqual(__testInternals.buildDatasetCommandHeaders('us-east-1'), {
    'Content-Type': 'application/json',
    'x-region': 'us-east-1',
  });
  assert.deepEqual(__testInternals.buildDatasetCommandHeaders('  '), {
    'Content-Type': 'application/json',
  });
  assert.deepEqual(
    __testInternals.buildDatasetCommandBody('create', {
      table: 'flows',
      id: 'flow-1',
      jsonOrdered: { flowDataSet: {} },
      ruleVerification: false,
    }),
    {
      table: 'flows',
      id: 'flow-1',
      jsonOrdered: { flowDataSet: {} },
      ruleVerification: false,
    },
  );
  assert.deepEqual(
    __testInternals.buildDatasetCommandBody('save_draft', {
      table: 'processes',
      id: 'proc-1',
      version: '01.00.001',
      jsonOrdered: { processDataSet: {} },
      modelId: 'model-1',
    }),
    {
      table: 'processes',
      id: 'proc-1',
      version: '01.00.001',
      jsonOrdered: { processDataSet: {} },
      modelId: 'model-1',
    },
  );
  assert.deepEqual(
    __testInternals.unwrapDatasetCommandPayload({
      ok: true,
      data: { id: 'flow-1', version: '01.00.001' },
    }),
    { id: 'flow-1', version: '01.00.001' },
  );
  assert.equal(
    __testInternals.unwrapDatasetCommandPayload({
      ok: true,
      data: null,
    }),
    null,
  );
  assert.equal(__testInternals.unwrapDatasetCommandPayload('plain-text'), 'plain-text');

  assert.throws(
    () =>
      __testInternals.unwrapDatasetCommandPayload({
        ok: false,
        code: 'DATASET_OWNER_REQUIRED',
        message: 'Only the dataset owner can save draft changes',
      }),
    (error) =>
      error instanceof CliError &&
      error.code === 'REMOTE_REQUEST_FAILED' &&
      error.details === 'DATASET_OWNER_REQUIRED: Only the dataset owner can save draft changes',
  );
});

test('dataset command client posts create and save-draft requests to edge-function endpoints', async () => {
  const observed: Array<{ url: string; method: string; headers: Headers; body: string }> = [];
  let callCount = 0;
  const client = createDatasetCommandClient({
    runtime,
    fetchImpl: async (url, init) => {
      observed.push({
        url,
        method: String(init?.method ?? ''),
        headers: new Headers(init?.headers),
        body: typeof init?.body === 'string' ? init.body : '',
      });
      callCount += 1;
      return makeResponse({
        ok: true,
        status: 200,
        body: JSON.stringify({
          ok: true,
          command: callCount === 1 ? 'dataset_create' : 'dataset_save_draft',
          data: callCount === 1 ? { id: 'flow-1' } : { id: 'flow-1', version: '01.00.001' },
        }),
      });
    },
    timeoutMs: 25,
    region: 'us-east-1',
  });

  assert.deepEqual(
    await client.create({
      table: 'flows',
      id: 'flow-1',
      jsonOrdered: { flowDataSet: {} },
      ruleVerification: null,
    }),
    { id: 'flow-1' },
  );
  assert.deepEqual(
    await client.saveDraft({
      table: 'processes',
      id: 'proc-1',
      version: '01.00.001',
      jsonOrdered: { processDataSet: {} },
      modelId: 'model-1',
    }),
    { id: 'flow-1', version: '01.00.001' },
  );

  assert.deepEqual(
    observed.map((entry) => [entry.method, entry.url]),
    [
      ['POST', 'https://example.supabase.co/functions/v1/app_dataset_create'],
      ['POST', 'https://example.supabase.co/functions/v1/app_dataset_save_draft'],
    ],
  );
  assert.equal(observed[0]?.headers.get('Authorization'), 'Bearer access-token');
  assert.equal(observed[0]?.headers.get('apikey'), 'sb-publishable-key');
  assert.equal(observed[0]?.headers.get('x-region'), 'us-east-1');
  assert.deepEqual(JSON.parse(observed[0]?.body ?? '{}'), {
    table: 'flows',
    id: 'flow-1',
    jsonOrdered: { flowDataSet: {} },
    ruleVerification: null,
  });
  assert.deepEqual(JSON.parse(observed[1]?.body ?? '{}'), {
    table: 'processes',
    id: 'proc-1',
    version: '01.00.001',
    jsonOrdered: { processDataSet: {} },
    modelId: 'model-1',
  });
});

test('dataset command client maps structured command failures to CliError', async () => {
  const client = createDatasetCommandClient({
    runtime,
    fetchImpl: async () =>
      makeResponse({
        ok: false,
        status: 403,
        body: JSON.stringify({
          ok: false,
          code: 'DATASET_OWNER_REQUIRED',
          message: 'Only the dataset owner can save draft changes',
        }),
      }),
    timeoutMs: 25,
    region: null,
  });

  await assert.rejects(
    () =>
      client.saveDraft({
        table: 'sources',
        id: 'src-1',
        version: '01.00.001',
        jsonOrdered: { sourceDataSet: {} },
      }),
    (error) =>
      error instanceof CliError &&
      error.code === 'REMOTE_REQUEST_FAILED' &&
      error.details === 'DATASET_OWNER_REQUIRED: Only the dataset owner can save draft changes',
  );
});

test('dataset command response parsing handles plain text and invalid JSON branches', async () => {
  assert.equal(
    __testInternals.parseDatasetCommandResponse(
      makeResponse({
        ok: true,
        status: 200,
        contentType: 'text/plain',
        body: 'created',
      }),
      'https://example.supabase.co/functions/v1/app_dataset_create',
      'created',
    ),
    'created',
  );

  assert.throws(
    () =>
      __testInternals.parseDatasetCommandResponse(
        makeResponse({
          ok: true,
          status: 200,
          contentType: 'application/json',
          body: '{broken-json',
        }),
        'https://example.supabase.co/functions/v1/app_dataset_create',
        '{broken-json',
      ),
    (error) => error instanceof CliError && error.code === 'REMOTE_INVALID_JSON',
  );

  assert.throws(
    () =>
      __testInternals.parseDatasetCommandResponse(
        makeResponse({
          ok: false,
          status: 500,
          contentType: 'text/plain',
          body: 'upstream unavailable',
        }),
        'https://example.supabase.co/functions/v1/app_dataset_save_draft',
        'upstream unavailable',
      ),
    (error) =>
      error instanceof CliError &&
      error.code === 'REMOTE_REQUEST_FAILED' &&
      error.details === 'upstream unavailable',
  );

  assert.throws(
    () =>
      __testInternals.parseDatasetCommandResponse(
        {
          ok: false,
          status: 500,
          headers: {
            get() {
              return null;
            },
          },
          async text() {
            return '';
          },
        },
        'https://example.supabase.co/functions/v1/app_dataset_save_draft',
        '',
      ),
    (error) =>
      error instanceof CliError &&
      error.code === 'REMOTE_REQUEST_FAILED' &&
      error.details === undefined,
  );
});
