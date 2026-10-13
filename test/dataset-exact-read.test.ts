import assert from 'node:assert/strict';
import { createHash, randomUUID } from 'node:crypto';
import fs from 'node:fs';
import { syncBuiltinESMExports } from 'node:module';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import {
  runDatasetReadExact,
  readExactRows,
  isReadScopeViolation,
  type DatasetBodyReadScope,
  type DatasetExactReadScope,
  type DatasetExactReadSelection,
  type RunDatasetReadExactOptions,
} from '../src/lib/dataset-exact-read.js';
import { sha256Json } from '../src/lib/dataset-maintenance-contract.js';
import { CliError } from '../src/lib/errors.js';
import { createSupabaseDataClient } from '../src/lib/supabase-client.js';
import { buildSupabaseTestEnv } from './helpers/supabase-auth.js';

const ACTOR = '11111111-1111-4111-8111-111111111111';
const PROCESS = '22222222-2222-4222-8222-222222222222';
const MODEL = '33333333-3333-4333-8333-333333333333';
const FOREIGN = '44444444-4444-4444-8444-444444444444';
const VERSION = '01.00.001';
const CANARY = 'PRIVATE_CREDENTIAL_CANARY';
const PROCESS_SELECTION: DatasetExactReadSelection = {
  table: 'processes',
  id: PROCESS,
  version: VERSION,
};
const MODEL_SELECTION: DatasetExactReadSelection = {
  table: 'lifecyclemodels',
  id: MODEL,
  version: VERSION,
  referenced_by: [{ id: PROCESS, version: VERSION }],
};

function hash(bytes: string | Buffer): string {
  return createHash('sha256').update(bytes).digest('hex');
}
function token(payload: unknown = { sub: ACTOR, email: 'user@example.com' }): string {
  return `e30.${Buffer.from(JSON.stringify(payload)).toString('base64url')}.${randomUUID()}`;
}
function processRow(overrides: Record<string, unknown> = {}) {
  return {
    id: PROCESS,
    version: VERSION,
    user_id: ACTOR,
    state_code: 0,
    modified_at: null,
    json_ordered: { processDataSet: { 'common:UUID': PROCESS, untouched: [1, 2, 3] } },
    model_id: null,
    model_version: null,
    team_id: null,
    review_id: null,
    ...overrides,
  };
}
function modelRow(overrides: Record<string, unknown> = {}) {
  return {
    id: MODEL,
    version: VERSION,
    user_id: null,
    state_code: 100,
    modified_at: null,
    json_ordered: { lifeCycleModelDataSet: { 'common:UUID': MODEL, graph: ['unaltered'] } },
    team_id: null,
    ...overrides,
  };
}

function serverSelectedRows(url: URL, rows: unknown[]): unknown[] {
  const publicPredicate = `(and(state_code.gte.100,state_code.lt.200),and(state_code.eq.0,user_id.eq.${ACTOR}))`;
  const publicScope = url.searchParams.get('or') === publicPredicate;
  const ownerScope =
    url.searchParams.get('user_id') === `eq.${ACTOR}` &&
    url.searchParams.get('state_code') === 'eq.0';
  assert.ok(
    publicScope || ownerScope,
    'the server requires an explicit fixed read predicate before serializing bodies',
  );
  assert.equal(url.searchParams.get('limit'), '2');
  return rows
    .filter((value) => {
      if (value === null || typeof value !== 'object' || Array.isArray(value)) return false;
      const row = value as Record<string, unknown>;
      return (
        url.searchParams.get('id') === `eq.${String(row.id)}` &&
        url.searchParams.get('version') === `eq.${String(row.version)}` &&
        ((row.state_code === 0 && row.user_id === ACTOR) ||
          (publicScope &&
            typeof row.state_code === 'number' &&
            row.state_code >= 100 &&
            row.state_code < 200))
      );
    })
    .slice(0, 2);
}

function fixture(rows: DatasetExactReadSelection[] = [PROCESS_SELECTION]) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'dataset-exact-read-'));
  const source = path.join(root, 'original-evidence.json');
  const scopeFile = path.join(root, 'scope.json');
  const outDir = path.join(root, 'capture');
  fs.writeFileSync(source, '{"original":"finite provenance"}\n');
  const scope: DatasetExactReadScope = {
    schema_version: 'dataset-exact-read-scope.v1',
    project_ref: 'example',
    actor: { user_id: ACTOR },
    sources: [{ path: source, sha256: hash(fs.readFileSync(source)) }],
    rows,
  };
  const calls: Array<{ url: URL; method: string }> = [];
  const dataCalls: URL[] = [];
  const transferredBodies: string[] = [];
  let contraryServer = false;
  let data: (url: URL) => unknown | Promise<unknown> = () => [processRow()];
  let auth: () => unknown = () => ({ id: ACTOR, email: 'user@example.com', role: 'authenticated' });
  const options: RunDatasetReadExactOptions = {
    scopeFile,
    scopeSha256: '',
    outDir,
    env: buildSupabaseTestEnv({
      TIANGONG_LCA_AUTH_MODE: 'access-token',
      TIANGONG_LCA_ACCESS_TOKEN: token(),
      XDG_STATE_HOME: path.join(root, 'state'),
    }),
    now: new Date('2026-10-12T12:00:00.000Z'),
    fetchImpl: async (input, init) => {
      const url = new URL(input);
      const method = init?.method ?? 'GET';
      calls.push({ url, method });
      if (url.pathname === '/auth/v1/user')
        return new Response(JSON.stringify(auth()), {
          headers: { 'content-type': 'application/json' },
        });
      assert.ok(url.pathname.startsWith('/rest/v1/'), 'only exact Data API reads are allowed');
      assert.equal(method, 'GET');
      dataCalls.push(url);
      const body = await data(url);
      if (body instanceof Response) return body;
      const text = JSON.stringify(
        Array.isArray(body) && !contraryServer ? serverSelectedRows(url, body) : body,
      );
      transferredBodies.push(text);
      return new Response(text, {
        headers: { 'content-type': 'application/json' },
      });
    },
  };
  const writeScope = (value: unknown = scope) => {
    fs.writeFileSync(scopeFile, JSON.stringify(value));
    options.scopeSha256 = hash(fs.readFileSync(scopeFile));
  };
  writeScope();
  return {
    root,
    source,
    scope,
    options,
    calls,
    dataCalls,
    transferredBodies,
    writeScope,
    setData: (next: typeof data) => {
      data = next;
    },
    setContraryServer: () => {
      contraryServer = true;
    },
    setAuth: (next: typeof auth) => {
      auth = next;
    },
    close: () => fs.rmSync(root, { recursive: true, force: true }),
  };
}

function safeError(error: unknown) {
  assert.ok(error instanceof CliError);
  assert.equal(error.details, undefined);
  assert.equal(JSON.stringify(error).includes(CANARY), false);
  assert.equal(error.message.includes(CANARY), false);
  return true;
}

test('exact read observes a public foreign Process unchanged with physical provenance and no write authority', async () => {
  const f = fixture();
  const row = processRow({
    user_id: FOREIGN,
    state_code: 100,
    modified_at: '2026-10-11T10:00:00+00:00',
    team_id: FOREIGN,
    review_id: MODEL,
    irrelevant_header: CANARY,
  });
  f.setData(() => [row]);
  try {
    const sourceBefore = fs.readFileSync(f.source);
    const scopeBefore = fs.readFileSync(f.options.scopeFile);
    const report = await runDatasetReadExact(f.options);
    assert.equal(report.status, 'observed');
    assert.equal(report.authorization_granted, false);
    assert.equal(report.adoption_performed, false);
    assert.equal(report.mode, 'read_only');
    assert.equal(report.read_scope, 'public_100_199_or_current_owner_state_0');
    assert.equal(report.identity.assertions.mode, 'intent-bound');
    assert.equal(report.rows[0]?.status, 'found');
    assert.equal(report.rows[0]?.reason, 'exact_row');
    assert.equal(report.rows[0]?.observed_at_utc, f.options.now?.toISOString());
    assert.equal(report.rows[0]?.actual_row?.user_id, FOREIGN);
    assert.equal(report.rows[0]?.actual_row?.state_code, 100);
    assert.deepEqual(report.rows[0]?.actual_row?.json_ordered, row.json_ordered);
    assert.equal(report.rows[0]?.actual_payload_sha256, sha256Json(row.json_ordered));
    assert.equal(report.rows[0]?.actual_row_sha256, sha256Json(report.rows[0]?.actual_row));
    assert.equal(report.scope.path, fs.realpathSync(f.options.scopeFile));
    assert.equal(report.scope.sha256, f.options.scopeSha256);
    assert.equal(report.sources[0]?.path, fs.realpathSync(f.source));
    assert.equal(JSON.stringify(report).includes(CANARY), false);
    assert.deepEqual(fs.readFileSync(f.source), sourceBefore);
    assert.deepEqual(fs.readFileSync(f.options.scopeFile), scopeBefore);
    assert.equal(f.dataCalls.length, 1);
    const query = f.dataCalls[0]!;
    assert.equal(query.pathname, '/rest/v1/processes');
    assert.equal(query.searchParams.get('id'), `eq.${PROCESS}`);
    assert.equal(query.searchParams.get('version'), `eq.${VERSION}`);
    assert.equal(query.searchParams.get('limit'), '2');
    assert.equal(query.searchParams.has('order'), false);
    assert.equal(
      query.searchParams.get('or'),
      `(and(state_code.gte.100,state_code.lt.200),and(state_code.eq.0,user_id.eq.${ACTOR}))`,
    );
    assert.deepEqual(
      query.searchParams.get('select')?.split(',').sort(),
      [
        'id',
        'version',
        'user_id',
        'state_code',
        'modified_at',
        'json_ordered',
        'team_id',
        'model_id',
        'model_version',
        'review_id',
      ].sort(),
    );
    const artifact = path.join(f.options.outDir, 'read-exact.json');
    assert.deepEqual(JSON.parse(fs.readFileSync(artifact, 'utf8')), report);
    if (process.platform !== 'win32') {
      assert.equal(fs.statSync(f.options.outDir).mode & 0o777, 0o700);
      assert.equal(fs.statSync(artifact).mode & 0o777, 0o600);
    }
    assert.ok(f.calls.every((call) => call.method === 'GET'));
  } finally {
    f.close();
  }
});

test('Models read after all referencing Processes even when selections list the Model first; report retains input order', async () => {
  const second = { ...PROCESS_SELECTION, id: FOREIGN };
  const model = {
    ...MODEL_SELECTION,
    referenced_by: [
      { id: PROCESS, version: VERSION },
      { id: FOREIGN, version: VERSION },
    ],
  };
  const f = fixture([model, second, PROCESS_SELECTION]);
  f.setData((url) =>
    url.pathname.endsWith('/lifecyclemodels')
      ? [modelRow()]
      : [
          processRow({
            id: url.searchParams.get('id')?.slice(3),
            model_id: MODEL,
            model_version: VERSION,
          }),
        ],
  );
  try {
    const report = await runDatasetReadExact(f.options);
    assert.equal(report.status, 'observed');
    assert.deepEqual(
      report.rows.map((row) => row.selection),
      [model, second, PROCESS_SELECTION],
    );
    assert.deepEqual(
      f.dataCalls.map((url) => url.pathname),
      ['/rest/v1/processes', '/rest/v1/processes', '/rest/v1/lifecyclemodels'],
    );
    assert.deepEqual(report.rows[0]?.actual_row, modelRow());
    assert.equal(Object.hasOwn(report.rows[0]!.actual_row!, 'model_id'), false);
    assert.equal(Object.hasOwn(report.rows[0]!.actual_row!, 'model_version'), false);
    assert.equal(Object.hasOwn(report.rows[0]!.actual_row!, 'review_id'), false);
    assert.equal(report.rows[0]?.model_association, null);
    assert.deepEqual(report.rows[1]?.model_association, {
      model_id: MODEL,
      resolved_version: VERSION,
      version_source: 'model_version',
    });
  } finally {
    f.close();
  }
});

test('the fixed source boundary is asserted or defaulted and only public states 100 through 199 or actor state 0 transfer bodies', async () => {
  for (const [owner, state, allowed] of [
    [ACTOR, 0, true],
    [FOREIGN, 0, false],
    [ACTOR, 20, false],
    [FOREIGN, 20, false],
    [ACTOR, 99, false],
    [FOREIGN, 100, true],
    [FOREIGN, 199, true],
    [ACTOR, 200, false],
    [null, 101, true],
  ] as const) {
    const f = fixture();
    f.scope.read_scope = 'public_100_199_or_current_owner_state_0';
    f.writeScope();
    const sentinel = `FORBIDDEN_BODY_${String(owner)}_${state}`;
    f.setData(() => [
      processRow({ user_id: owner, state_code: state, json_ordered: { sentinel } }),
    ]);
    try {
      const report = await runDatasetReadExact(f.options);
      assert.equal(report.rows[0]?.status, allowed ? 'found' : 'absent');
      assert.equal(report.rows[0]?.reason, allowed ? 'exact_row' : 'not_visible');
      assert.equal(
        f.transferredBodies.some((body) => body.includes(sentinel)),
        allowed,
        'the server applies scope before response serialization',
      );
      assert.equal(JSON.stringify(report).includes(sentinel), allowed);
      if (!allowed) {
        assert.equal(report.rows[0]?.actual_row, null);
        assert.equal(report.rows[0]?.actual_payload_sha256, null);
      }
    } finally {
      f.close();
    }
  }
});

test('state and ownership drift before the actual Process GET prevents body transfer and dependent Model GET', async () => {
  for (const planned of [
    { user_id: FOREIGN, state_code: 100 },
    { user_id: ACTOR, state_code: 0 },
  ]) {
    for (const current of [
      { user_id: ACTOR, state_code: 20 },
      { user_id: FOREIGN, state_code: 0 },
    ]) {
      const f = fixture([PROCESS_SELECTION, MODEL_SELECTION]);
      const plannedRow = processRow({ ...planned, model_id: MODEL, model_version: VERSION });
      const sentinel = 'FORBIDDEN_DRIFTED_PROCESS_BODY';
      f.setData(() => [processRow({ ...plannedRow, ...current, json_ordered: { sentinel } })]);
      try {
        const report = await runDatasetReadExact(f.options);
        assert.equal(report.rows[0]?.status, 'absent');
        assert.equal(report.rows[1]?.reason, 'referenced_process_drift');
        assert.equal(f.dataCalls.length, 1);
        assert.equal(
          f.transferredBodies.some((body) => body.includes(sentinel)),
          false,
        );
        assert.equal(JSON.stringify(report).includes(sentinel), false);
      } finally {
        f.close();
      }
    }
  }
});

test('matching readable Processes allow only explicitly selected Models within the same server read boundary', async () => {
  for (const [owner, state, allowed] of [
    [ACTOR, 0, true],
    [FOREIGN, 199, true],
    [ACTOR, 20, false],
    [FOREIGN, 0, false],
    [FOREIGN, 20, false],
    [ACTOR, 200, false],
  ] as const) {
    const f = fixture([PROCESS_SELECTION, MODEL_SELECTION]);
    const sentinel = `MODEL_BODY_${owner}_${state}`;
    f.setData((url) =>
      url.pathname.endsWith('/processes')
        ? [processRow({ model_id: MODEL, model_version: VERSION })]
        : [modelRow({ user_id: owner, state_code: state, json_ordered: { sentinel } })],
    );
    try {
      const report = await runDatasetReadExact(f.options);
      assert.equal(report.rows[0]?.status, 'found');
      assert.equal(report.rows[1]?.status, allowed ? 'found' : 'absent');
      assert.equal(f.dataCalls.length, 2);
      assert.equal(
        f.dataCalls[1]?.searchParams.get('or'),
        `(and(state_code.gte.100,state_code.lt.200),and(state_code.eq.0,user_id.eq.${ACTOR}))`,
      );
      assert.equal(
        f.transferredBodies.some((body) => body.includes(sentinel)),
        allowed,
      );
      assert.equal(JSON.stringify(report).includes(sentinel), allowed);
    } finally {
      f.close();
    }
  }
});

test('Model state or ownership drift before its actual GET never transfers a newly unreadable body', async () => {
  for (const planned of [
    { user_id: FOREIGN, state_code: 100 },
    { user_id: ACTOR, state_code: 0 },
  ]) {
    for (const current of [
      { user_id: ACTOR, state_code: 20 },
      { user_id: FOREIGN, state_code: 0 },
    ]) {
      const f = fixture([PROCESS_SELECTION, MODEL_SELECTION]);
      const plannedModel = modelRow(planned);
      const sentinel = 'FORBIDDEN_DRIFTED_MODEL_BODY';
      f.setData((url) =>
        url.pathname.endsWith('/processes')
          ? [processRow({ model_id: MODEL, model_version: VERSION })]
          : [modelRow({ ...plannedModel, ...current, json_ordered: { sentinel } })],
      );
      try {
        const report = await runDatasetReadExact(f.options);
        assert.equal(report.rows[1]?.status, 'absent');
        assert.equal(f.dataCalls.length, 2);
        assert.equal(
          f.transferredBodies.some((body) => body.includes(sentinel)),
          false,
        );
        assert.equal(JSON.stringify(report).includes(sentinel), false);
      } finally {
        f.close();
      }
    }
  }
});

test('contradictory server Process and Model responses retain only a static protocol scope violation', async () => {
  for (const table of ['processes', 'lifecyclemodels']) {
    for (const forbidden of [
      { user_id: ACTOR, state_code: 20 },
      { user_id: FOREIGN, state_code: 0 },
      { user_id: FOREIGN, state_code: 200 },
    ]) {
      const f = fixture(
        table === 'processes' ? [PROCESS_SELECTION] : [PROCESS_SELECTION, MODEL_SELECTION],
      );
      f.setContraryServer();
      f.setData((url) =>
        url.pathname.endsWith('/processes') && table === 'lifecyclemodels'
          ? [processRow({ model_id: MODEL, model_version: VERSION })]
          : [
              table === 'processes'
                ? processRow({ ...forbidden, json_ordered: { secret: CANARY } })
                : modelRow({ ...forbidden, json_ordered: { secret: CANARY } }),
            ],
      );
      try {
        const report = await runDatasetReadExact(f.options);
        const observed = report.rows.at(-1)!;
        assert.equal(report.status, 'indeterminate');
        assert.equal(observed.reason, 'protocol_scope_violation');
        assert.equal(observed.actual_row, null);
        assert.equal(observed.actual_row_sha256, null);
        assert.equal(observed.actual_payload_sha256, null);
        assert.equal(JSON.stringify(report).includes(CANARY), false);
        assert.equal(
          fs.readFileSync(path.join(f.options.outDir, 'read-exact.json'), 'utf8').includes(CANARY),
          false,
        );
      } finally {
        f.close();
      }
    }
  }
});

test('the shared exact reader enforces owner-draft server filters and defends contradictory or malformed membership', async () => {
  for (const [owner, state, allowed] of [
    [ACTOR, 0, true],
    [FOREIGN, 0, false],
    [ACTOR, 20, false],
    [FOREIGN, 100, false],
  ] as const) {
    const f = fixture();
    const sentinel = `OWNER_SCOPE_BODY_${owner}_${state}`;
    f.setData(() => [
      processRow({ user_id: owner, state_code: state, json_ordered: { sentinel } }),
    ]);
    const { client, restBaseUrl } = createSupabaseDataClient(
      {
        apiBaseUrl: 'https://example.supabase.co',
        publishableKey: 'synthetic-key',
        getAccessToken: async () => token(),
      },
      f.options.fetchImpl,
      10_000,
    );
    try {
      const rows = await readExactRows({
        client,
        restBaseUrl,
        table: 'processes',
        id: PROCESS,
        version: VERSION,
        columns: Object.keys(processRow()).join(','),
        readScope: { mode: 'owner_draft', actorUserId: ACTOR },
      });
      assert.equal(rows.length, allowed ? 1 : 0);
      assert.equal(f.dataCalls[0]?.searchParams.get('user_id'), `eq.${ACTOR}`);
      assert.equal(f.dataCalls[0]?.searchParams.get('state_code'), 'eq.0');
      assert.equal(f.dataCalls[0]?.searchParams.has('or'), false);
      assert.equal(
        f.transferredBodies.some((body) => body.includes(sentinel)),
        allowed,
      );
      if (!allowed) {
        f.setContraryServer();
        await assert.rejects(
          readExactRows({
            client,
            restBaseUrl,
            table: 'processes',
            id: PROCESS,
            version: VERSION,
            columns: Object.keys(processRow()).join(','),
            readScope: { mode: 'owner_draft', actorUserId: ACTOR },
          }),
          (error) => {
            assert.ok(isReadScopeViolation(error));
            safeError(error);
            assert.equal((error as CliError).code, 'DATASET_READ_SCOPE_VIOLATION');
            assert.equal((error as Error).message.includes(sentinel), false);
            return true;
          },
        );
      }
    } finally {
      f.close();
    }
  }
  assert.equal(isReadScopeViolation(null), false);
  assert.equal(isReadScopeViolation(new Error('ordinary error')), false);
  assert.equal(isReadScopeViolation(new CliError('ordinary error', { code: 'OTHER' })), false);
});

test('a shared reader requires a canonical actor and fixed mode before creating any network request', async () => {
  const f = fixture();
  const { client, restBaseUrl } = createSupabaseDataClient(
    {
      apiBaseUrl: 'https://example.supabase.co',
      publishableKey: 'synthetic-key',
      getAccessToken: async () => token(),
    },
    f.options.fetchImpl,
    10_000,
  );
  try {
    for (const value of [
      undefined,
      { mode: 'all_visible', actorUserId: ACTOR },
      { mode: 'owner_draft', actorUserId: `x),state_code.gte.0,(${CANARY}` },
      { mode: 'owner_draft', actorUserId: ACTOR, exception: true },
      { mode: 'owner_draft', actorUserId: 7 },
    ]) {
      await assert.rejects(
        readExactRows({
          client,
          restBaseUrl,
          table: 'processes',
          id: PROCESS,
          version: VERSION,
          columns: Object.keys(processRow()).join(','),
          readScope: value as DatasetBodyReadScope,
        }),
        safeError,
      );
    }
    assert.equal(f.calls.length, 0);
  } finally {
    f.close();
  }
});

test('raw null Model version stays null while Process version fallback gates only the explicit exact Model', async () => {
  const f = fixture([
    { ...PROCESS_SELECTION, expected_model: { id: MODEL, version: VERSION } },
    MODEL_SELECTION,
  ]);
  f.setData((url) =>
    url.pathname.endsWith('/lifecyclemodels') ? [modelRow()] : [processRow({ model_id: MODEL })],
  );
  try {
    const report = await runDatasetReadExact(f.options);
    assert.equal(report.status, 'observed');
    assert.equal(report.rows[0]?.actual_row?.model_version, null);
    assert.deepEqual(report.rows[0]?.model_association, {
      model_id: MODEL,
      resolved_version: VERSION,
      version_source: 'process_version_fallback',
    });
    assert.equal(f.dataCalls.length, 2);
  } finally {
    f.close();
  }
});

test('omitted expected association observes a linked Process without following any unselected Model', async () => {
  const f = fixture();
  f.setData(() => [processRow({ model_id: MODEL, model_version: '02.00.003' })]);
  try {
    const report = await runDatasetReadExact(f.options);
    assert.equal(report.rows[0]?.status, 'found');
    assert.deepEqual(report.rows[0]?.model_association, {
      model_id: MODEL,
      resolved_version: '02.00.003',
      version_source: 'model_version',
    });
    assert.equal(f.dataCalls.length, 1);
  } finally {
    f.close();
  }
});

test('expected association drift preserves actual metadata and prevents Model reads', async () => {
  for (const expected of [
    null,
    { id: FOREIGN, version: VERSION },
    { id: MODEL, version: '02.00.001' },
  ]) {
    const f = fixture([{ ...PROCESS_SELECTION, expected_model: expected }, MODEL_SELECTION]);
    f.setData(() => [processRow({ model_id: MODEL, model_version: VERSION })]);
    try {
      const report = await runDatasetReadExact(f.options);
      assert.equal(report.status, 'drift');
      assert.equal(report.rows[0]?.reason, 'model_association_changed');
      assert.equal(report.rows[0]?.actual_row?.model_id, MODEL);
      assert.equal(report.rows[1]?.status, 'drift');
      assert.equal(report.rows[1]?.reason, 'referenced_process_drift');
      assert.equal(report.rows[1]?.actual_row, null);
      assert.equal(f.dataCalls.length, 1);
    } finally {
      f.close();
    }
  }
  const f = fixture([{ ...PROCESS_SELECTION, expected_model: null }]);
  try {
    assert.equal((await runDatasetReadExact(f.options)).rows[0]?.status, 'found');
  } finally {
    f.close();
  }
});

test('a Model gate never reads after absent, ambiguous, failed, unlinked or differently linked Process observation', async () => {
  for (const response of [
    [],
    [processRow(), processRow()],
    null,
    [processRow()],
    [processRow({ model_id: FOREIGN, model_version: VERSION })],
    [processRow({ model_id: MODEL, model_version: '02.00.001' })],
  ]) {
    const f = fixture([PROCESS_SELECTION, MODEL_SELECTION]);
    f.setData(() => response);
    try {
      const report = await runDatasetReadExact(f.options);
      assert.equal(report.rows[1]?.reason, 'referenced_process_drift');
      assert.equal(f.dataCalls.length, 1);
    } finally {
      f.close();
    }
  }
});

test('absence, duplicates, malformed success, HTTP rejection and thrown transport have distinct bounded observations', async () => {
  const cases: Array<{ body: unknown; status: string; reason: string }> = [
    { body: [], status: 'absent', reason: 'not_visible' },
    { body: [processRow(), processRow()], status: 'indeterminate', reason: 'ambiguous_rows' },
    { body: null, status: 'indeterminate', reason: 'read_failed' },
    { body: { message: CANARY }, status: 'indeterminate', reason: 'read_failed' },
    { body: new Response(null, { status: 204 }), status: 'indeterminate', reason: 'read_failed' },
    {
      body: new Response(CANARY, { status: 200, headers: { 'content-type': 'application/json' } }),
      status: 'indeterminate',
      reason: 'read_failed',
    },
    {
      body: new Response(JSON.stringify({ message: CANARY }), {
        status: 500,
        headers: { 'content-type': 'application/json' },
      }),
      status: 'indeterminate',
      reason: 'read_failed',
    },
  ];
  for (const scenario of cases) {
    const f = fixture();
    f.setData(() => scenario.body);
    try {
      const report = await runDatasetReadExact(f.options);
      assert.equal(report.rows[0]?.status, scenario.status);
      assert.equal(report.rows[0]?.reason, scenario.reason);
      assert.equal(report.rows[0]?.actual_row, null);
      assert.equal(report.rows[0]?.actual_payload_sha256, null);
      assert.equal(report.status, scenario.status === 'absent' ? 'observed' : 'indeterminate');
      assert.equal(
        fs.readFileSync(path.join(f.options.outDir, 'read-exact.json'), 'utf8').includes(CANARY),
        false,
      );
    } finally {
      f.close();
    }
  }
  const f = fixture();
  f.setData(() => {
    throw new Error(CANARY);
  });
  try {
    const report = await runDatasetReadExact(f.options);
    assert.equal(report.rows[0]?.reason, 'read_failed');
    assert.equal(JSON.stringify(report).includes(CANARY), false);
  } finally {
    f.close();
  }
});

test('all selected fields must be present; missing null metadata never becomes an observed null', async () => {
  const malformed: unknown[] = [null, [], CANARY];
  const original = processRow() as Record<string, unknown>;
  for (const key of Object.keys(original)) {
    const missing = { ...original };
    delete missing[key];
    malformed.push(missing);
  }
  for (const overrides of [
    { id: FOREIGN },
    { version: '02.00.001' },
    { user_id: 'invalid' },
    { team_id: CANARY },
    { state_code: '0' },
    { state_code: 0.5 },
    { state_code: Number.MAX_SAFE_INTEGER + 1 },
    { json_ordered: [] },
    { modified_at: 7 },
    { modified_at: 'not a timestamp' },
    { model_id: CANARY },
    { model_version: 'latest' },
    { model_id: null, model_version: VERSION },
    { review_id: CANARY },
  ])
    malformed.push(processRow(overrides));
  for (const row of malformed) {
    const f = fixture();
    f.setContraryServer();
    f.setData(() => [row]);
    try {
      const report = await runDatasetReadExact(f.options);
      assert.equal(report.rows[0]?.status, 'indeterminate');
      const facts =
        row !== null && typeof row === 'object' && !Array.isArray(row)
          ? (row as Record<string, unknown>)
          : null;
      const invalidScopeFacts =
        !facts ||
        facts.id !== PROCESS ||
        facts.version !== VERSION ||
        facts.user_id !== ACTOR ||
        typeof facts.state_code !== 'number' ||
        !Number.isSafeInteger(facts.state_code);
      assert.equal(
        report.rows[0]?.reason,
        invalidScopeFacts ? 'protocol_scope_violation' : 'invalid_or_incomplete_row',
      );
      assert.equal(report.rows[0]?.actual_row, null);
      assert.equal(JSON.stringify(report).includes(CANARY), false);
    } finally {
      f.close();
    }
  }
});

test('nullable permitted metadata is retained exactly and Model field validation uses its actual selected columns', async () => {
  const f = fixture([PROCESS_SELECTION, MODEL_SELECTION]);
  f.setData((url) =>
    url.pathname.endsWith('/lifecyclemodels')
      ? [modelRow({ team_id: FOREIGN })]
      : [processRow({ user_id: null, state_code: 100, model_id: MODEL, model_version: VERSION })],
  );
  try {
    const report = await runDatasetReadExact(f.options);
    assert.equal(report.rows[0]?.actual_row?.user_id, null);
    assert.equal(report.rows[1]?.actual_row?.team_id, FOREIGN);
    const query = f.dataCalls[1]!;
    assert.deepEqual(
      query.searchParams.get('select')?.split(',').sort(),
      Object.keys(modelRow()).sort(),
    );
  } finally {
    f.close();
  }
  const invalidModel = fixture([PROCESS_SELECTION, MODEL_SELECTION]);
  invalidModel.setData((url) =>
    url.pathname.endsWith('/lifecyclemodels')
      ? [modelRow({ team_id: CANARY })]
      : [processRow({ model_id: MODEL, model_version: VERSION })],
  );
  try {
    assert.equal(
      (await runDatasetReadExact(invalidModel.options)).rows[1]?.reason,
      'invalid_or_incomplete_row',
    );
  } finally {
    invalidModel.close();
  }
});

test('strict scope rejects unsupported, duplicate and unbound selections before identity or output', async () => {
  const mutations: Array<(scope: Record<string, unknown>) => void> = [
    (s) => {
      s.extra = CANARY;
    },
    (s) => {
      s.read_scope = 'all_rls_visible';
    },
    (s) => {
      delete s.schema_version;
    },
    (s) => {
      s.schema_version = 'other';
    },
    (s) => {
      s.project_ref = 'wrong project';
    },
    (s) => {
      s.actor = null;
    },
    (s) => {
      s.actor = { user_id: 'wrong' };
    },
    (s) => {
      s.actor = { user_id: ACTOR, email: 'wrong' };
    },
    (s) => {
      s.actor = { user_id: ACTOR, email: 'USER@example.com' };
    },
    (s) => {
      s.actor = { user_id: ACTOR, email: 'a'.repeat(255) + '@x.co' };
    },
    (s) => {
      s.actor = { user_id: ACTOR, ignored: true };
    },
    (s) => {
      s.sources = null;
    },
    (s) => {
      s.sources = [];
    },
    (s) => {
      s.sources = Array(17).fill((s.sources as unknown[])[0]);
    },
    (s) => {
      s.sources = [{ path: 'relative', sha256: 'a'.repeat(64) }];
    },
    (s) => {
      s.sources = [{ path: '/tmp/evidence', sha256: 'invalid' }];
    },
    (s) => {
      s.sources = [{ ...(s.sources as object[])[0], extra: true }];
    },
    (s) => {
      s.sources = [(s.sources as unknown[])[0], (s.sources as unknown[])[0]];
    },
    (s) => {
      s.rows = null;
    },
    (s) => {
      s.rows = [];
    },
    (s) => {
      s.rows = Array(257).fill(PROCESS_SELECTION);
    },
    (s) => {
      s.rows = [null];
    },
    (s) => {
      s.rows = [{ ...PROCESS_SELECTION, table: 'flows' }];
    },
    (s) => {
      s.rows = [{ ...PROCESS_SELECTION, id: 'invalid' }];
    },
    (s) => {
      s.rows = [{ ...PROCESS_SELECTION, version: 'latest' }];
    },
    (s) => {
      s.rows = [{ ...PROCESS_SELECTION, extra: true }];
    },
    (s) => {
      s.rows = [PROCESS_SELECTION, PROCESS_SELECTION];
    },
    (s) => {
      s.rows = [
        { ...PROCESS_SELECTION, expected_model: { id: MODEL, version: VERSION, extra: true } },
      ];
    },
    (s) => {
      s.rows = [{ ...MODEL_SELECTION, referenced_by: [] }, PROCESS_SELECTION];
    },
    (s) => {
      s.rows = [{ ...MODEL_SELECTION, referenced_by: null }, PROCESS_SELECTION];
    },
    (s) => {
      s.rows = [
        { ...MODEL_SELECTION, referenced_by: Array(257).fill({ id: PROCESS, version: VERSION }) },
        PROCESS_SELECTION,
      ];
    },
    (s) => {
      s.rows = [
        {
          ...MODEL_SELECTION,
          referenced_by: [
            { id: PROCESS, version: VERSION },
            { id: PROCESS, version: VERSION },
          ],
        },
        PROCESS_SELECTION,
      ];
    },
    (s) => {
      s.rows = [MODEL_SELECTION];
    },
  ];
  for (const mutate of mutations) {
    const f = fixture();
    const scope = structuredClone(f.scope) as unknown as Record<string, unknown>;
    mutate(scope);
    f.writeScope(scope);
    try {
      await assert.rejects(runDatasetReadExact(f.options), safeError);
      assert.equal(f.calls.length, 0);
      assert.equal(fs.existsSync(f.options.outDir), false);
    } finally {
      f.close();
    }
  }
});

test('scope SHA, provenance hashes, malformed JSON and unsafe files fail without network or completed output', async () => {
  for (const mode of [
    'invalid-sha',
    'mismatching-scope',
    'mismatching-source',
    'malformed-json',
    'source-symlink',
    'missing-source',
    'oversized-source',
    'source-directory',
  ]) {
    const f = fixture();
    try {
      if (mode === 'invalid-sha') f.options.scopeSha256 = 'invalid';
      if (mode === 'mismatching-scope') f.options.scopeSha256 = 'a'.repeat(64);
      if (mode === 'mismatching-source') {
        f.scope.sources[0]!.sha256 = 'a'.repeat(64);
        f.writeScope();
      }
      if (mode === 'malformed-json') {
        fs.writeFileSync(f.options.scopeFile, CANARY);
        f.options.scopeSha256 = hash(CANARY);
      }
      if (mode === 'source-symlink') {
        const alias = path.join(f.root, 'source-link');
        fs.symlinkSync(f.source, alias, 'file');
        f.scope.sources[0]!.path = alias;
        f.writeScope();
      }
      if (mode === 'missing-source') fs.unlinkSync(f.source);
      if (mode === 'oversized-source') {
        const fd = fs.openSync(f.source, 'r+');
        fs.ftruncateSync(fd, 64 * 1024 * 1024 + 1);
        fs.closeSync(fd);
      }
      if (mode === 'source-directory') {
        f.scope.sources[0]!.path = f.root;
        f.writeScope();
      }
      await assert.rejects(runDatasetReadExact(f.options), safeError);
      assert.equal(f.calls.length, 0);
      assert.equal(fs.existsSync(f.options.outDir), false);
    } finally {
      f.close();
    }
  }
});

test('the public exact reader parses the same scope snapshot it hashes despite A-to-B-to-A pathname substitution', async (t) => {
  const f = fixture();
  const read = fs.readFileSync;
  const scopeA = read(f.options.scopeFile);
  const scopeB = JSON.stringify({ ...f.scope, rows: [{ ...PROCESS_SELECTION, id: FOREIGN }] });
  let pathnameReads = 0;
  f.setData((url) => [processRow({ id: url.searchParams.get('id')?.slice(3) })]);
  try {
    t.mock.method(fs, 'readFileSync', (...args: Parameters<typeof fs.readFileSync>) => {
      if (typeof args[0] !== 'number' && String(args[0]) === f.options.scopeFile) {
        pathnameReads += 1;
        // The old seam hashed A through a descriptor, parsed this transient B pathname
        // observation, and then reobserved unchanged A through descriptors at publication.
        return scopeB;
      }
      return read(...args);
    });
    syncBuiltinESMExports();
    const report = await runDatasetReadExact(f.options);
    assert.equal(report.status, 'observed');
    assert.equal(report.scope.sha256, hash(scopeA));
    assert.equal(report.rows[0]?.selection.id, PROCESS);
    assert.equal(report.rows[0]?.actual_row?.id, PROCESS);
    assert.equal(f.dataCalls.length, 1);
    assert.equal(f.dataCalls[0]?.searchParams.get('id'), `eq.${PROCESS}`);
    assert.equal(pathnameReads, 0, 'scope parsing cannot obtain another pathname observation');
    assert.deepEqual(read(f.options.scopeFile), scopeA);
    const artifact = JSON.parse(
      read(path.join(f.options.outDir, 'read-exact.json'), 'utf8'),
    ) as typeof report;
    assert.equal(artifact.scope.sha256, hash(scopeA));
    assert.equal(artifact.rows[0]?.selection.id, PROCESS);
  } finally {
    t.mock.restoreAll();
    syncBuiltinESMExports();
    f.close();
  }
});

test('physically duplicate provenance through a symlink parent is rejected', async () => {
  const f = fixture();
  try {
    const alias = path.join(f.root, 'parent-alias');
    fs.symlinkSync(f.root, alias, 'junction');
    f.scope.sources.push({
      path: path.join(alias, path.basename(f.source)),
      sha256: f.scope.sources[0]!.sha256,
    });
    f.writeScope();
    await assert.rejects(runDatasetReadExact(f.options), safeError);
    assert.equal(f.calls.length, 0);
  } finally {
    f.close();
  }
});

test('bounded source provenance above 32 MiB remains readable and fingerprinted', async () => {
  const f = fixture();
  try {
    fs.writeFileSync(f.source, Buffer.alloc(33 * 1024 * 1024));
    f.scope.sources[0]!.sha256 = hash(fs.readFileSync(f.source));
    f.writeScope();
    const report = await runDatasetReadExact(f.options);
    assert.equal(report.sources[0]?.bytes, 33 * 1024 * 1024);
    assert.equal(report.status, 'observed');
  } finally {
    f.close();
  }
});

test('file changes during capture prevent evidence publication', async () => {
  for (const target of ['source', 'scope']) {
    const f = fixture();
    f.setData(() => {
      fs.writeFileSync(target === 'source' ? f.source : f.options.scopeFile, CANARY);
      return [processRow()];
    });
    try {
      await assert.rejects(runDatasetReadExact(f.options), safeError);
      assert.equal(fs.existsSync(f.options.outDir), false);
    } finally {
      f.close();
    }
  }
});

test('identity checks reject wrong project, user, optional email and undecodable current token without publishing', async () => {
  for (const mode of [
    'project',
    'user',
    'email',
    'malformed-token',
    'missing-payload',
    'nonobject-token',
    'wrong-token-user',
    'missing-token-email',
  ]) {
    const f = fixture();
    try {
      if (mode === 'project') {
        f.scope.project_ref = 'other';
        f.writeScope();
      }
      if (mode === 'user')
        f.setAuth(() => ({ id: FOREIGN, email: 'user@example.com', role: 'authenticated' }));
      if (mode === 'email') {
        f.scope.actor.email = 'expected@example.com';
        f.writeScope();
      }
      if (mode === 'malformed-token')
        f.options.env.TIANGONG_LCA_ACCESS_TOKEN = `e30.${Buffer.from(CANARY).toString('base64url')}.signature`;
      if (mode === 'missing-payload') f.options.env.TIANGONG_LCA_ACCESS_TOKEN = CANARY;
      if (mode === 'nonobject-token') f.options.env.TIANGONG_LCA_ACCESS_TOKEN = token(null);
      if (mode === 'wrong-token-user')
        f.options.env.TIANGONG_LCA_ACCESS_TOKEN = token({ sub: FOREIGN });
      if (mode === 'missing-token-email') {
        f.scope.actor.email = 'user@example.com';
        f.writeScope();
        f.options.env.TIANGONG_LCA_ACCESS_TOKEN = token({ sub: ACTOR });
      }
      await assert.rejects(runDatasetReadExact(f.options), safeError);
      assert.equal(f.dataCalls.length, 0);
      assert.equal(fs.existsSync(f.options.outDir), false);
    } finally {
      f.close();
    }
  }
});

test('explicit matching email and user-only token selection work without leaking tokens', async () => {
  for (const includeEmail of [false, true]) {
    const f = fixture();
    try {
      if (includeEmail) {
        f.scope.actor.email = 'user@example.com';
        f.writeScope();
      } else f.options.env.TIANGONG_LCA_ACCESS_TOKEN = token({ sub: ACTOR });
      const report = await runDatasetReadExact(f.options);
      assert.equal(report.status, 'observed');
      assert.equal(
        JSON.stringify(report).includes(f.options.env.TIANGONG_LCA_ACCESS_TOKEN!),
        false,
      );
    } finally {
      f.close();
    }
  }
});

test('late session change rejects publication of otherwise matching reads', async () => {
  const f = fixture();
  const first = token();
  f.options.env = buildSupabaseTestEnv({
    TIANGONG_LCA_ACCESS_TOKEN: first,
    XDG_STATE_HOME: path.join(f.root, 'state'),
  });
  f.setData(() => {
    const file = f.options.env.TIANGONG_LCA_SESSION_FILE!;
    const session = JSON.parse(fs.readFileSync(file, 'utf8')) as Record<string, unknown>;
    session.access_token = token({ sub: FOREIGN, email: 'user@example.com' });
    fs.writeFileSync(file, JSON.stringify(session));
    return [processRow()];
  });
  try {
    await assert.rejects(runDatasetReadExact(f.options), safeError);
    assert.equal(f.dataCalls.length, 1);
    assert.equal(fs.existsSync(f.options.outDir), false);
  } finally {
    f.close();
  }
});

test('a real OAuth session switch after the Process read blocks the next Model request before foreign Authorization is sent', async () => {
  const f = fixture([PROCESS_SELECTION, MODEL_SELECTION]);
  const first = token();
  const foreign = token({ sub: FOREIGN, email: 'user@example.com' });
  f.options.env = buildSupabaseTestEnv({
    TIANGONG_LCA_ACCESS_TOKEN: first,
    XDG_STATE_HOME: path.join(f.root, 'state'),
  });
  const sessionFile = f.options.env.TIANGONG_LCA_SESSION_FILE!;
  const bodyAuthorization: string[] = [];
  const fetch = f.options.fetchImpl;
  f.options.fetchImpl = async (input, init) => {
    if (new URL(input).pathname.startsWith('/rest/v1/'))
      bodyAuthorization.push(new Headers(init?.headers).get('authorization')!);
    return fetch(input, init);
  };
  f.setData((url) => {
    assert.equal(
      url.pathname,
      '/rest/v1/processes',
      'the unverified Model actor must never reach fetch',
    );
    const record = JSON.parse(fs.readFileSync(sessionFile, 'utf8')) as Record<string, unknown>;
    fs.writeFileSync(sessionFile, JSON.stringify({ ...record, access_token: foreign }));
    return [processRow({ model_id: MODEL, model_version: VERSION })];
  });
  try {
    await assert.rejects(runDatasetReadExact(f.options), safeError);
    assert.deepEqual(
      f.dataCalls.map((url) => url.pathname),
      ['/rest/v1/processes'],
    );
    assert.deepEqual(bodyAuthorization, [`Bearer ${first}`]);
    assert.equal(bodyAuthorization.includes(`Bearer ${foreign}`), false);
    assert.equal(fs.existsSync(f.options.outDir), false);
  } finally {
    f.close();
  }
});

test('restoring the actor after a detected mismatch cannot turn SDK retry into a completed all-found capture', async (t) => {
  const f = fixture([PROCESS_SELECTION, MODEL_SELECTION]);
  const read = fs.readFileSync;
  const first = token();
  f.options.env = buildSupabaseTestEnv({
    TIANGONG_LCA_ACCESS_TOKEN: first,
    XDG_STATE_HOME: path.join(f.root, 'state'),
  });
  const file = f.options.env.TIANGONG_LCA_SESSION_FILE!;
  const original = read(file, 'utf8');
  let pendingRestore = false;
  let restored = 0;
  const bodyAuthorization: string[] = [];
  const fetch = f.options.fetchImpl;
  f.options.fetchImpl = async (input, init) => {
    if (new URL(input).pathname.startsWith('/rest/v1/'))
      bodyAuthorization.push(new Headers(init?.headers).get('authorization')!);
    return fetch(input, init);
  };
  f.setData((url) => {
    if (url.pathname.endsWith('/lifecyclemodels')) return [modelRow()];
    fs.writeFileSync(
      file,
      JSON.stringify({
        ...JSON.parse(original),
        access_token: token({ sub: FOREIGN, email: 'user@example.com' }),
      }),
    );
    pendingRestore = true;
    return [processRow({ model_id: MODEL, model_version: VERSION })];
  });
  try {
    t.mock.method(fs, 'readFileSync', (...args: Parameters<typeof fs.readFileSync>) => {
      const bytes = read(...args);
      if (String(args[0]) === file && pendingRestore) {
        pendingRestore = false;
        restored += 1;
        fs.writeFileSync(file, original);
      }
      return bytes;
    });
    syncBuiltinESMExports();
    await assert.rejects(runDatasetReadExact(f.options), safeError);
    assert.equal(
      restored,
      1,
      'the guard observed foreign bytes while the original actor was restored',
    );
    assert.deepEqual(
      f.dataCalls.map((url) => url.pathname),
      ['/rest/v1/processes'],
    );
    assert.deepEqual(bodyAuthorization, [`Bearer ${first}`]);
    assert.equal(fs.existsSync(f.options.outDir), false);
  } finally {
    t.mock.restoreAll();
    syncBuiltinESMExports();
    fs.rmSync(path.dirname(file), { recursive: true, force: true });
    f.close();
  }
});

test('invalid timing, preexisting output and output inside the CLI state registry fail before requests', async () => {
  for (const mode of [
    'timeout-zero',
    'timeout-fraction',
    'timeout-too-large',
    'invalid-time',
    'existing-output',
    'state-output',
  ]) {
    const f = fixture();
    try {
      if (mode === 'timeout-zero') f.options.timeoutMs = 0;
      if (mode === 'timeout-fraction') f.options.timeoutMs = 1.5;
      if (mode === 'timeout-too-large') f.options.timeoutMs = 2_147_483_648;
      if (mode === 'invalid-time') f.options.now = new Date('invalid');
      if (mode === 'existing-output') fs.mkdirSync(f.options.outDir);
      if (mode === 'state-output') {
        const state = path.join(f.root, 'state', 'tiangong-lca-cli');
        fs.mkdirSync(state, { recursive: true });
        f.options.outDir = path.join(state, 'capture');
      }
      await assert.rejects(runDatasetReadExact(f.options), safeError);
      assert.equal(f.calls.length, 0);
    } finally {
      f.close();
    }
  }
});

test('fresh capture uses current timestamps and reruns cannot overwrite observations', async () => {
  const f = fixture();
  delete f.options.now;
  f.options.timeoutMs = 20_000;
  try {
    const before = Date.now();
    const report = await runDatasetReadExact(f.options);
    assert.ok(Date.parse(report.rows[0]!.observed_at_utc) >= before);
    assert.ok(Date.parse(report.rows[0]!.observed_at_utc) <= Date.now());
    const artifact = path.join(f.options.outDir, 'read-exact.json');
    const bytes = fs.readFileSync(artifact);
    const calls = f.calls.length;
    await assert.rejects(runDatasetReadExact(f.options), safeError);
    assert.equal(f.calls.length, calls);
    assert.deepEqual(fs.readFileSync(artifact), bytes);
  } finally {
    f.close();
  }
});
