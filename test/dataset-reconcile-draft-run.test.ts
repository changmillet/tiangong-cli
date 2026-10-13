import assert from 'node:assert/strict';
import fs from 'node:fs';
import { syncBuiltinESMExports } from 'node:module';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { executeCli } from '../src/cli.js';
import {
  runDatasetDraftReconciliation,
  __testInternals,
} from '../src/lib/dataset-save-draft-run.js';
import { sha256Json } from '../src/lib/dataset-maintenance-contract.js';
import type { FetchLike } from '../src/lib/http.js';
import { buildSupabaseTestEnv } from './helpers/supabase-auth.js';
import { processTransportFixture } from './helpers/process-allocation-fixture.js';

type Json = Record<string, unknown>;
const owner = '11111111-1111-4111-8111-111111111111';
const token = (email = 'user@example.com', sub = owner) =>
  'header.' + Buffer.from(JSON.stringify({ sub, email })).toString('base64url') + '.signature';
const response = (body: unknown): Awaited<ReturnType<FetchLike>> => ({
  ok: true,
  status: 200,
  headers: { get: () => 'application/json' },
  text: async () => JSON.stringify(body),
});
function setup() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'cli-reconcile-process-'));
  const payload = processTransportFixture();
  const root = payload.processDataSet as Json;
  const id = ((root.processInformation as Json).dataSetInformation as Json)[
    'common:UUID'
  ] as string;
  const version = ((root.administrativeInformation as Json).publicationAndOwnership as Json)[
    'common:dataSetVersion'
  ] as string;
  const action = {
    action_id: 'original-process',
    table: 'processes',
    id,
    version,
    desired_sha256: sha256Json(payload),
    expected_operation: 'insert',
    before_sha256: null,
    dependency_action_ids: [],
  };
  const contract = {
    schema_version: 'dataset-save-draft-execution-contract.v1',
    execution_id: 'original-request',
    project_ref: 'example',
    target_mode: 'owner_draft',
    owner: { user_id: owner, email: 'user@example.com', state_code: 0 },
    actions: [action],
  };
  const inputPath = path.join(dir, 'input.json'),
    contractPath = path.join(dir, 'contract.json');
  fs.writeFileSync(inputPath, JSON.stringify(payload));
  fs.writeFileSync(contractPath, JSON.stringify(contract));
  const env = {
    TIANGONG_LCA_API_BASE_URL: 'https://example.supabase.co',
    TIANGONG_LCA_AUTH_MODE: 'access-token',
    TIANGONG_LCA_SUPABASE_PUBLISHABLE_KEY: 'public-key',
    TIANGONG_LCA_ACCESS_TOKEN: token(),
    TIANGONG_LCA_DISABLE_SESSION_CACHE: 'true',
    XDG_STATE_HOME: path.join(dir, 'state'),
  };
  const row = {
    id,
    version,
    user_id: owner,
    state_code: 0,
    json_ordered: payload,
    model_id: null,
    model_version: null,
    team_id: null,
    review_id: null,
  };
  const calls: string[] = [];
  const fetch =
    (read: (url: URL) => unknown): FetchLike =>
    async (url, init) => {
      const parsed = new URL(String(url));
      assert.equal(init?.method ?? 'GET', 'GET', 'reconciliation performs no mutation or RPC');
      calls.push(parsed.pathname);
      if (parsed.pathname === '/auth/v1/user')
        return response({ id: owner, email: 'user@example.com', role: 'authenticated' });
      assert.equal(parsed.pathname, '/rest/v1/processes');
      assert.equal(parsed.searchParams.get('id'), 'eq.' + id);
      assert.equal(parsed.searchParams.get('version'), 'eq.' + version);
      assert.equal(parsed.searchParams.get('limit'), '2');
      assert.equal(
        parsed.searchParams.get('select'),
        'id,version,user_id,state_code,json_ordered,model_id,model_version,team_id,review_id',
      );
      return response(read(parsed));
    };
  const options = {
    inputPath,
    executionContractPath: contractPath,
    outDir: path.join(dir, 'observation-1'),
    env,
    fetchImpl: fetch(() => [row]),
  };
  return {
    dir,
    payload,
    row,
    contract,
    action,
    calls,
    fetch,
    options,
    cleanup: () => fs.rmSync(dir, { recursive: true, force: true }),
  };
}

test('public reconciliation exposes exact complete Process and eligibility metadata without an attempt', async () => {
  const s = setup();
  try {
    const result = await executeCli(
      [
        'dataset',
        'reconcile-draft',
        '--input',
        s.options.inputPath,
        '--execution-contract',
        s.options.executionContractPath,
        '--out-dir',
        s.options.outDir,
        '--type',
        'process',
        '--json',
      ],
      {
        env: s.options.env,
        fetchImpl: s.options.fetchImpl,
        dotEnvStatus: { loaded: false, path: '', count: 0 },
      },
    );
    assert.equal(result.exitCode, 0, result.stderr);
    const report = JSON.parse(result.stdout);
    assert.equal(report.schema_version, 'dataset-draft-reconciliation.v1');
    assert.equal(report.mode, 'read_only');
    assert.equal(report.authorization_granted, false);
    assert.equal(report.historical_execution_changed, false);
    assert.equal(report.rows[0].status, 'equal_eligible');
    assert.deepEqual(report.rows[0].actual_row, s.row);
    assert.equal(report.rows[0].history.attempt_consumed, false);
    assert.equal(report.rows[0].history.outcome, null);
    assert.equal(fs.existsSync(s.options.env.XDG_STATE_HOME), false);
    const saved = fs.readFileSync(path.join(s.options.outDir, 'reconciliation.json'));
    const again = await runDatasetDraftReconciliation({
      ...s.options,
      outDir: path.join(s.dir, 'observation-2'),
    });
    assert.deepEqual(again.rows[0]?.actual_row, s.row);
    assert.deepEqual(fs.readFileSync(path.join(s.options.outDir, 'reconciliation.json')), saved);
    assert.equal(fs.existsSync(s.options.env.XDG_STATE_HOME), false);
    assert.ok(s.calls.every((call) => call === '/auth/v1/user' || call === '/rest/v1/processes'));
  } finally {
    s.cleanup();
  }
});

test('public read-only classification retains discrepancy and distinguishes absence from unavailable reads', async () => {
  const variants: Array<[string, (row: Json) => unknown, string]> = [
    ['different', (row) => [{ ...row, json_ordered: { changed: true } }], 'different'],
    [
      'foreign',
      (row) => [{ ...row, user_id: '22222222-2222-4222-8222-222222222222' }],
      'indeterminate',
    ],
    ['state', (row) => [{ ...row, state_code: 100 }], 'indeterminate'],
    ['linked', (row) => [{ ...row, model_id: owner }], 'ineligible'],
    ['absent', () => [], 'absent'],
    ['null-result', () => null, 'indeterminate'],
    ['object-result', () => ({}), 'indeterminate'],
    ['duplicate', (row) => [row, row], 'indeterminate'],
    [
      'missing-metadata',
      (row) => {
        const copy = { ...row };
        delete copy.review_id;
        return [copy];
      },
      'indeterminate',
    ],
    [
      'read-failure',
      () => {
        throw new Error('Bearer SECRET-CANARY response body');
      },
      'indeterminate',
    ],
  ];
  for (const [name, variant, expected] of variants) {
    const s = setup();
    try {
      const report = await runDatasetDraftReconciliation({
        ...s.options,
        fetchImpl: s.fetch(() => variant(s.row)),
      });
      assert.equal(report.rows[0]?.status, expected, name);
      assert.equal(report.rows[0]?.history.outcome, null);
      assert.equal(report.authorization_granted, false);
      assert.equal(JSON.stringify(report).includes('SECRET-CANARY'), false);
      assert.equal(fs.existsSync(s.options.env.XDG_STATE_HOME), false);
      if (name === 'read-failure') assert.equal(report.rows[0]?.reason, 'read_failed');
    } finally {
      s.cleanup();
    }
  }
});

test('contract, input and observed ledger CAS changes cannot publish reconciliation evidence', async () => {
  for (const kind of ['input', 'contract', 'ledger'] as const) {
    const s = setup();
    try {
      const parsed = __testInternals.parseExecutionContract(s.contract);
      const ledgerFile = __testInternals.executionLedgerPath(
        __testInternals.executionLedgerRoot(s.options.env, parsed),
        parsed.actions[0]!,
      );
      const target =
        kind === 'input'
          ? s.options.inputPath
          : kind === 'contract'
            ? s.options.executionContractPath
            : ledgerFile;
      await assert.rejects(
        runDatasetDraftReconciliation({
          ...s.options,
          fetchImpl: s.fetch(() => {
            fs.mkdirSync(path.dirname(target), { recursive: true });
            fs.appendFileSync(target, ' ');
            return [s.row];
          }),
        }),
        /changed during reconciliation/u,
      );
      assert.equal(fs.existsSync(s.options.outDir), false);
    } finally {
      s.cleanup();
    }
  }
});

test('original owner and project bindings fail closed before row evidence', async () => {
  for (const drift of ['project', 'email', 'actor-after-read']) {
    const s = setup();
    try {
      if (drift === 'actor-after-read')
        Object.assign(
          s.options.env,
          buildSupabaseTestEnv({
            TIANGONG_LCA_ACCESS_TOKEN: token(),
            XDG_STATE_HOME: s.options.env.XDG_STATE_HOME,
          }),
          { TIANGONG_LCA_DISABLE_SESSION_CACHE: 'false' },
        );
      if (drift === 'project') {
        s.contract.project_ref = 'other';
        fs.writeFileSync(s.options.executionContractPath, JSON.stringify(s.contract));
      }
      if (drift === 'email') {
        s.contract.owner.email = 'different@example.com';
        fs.writeFileSync(s.options.executionContractPath, JSON.stringify(s.contract));
      }
      await assert.rejects(
        runDatasetDraftReconciliation({
          ...s.options,
          fetchImpl: s.fetch(() => {
            if (drift === 'actor-after-read') {
              const sessionPath = (s.options.env as NodeJS.ProcessEnv).TIANGONG_LCA_SESSION_FILE!;
              const session = JSON.parse(fs.readFileSync(sessionPath, 'utf8'));
              fs.writeFileSync(
                sessionPath,
                JSON.stringify({ ...session, access_token: token('drift@example.com') }),
              );
            }
            return [s.row];
          }),
        }),
      );
      assert.equal(
        s.calls.filter((call) => call === '/rest/v1/processes').length,
        drift === 'actor-after-read' ? 1 : 0,
      );
      assert.equal(fs.existsSync(s.options.outDir), false);
    } finally {
      s.cleanup();
    }
  }
});

test('reconciliation rejects missing arguments, changed desired input and unbounded action counts', async () => {
  const s = setup();
  try {
    await assert.rejects(
      runDatasetDraftReconciliation({ ...s.options, inputPath: '' }),
      /requires input/u,
    );
    await assert.rejects(
      runDatasetDraftReconciliation({ ...s.options, executionContractPath: '' }),
      /requires input/u,
    );
    await assert.rejects(
      runDatasetDraftReconciliation({ ...s.options, outDir: '' }),
      /requires input/u,
    );
    const root = s.payload.processDataSet as Json;
    root.extra = 'changed';
    fs.writeFileSync(s.options.inputPath, JSON.stringify(s.payload));
    await assert.rejects(runDatasetDraftReconciliation(s.options), /does not bind/u);
    s.contract.actions = Array.from({ length: 65 }, (_, i) => ({
      ...s.action,
      action_id: 'action-' + i,
    }));
    fs.writeFileSync(s.options.executionContractPath, JSON.stringify(s.contract));
    await assert.rejects(runDatasetDraftReconciliation(s.options), /limited to 64/u);
    assert.equal(s.calls.length, 0);
  } finally {
    s.cleanup();
  }
});

test('reconciliation restricts the server query to current-owner state 0 before any body transfer', async () => {
  for (const [state, actor] of [
    [0, owner],
    [20, owner],
    [100, owner],
    [199, owner],
    [0, '22222222-2222-4222-8222-222222222222'],
    [20, '22222222-2222-4222-8222-222222222222'],
    [100, '22222222-2222-4222-8222-222222222222'],
  ] as const) {
    const s = setup();
    let bodyTransfers = 0;
    let query: URL | undefined;
    const actual = {
      ...s.row,
      state_code: state,
      user_id: actor,
      ...(state !== 0 || actor !== owner
        ? { json_ordered: { forbidden: 'FORBIDDEN_BODY_SENTINEL' } }
        : {}),
    };
    try {
      const report = await runDatasetDraftReconciliation({
        ...s.options,
        fetchImpl: s.fetch((url) => {
          query = url;
          // Model the server applying predicates before serializing its JSON response.
          const userFilter = url.searchParams.get('user_id');
          const stateFilter = url.searchParams.get('state_code');
          const selected =
            (!userFilter || userFilter === 'eq.' + actual.user_id) &&
            (!stateFilter || stateFilter === 'eq.' + actual.state_code);
          if (selected) bodyTransfers += 1;
          return selected ? [actual] : [];
        }),
      });
      assert.equal(query?.searchParams.get('user_id'), 'eq.' + owner);
      assert.equal(query?.searchParams.get('state_code'), 'eq.0');
      assert.equal(query?.searchParams.has('or'), false);
      assert.equal(report.read_scope, 'current_owner_state_0');
      const allowed = state === 0 && actor === owner;
      assert.equal(bodyTransfers, allowed ? 1 : 0);
      assert.equal(report.rows[0]?.status, allowed ? 'equal_eligible' : 'absent');
      assert.equal(JSON.stringify(report).includes('FORBIDDEN_BODY_SENTINEL'), false);
      assert.equal(report.rows[0]?.history.attempt_consumed, false);
      assert.equal(fs.existsSync(s.options.env.XDG_STATE_HOME), false);
    } finally {
      s.cleanup();
    }
  }
});

test('reconciliation never publishes a body when a server violates the requested read scope', async () => {
  const s = setup();
  try {
    const report = await runDatasetDraftReconciliation({
      ...s.options,
      fetchImpl: s.fetch((url) => {
        assert.equal(url.searchParams.get('state_code'), 'eq.0');
        return [
          { ...s.row, state_code: 20, json_ordered: { forbidden: 'FORBIDDEN_BODY_SENTINEL' } },
        ];
      }),
    });
    assert.equal(report.rows[0]?.status, 'indeterminate');
    assert.equal(report.rows[0]?.reason, 'protocol_scope_violation');
    assert.equal(report.rows[0]?.actual_row, null);
    assert.equal(report.rows[0]?.actual_payload_sha256, null);
    assert.equal(JSON.stringify(report).includes('FORBIDDEN_BODY_SENTINEL'), false);
  } finally {
    s.cleanup();
  }
});

test('multi-row reconciliation cannot send a foreign token after the first body response changes the session', async () => {
  const s = setup();
  const foreign = '22222222-2222-4222-8222-222222222222';
  const secondId = '33333333-3333-4333-8333-333333333333';
  const secondPayload = structuredClone(s.payload);
  const root = secondPayload.processDataSet as Json;
  ((root.processInformation as Json).dataSetInformation as Json)['common:UUID'] = secondId;
  const second = { ...s.row, id: secondId, json_ordered: secondPayload };
  const env = buildSupabaseTestEnv({
    TIANGONG_LCA_ACCESS_TOKEN: token(),
    XDG_STATE_HOME: path.join(s.dir, 'state'),
  });
  const sentSubjects: string[] = [];
  const readIds: string[] = [];
  fs.writeFileSync(s.options.inputPath, JSON.stringify([s.payload, secondPayload]));
  s.contract.actions.push({
    ...s.action,
    action_id: 'second-row',
    id: secondId,
    desired_sha256: sha256Json(secondPayload),
  });
  fs.writeFileSync(s.options.executionContractPath, JSON.stringify(s.contract));
  const sessionPath = env.TIANGONG_LCA_SESSION_FILE!;
  const switchSession = () => {
    const current = JSON.parse(fs.readFileSync(sessionPath, 'utf8'));
    fs.writeFileSync(
      sessionPath,
      JSON.stringify({
        ...current,
        user_email: 'foreign@example.com',
        access_token: token('foreign@example.com', foreign),
      }),
    );
  };
  const fetchImpl: FetchLike = async (url, init) => {
    const u = new URL(String(url));
    if (u.pathname === '/auth/v1/user')
      return response({ id: owner, email: 'user@example.com', role: 'authenticated' });
    assert.equal(u.pathname, '/rest/v1/processes');
    assert.equal(init?.method ?? 'GET', 'GET');
    const bearer = new Headers(init?.headers).get('authorization')!.slice('Bearer '.length);
    sentSubjects.push(JSON.parse(Buffer.from(bearer.split('.')[1]!, 'base64url').toString()).sub);
    const id = u.searchParams.get('id')!.slice(3);
    readIds.push(id);
    assert.equal(u.searchParams.get('user_id'), 'eq.' + owner);
    assert.equal(u.searchParams.get('state_code'), 'eq.0');
    if (readIds.length === 1) {
      switchSession();
      return response([s.row]);
    }
    return response([second]);
  };
  try {
    await assert.rejects(runDatasetDraftReconciliation({ ...s.options, env, fetchImpl }));
    assert.deepEqual(sentSubjects, [owner]);
    assert.deepEqual(readIds, [s.row.id]);
    assert.equal(fs.existsSync(s.options.outDir), false);
    assert.equal(fs.existsSync(path.join(s.dir, 'state')), false);
  } finally {
    s.cleanup();
  }
});

test('reconciliation rejects a replaced session before its first body query', async () => {
  const s = setup();
  const env = buildSupabaseTestEnv({
    TIANGONG_LCA_ACCESS_TOKEN: token(),
    XDG_STATE_HOME: path.join(s.dir, 'state'),
  });
  let bodyQueries = 0;
  const fetchImpl: FetchLike = async (url) => {
    if (new URL(String(url)).pathname === '/auth/v1/user') {
      const sessionPath = env.TIANGONG_LCA_SESSION_FILE!;
      const current = JSON.parse(fs.readFileSync(sessionPath, 'utf8'));
      fs.writeFileSync(
        sessionPath,
        JSON.stringify({
          ...current,
          user_email: 'foreign@example.com',
          access_token: token('foreign@example.com', '22222222-2222-4222-8222-222222222222'),
        }),
      );
      return response({ id: owner, email: 'user@example.com', role: 'authenticated' });
    }
    bodyQueries += 1;
    return response([s.row]);
  };
  try {
    await assert.rejects(runDatasetDraftReconciliation({ ...s.options, env, fetchImpl }));
    assert.equal(bodyQueries, 0);
    assert.equal(fs.existsSync(s.options.outDir), false);
  } finally {
    s.cleanup();
  }
});

test('contract and input pathname substitution cannot change the rows bound to descriptor snapshots', async (t) => {
  const s = setup();
  const read = fs.readFileSync;
  const replacementId = '33333333-3333-4333-8333-333333333333';
  const replacement = structuredClone(s.payload);
  const root = replacement.processDataSet as Json;
  ((root.processInformation as Json).dataSetInformation as Json)['common:UUID'] = replacementId;
  const substitutedContract = {
    ...s.contract,
    execution_id: 'substituted-request',
    actions: [{ ...s.action, id: replacementId, desired_sha256: sha256Json(replacement) }],
  };
  const requested: string[] = [];
  let pathnameReads = 0;
  try {
    t.mock.method(fs, 'readFileSync', (...args: Parameters<typeof fs.readFileSync>) => {
      if (args[0] === s.options.inputPath || args[0] === s.options.executionContractPath) {
        pathnameReads += 1;
        const text = JSON.stringify(
          args[0] === s.options.inputPath ? replacement : substitutedContract,
        );
        return args[1] === 'utf8' ? text : Buffer.from(text);
      }
      return read(...args);
    });
    syncBuiltinESMExports();
    const report = await runDatasetDraftReconciliation({
      ...s.options,
      fetchImpl: async (url) => {
        const u = new URL(String(url));
        if (u.pathname === '/auth/v1/user')
          return response({ id: owner, email: 'user@example.com', role: 'authenticated' });
        const id = u.searchParams.get('id')!.slice(3);
        requested.push(id);
        return response([
          { ...s.row, id, json_ordered: id === replacementId ? replacement : s.payload },
        ]);
      },
    });
    assert.deepEqual(requested, [s.row.id]);
    assert.equal(pathnameReads, 0);
    assert.equal(report.execution_contract.execution_id, s.contract.execution_id);
    assert.equal(report.execution_contract.sha256, sha256Json(s.contract));
    assert.equal(report.rows[0]?.status, 'equal_eligible');
  } finally {
    t.mock.restoreAll();
    syncBuiltinESMExports();
    s.cleanup();
  }
});

test('ledger pathname substitution cannot erase the consumed UNKNOWN history in its descriptor snapshot', async (t) => {
  const s = setup();
  const read = fs.readFileSync;
  const contract = __testInternals.parseExecutionContract(s.contract);
  const action = contract.actions[0]!;
  const ledgerFile = __testInternals.executionLedgerPath(
    __testInternals.executionLedgerRoot(s.options.env, contract),
    action,
  );
  const attemptCore = {
    schema_version: 'dataset-save-draft-execution-event.v1',
    sequence: 1,
    contract_sha256: sha256Json(contract),
    action_id: action.action_id,
    desired_sha256: action.desired_sha256,
    action_binding_sha256: __testInternals.executionActionBindingSha256(action),
    event_type: 'attempt_emitted',
    operation: action.expected_operation,
    outcome: null,
    recovered: false,
    recorded_at_utc: '2026-10-13T00:00:00.000Z',
    previous_event_sha256: null,
  };
  const attempt = { ...attemptCore, event_sha256: sha256Json(attemptCore) };
  const outcomeCore = {
    ...attemptCore,
    sequence: 2,
    event_type: 'outcome',
    outcome: 'unknown',
    previous_event_sha256: attempt.event_sha256,
  };
  const outcome = { ...outcomeCore, event_sha256: sha256Json(outcomeCore) };
  const original = `${JSON.stringify(attempt)}\n${JSON.stringify(outcome)}\n`;
  fs.mkdirSync(path.dirname(ledgerFile), { recursive: true });
  fs.writeFileSync(ledgerFile, original);
  let pathnameReads = 0;
  try {
    t.mock.method(fs, 'readFileSync', (...args: Parameters<typeof fs.readFileSync>) => {
      if (args[0] === ledgerFile) {
        pathnameReads += 1;
        return args[1] === 'utf8' ? '' : Buffer.alloc(0);
      }
      return read(...args);
    });
    syncBuiltinESMExports();
    const report = await runDatasetDraftReconciliation(s.options);
    assert.equal(report.rows[0]?.history.attempt_consumed, true);
    assert.equal(report.rows[0]?.history.outcome, 'unknown');
    assert.equal(report.rows[0]?.history.event_count, 2);
    assert.equal(report.rows[0]?.history.event_tip_sha256, outcome.event_sha256);
    assert.equal(pathnameReads, 0);
    assert.equal(read(ledgerFile, 'utf8'), original);
  } finally {
    t.mock.restoreAll();
    syncBuiltinESMExports();
    s.cleanup();
  }
});

test('reconciliation accepts bound JSONL snapshots through the existing row normalization', async () => {
  const s = setup();
  try {
    const inputPath = path.join(s.dir, 'rows.JSONL');
    fs.writeFileSync(inputPath, `\n ${JSON.stringify(s.payload)} \r\n\n`);
    const report = await runDatasetDraftReconciliation({ ...s.options, inputPath });
    assert.equal(report.rows[0]?.status, 'equal_eligible');
    assert.equal(report.input.path, inputPath);
    assert.equal(report.rows[0]?.history.event_count, 0);
  } finally {
    s.cleanup();
  }
});

test('malformed captured JSON and JSONL stop before identity without exposing input bytes', async () => {
  for (const extension of ['json', 'jsonl']) {
    const s = setup();
    try {
      const inputPath = path.join(s.dir, `malformed.${extension}`);
      fs.writeFileSync(inputPath, '{"private":"CANARY_SECRET"');
      await assert.rejects(
        runDatasetDraftReconciliation({ ...s.options, inputPath }),
        (error: unknown) => {
          assert.ok(error instanceof Error);
          assert.match(error.message, /valid JSON or JSONL/u);
          assert.equal(JSON.stringify(error).includes('CANARY_SECRET'), false);
          return true;
        },
      );
      assert.equal(s.calls.length, 0);
      assert.equal(fs.existsSync(s.options.outDir), false);
    } finally {
      s.cleanup();
    }
  }
});
