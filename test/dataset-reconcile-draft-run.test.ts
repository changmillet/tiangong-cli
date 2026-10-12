import assert from 'node:assert/strict';
import fs from 'node:fs';
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
    (read: () => unknown): FetchLike =>
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
      return response(read());
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
    ['foreign', (row) => [{ ...row, user_id: '22222222-2222-4222-8222-222222222222' }], 'foreign'],
    ['state', (row) => [{ ...row, state_code: 100 }], 'ineligible'],
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
