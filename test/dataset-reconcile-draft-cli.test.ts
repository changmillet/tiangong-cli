import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { executeCli, type CliDeps } from '../src/cli.js';
import type {
  DatasetDraftReconciliationReport,
  RunDatasetDraftReconciliationOptions,
} from '../src/lib/dataset-save-draft-run.js';
import { CliError } from '../src/lib/errors.js';

function makeDeps(overrides: Partial<CliDeps> = {}): CliDeps {
  return {
    env: {},
    dotEnvStatus: { loaded: false, path: '/not-loaded/.env', count: 0 },
    fetchImpl: async () => {
      throw new Error('unexpected network');
    },
    ...overrides,
  };
}

const requiredFlags = [
  '--input',
  'original-rows.jsonl',
  '--execution-contract',
  'original-contract.json',
  '--out-dir',
  'new-reconciliation',
];

function report(
  rowStatus: DatasetDraftReconciliationReport['rows'][number]['status'] = 'equal_eligible',
  status: DatasetDraftReconciliationReport['status'] = 'observed',
): DatasetDraftReconciliationReport {
  const generatedAt = '2026-10-12T00:00:00.000Z';
  const ownerId = '11111111-1111-4111-8111-111111111111';
  const datasetId = '22222222-2222-4222-8222-222222222222';
  const hash = 'a'.repeat(64);
  const reasons = {
    equal_eligible: 'equal_current_owner_draft',
    different: 'different_body',
    foreign: 'foreign_owner',
    ineligible: 'ineligible_state_or_metadata',
    absent: 'not_visible',
    indeterminate: 'read_failed',
  } as const;
  return {
    schema_version: 'dataset-draft-reconciliation.v1',
    mode: 'read_only',
    read_scope: 'current_owner_state_0',
    status,
    generated_at_utc: generatedAt,
    authorization_granted: false,
    historical_execution_changed: false,
    eligibility_policy: 'current-owner-state-0-standalone-unreviewed-process',
    identity: {
      schema: 'tiangong-lca.auth-identity-receipt.v1',
      status: 'passed',
      operation: 'current-user-read',
      remote_write_mode: 'read-only',
      captured_at_utc: generatedAt,
      cli: { package_name: '@tiangong-lca/cli', package_version: '0.1.28' },
      project: {
        project_ref: 'synthetic-project',
        project_base_url: 'https://synthetic-project.supabase.co',
      },
      identity: { user_id: ownerId, display_email: 's***@example.invalid' },
      session: {
        source: 'access_token',
        cache_mode: 'disabled',
        force_reauth: false,
        expires_at_utc: null,
      },
      bindings: { request_sha256: hash, response_sha256: hash },
      assertions: {
        mode: 'intent-bound',
        requested_count: 2,
        expected_project_ref: 'synthetic-project',
        expected_user_id: ownerId,
        project_ref_passed: true,
        user_id_passed: true,
        passed: true,
      },
      receipt_scope_sha256: hash,
    },
    execution_contract: {
      path: '/synthetic/original-contract.json',
      file_sha256: hash,
      sha256: hash,
      execution_id: 'synthetic-original-execution',
      project_ref: 'synthetic-project',
      owner: { user_id: ownerId, email: 'synthetic@example.invalid', state_code: 0 },
    },
    input: { path: '/synthetic/original-rows.jsonl', sha256: hash, bytes: 10 },
    reference_intent: null,
    rows: [
      {
        status: rowStatus,
        reason: reasons[rowStatus],
        actual_row:
          rowStatus === 'absent' || rowStatus === 'indeterminate'
            ? null
            : {
                id: datasetId,
                version: '00.00.001',
                user_id: rowStatus === 'foreign' ? '33333333-3333-4333-8333-333333333333' : ownerId,
                state_code: rowStatus === 'ineligible' ? 1 : 0,
                json_ordered: { synthetic: rowStatus === 'different' ? 'different' : 'expected' },
                model_id: null,
                model_version: null,
                team_id: null,
                review_id: null,
              },
        actual_row_sha256: rowStatus === 'absent' || rowStatus === 'indeterminate' ? null : hash,
        actual_payload_sha256:
          rowStatus === 'absent' || rowStatus === 'indeterminate' ? null : hash,
        action: {
          action_id: 'synthetic-admitted-action',
          desired_sha256: hash,
          expected_operation: 'save_draft',
          table: 'processes',
          id: datasetId,
          version: '00.00.001',
          before_sha256: hash,
          dependency_action_ids: [],
        },
        observed_at_utc: generatedAt,
        history: {
          ledger: { path: '/synthetic/original-ledger.events.jsonl', sha256: hash, bytes: 10 },
          attempt_consumed: true,
          outcome: 'unknown',
          event_count: 2,
          event_tip_sha256: hash,
          reference_intent_admission: null,
        },
      },
    ],
  };
}

test('dataset reconcile-draft is discoverable and its help describes read-only scope', async () => {
  const deps = makeDeps({
    runDatasetDraftReconciliationImpl: async () => {
      throw new Error('help must not run reconciliation');
    },
  });
  for (const argv of [[], ['dataset'], ['dataset', '--help']]) {
    const result = await executeCli(argv, deps);
    assert.equal(result.exitCode, 0);
    assert.match(result.stdout, /reconcile-draft/u);
  }
  for (const flag of ['--help', '-h']) {
    const result = await executeCli(['dataset', 'reconcile-draft', flag], deps);
    assert.equal(result.exitCode, 0);
    assert.equal(result.stderr, '');
    assert.match(result.stdout, /--out-dir <NEW dir>/u);
    assert.match(result.stdout, /Create-only output directory/u);
    assert.match(result.stdout, /Original content-bound execution contract \(mandatory\)/u);
    assert.match(result.stdout, /exact admitted rows only/u);
    assert.match(result.stdout, /current owner, body and metadata observations/u);
    assert.match(result.stdout, /No writes, no replay, no historical outcome rewrite/u);
    assert.match(result.stdout, /no automatic authorization/u);
  }
});

test('dataset reconcile-draft forwards bindings and the full report without mutation controls', async () => {
  const calls: RunDatasetDraftReconciliationOptions[] = [];
  const expectedReport = report();
  const deps = makeDeps({
    runDatasetDraftReconciliationImpl: async (options) => {
      calls.push(options);
      return expectedReport;
    },
    runDatasetSaveDraftImpl: async () => {
      throw new Error('reconciliation must not dispatch save-draft');
    },
  });
  const result = await executeCli(
    [
      'dataset',
      'reconcile-draft',
      ...requiredFlags,
      '--type',
      'flow',
      '--reference-intent-file=original-selection.json',
      '--json',
    ],
    deps,
  );
  assert.equal(result.exitCode, 0);
  assert.equal(result.stderr, '');
  assert.deepEqual(JSON.parse(result.stdout), expectedReport);
  assert.equal(result.stdout, `${JSON.stringify(expectedReport)}\n`);
  assert.deepEqual(calls, [
    {
      inputPath: 'original-rows.jsonl',
      executionContractPath: 'original-contract.json',
      outDir: 'new-reconciliation',
      type: 'flow',
      referenceIntentFile: 'original-selection.json',
      env: deps.env,
      fetchImpl: deps.fetchImpl,
    },
  ]);

  const defaults = await executeCli(['dataset', 'reconcile-draft', ...requiredFlags], deps);
  assert.equal(defaults.exitCode, 0);
  assert.deepEqual(JSON.parse(defaults.stdout), expectedReport);
  assert.deepEqual(calls[1], {
    inputPath: 'original-rows.jsonl',
    executionContractPath: 'original-contract.json',
    outDir: 'new-reconciliation',
    env: deps.env,
    fetchImpl: deps.fetchImpl,
  });
});

test('dataset reconcile-draft exits 1 for every non-equal row and indeterminate observation', async () => {
  for (const rowStatus of [
    'different',
    'foreign',
    'ineligible',
    'absent',
    'indeterminate',
  ] as const) {
    const expectedReport = report(rowStatus);
    expectedReport.rows.unshift(report().rows[0]!);
    const result = await executeCli(
      ['dataset', 'reconcile-draft', ...requiredFlags, '--json'],
      makeDeps({ runDatasetDraftReconciliationImpl: async () => expectedReport }),
    );
    assert.equal(result.exitCode, 1, rowStatus);
    assert.deepEqual(JSON.parse(result.stdout), expectedReport);
    assert.equal(result.stderr, '');
  }
  const expectedReport = report('equal_eligible', 'indeterminate');
  const result = await executeCli(
    ['dataset', 'reconcile-draft', ...requiredFlags, '--json'],
    makeDeps({ runDatasetDraftReconciliationImpl: async () => expectedReport }),
  );
  assert.equal(result.exitCode, 1);
  assert.deepEqual(JSON.parse(result.stdout), expectedReport);
});

test('dataset reconcile-draft cannot accept an empty observed report', async () => {
  const expectedReport = report();
  expectedReport.rows = [];
  const result = await executeCli(
    ['dataset', 'reconcile-draft', ...requiredFlags, '--json'],
    makeDeps({ runDatasetDraftReconciliationImpl: async () => expectedReport }),
  );
  assert.equal(result.exitCode, 1);
  assert.deepEqual(JSON.parse(result.stdout), expectedReport);
});

test('dataset reconcile-draft requires nonblank original paths before invoking runtime', async () => {
  let calls = 0;
  const deps = makeDeps({
    runDatasetDraftReconciliationImpl: async () => {
      calls += 1;
      return report();
    },
  });
  for (const optionName of ['input', 'execution-contract', 'out-dir']) {
    const optionIndex = requiredFlags.indexOf(`--${optionName}`);
    for (const flags of [
      requiredFlags.filter((_, index) => index !== optionIndex && index !== optionIndex + 1),
      requiredFlags.map((value, index) => (index === optionIndex + 1 ? '' : value)),
      requiredFlags.map((value, index) => (index === optionIndex + 1 ? '  ' : value)),
    ]) {
      const result = await executeCli(['dataset', 'reconcile-draft', ...flags], deps);
      assert.equal(result.exitCode, 2);
      assert.equal(result.stdout, '');
      assert.match(result.stderr, new RegExp(`requires --${optionName}`, 'u'));
    }
  }
  assert.equal(calls, 0);
});

test('dataset reconcile-draft rejects unsupported flags, missing values and positionals even with help', async () => {
  let calls = 0;
  const deps = makeDeps({
    runDatasetDraftReconciliationImpl: async () => {
      calls += 1;
      return report();
    },
  });
  for (const flags of [
    ['--commit'],
    ['--dry-run'],
    ['--max-parallel', '2'],
    ['--allow-account-local-support'],
    ['--timeout-ms', '1000'],
    ['--unknown'],
    ['--input'],
    ['--execution-contract'],
    ['--out-dir'],
    ['--type'],
    ['--reference-intent-file'],
    ['positional'],
  ]) {
    for (const help of [[], ['--help']]) {
      const result = await executeCli(
        ['dataset', 'reconcile-draft', ...requiredFlags, ...help, ...flags],
        deps,
      );
      assert.equal(result.exitCode, 2, flags.join(' '));
      assert.equal(result.stdout, '');
      assert.match(result.stderr, /INVALID_ARGS/u);
    }
  }
  assert.equal(calls, 0);
});

test('dataset reconcile-draft rejects repeated value flags in separate and inline forms', async () => {
  let calls = 0;
  const deps = makeDeps({
    runDatasetDraftReconciliationImpl: async () => {
      calls += 1;
      return report();
    },
  });
  for (const optionName of [
    'input',
    'execution-contract',
    'out-dir',
    'type',
    'reference-intent-file',
  ]) {
    const base = requiredFlags.includes(`--${optionName}`)
      ? requiredFlags
      : [...requiredFlags, `--${optionName}`, 'first'];
    for (const repeated of [[`--${optionName}`, 'second'], [`--${optionName}=second`]]) {
      const result = await executeCli(['dataset', 'reconcile-draft', ...base, ...repeated], deps);
      assert.equal(result.exitCode, 2, optionName);
      assert.equal(result.stdout, '');
      assert.match(
        result.stderr,
        new RegExp(`Option --${optionName} may be provided only once`, 'u'),
      );
    }
  }
  assert.equal(calls, 0);
});

test('dataset reconcile-draft preserves runtime errors and uses its real default runner', async () => {
  const result = await executeCli(
    ['dataset', 'reconcile-draft', ...requiredFlags],
    makeDeps({
      runDatasetDraftReconciliationImpl: async () => {
        throw new CliError('Original attempt evidence unavailable.', {
          code: 'RECONCILIATION_EVIDENCE_REQUIRED',
          exitCode: 2,
        });
      },
    }),
  );
  assert.equal(result.exitCode, 2);
  assert.equal(result.stdout, '');
  assert.match(result.stderr, /RECONCILIATION_EVIDENCE_REQUIRED/u);

  const tmpDir = mkdtempSync(path.join(os.tmpdir(), 'dataset-reconcile-cli-'));
  try {
    const missing = await executeCli(
      [
        'dataset',
        'reconcile-draft',
        '--input',
        path.join(tmpDir, 'missing-rows.jsonl'),
        '--execution-contract',
        path.join(tmpDir, 'missing-contract.json'),
        '--out-dir',
        path.join(tmpDir, 'output'),
      ],
      makeDeps(),
    );
    assert.notEqual(missing.exitCode, 0);
    assert.equal(missing.stdout, '');
  } finally {
    rmSync(tmpDir, { recursive: true, force: true });
  }
});
