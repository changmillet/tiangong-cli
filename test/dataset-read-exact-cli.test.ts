import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { executeCli, type CliDeps } from '../src/cli.js';
import type {
  DatasetReadExactReport,
  RunDatasetReadExactOptions,
} from '../src/lib/dataset-exact-read.js';
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

const scopeSha256 = 'a'.repeat(64);
const requiredFlags = [
  '--scope-file',
  'original-scope.json',
  '--scope-sha256',
  scopeSha256,
  '--out-dir',
  'new-exact-read',
];

function report(
  rowStatus: DatasetReadExactReport['rows'][number]['status'] = 'found',
  status: DatasetReadExactReport['status'] = 'observed',
): DatasetReadExactReport {
  const capturedAt = '2026-10-12T00:00:00.000Z';
  const actorId = '11111111-1111-4111-8111-111111111111';
  const sourceOwnerId = '33333333-3333-4333-8333-333333333333';
  const processId = '22222222-2222-4222-8222-222222222222';
  const modelId = '44444444-4444-4444-8444-444444444444';
  const version = '00.00.001';
  const actualRow = {
    id: processId,
    version,
    user_id: sourceOwnerId,
    state_code: 100,
    modified_at: capturedAt,
    json_ordered: { processDataSet: { synthetic: 'observed public/source body' } },
    team_id: null,
    model_id: modelId,
    model_version: null,
    review_id: null,
  };
  const hasRow = rowStatus === 'found' || rowStatus === 'drift';
  const reasons = {
    found: 'exact_row',
    absent: 'not_visible',
    indeterminate: 'read_failed',
    drift: 'model_association_changed',
  } as const;
  return {
    schema_version: 'dataset-exact-read-report.v1',
    mode: 'read_only',
    read_scope: 'public_100_199_or_current_owner_state_0',
    status,
    authorization_granted: false,
    adoption_performed: false,
    identity: {
      schema: 'tiangong-lca.auth-identity-receipt.v1',
      status: 'passed',
      operation: 'current-user-read',
      remote_write_mode: 'read-only',
      captured_at_utc: capturedAt,
      cli: { package_name: '@tiangong-lca/cli', package_version: '0.1.28' },
      project: {
        project_ref: 'synthetic-project',
        project_base_url: 'https://synthetic-project.supabase.co',
      },
      identity: { user_id: actorId, display_email: 's***@example.invalid' },
      session: {
        source: 'access_token',
        cache_mode: 'disabled',
        force_reauth: false,
        expires_at_utc: null,
      },
      bindings: { request_sha256: scopeSha256, response_sha256: scopeSha256 },
      assertions: {
        mode: 'intent-bound',
        requested_count: 2,
        expected_project_ref: 'synthetic-project',
        expected_user_id: actorId,
        project_ref_passed: true,
        user_id_passed: true,
        passed: true,
      },
      receipt_scope_sha256: scopeSha256,
    },
    scope: { path: '/synthetic/original-scope.json', sha256: scopeSha256, bytes: 10 },
    sources: [{ path: '/synthetic/original-finite-source.json', sha256: scopeSha256, bytes: 100 }],
    rows: [
      {
        selection: {
          table: 'processes',
          id: processId,
          version,
          expected_model: { id: modelId, version },
        },
        status: rowStatus,
        reason: reasons[rowStatus],
        observed_at_utc: capturedAt,
        actual_row: hasRow ? actualRow : null,
        actual_row_sha256: hasRow ? scopeSha256 : null,
        actual_payload_sha256: hasRow ? scopeSha256 : null,
        model_association: hasRow
          ? {
              model_id: modelId,
              resolved_version: version,
              version_source: 'process_version_fallback',
            }
          : null,
      },
    ],
  };
}

test('dataset read-exact is discoverable with finite read-only scope help', async () => {
  const deps = makeDeps({
    runDatasetReadExactImpl: async () => {
      throw new Error('help must not run exact reads');
    },
  });
  for (const argv of [[], ['dataset'], ['dataset', '--help']]) {
    const result = await executeCli(argv, deps);
    assert.equal(result.exitCode, 0);
    assert.match(result.stdout, /read-exact/u);
  }
  for (const flag of ['--help', '-h']) {
    const result = await executeCli(['dataset', 'read-exact', flag], deps);
    assert.equal(result.exitCode, 0);
    assert.equal(result.stderr, '');
    assert.match(result.stdout, /--scope-file <file>/u);
    assert.match(result.stdout, /--scope-sha256 <sha256>/u);
    assert.match(result.stdout, /--out-dir <NEW dir>/u);
    assert.match(result.stdout, /Create-only private evidence directory/u);
    assert.match(result.stdout, /exact Process and explicitly linked Model rows/u);
    assert.match(result.stdout, /current-user RLS/u);
    assert.match(result.stdout, /original scope hash and source file facts/u);
    assert.match(result.stdout, /no account census and no automatic graph expansion/u);
    assert.match(result.stdout, /public\/source reads require no owner write contract/u);
    assert.match(
      result.stdout,
      /no mutation authority, owner-draft eligibility, scientific acceptance or adoption/u,
    );
  }
});

test('dataset read-exact forwards exact scope binding and the full report', async () => {
  const calls: RunDatasetReadExactOptions[] = [];
  const expectedReport = report();
  expectedReport.rows.push({
    selection: {
      table: 'lifecyclemodels',
      id: '44444444-4444-4444-8444-444444444444',
      version: '00.00.001',
      referenced_by: [{ id: '22222222-2222-4222-8222-222222222222', version: '00.00.001' }],
    },
    status: 'found',
    reason: 'exact_row',
    observed_at_utc: '2026-10-12T00:00:00.000Z',
    actual_row: {
      id: '44444444-4444-4444-8444-444444444444',
      version: '00.00.001',
      user_id: '33333333-3333-4333-8333-333333333333',
      state_code: 100,
      modified_at: '2026-10-12T00:00:00.000Z',
      json_ordered: { lifeCycleModelDataSet: { synthetic: 'original selected model body' } },
      team_id: null,
    },
    actual_row_sha256: scopeSha256,
    actual_payload_sha256: scopeSha256,
    model_association: null,
  });
  const deps = makeDeps({
    runDatasetReadExactImpl: async (options) => {
      calls.push(options);
      return expectedReport;
    },
    runDatasetSaveDraftImpl: async () => {
      throw new Error('read-exact must not dispatch save-draft');
    },
    runDatasetDraftReconciliationImpl: async () => {
      throw new Error('read-exact must not require a draft execution contract');
    },
  });
  const result = await executeCli(
    [
      'dataset',
      'read-exact',
      '--scope-file=original-scope.json',
      '--scope-sha256',
      scopeSha256,
      '--out-dir',
      'new-exact-read',
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
      scopeFile: 'original-scope.json',
      scopeSha256,
      outDir: 'new-exact-read',
      env: deps.env,
      fetchImpl: deps.fetchImpl,
    },
  ]);
  const pretty = await executeCli(['dataset', 'read-exact', ...requiredFlags], deps);
  assert.equal(pretty.exitCode, 0);
  assert.match(pretty.stdout, /\n  "schema_version": "dataset-exact-read-report.v1"/u);
  assert.deepEqual(JSON.parse(pretty.stdout), expectedReport);
});

test('dataset read-exact exits 1 for absent, indeterminate or drift rows and report status', async () => {
  for (const rowStatus of ['absent', 'indeterminate', 'drift'] as const) {
    const expectedReport = report(rowStatus);
    expectedReport.rows.unshift(report().rows[0]!);
    const result = await executeCli(
      ['dataset', 'read-exact', ...requiredFlags, '--json'],
      makeDeps({ runDatasetReadExactImpl: async () => expectedReport }),
    );
    assert.equal(result.exitCode, 1, rowStatus);
    assert.deepEqual(JSON.parse(result.stdout), expectedReport);
    assert.equal(result.stderr, '');
  }
  for (const status of ['indeterminate', 'drift'] as const) {
    const expectedReport = report('found', status);
    const result = await executeCli(
      ['dataset', 'read-exact', ...requiredFlags, '--json'],
      makeDeps({ runDatasetReadExactImpl: async () => expectedReport }),
    );
    assert.equal(result.exitCode, 1, status);
    assert.deepEqual(JSON.parse(result.stdout), expectedReport);
  }
  const emptyReport = report();
  emptyReport.rows = [];
  const empty = await executeCli(
    ['dataset', 'read-exact', ...requiredFlags, '--json'],
    makeDeps({ runDatasetReadExactImpl: async () => emptyReport }),
  );
  assert.equal(empty.exitCode, 1);
  assert.deepEqual(JSON.parse(empty.stdout), emptyReport);
});

test('dataset read-exact requires nonblank scope, scope hash and output directory', async () => {
  let calls = 0;
  const deps = makeDeps({
    runDatasetReadExactImpl: async () => {
      calls += 1;
      return report();
    },
  });
  for (const optionName of ['scope-file', 'scope-sha256', 'out-dir']) {
    const optionIndex = requiredFlags.indexOf(`--${optionName}`);
    for (const flags of [
      requiredFlags.filter((_, index) => index !== optionIndex && index !== optionIndex + 1),
      requiredFlags.map((value, index) => (index === optionIndex + 1 ? '' : value)),
      requiredFlags.map((value, index) => (index === optionIndex + 1 ? '  ' : value)),
    ]) {
      const result = await executeCli(['dataset', 'read-exact', ...flags], deps);
      assert.equal(result.exitCode, 2);
      assert.equal(result.stdout, '');
      assert.match(result.stderr, new RegExp(`requires --${optionName}`, 'u'));
    }
  }
  assert.equal(calls, 0);
});

test('dataset read-exact rejects write controls, unsupported flags, missing values and positionals', async () => {
  let calls = 0;
  const deps = makeDeps({
    runDatasetReadExactImpl: async () => {
      calls += 1;
      return report();
    },
  });
  for (const flags of [
    ['--commit'],
    ['--dry-run'],
    ['--max-parallel', '2'],
    ['--allow-account-local-support'],
    ['--input', 'rows.jsonl'],
    ['--execution-contract', 'contract.json'],
    ['--reference-intent-file', 'intent.json'],
    ['--type', 'process'],
    ['--timeout-ms', '1000'],
    ['--census'],
    ['--expand-graph'],
    ['--unknown'],
    ['--scope-file'],
    ['--scope-sha256'],
    ['--out-dir'],
    ['positional'],
  ]) {
    for (const help of [[], ['--help']]) {
      const result = await executeCli(
        ['dataset', 'read-exact', ...requiredFlags, ...help, ...flags],
        deps,
      );
      assert.equal(result.exitCode, 2, flags.join(' '));
      assert.equal(result.stdout, '');
      assert.match(result.stderr, /INVALID_ARGS/u);
    }
  }
  assert.equal(calls, 0);
});

test('dataset read-exact rejects repeated value flags including inline values and help', async () => {
  let calls = 0;
  const deps = makeDeps({
    runDatasetReadExactImpl: async () => {
      calls += 1;
      return report();
    },
  });
  for (const optionName of ['scope-file', 'scope-sha256', 'out-dir']) {
    for (const repeated of [[`--${optionName}`, 'second'], [`--${optionName}=second`]]) {
      for (const help of [[], ['--help']]) {
        const result = await executeCli(
          ['dataset', 'read-exact', ...requiredFlags, ...help, ...repeated],
          deps,
        );
        assert.equal(result.exitCode, 2, optionName);
        assert.equal(result.stdout, '');
        assert.match(
          result.stderr,
          new RegExp(`Option --${optionName} may be provided only once`, 'u'),
        );
      }
    }
  }
  assert.equal(calls, 0);
});

test('dataset read-exact preserves runtime errors and defaults to the real read runner', async () => {
  const invalid = await executeCli(
    ['dataset', 'read-exact', ...requiredFlags],
    makeDeps({
      runDatasetReadExactImpl: async () => {
        throw new CliError('Original scope hash differs.', {
          code: 'DATASET_EXACT_READ_INVALID',
          exitCode: 2,
        });
      },
    }),
  );
  assert.equal(invalid.exitCode, 2);
  assert.equal(invalid.stdout, '');
  assert.match(invalid.stderr, /DATASET_EXACT_READ_INVALID/u);

  const tmpDir = mkdtempSync(path.join(os.tmpdir(), 'dataset-read-exact-cli-'));
  try {
    const missing = await executeCli(
      [
        'dataset',
        'read-exact',
        '--scope-file',
        path.join(tmpDir, 'missing-scope.json'),
        '--scope-sha256',
        scopeSha256,
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
