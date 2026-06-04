import assert from 'node:assert/strict';
import test from 'node:test';
import { executeCli } from '../src/cli.js';
import { __testInternals } from '../src/lib/dataset-save-draft-run.js';
import type { DatasetSaveDraftReport } from '../src/lib/dataset-save-draft-run.js';
import type { DotEnvLoadResult } from '../src/lib/dotenv.js';

const dotEnvStatus: DotEnvLoadResult = {
  loaded: false,
  path: '/tmp/.env',
  count: 0,
};

function makeDeps(overrides = {}) {
  return {
    env: {} as NodeJS.ProcessEnv,
    dotEnvStatus,
    fetchImpl: async () => ({
      ok: true,
      status: 200,
      headers: {
        get: () => 'application/json',
      },
      text: async () => JSON.stringify({ ok: true }),
    }),
    ...overrides,
  };
}

function report(status: DatasetSaveDraftReport['status'] = 'completed'): DatasetSaveDraftReport {
  return {
    schema_version: 1,
    generated_at_utc: '2026-06-02T00:00:00.000Z',
    input_path: 'contacts.jsonl',
    requested_type: 'contact',
    out_dir: 'out',
    commit: true,
    mode: 'commit',
    status,
    counts: {
      selected: 1,
      prepared: status === 'completed' ? 0 : 1,
      executed: status === 'completed' ? 1 : 0,
      failed: status === 'completed' ? 0 : 1,
      by_table: {
        contacts: 1,
      },
      operations: {
        [status === 'completed' ? 'insert' : 'skipped_invalid']: 1,
      },
    },
    files: {
      selected_rows: 'out/outputs/dataset-save-draft/selected-rows.jsonl',
      progress_jsonl: 'out/outputs/dataset-save-draft/progress.jsonl',
      failures_jsonl: 'out/outputs/dataset-save-draft/failures.jsonl',
      summary_json: 'out/outputs/dataset-save-draft/summary.json',
    },
    rows: [],
  };
}

test('executeCli exposes generic dataset save-draft for support rows', async () => {
  const observed: unknown[] = [];
  const help = await executeCli(['dataset', 'save-draft', '--help'], makeDeps());
  assert.equal(help.exitCode, 0);
  assert.match(help.stdout, /tiangong-lca dataset save-draft --input <file>/u);
  assert.match(help.stdout, /auto, contact, source, flow, process/u);
  assert.match(help.stdout, /Unit group and flow property rows are reference-only/u);

  const result = await executeCli(
    [
      'dataset',
      'save-draft',
      '--json',
      '--input',
      'contacts.jsonl',
      '--type',
      'contact',
      '--out-dir',
      'contact-save',
      '--commit',
    ],
    makeDeps({
      runDatasetSaveDraftImpl: async (options: unknown) => {
        observed.push(options);
        return report();
      },
    }),
  );

  assert.equal(result.exitCode, 0);
  assert.deepEqual(JSON.parse(result.stdout), report());
  assert.equal(observed.length, 1);
  assert.deepEqual(observed[0], {
    inputPath: 'contacts.jsonl',
    type: 'contact',
    outDir: 'contact-save',
    commit: true,
    env: {},
    fetchImpl: (observed[0] as { fetchImpl: unknown }).fetchImpl,
  });
});

test('executeCli maps generic dataset save-draft failures and rejects mode conflicts', async () => {
  const failed = await executeCli(
    ['dataset', 'save-draft', '--input', 'contacts.jsonl', '--type', 'contact'],
    makeDeps({
      runDatasetSaveDraftImpl: async () => report('completed_with_failures'),
    }),
  );
  assert.equal(failed.exitCode, 1);

  const conflict = await executeCli(
    ['dataset', 'save-draft', '--input', 'contacts.jsonl', '--type', 'contact', '--commit', '--dry-run'],
    makeDeps(),
  );
  assert.equal(conflict.exitCode, 2);
  assert.match(conflict.stderr, /Cannot pass both --commit and --dry-run/u);
});

test('dataset save-draft rejects explicit reference-only support types', () => {
  assert.throws(
    () => __testInternals.normalizeType('flowproperty'),
    /Flow properties are reference-only support data/u,
  );
  assert.throws(
    () => __testInternals.normalizeType('unitgroup'),
    /Unit groups are reference-only support data/u,
  );
});
