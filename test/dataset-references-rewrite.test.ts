import assert from 'node:assert/strict';
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import {
  __testInternals,
  runDatasetReferencesRewrite,
} from '../src/lib/dataset-references-rewrite.js';

function writeJsonl(filePath: string, rows: unknown[]): void {
  writeFileSync(filePath, `${rows.map((row) => JSON.stringify(row)).join('\n')}\n`, 'utf8');
}

function readJson(filePath: string): unknown {
  return JSON.parse(readFileSync(filePath, 'utf8'));
}

function readJsonl(filePath: string): unknown[] {
  return readFileSync(filePath, 'utf8')
    .split(/\r?\n/u)
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => JSON.parse(line));
}

test('runDatasetReferencesRewrite patches process and lifecyclemodel flow references locally', async () => {
  const dir = mkdtempSync(path.join(os.tmpdir(), 'tg-cli-dataset-references-'));
  const inputPath = path.join(dir, 'rows.jsonl');
  const outDir = path.join(dir, 'out');
  writeJsonl(inputPath, [
    {
      id: 'proc-1',
      version: '01.00.000',
      json_ordered: {
        processDataSet: {
          exchanges: {
            exchange: [
              {
                referenceToFlowDataSet: {
                  '@refObjectId': 'old-flow',
                  '@version': '01.00.000',
                },
              },
            ],
          },
        },
      },
    },
    {
      id: 'lm-1',
      version: '01.00.000',
      json_ordered: {
        lifeCycleModelDataSet: {
          lifeCycleModelInformation: {
            technology: {
              processes: {
                processInstance: {
                  '@dataSetInternalID': '1',
                  connections: {
                    outputExchange: {
                      '@flowUUID': 'old-flow',
                    },
                  },
                },
              },
            },
          },
        },
      },
    },
  ]);

  try {
    const report = await runDatasetReferencesRewrite({
      inputPath,
      outDir,
      from: 'flow:old-flow@01.00.000',
      to: 'flow:new-flow@01.01.000',
      now: new Date('2026-05-05T00:00:00.000Z'),
    });

    assert.equal(report.status, 'completed');
    assert.deepEqual(report.counts, {
      input_rows: 2,
      patched_rows: 2,
      changes: 3,
      process_rows: 1,
      lifecyclemodel_rows: 1,
    });
    assert.equal(existsSync(report.files.summary), true);
    assert.deepEqual(readJson(report.files.summary), report);

    const patchedRows = readJsonl(report.files.patched_rows) as Array<{
      json_ordered: Record<string, unknown>;
    }>;
    assert.equal(
      (
        (
          (
            (patchedRows[0]?.json_ordered.processDataSet as Record<string, unknown>)
              .exchanges as Record<string, unknown>
          ).exchange as Array<Record<string, unknown>>
        )[0]?.referenceToFlowDataSet as Record<string, unknown>
      )['@refObjectId'],
      'new-flow',
    );
    assert.match(JSON.stringify(patchedRows[1]), /new-flow/u);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('runDatasetReferencesRewrite accepts comma-separated type aliases', () => {
  assert.deepEqual(__testInternals.normalizeTypes(['process,lifecyclemodel']), [
    'process',
    'lifecyclemodel',
  ]);
});

test('runDatasetReferencesRewrite validates flags and supports commit delegates', async () => {
  const dir = mkdtempSync(path.join(os.tmpdir(), 'tg-cli-dataset-references-commit-'));
  const inputPath = path.join(dir, 'rows.jsonl');
  const outDir = path.join(dir, 'out');
  writeJsonl(inputPath, [
    {
      id: 'proc-commit',
      version: '01.00.000',
      json_ordered: {
        processDataSet: {
          exchanges: {
            exchange: {
              referenceToFlowDataSet: {
                '@refObjectId': 'old-flow',
              },
            },
          },
        },
      },
    },
    {
      id: 'lm-commit',
      version: '01.00.000',
      json_ordered: {
        lifeCycleModelDataSet: {
          lifeCycleModelInformation: {
            technology: {
              processes: {
                processInstance: {
                  connections: {
                    outputExchange: {
                      '@flowUUID': 'old-flow',
                    },
                  },
                },
              },
            },
          },
        },
      },
    },
    { id: 'flow-skip', json_ordered: { flowDataSet: {} } },
  ]);

  try {
    await assert.rejects(
      () =>
        runDatasetReferencesRewrite({
          inputPath,
          outDir,
          from: 'bad-reference',
          to: 'flow:new-flow',
        }),
      /Expected --from reference/u,
    );
    await assert.rejects(
      () =>
        runDatasetReferencesRewrite({
          inputPath,
          outDir: '',
          from: 'flow:old-flow',
          to: 'flow:new-flow',
        }),
      /Missing required --out-dir/u,
    );
    assert.throws(
      () => __testInternals.normalizeTypes(['process,unknown']),
      /Expected --type or --types/u,
    );

    const processCalls: unknown[] = [];
    const lifecyclemodelCalls: unknown[] = [];
    const report = await runDatasetReferencesRewrite({
      inputPath,
      outDir,
      from: 'flow:old-flow',
      to: 'flow:new-flow',
      commit: true,
      scope: ' current scope ',
      env: { TEST: '1' },
      fetchImpl: async () => ({
        ok: true,
        status: 200,
        headers: { get: () => 'application/json' },
        text: async () => '{}',
      }),
      runProcessSaveDraftImpl: async (options) => {
        processCalls.push(options);
        return {
          generated_at_utc: '2026-05-05T00:00:00.000Z',
          input_path: options.inputPath,
          input_kind: 'rows_file',
          out_dir: options.outDir ?? '',
          commit: true,
          mode: 'commit',
          target_user_id: null,
          account_guard: {
            target_user_id_required: false,
            target_user_id: null,
            commit_account_binding: 'current_cli_auth_session',
            post_write_verify_required: false,
          },
          status: 'completed',
          counts: { selected: 1, prepared: 0, executed: 1, failed: 0 },
          files: {
            normalized_input: '',
            selected_processes: '',
            progress_jsonl: '',
            failures_jsonl: '',
            summary_json: '',
          },
          processes: [],
        };
      },
      runLifecyclemodelSaveDraftImpl: async (options) => {
        lifecyclemodelCalls.push(options);
        return {
          generated_at_utc: '2026-05-05T00:00:00.000Z',
          input_path: options.inputPath,
          out_dir: options.outDir ?? '',
          commit: true,
          mode: 'commit',
          status: 'completed_with_failures',
          counts: { selected: 1, prepared: 0, executed: 0, failed: 1 },
          files: {
            normalized_input: '',
            selected_lifecyclemodels: '',
            progress_jsonl: '',
            failures_jsonl: '',
            summary_json: '',
          },
          lifecyclemodels: [],
        };
      },
      now: new Date('2026-05-05T00:00:00.000Z'),
    });

    assert.equal(report.status, 'completed_with_failures');
    assert.equal(report.filters.scope, 'current scope');
    assert.equal(report.counts.process_rows, 1);
    assert.equal(report.counts.lifecyclemodel_rows, 1);
    assert.equal(processCalls.length, 1);
    assert.equal(lifecyclemodelCalls.length, 1);

    const generatedNow = await runDatasetReferencesRewrite({
      inputPath,
      outDir: path.join(dir, 'generated-now-out'),
      from: 'flow:old-flow',
      to: 'flow:new-flow',
      types: ['process'],
    });
    assert.equal(generatedNow.status, 'completed');
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('reference rewrite internals cover no-op and nested fallback branches', () => {
  const from = __testInternals.parseReference('flow:old-flow', '--from');
  const to = __testInternals.parseReference('flow:old-flow', '--to');
  const processPayload = {
    array: [
      null,
      {
        referenceToFlowDataSet: {
          '@refObjectId': 'old-flow',
          '@version': '01.00.000',
        },
      },
    ],
  };
  const processVersionPayload = {
    referenceToFlowDataSet: {
      '@refObjectId': 'old-flow',
      '@version': 1,
    },
  };
  const lifecyclemodelPayload = [{ '@flowUUID': 'old-flow' }, 1];
  const changes: unknown[] = [];

  __testInternals.rewriteProcessReferences(processPayload, from, to, '', (...args) =>
    changes.push(args),
  );
  __testInternals.rewriteLifecyclemodelReferences(lifecyclemodelPayload, from, to, '', (...args) =>
    changes.push(args),
  );

  assert.equal(changes.length, 0);

  __testInternals.rewriteProcessReferences(
    processVersionPayload,
    from,
    __testInternals.parseReference('flow:new-flow@01.00.000', '--to'),
    '',
    (...args) => changes.push(args),
  );
  assert.deepEqual(
    changes.find((change) => Array.isArray(change) && change[1] === '@version'),
    ['referenceToFlowDataSet.@version', '@version', null, '01.00.000'],
  );
});
