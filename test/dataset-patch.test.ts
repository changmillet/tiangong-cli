import assert from 'node:assert/strict';
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { executeCli } from '../src/cli.js';
import { runDatasetPatchApply, type DatasetPatchApplyReport } from '../src/lib/dataset-patch.js';
import type { DotEnvLoadResult } from '../src/lib/dotenv.js';

const dotEnvStatus: DotEnvLoadResult = {
  loaded: false,
  path: '/tmp/.env',
  count: 0,
};

function makeDeps() {
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
  };
}

function writeJson(filePath: string, value: unknown): void {
  writeFileSync(filePath, `${JSON.stringify(value, null, 2)}\n`, 'utf8');
}

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

function sampleProcessRow() {
  return {
    processDataSet: {
      processInformation: {
        dataSetInformation: {
          'common:UUID': 'proc-1',
          name: {
            baseName: {
              '@xml:lang': 'en',
              '#text': 'Old process name',
            },
          },
        },
      },
      administrativeInformation: {
        publicationAndOwnership: {
          'common:dataSetVersion': '00.00.001',
        },
      },
    },
  };
}

test('runDatasetPatchApply applies evidenced JSON patch operations transactionally', async () => {
  const dir = mkdtempSync(path.join(os.tmpdir(), 'tg-cli-dataset-patch-'));
  const inputPath = path.join(dir, 'processes.jsonl');
  const patchPath = path.join(dir, 'patches.json');
  const outPath = path.join(dir, 'patched.jsonl');
  const outDir = path.join(dir, 'out');
  writeJsonl(inputPath, [sampleProcessRow()]);
  writeJson(patchPath, {
    schema_version: 1,
    patch_status: 'completed',
    patches: [
      {
        row_index: 0,
        dataset_id: 'proc-1',
        version: '00.00.001',
        authoring_package: 'authoring/packages/proc-1.json',
        operations: [
          {
            op: 'test',
            path: '/processDataSet/processInformation/dataSetInformation/name/baseName/#text',
            value: 'Old process name',
          },
          {
            op: 'replace',
            path: '/processDataSet/processInformation/dataSetInformation/name/baseName/#text',
            value: 'Curated process name',
            basis: 'Source-language evidence says this is the process name.',
            evidence: {
              source: 'authoring-package',
              quote: 'Curated process name',
            },
            resolution: {
              mode: 'evidence_backed_completion',
              used_context_kinds: ['schema', 'methodology_yaml', 'ruleset'],
              summary: 'Completed the visible name from source-language package evidence.',
            },
          },
        ],
      },
    ],
  });

  try {
    const report = await runDatasetPatchApply({
      inputPath,
      patchPath,
      outPath,
      outDir,
      now: new Date('2026-06-02T00:00:00.000Z'),
    });

    assert.equal(report.status, 'completed');
    assert.equal(report.applied_operation_count, 1);
    assert.equal(report.evidence_count, 1);
    const rows = readJsonl(outPath);
    assert.equal(
      (
        rows[0] as {
          processDataSet: {
            processInformation: { dataSetInformation: { name: { baseName: { '#text': string } } } };
          };
        }
      ).processDataSet.processInformation.dataSetInformation.name.baseName['#text'],
      'Curated process name',
    );
    assert.equal(existsSync(report.files.patch_evidence ?? ''), true);
    const evidence = readJsonl(report.files.patch_evidence ?? '') as Array<{
      resolution?: { mode?: string; used_context_kinds?: string[] };
    }>;
    assert.equal(evidence.length, 1);
    assert.equal(evidence[0]?.resolution?.mode, 'evidence_backed_completion');
    assert.deepEqual(evidence[0]?.resolution?.used_context_kinds, [
      'schema',
      'methodology_yaml',
      'ruleset',
    ]);
    assert.deepEqual(readJson(report.files.report ?? ''), report);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('runDatasetPatchApply blocks patches without completed status before applying', async () => {
  const dir = mkdtempSync(path.join(os.tmpdir(), 'tg-cli-dataset-patch-status-'));
  const inputPath = path.join(dir, 'processes.jsonl');
  const patchPath = path.join(dir, 'patches.json');
  const outPath = path.join(dir, 'patched.jsonl');
  writeJsonl(inputPath, [sampleProcessRow()]);
  writeJson(patchPath, {
    schema_version: 1,
    patches: [
      {
        row_index: 0,
        operations: [
          {
            op: 'replace',
            path: '/processDataSet/processInformation/dataSetInformation/name/baseName/#text',
            value: 'Curated process name',
            basis: 'Source-language evidence says this is the process name.',
          },
        ],
      },
    ],
  });

  try {
    const report = await runDatasetPatchApply({
      inputPath,
      patchPath,
      outPath,
      now: new Date('2026-06-02T00:00:00.000Z'),
    });

    assert.equal(report.status, 'blocked');
    assert.equal(report.blockers[0]?.code, 'ai_patch_status_not_completed');
    assert.equal(report.patch_count, 0);
    assert.equal(report.applied_operation_count, 0);
    const rows = readJsonl(outPath);
    assert.equal(
      (
        rows[0] as {
          processDataSet: {
            processInformation: { dataSetInformation: { name: { baseName: { '#text': string } } } };
          };
        }
      ).processDataSet.processInformation.dataSetInformation.name.baseName['#text'],
      'Old process name',
    );
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('runDatasetPatchApply blocks unevidenced changes and leaves output rows unchanged', async () => {
  const dir = mkdtempSync(path.join(os.tmpdir(), 'tg-cli-dataset-patch-blocked-'));
  const inputPath = path.join(dir, 'processes.jsonl');
  const patchPath = path.join(dir, 'patches.json');
  const outPath = path.join(dir, 'patched.jsonl');
  writeJsonl(inputPath, [sampleProcessRow()]);
  writeJson(patchPath, {
    patch_status: 'completed',
    patches: [
      {
        row_index: 0,
        operations: [
          {
            op: 'replace',
            path: '/processDataSet/processInformation/dataSetInformation/name/baseName/#text',
            value: 'Unevidenced name',
          },
        ],
      },
    ],
  });

  try {
    const report = await runDatasetPatchApply({
      inputPath,
      patchPath,
      outPath,
      now: new Date('2026-06-02T00:00:00.000Z'),
    });

    assert.equal(report.status, 'blocked');
    assert.equal(report.blockers[0]?.code, 'patch_evidence_required');
    assert.equal(report.applied_operation_count, 0);
    const rows = readJsonl(outPath);
    assert.equal(
      (
        rows[0] as {
          processDataSet: {
            processInformation: { dataSetInformation: { name: { baseName: { '#text': string } } } };
          };
        }
      ).processDataSet.processInformation.dataSetInformation.name.baseName['#text'],
      'Old process name',
    );
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('runDatasetPatchApply can require authoring package lineage and action item closure', async () => {
  const dir = mkdtempSync(path.join(os.tmpdir(), 'tg-cli-dataset-patch-authoring-'));
  const inputPath = path.join(dir, 'processes.jsonl');
  const patchPath = path.join(dir, 'patches.json');
  const outPath = path.join(dir, 'patched.jsonl');
  const packageDir = path.join(dir, 'authoring-packages');
  mkdirSync(packageDir, { recursive: true });
  writeJsonl(inputPath, [sampleProcessRow()]);
  writeJson(path.join(packageDir, 'proc-1.authoring-package.json'), {
    schema_version: 2,
    entity_id: 'proc-1',
    version: '00.00.001',
    action_items: [
      {
        code: 'process_missing_functional_unit',
        path: '/processDataSet/processInformation/dataSetInformation/quantitativeReference/functionalUnitOrOther',
        ai_required: true,
      },
      {
        code: 'identity_preflight_manual_review',
        action_kind: 'identity_decision_authoring',
        ai_required: true,
      },
      {
        code: 'process_classification_requires_authoring',
        action_kind: 'classification_decision_authoring',
        ai_required: true,
      },
    ],
  });
  writeJson(patchPath, {
    patch_status: 'completed',
    patches: [
      {
        dataset_id: 'proc-1',
        version: '00.00.001',
        authoring_package: 'proc-1.authoring-package.json',
        operations: [
          {
            op: 'add',
            path: '/processDataSet/processInformation/dataSetInformation/quantitativeReference',
            value: {
              functionalUnitOrOther: [{ '@xml:lang': 'en', '#text': '1 kg source product' }],
            },
            basis: 'Source package states the quantitative reference.',
            evidence: { source: 'authoring_package', quote: '1 kg source product' },
            closes_action_items: [
              {
                code: 'process_missing_functional_unit',
                path: '/processDataSet/processInformation/dataSetInformation/quantitativeReference/functionalUnitOrOther',
              },
            ],
          },
        ],
      },
    ],
  });

  try {
    const report = await runDatasetPatchApply({
      inputPath,
      patchPath,
      outPath,
      authoringPackageDir: packageDir,
      requireAuthoringPackage: true,
      requireActionItemClosure: true,
      now: new Date('2026-06-02T00:00:00.000Z'),
    });

    assert.equal(report.status, 'completed');
    assert.equal(report.closed_action_item_count, 1);
    const evidence = readJsonl(report.files.patch_evidence ?? '') as Array<{
      authoring_package_sha256?: string;
      closes_action_items?: Array<{ code: string }>;
    }>;
    assert.equal(evidence[0]?.closes_action_items?.[0]?.code, 'process_missing_functional_unit');
    assert.equal(typeof evidence[0]?.authoring_package_sha256, 'string');
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('runDatasetPatchApply blocks strict authoring patches that do not close package action items', async () => {
  const dir = mkdtempSync(path.join(os.tmpdir(), 'tg-cli-dataset-patch-unclosed-'));
  const inputPath = path.join(dir, 'processes.jsonl');
  const patchPath = path.join(dir, 'patches.json');
  const outPath = path.join(dir, 'patched.jsonl');
  const packageDir = path.join(dir, 'authoring-packages');
  mkdirSync(packageDir, { recursive: true });
  writeJsonl(inputPath, [sampleProcessRow()]);
  writeJson(path.join(packageDir, 'proc-1.authoring-package.json'), {
    entity_id: 'proc-1',
    version: '00.00.001',
    action_items: [{ code: 'process_missing_functional_unit', ai_required: true }],
  });
  writeJson(patchPath, {
    patch_status: 'completed',
    patches: [
      {
        row_index: 0,
        authoring_package: 'proc-1.authoring-package.json',
        operations: [
          {
            op: 'replace',
            path: '/processDataSet/processInformation/dataSetInformation/name/baseName/#text',
            value: 'Still not closing FU',
            basis: 'Evidence exists for the name only.',
          },
        ],
      },
    ],
  });

  try {
    const report = await runDatasetPatchApply({
      inputPath,
      patchPath,
      outPath,
      authoringPackageDir: packageDir,
      requireActionItemClosure: true,
      now: new Date('2026-06-02T00:00:00.000Z'),
    });

    assert.equal(report.status, 'blocked');
    assert.ok(report.blockers.some((blocker) => blocker.code === 'authoring_action_item_unclosed'));
    assert.equal(report.applied_operation_count, 0);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('executeCli routes dataset patch apply and maps blockers to exit code one', async () => {
  const help = await executeCli(['dataset', 'patch', 'apply', '--help'], makeDeps());
  assert.equal(help.exitCode, 0);
  assert.match(help.stdout, /dataset patch apply/u);

  const result = await executeCli(
    [
      'dataset',
      'patch',
      'apply',
      '--json',
      '--input',
      'rows.jsonl',
      '--patch',
      'patches.json',
      '--out',
      'patched.jsonl',
      '--out-dir',
      'patch-out',
      '--authoring-package-dir',
      'packages',
      '--require-authoring-package',
      '--require-action-item-closure',
    ],
    {
      ...makeDeps(),
      runDatasetPatchApplyImpl: async (options): Promise<DatasetPatchApplyReport> => {
        assert.equal(options.inputPath, 'rows.jsonl');
        assert.equal(options.patchPath, 'patches.json');
        assert.equal(options.outPath, 'patched.jsonl');
        assert.equal(options.outDir, 'patch-out');
        assert.equal(options.authoringPackageDir, 'packages');
        assert.equal(options.requireAuthoringPackage, true);
        assert.equal(options.requireActionItemClosure, true);
        return {
          schema_version: 1,
          generated_at_utc: '2026-06-02T00:00:00.000Z',
          input_path: options.inputPath,
          patch_path: options.patchPath,
          out_path: options.outPath,
          status: 'blocked',
          row_count: 1,
          patch_count: 1,
          operation_count: 1,
          applied_operation_count: 0,
          evidence_count: 0,
          closed_action_item_count: 0,
          blockers: [{ code: 'patch_test_failed', message: 'Patch test failed.' }],
          files: {
            patched_rows: options.outPath,
            patch_evidence: null,
            report: null,
          },
        };
      },
    },
  );

  assert.equal(result.exitCode, 1);
  assert.equal(JSON.parse(result.stdout).status, 'blocked');

  const invalidAction = await executeCli(['dataset', 'patch', 'plan'], makeDeps());
  assert.equal(invalidAction.exitCode, 2);
  assert.match(invalidAction.stderr, /DATASET_PATCH_ACTION_INVALID/u);
});
