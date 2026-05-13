import assert from 'node:assert/strict';
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import {
  __testInternals,
  runDatasetValidate,
  type RunDatasetValidateOptions,
} from '../src/lib/dataset-validate.js';
import {
  datasetIdentity,
  datasetRoot,
  detectDatasetKind,
  firstNonEmpty,
  materializeDatasetRows,
  readDatasetRowsInput,
  trimToken,
  unwrapDatasetPayload,
} from '../src/lib/dataset-local.js';

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

const schemas = {
  flow: {
    safeParse: (value: unknown) => ({ success: !(value as { invalid?: boolean }).invalid }),
  },
  process: {
    safeParse: (value: unknown) => ({
      success: !(value as { invalid?: boolean }).invalid,
      error: { issues: [{ path: ['invalid'], message: 'marked invalid', code: 'custom' }] },
    }),
  },
  lifecyclemodel: {
    safeParse: (value: unknown) => ({ success: !(value as { invalid?: boolean }).invalid }),
  },
} satisfies RunDatasetValidateOptions['schemas'];

test('runDatasetValidate validates local rows and writes split artifacts', async () => {
  const dir = mkdtempSync(path.join(os.tmpdir(), 'tg-cli-dataset-validate-'));
  const inputPath = path.join(dir, 'rows.jsonl');
  const outDir = path.join(dir, 'out');
  writeJsonl(inputPath, [
    {
      id: 'proc-ok',
      version: '01.01.000',
      json_ordered: { processDataSet: {} },
    },
    {
      id: 'proc-bad',
      version: '01.01.000',
      json_ordered: { processDataSet: {}, invalid: true },
    },
    {
      id: 'flow-ok',
      version: '01.01.000',
      json_ordered: { flowDataSet: {} },
    },
  ]);

  try {
    const report = await runDatasetValidate({
      inputPath,
      outDir,
      type: 'auto',
      schemas,
      now: new Date('2026-05-05T00:00:00.000Z'),
    });

    assert.equal(report.status, 'completed_with_failures');
    assert.deepEqual(report.counts, {
      total: 3,
      valid: 2,
      invalid: 1,
      by_type: {
        flow: 1,
        process: 2,
        lifecyclemodel: 0,
      },
    });
    assert.equal(existsSync(report.files.report ?? ''), true);
    assert.deepEqual(readJson(report.files.report ?? ''), report);
    assert.equal(readJsonl(report.files.valid_rows ?? '').length, 2);
    assert.equal(readJsonl(report.files.invalid_rows ?? '').length, 1);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('dataset local helpers cover input parsing, wrappers, identities, and errors', () => {
  const dir = mkdtempSync(path.join(os.tmpdir(), 'tg-cli-dataset-local-'));
  const jsonPath = path.join(dir, 'rows.json');
  const jsonlPath = path.join(dir, 'rows.jsonl');
  const badJsonlPath = path.join(dir, 'bad.jsonl');
  const primitiveJsonlPath = path.join(dir, 'primitive.jsonl');
  writeFileSync(
    jsonPath,
    JSON.stringify({
      rows: [
        {
          jsonOrdered: {
            flowDataSet: {
              flowInformation: {
                dataSetInformation: { 'common:UUID': 'flow-from-payload' },
              },
              administrativeInformation: {
                publicationAndOwnership: { 'common:dataSetVersion': '01.00.000' },
              },
            },
          },
        },
      ],
    }),
    'utf8',
  );
  writeJsonl(jsonlPath, [
    {
      process: {
        processDataSet: {
          processInformation: {
            dataSetInformation: { 'common:UUID': 'proc-from-wrapper' },
          },
          administrativeInformation: {
            publicationAndOwnership: { 'common:dataSetVersion': '01.02.000' },
          },
        },
      },
    },
    {
      lifecyclemodel: {
        lifeCycleModelDataSet: {
          lifeCycleModelInformation: {
            dataSetInformation: { 'common:UUID': 'lm-from-wrapper' },
          },
          administrativeInformation: {
            publicationAndOwnership: { 'common:dataSetVersion': '01.03.000' },
          },
        },
      },
    },
  ]);
  writeFileSync(badJsonlPath, '{bad-json}\n', 'utf8');
  writeFileSync(primitiveJsonlPath, '1\n', 'utf8');

  try {
    assert.equal(trimToken(123), null);
    assert.equal(trimToken(' value '), 'value');
    assert.equal(firstNonEmpty(null, ' ', 'winner'), 'winner');
    assert.equal(firstNonEmpty(null, undefined, ''), null);

    assert.equal(readDatasetRowsInput(jsonPath).length, 1);
    assert.equal(
      readDatasetRowsInput('memory', { rows: [{ payload: { processDataSet: {} } }] }).length,
      1,
    );
    assert.deepEqual(readDatasetRowsInput('memory', { id: 'single-row' }), [{ id: 'single-row' }]);
    assert.throws(() => readDatasetRowsInput('', []), /Missing required --input value/u);
    assert.throws(
      () => readDatasetRowsInput(path.join(dir, 'missing.jsonl')),
      /Input file not found/u,
    );
    assert.throws(() => readDatasetRowsInput(badJsonlPath), /invalid JSONL/u);
    assert.throws(() => readDatasetRowsInput(primitiveJsonlPath), /Expected JSON object rows/u);
    assert.throws(
      () => readDatasetRowsInput('memory', { rows: [1] }),
      /Expected JSON object rows/u,
    );

    const materialized = materializeDatasetRows(jsonlPath);
    const materializedFlow = materializeDatasetRows(jsonPath);
    assert.equal(materializedFlow[0]?.id, 'flow-from-payload');
    assert.equal(materializedFlow[0]?.version, '01.00.000');
    assert.equal(materialized[0]?.kind, 'process');
    assert.equal(materialized[0]?.id, 'proc-from-wrapper');
    assert.equal(materialized[1]?.kind, 'lifecyclemodel');
    assert.equal(materialized[1]?.version, '01.03.000');
    assert.equal(detectDatasetKind({ flow: {} }), 'flow');
    assert.equal(detectDatasetKind({ process: {} }), 'process');
    assert.equal(detectDatasetKind({ lifecyclemodel: {} }), 'lifecyclemodel');
    assert.equal(detectDatasetKind({ unknown: true }), null);
    assert.deepEqual(datasetIdentity({ id: 'row-id', version: 'row-version' }, {}, null), {
      id: 'row-id',
      version: 'row-version',
    });
    assert.deepEqual(datasetIdentity({}, { flowDataSet: {} }, 'flow'), {
      id: null,
      version: null,
    });
    assert.deepEqual(datasetRoot({ processDataSet: { id: 'wrapped-process' } }, 'process'), {
      id: 'wrapped-process',
    });
    assert.deepEqual(
      datasetRoot({ lifeCycleModelDataSet: { id: 'wrapped-model' } }, 'lifecyclemodel'),
      { id: 'wrapped-model' },
    );
    assert.deepEqual(datasetRoot({ id: 'unwrapped-flow' }, 'flow'), { id: 'unwrapped-flow' });
    assert.deepEqual(datasetRoot({ id: 'unwrapped-process' }, 'process'), {
      id: 'unwrapped-process',
    });
    assert.deepEqual(datasetRoot({ id: 'unwrapped-model' }, 'lifecyclemodel'), {
      id: 'unwrapped-model',
    });
    assert.deepEqual(unwrapDatasetPayload({ json: { flowDataSet: {} } }), { flowDataSet: {} });
    assert.deepEqual(unwrapDatasetPayload({ payload: { processDataSet: {} } }), {
      processDataSet: {},
    });
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('runDatasetValidate covers aliases, unsupported rows, default schemas, and no-output mode', async () => {
  const noOutput = await runDatasetValidate({
    inputPath: 'memory',
    rawInput: [{ id: 'unknown-row' }],
    now: new Date('2026-05-05T00:00:00.000Z'),
  });
  assert.equal(noOutput.status, 'completed_with_failures');
  assert.deepEqual(noOutput.files, { report: null, valid_rows: null, invalid_rows: null });
  assert.equal(noOutput.rows[0]?.issues[0]?.code, 'dataset_type_unknown');

  const aliases = await runDatasetValidate({
    inputPath: 'memory',
    rawInput: [{ json_ordered: { processDataSet: {} } }],
    type: 'processes',
    schemas,
  });
  assert.equal(aliases.requested_type, 'process');
  assert.equal(aliases.rows[0]?.status, 'valid');

  const modelAlias = await runDatasetValidate({
    inputPath: 'memory',
    rawInput: [{ json_ordered: { lifeCycleModelDataSet: {} } }],
    type: 'models',
    schemas,
  });
  assert.equal(modelAlias.requested_type, 'lifecyclemodel');

  const defaultSchema = await runDatasetValidate({
    inputPath: 'memory',
    rawInput: [{ json_ordered: { flowDataSet: {} } }],
    type: 'flow',
  });
  assert.equal(defaultSchema.requested_type, 'flow');
  assert.equal(defaultSchema.rows[0]?.validator?.includes('FlowSchema'), true);

  const fallbackIssue = await runDatasetValidate({
    inputPath: 'memory',
    rawInput: [{ json_ordered: { processDataSet: { invalid: true } } }],
    type: 'process',
    schemas: {
      process: {
        safeParse: () => ({ success: false, error: { issues: [{}] } }),
      },
    },
  });
  assert.deepEqual(fallbackIssue.rows[0]?.issues, [
    { path: '<root>', message: 'Validation failed', code: 'custom' },
  ]);

  const issueLessFailure = await runDatasetValidate({
    inputPath: 'memory',
    rawInput: [{ json_ordered: { processDataSet: { invalid: true } } }],
    type: 'process',
    schemas: {
      process: {
        safeParse: () => ({ success: false }),
      },
    },
  });
  assert.equal(issueLessFailure.rows[0]?.issue_count, 0);

  await assert.rejects(
    () => runDatasetValidate({ inputPath: 'memory', rawInput: [], type: 'bad-type' }),
    /Expected --type/u,
  );

  const originalLifecyclemodelExport = __testInternals.SCHEMA_EXPORTS.lifecyclemodel;
  try {
    __testInternals.SCHEMA_EXPORTS.lifecyclemodel = 'MissingSchemaForTest' as never;
    assert.throws(
      () => __testInternals.schemaForKind('lifecyclemodel', undefined),
      /MissingSchemaForTest is unavailable/u,
    );
  } finally {
    __testInternals.SCHEMA_EXPORTS.lifecyclemodel = originalLifecyclemodelExport;
  }
});
