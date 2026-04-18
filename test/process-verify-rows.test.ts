import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import {
  __testInternals,
  runProcessVerifyRows,
  type ProcessVerifyRowRecord,
} from '../src/lib/process-verify-rows.js';

function lang(text: string, langCode = 'en') {
  return { '@xml:lang': langCode, '#text': text };
}

function readJson(filePath: string): unknown {
  return JSON.parse(readFileSync(filePath, 'utf8'));
}

test('runProcessVerifyRows accepts process list reports and writes invalid-row artifacts', async () => {
  const tempDir = mkdtempSync(path.join(os.tmpdir(), 'tg-cli-process-verify-rows-'));
  try {
    const rowsFile = path.join(tempDir, 'rows.json');
    writeFileSync(
      rowsFile,
      `${JSON.stringify(
        {
          rows: [
            {
              id: 'proc-valid',
              version: '01.00.001',
              process: {
                processDataSet: {
                  processInformation: {
                    dataSetInformation: {
                      'common:UUID': 'proc-valid',
                      name: {
                        baseName: [lang('Valid process')],
                        treatmentStandardsRoutes: [lang('route')],
                        mixAndLocationTypes: [lang('mix')],
                      },
                    },
                  },
                },
              },
            },
            {
              id: 'proc-invalid',
              version: '01.00.002',
              process: {
                test_invalid: true,
                processDataSet: {
                  processInformation: {
                    dataSetInformation: {
                      'common:UUID': 'proc-invalid',
                      name: {
                        baseName: [lang('Invalid process')],
                        treatmentStandardsRoutes: [lang('route')],
                      },
                    },
                  },
                },
              },
            },
          ],
        },
        null,
        2,
      )}\n`,
      'utf8',
    );

    const report = await runProcessVerifyRows({
      rowsFile,
      outDir: tempDir,
      validateProcessPayloadImpl: (payload) =>
        payload.test_invalid === true
          ? {
              ok: false,
              validator: 'test-validator',
              issue_count: 2,
              issues: [
                {
                  path: 'processDataSet.processInformation',
                  message: 'Broken payload',
                  code: 'custom',
                },
                {
                  path: 'processDataSet.exchanges',
                  message: 'Missing exchanges',
                  code: 'custom',
                },
              ],
            }
          : {
              ok: true,
              validator: 'test-validator',
              issue_count: 0,
              issues: [],
            },
    });

    assert.equal(report.status, 'completed_with_invalid_process_rows');
    assert.equal(report.row_count, 2);
    assert.equal(report.invalid_count, 1);
    assert.equal(report.schema_invalid_count, 1);
    assert.equal(report.missing_required_name_field_count, 1);
    assert.deepEqual(report.invalid_rows, [
      {
        id: 'proc-invalid',
        version: '01.00.002',
        row_index: 1,
        missing_required_fields: ['mixAndLocationTypes'],
        schema_issue_count: 2,
      },
    ]);

    const summary = readJson(report.files.summary_json) as {
      invalid_count: number;
      invalid_rows: unknown[];
    };
    assert.equal(summary.invalid_count, 1);
    assert.equal(summary.invalid_rows.length, 1);

    const verificationRows = readFileSync(report.files.verification_jsonl, 'utf8')
      .trim()
      .split('\n')
      .map((line) => JSON.parse(line) as ProcessVerifyRowRecord);
    assert.equal(verificationRows.length, 2);
    assert.equal(verificationRows[0]?.status, 'ok');
    assert.equal(verificationRows[1]?.status, 'invalid');
    assert.deepEqual(verificationRows[1]?.name_summary.missing_required_fields, [
      'mixAndLocationTypes',
    ]);
  } finally {
    rmSync(tempDir, { recursive: true, force: true });
  }
});

test('process verify helper internals cover row loading, payload selection, identities, and name summaries', () => {
  const tempDir = mkdtempSync(path.join(os.tmpdir(), 'tg-cli-process-verify-helper-'));

  try {
    const missingFile = path.join(tempDir, 'missing.jsonl');
    assert.throws(
      () => __testInternals.readRowsFile(missingFile),
      (error: unknown) => {
        assert.equal((error as { code?: string }).code, 'PROCESS_VERIFY_ROWS_FILE_NOT_FOUND');
        return true;
      },
    );

    const invalidJsonl = path.join(tempDir, 'invalid.jsonl');
    writeFileSync(invalidJsonl, '{"ok":true}\n{broken\n', 'utf8');
    assert.throws(
      () => __testInternals.readRowsFile(invalidJsonl),
      (error: unknown) => {
        assert.equal((error as { code?: string }).code, 'PROCESS_VERIFY_ROWS_FILE_INVALID_JSONL');
        return true;
      },
    );

    const invalidJson = path.join(tempDir, 'invalid.json');
    writeFileSync(invalidJson, '{', 'utf8');
    assert.throws(
      () => __testInternals.readRowsFile(invalidJson),
      (error: unknown) => {
        assert.equal((error as { code?: string }).code, 'PROCESS_VERIFY_ROWS_FILE_INVALID_JSON');
        return true;
      },
    );

    const arrayFile = path.join(tempDir, 'rows.json');
    writeFileSync(arrayFile, `${JSON.stringify([{ id: 'proc-array' }])}\n`, 'utf8');
    assert.deepEqual(__testInternals.readRowsFile(arrayFile), [{ id: 'proc-array' }]);

    const wrappedRowsFile = path.join(tempDir, 'wrapped.json');
    writeFileSync(wrappedRowsFile, `${JSON.stringify({ rows: [{ id: 'proc-rows' }] })}\n`, 'utf8');
    assert.deepEqual(__testInternals.readRowsFile(wrappedRowsFile), [{ id: 'proc-rows' }]);

    const singleObjectFile = path.join(tempDir, 'single.json');
    writeFileSync(singleObjectFile, `${JSON.stringify({ id: 'proc-single' })}\n`, 'utf8');
    assert.deepEqual(__testInternals.readRowsFile(singleObjectFile), [{ id: 'proc-single' }]);

    assert.deepEqual(__testInternals.getPayload({ process: { id: 'proc-process' } }), {
      id: 'proc-process',
    });
    assert.deepEqual(__testInternals.getPayload({ json_ordered: { id: 'proc-json-ordered' } }), {
      id: 'proc-json-ordered',
    });
    assert.deepEqual(__testInternals.getPayload({ json: { id: 'proc-json' } }), {
      id: 'proc-json',
    });
    assert.deepEqual(__testInternals.getPayload({ id: 'proc-root' }), { id: 'proc-root' });
    assert.deepEqual(__testInternals.getPayload('not-an-object'), {});

    assert.deepEqual(
      __testInternals.getIdentity({ process_id: 'proc-row', version: '01.00.001' }, {}, 2),
      {
        row_index: 2,
        id: 'proc-row',
        version: '01.00.001',
      },
    );
    assert.deepEqual(
      __testInternals.getIdentity(
        {},
        {
          processDataSet: {
            processInformation: {
              dataSetInformation: {
                'common:UUID': 'proc-payload',
              },
            },
            administrativeInformation: {
              publicationAndOwnership: {
                'common:dataSetVersion': '01.00.002',
              },
            },
          },
        },
        0,
      ),
      {
        row_index: 0,
        id: 'proc-payload',
        version: '01.00.002',
      },
    );
    assert.deepEqual(
      __testInternals.getIdentity(
        {},
        {
          id: 'proc-root',
          '@version': '01.00.003',
        },
        1,
      ),
      {
        row_index: 1,
        id: 'proc-root',
        version: '01.00.003',
      },
    );
    assert.deepEqual(__testInternals.getIdentity('not-an-object', {}, 3), {
      row_index: 3,
      id: null,
      version: null,
    });

    assert.deepEqual(__testInternals.getLangList('Plain English'), [
      { '@xml:lang': 'en', '#text': 'Plain English' },
    ]);
    assert.deepEqual(
      __testInternals.getLangList({
        'common:langString': {
          '@xml:lang': 'zh',
          '#text': '中文',
        },
      }),
      [{ '@xml:lang': 'zh', '#text': '中文' }],
    );
    assert.deepEqual(
      __testInternals.getLangList({
        'common:langString': [
          { '@xml:lang': 'en', '#text': 'English' },
          { '@xml:lang': 'zh', '#text': '中文' },
        ],
      }),
      [
        { '@xml:lang': 'en', '#text': 'English' },
        { '@xml:lang': 'zh', '#text': '中文' },
      ],
    );
    assert.deepEqual(__testInternals.getLangList({ '@xml:lang': 'en', '#text': 'Direct text' }), [
      { '@xml:lang': 'en', '#text': 'Direct text' },
    ]);
    assert.equal(
      __testInternals.getLangText(
        [
          { '@xml:lang': 'en', '#text': 'English' },
          { '@xml:lang': 'zh', '#text': '中文' },
        ],
        'zh',
      ),
      '中文',
    );
    assert.equal(
      __testInternals.getLangText([{ '#text': 'Fallback text' }], 'zh'),
      'Fallback text',
    );
    assert.deepEqual(__testInternals.getLangList({ other: true }), []);

    const summary = __testInternals.summarizeNameFields({
      processDataSet: {
        processInformation: {
          dataSetInformation: {
            name: {
              baseName: [lang('Valid process')],
              functionalUnitFlowProperties: [lang('1 kg output')],
            },
          },
        },
      },
    });
    assert.deepEqual(summary.missing_required_fields, [
      'treatmentStandardsRoutes',
      'mixAndLocationTypes',
    ]);
    assert.equal(summary.fields.baseName.present, true);
    assert.equal(summary.fields.baseName.en, 'Valid process');
    assert.equal(summary.fields.functionalUnitFlowProperties.any, '1 kg output');
    assert.deepEqual(__testInternals.summarizeNameFields({}).missing_required_fields, [
      'baseName',
      'treatmentStandardsRoutes',
      'mixAndLocationTypes',
    ]);
  } finally {
    rmSync(tempDir, { recursive: true, force: true });
  }
});

test('runProcessVerifyRows returns a completed status for fully valid rows and validates required args', async () => {
  const tempDir = mkdtempSync(path.join(os.tmpdir(), 'tg-cli-process-verify-valid-'));

  try {
    const rowsFile = path.join(tempDir, 'rows.json');
    writeFileSync(
      rowsFile,
      `${JSON.stringify({
        id: 'proc-valid',
        version: '01.00.001',
        process: {
          processDataSet: {
            processInformation: {
              dataSetInformation: {
                'common:UUID': 'proc-valid',
                name: {
                  baseName: [lang('Valid process')],
                  treatmentStandardsRoutes: [lang('route')],
                  mixAndLocationTypes: [lang('mix')],
                  functionalUnitFlowProperties: [lang('1 kg output')],
                },
              },
            },
          },
        },
      })}\n`,
      'utf8',
    );

    await assert.rejects(
      () => runProcessVerifyRows({ rowsFile: '', outDir: tempDir }),
      (error: unknown) => {
        assert.equal((error as { code?: string }).code, 'PROCESS_VERIFY_ROWS_FILE_REQUIRED');
        return true;
      },
    );
    await assert.rejects(
      () => runProcessVerifyRows({ rowsFile, outDir: '' }),
      (error: unknown) => {
        assert.equal((error as { code?: string }).code, 'PROCESS_VERIFY_OUT_DIR_REQUIRED');
        return true;
      },
    );

    const report = await runProcessVerifyRows({
      rowsFile,
      outDir: tempDir,
      validateProcessPayloadImpl: () => ({
        ok: true,
        validator: 'test-validator',
        issue_count: 0,
        issues: [],
      }),
    });

    assert.equal(report.status, 'completed_process_row_verification');
    assert.equal(report.invalid_count, 0);
    assert.equal(report.schema_invalid_count, 0);
    assert.equal(report.missing_required_name_field_count, 0);
    assert.deepEqual(report.invalid_rows, []);

    const tidasSdk = await import('@tiangong-lca/tidas-sdk');
    const originalSafeParse = tidasSdk.ProcessSchema.safeParse;
    tidasSdk.ProcessSchema.safeParse = (() =>
      ({
        success: true,
        data: {},
      }) as unknown as ReturnType<typeof originalSafeParse>) as typeof originalSafeParse;

    try {
      const defaultValidationReport = await runProcessVerifyRows({
        rowsFile,
        outDir: tempDir,
      });
      assert.equal(defaultValidationReport.status, 'completed_process_row_verification');
    } finally {
      tidasSdk.ProcessSchema.safeParse = originalSafeParse;
    }
  } finally {
    rmSync(tempDir, { recursive: true, force: true });
  }
});
