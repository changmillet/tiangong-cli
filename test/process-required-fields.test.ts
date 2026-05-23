import assert from 'node:assert/strict';
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import {
  __testInternals,
  collectProcessRequiredFieldIssues,
  runProcessRequiredFieldsComplete,
} from '../src/lib/process-required-fields.js';

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

function processRow(overrides: Record<string, unknown> = {}) {
  return {
    id: 'proc-1',
    version: '01.01.000',
    json_ordered: {
      processDataSet: {
        processInformation: {
          quantitativeReference: {
            referenceToReferenceFlow: '5',
          },
        },
        exchanges: {
          exchange: [
            {
              '@dataSetInternalID': '5',
              exchangeDirection: 'Output',
              meanAmount: '3.6',
              referenceToFlowDataSet: {
                'common:shortDescription': [{ '@xml:lang': 'en', '#text': 'Net calorific value' }],
              },
            },
          ],
        },
        modellingAndValidation: {
          dataSourcesTreatmentAndRepresentativeness: {},
        },
      },
    },
    ...overrides,
  };
}

function annualSupplyFrom(row: unknown) {
  return (
    row as {
      json_ordered: {
        processDataSet: {
          modellingAndValidation: {
            dataSourcesTreatmentAndRepresentativeness: {
              annualSupplyOrProductionVolume: Array<{ '#text': string; '@xml:lang': string }>;
            };
          };
        };
      };
    }
  ).json_ordered.processDataSet.modellingAndValidation.dataSourcesTreatmentAndRepresentativeness
    .annualSupplyOrProductionVolume;
}

test('runProcessRequiredFieldsComplete completes annual volume from reference exchange amount', async () => {
  const dir = mkdtempSync(path.join(os.tmpdir(), 'tg-cli-process-required-fields-'));
  const inputPath = path.join(dir, 'processes.jsonl');
  const outPath = path.join(dir, 'completed.jsonl');
  const outDir = path.join(dir, 'artifacts');
  writeJsonl(inputPath, [processRow(), { id: 'flow-1', json_ordered: { flowDataSet: {} } }]);

  try {
    const report = await runProcessRequiredFieldsComplete({
      inputPath,
      outPath,
      outDir,
      defaultUnit: 'kg',
      now: new Date('2026-05-23T00:00:00.000Z'),
    });

    assert.equal(report.status, 'completed');
    assert.deepEqual(report.counts, {
      total: 2,
      processes: 1,
      completed: 1,
      existing: 0,
      blocked: 0,
      skipped: 1,
    });
    assert.equal(existsSync(report.files.output_rows), true);
    assert.deepEqual(readJson(report.files.report ?? ''), report);
    assert.deepEqual(annualSupplyFrom(readJsonl(outPath)[0]), [
      { '@xml:lang': 'en', '#text': '3.6 MJ/year' },
      { '@xml:lang': 'zh', '#text': '3.6 MJ/年' },
    ]);
    const evidenceRows = readJsonl(report.files.evidence ?? '') as Array<{
      source: string;
      reference_exchange_internal_id: string;
    }>;
    assert.equal(evidenceRows.length, 1);
    assert.equal(evidenceRows[0]?.source, 'reference_flow_amount');
    assert.equal(evidenceRows[0]?.reference_exchange_internal_id, '5');
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('runProcessRequiredFieldsComplete prefers explicit evidence values over reference amounts', async () => {
  const dir = mkdtempSync(path.join(os.tmpdir(), 'tg-cli-process-required-fields-evidence-'));
  const inputPath = path.join(dir, 'processes.jsonl');
  const outPath = path.join(dir, 'completed.jsonl');
  writeJsonl(inputPath, [
    processRow({
      evidence_manifest: {
        field_bindings: [
          {
            field_path:
              'processDataSet.modellingAndValidation.dataSourcesTreatmentAndRepresentativeness.annualSupplyOrProductionVolume',
            amount: '125000',
            unit: 'kWh',
          },
        ],
      },
    }),
  ]);

  try {
    const report = await runProcessRequiredFieldsComplete({
      inputPath,
      outPath,
      defaultUnit: 'unit',
    });

    assert.equal(report.status, 'completed');
    assert.equal(report.rows[0]?.completions[0]?.source, 'evidence');
    assert.deepEqual(annualSupplyFrom(readJsonl(outPath)[0]), [
      { '@xml:lang': 'en', '#text': '125000 kWh/year' },
      { '@xml:lang': 'zh', '#text': '125000 kWh/年' },
    ]);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('runProcessRequiredFieldsComplete keeps existing valid values and blocks missing amounts', async () => {
  const existing = processRow();
  const processRoot = (
    existing.json_ordered as {
      processDataSet: {
        modellingAndValidation: {
          dataSourcesTreatmentAndRepresentativeness: Record<string, unknown>;
        };
      };
    }
  ).processDataSet;
  processRoot.modellingAndValidation.dataSourcesTreatmentAndRepresentativeness.annualSupplyOrProductionVolume =
    [
      { '@xml:lang': 'en', '#text': '99 kg/year' },
      { '@xml:lang': 'zh', '#text': '99 kg/年' },
    ];

  const blocked = processRow({ id: 'proc-blocked' });
  (blocked.json_ordered as { processDataSet: { exchanges: unknown } }).processDataSet.exchanges = {
    exchange: [{ '@dataSetInternalID': '5', exchangeDirection: 'Output' }],
  };

  const report = await runProcessRequiredFieldsComplete({
    inputPath: 'memory',
    outPath: path.join(os.tmpdir(), `completed-${process.pid}.jsonl`),
    rawInput: [existing, blocked],
  });

  assert.equal(report.status, 'completed_with_blockers');
  assert.equal(report.counts.existing, 1);
  assert.equal(report.counts.blocked, 1);
  assert.equal(report.rows[0]?.status, 'existing');
  assert.equal(report.rows[1]?.issues.at(-1)?.code, 'annual_supply_reference_amount_missing');
});

test('runProcessRequiredFieldsComplete validates required output flags and resultingAmount fallback', async () => {
  await assert.rejects(
    () =>
      runProcessRequiredFieldsComplete({
        inputPath: 'memory',
        outPath: '   ',
        rawInput: [],
      }),
    /Missing required --out value/u,
  );

  const row = processRow();
  const exchange = ((
    row.json_ordered as {
      processDataSet: { exchanges: { exchange: Array<Record<string, unknown>> } };
    }
  ).processDataSet.exchanges.exchange[0] ?? {}) as Record<string, unknown>;
  delete exchange.meanAmount;
  exchange.resultingAmount = '4.2';
  exchange.unit = 'kg';

  const report = await runProcessRequiredFieldsComplete({
    inputPath: 'memory',
    outPath: path.join(os.tmpdir(), `completed-resulting-${process.pid}.jsonl`),
    rawInput: [row],
  });

  assert.equal(report.status, 'completed');
  assert.equal(report.rows[0]?.completions[0]?.amount, '4.2');
  assert.equal(report.rows[0]?.completions[0]?.unit, 'kg');
});

test('process required field issue collector detects missing and invalid annual volumes', () => {
  assert.deepEqual(
    collectProcessRequiredFieldIssues({
      processDataSet: {
        modellingAndValidation: {
          dataSourcesTreatmentAndRepresentativeness: {},
        },
      },
    }).map((issue) => issue.code),
    ['annual_supply_or_production_volume_missing'],
  );
  assert.deepEqual(
    collectProcessRequiredFieldIssues({
      processDataSet: {
        modellingAndValidation: {
          dataSourcesTreatmentAndRepresentativeness: {
            annualSupplyOrProductionVolume: [{ '@xml:lang': 'en', '#text': 'not quantified' }],
          },
        },
      },
    }).map((issue) => issue.code),
    ['annual_supply_or_production_volume_invalid'],
  );
  assert.equal(
    __testInternals.findAnnualSupplyEvidenceValue(
      {
        required_fields: {
          annualSupplyOrProductionVolume: [{ '@xml:lang': 'en', '#text': '10 t/year' }],
        },
      },
      {},
      { defaultUnit: 'kg' },
    )?.amount,
    '10',
  );
});

test('process required field internals cover evidence normalization and helper fallbacks', () => {
  assert.equal(__testInternals.textValue('   '), null);
  assert.equal(__testInternals.textValue(12), '12');
  assert.equal(__testInternals.textValue(false), null);
  assert.equal(__testInternals.valueAtPath({ a: { b: 1 } }, 'a.b'), 1);
  assert.equal(__testInternals.valueAtPath({ a: 1 }, 'a.b'), undefined);

  assert.equal(__testInternals.annualSupplyTextParts('not quantified'), null);
  assert.deepEqual(__testInternals.annualSupplyTextParts('15 kg/year'), {
    amount: '15',
    unit: 'kg/year',
  });
  assert.equal(__testInternals.annualSupplyValueFromText('not quantified'), null);
  assert.deepEqual(__testInternals.annualSupplyValueFromText('5 kg/year')?.value, [
    { '@xml:lang': 'en', '#text': '5 kg/year' },
    { '@xml:lang': 'zh', '#text': '5 kg/year' },
  ]);
  assert.equal(
    __testInternals.normalizeAnnualSupplyEvidenceValue(6, { defaultUnit: 'MWh' })?.value[0]?.[
      '#text'
    ],
    '6 MWh/year',
  );
  assert.equal(
    __testInternals.normalizeAnnualSupplyEvidenceValue('bad', { defaultUnit: 'kg' }),
    null,
  );
  assert.equal(
    __testInternals.normalizeAnnualSupplyEvidenceValue(
      [{ '@xml:lang': 'en', '#text': '7 t/year' }],
      { defaultUnit: 'kg' },
    )?.amount,
    '7',
  );
  assert.equal(
    __testInternals.normalizeAnnualSupplyEvidenceValue(
      { value: { amount: '8', unit: 'm3' } },
      { defaultUnit: 'kg' },
    )?.unit,
    'm3',
  );
  assert.equal(
    __testInternals.normalizeAnnualSupplyEvidenceValue(
      { en: '9 kg/year', zh: '9 kg/年' },
      { defaultUnit: 'kg' },
    )?.amount,
    '9',
  );
  assert.equal(
    __testInternals.normalizeAnnualSupplyEvidenceValue(
      { amount: '10', referenceUnit: 'kg' },
      { defaultUnit: 'unit' },
    )?.value[1]?.['#text'],
    '10 kg/年',
  );
  assert.equal(__testInternals.normalizeAnnualSupplyEvidenceValue({}, { defaultUnit: 'kg' }), null);

  assert.equal(__testInternals.isAnnualSupplyEvidencePath(null), false);
  assert.equal(
    __testInternals.isAnnualSupplyEvidencePath(
      'processDataSet.modellingAndValidation.dataSourcesTreatmentAndRepresentativeness.annualSupplyOrProductionVolume',
    ),
    true,
  );
  assert.equal(
    __testInternals.isAnnualSupplyEvidencePath(
      'payload.modellingAndValidation.dataSourcesTreatmentAndRepresentativeness.annualSupplyOrProductionVolume',
    ),
    true,
  );
  assert.equal(__testInternals.fieldPathFromEvidenceEntry({ fieldPath: 'field-a' }), 'field-a');
  assert.equal(
    __testInternals.findAnnualSupplyEvidenceEntry(
      {
        field_path:
          'processDataSet.modellingAndValidation.dataSourcesTreatmentAndRepresentativeness.annualSupplyOrProductionVolume',
        text: '11 kg/year',
      },
      { defaultUnit: 'kg' },
    )?.amount,
    '11',
  );
  assert.equal(__testInternals.findAnnualSupplyEvidenceEntry(null, { defaultUnit: 'kg' }), null);
  assert.equal(
    __testInternals.findAnnualSupplyEvidenceEntry(
      {
        field_bindings: [
          { field_path: 'other', amount: '1', unit: 'kg' },
          {
            path: 'modellingAndValidation.dataSourcesTreatmentAndRepresentativeness.annualSupplyOrProductionVolume',
            amount: '12',
            unit: 'kg',
          },
        ],
      },
      { defaultUnit: 'kg' },
    )?.amount,
    '12',
  );
  assert.equal(
    __testInternals.findAnnualSupplyEvidenceEntry(
      { field_bindings: [{ field_path: 'other' }] },
      {
        defaultUnit: 'kg',
      },
    ),
    null,
  );
});

test('process required field internals cover exchange, unit, and row wrapper fallbacks', () => {
  assert.equal(__testInternals.selectReferenceExchange({}), null);
  assert.deepEqual(
    __testInternals.selectReferenceExchange({
      exchanges: { exchange: { quantitativeReference: true, meanAmount: '1' } },
    }),
    { quantitativeReference: true, meanAmount: '1' },
  );
  assert.deepEqual(
    __testInternals.selectReferenceExchange({
      exchanges: { exchange: [{ exchangeDirection: 'Output', meanAmount: '2' }] },
    }),
    { exchangeDirection: 'Output', meanAmount: '2' },
  );
  assert.equal(
    __testInternals.selectReferenceExchange({
      exchanges: { exchange: [{ meanAmount: '3' }] },
    }),
    null,
  );

  assert.equal(__testInternals.inferUnitFromReferenceExchange({ unit: 'kg' }, 'unit'), 'kg');
  assert.equal(
    __testInternals.inferUnitFromReferenceExchange(
      {
        referenceToFlowDataSet: {
          '@refObjectId': '93a60a56-a3c8-11da-a746-0800200c9a66',
        },
      },
      'unit',
    ),
    'MJ',
  );
  assert.equal(
    __testInternals.inferUnitFromReferenceExchange(
      {
        flowProperty: {
          referenceToFlowPropertyDataSet: {
            '@refObjectId': '93a60a56-a3c8-11da-a746-0800200c9a66',
          },
        },
      },
      'unit',
    ),
    'MJ',
  );
  assert.equal(
    __testInternals.inferUnitFromReferenceExchange(
      {
        flowProperty: {
          referenceToFlowPropertyDataSet: {
            'common:shortDescription': [{ '@xml:lang': 'en', '#text': 'Net calorific value' }],
          },
        },
      },
      'unit',
    ),
    'MJ',
  );
  assert.equal(__testInternals.inferUnitFromReferenceExchange({}, 'unit'), 'unit');

  assert.deepEqual(
    __testInternals.cloneRowWithPayload({
      index: 0,
      id: null,
      version: null,
      kind: 'process',
      row: { jsonOrdered: { processDataSet: { id: 'jsonOrdered' } } },
      payload: {},
    })?.payload,
    { processDataSet: { id: 'jsonOrdered' } },
  );
  assert.deepEqual(
    __testInternals.cloneRowWithPayload({
      index: 0,
      id: null,
      version: null,
      kind: 'process',
      row: { json: { processDataSet: { id: 'json' } } },
      payload: {},
    })?.payload,
    { processDataSet: { id: 'json' } },
  );
  assert.deepEqual(
    __testInternals.cloneRowWithPayload({
      index: 0,
      id: null,
      version: null,
      kind: 'process',
      row: { payload: { processDataSet: { id: 'payload' } } },
      payload: {},
    })?.payload,
    { processDataSet: { id: 'payload' } },
  );
  assert.deepEqual(
    __testInternals.cloneRowWithPayload({
      index: 0,
      id: null,
      version: null,
      kind: 'process',
      row: { process: { processDataSet: { id: 'process' } } },
      payload: {},
    })?.payload,
    { processDataSet: { id: 'process' } },
  );
  assert.deepEqual(
    __testInternals.cloneRowWithPayload({
      index: 0,
      id: null,
      version: null,
      kind: 'process',
      row: { processDataSet: { id: 'root' } },
      payload: {},
    })?.payload,
    { processDataSet: { id: 'root' } },
  );

  const completed = __testInternals.completeProcessRow(
    {
      index: 0,
      id: 'proc-evidence',
      version: '01.00.000',
      kind: 'process',
      row: {
        required_fields: {
          annualSupplyOrProductionVolume: { amount: '13', unit: 'kg' },
        },
        json_ordered: {
          processDataSet: {
            processInformation: {},
            exchanges: {},
          },
        },
      },
      payload: {},
    },
    { defaultUnit: 'unit' },
  );
  assert.equal(completed.report.status, 'completed');
  assert.equal(
    (
      completed.row.json_ordered as {
        processDataSet: {
          modellingAndValidation: {
            dataSourcesTreatmentAndRepresentativeness: {
              annualSupplyOrProductionVolume: Array<{ '#text': string }>;
            };
          };
        };
      }
    ).processDataSet.modellingAndValidation.dataSourcesTreatmentAndRepresentativeness
      .annualSupplyOrProductionVolume[0]?.['#text'],
    '13 kg/year',
  );

  const unwrappedCompleted = __testInternals.completeProcessRow(
    {
      index: 0,
      id: 'proc-unwrapped',
      version: '01.00.000',
      kind: 'process',
      row: {
        annualSupplyOrProductionVolume: { amount: '14', unit: 'kg' },
        modellingAndValidation: {
          dataSourcesTreatmentAndRepresentativeness: {},
        },
      },
      payload: {},
    },
    { defaultUnit: 'unit' },
  );
  assert.equal(
    (
      unwrappedCompleted.row as {
        modellingAndValidation: {
          dataSourcesTreatmentAndRepresentativeness: {
            annualSupplyOrProductionVolume: Array<{ '#text': string }>;
          };
        };
      }
    ).modellingAndValidation.dataSourcesTreatmentAndRepresentativeness
      .annualSupplyOrProductionVolume[0]?.['#text'],
    '14 kg/year',
  );

  const blockedWithoutReferenceExchange = __testInternals.completeProcessRow(
    {
      index: 0,
      id: 'proc-no-exchange',
      version: '01.00.000',
      kind: 'process',
      row: {
        processDataSet: {
          modellingAndValidation: {
            dataSourcesTreatmentAndRepresentativeness: {},
          },
        },
      },
      payload: {},
    },
    { defaultUnit: 'unit' },
  );
  assert.equal(blockedWithoutReferenceExchange.report.status, 'blocked');
  assert.equal(
    blockedWithoutReferenceExchange.report.issues.at(-1)?.code,
    'annual_supply_reference_amount_missing',
  );
});
