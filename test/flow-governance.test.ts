import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { CliError } from '../src/lib/errors.js';
import {
  __testInternals,
  coerceText,
  datasetPayloadFromRow,
  deepGet,
  extractFlowRecord,
  firstLangEntry,
  flowDatasetFromRow,
  listify,
  loadRowsFromFile,
  normalizeText,
} from '../src/lib/flow-governance.js';

test('flow-governance helpers normalize nested values and extract flow identity', () => {
  assert.equal(__testInternals.isRecord({ ok: true }), true);
  assert.equal(__testInternals.isRecord(['nope']), false);
  assert.deepEqual(listify(['a', 'b']), ['a', 'b']);
  assert.deepEqual(listify('single'), ['single']);
  assert.deepEqual(listify(null), []);
  assert.equal(deepGet({ a: { b: 'c' } }, ['a', 'b']), 'c');
  assert.equal(deepGet({ a: {} }, ['a', 'b'], 'fallback'), 'fallback');
  assert.equal(deepGet('not-an-object', ['a'], 'fallback'), 'fallback');
  assert.equal(coerceText(' hello '), 'hello');
  assert.equal(coerceText({ '#text': ' world ' }), 'world');
  assert.equal(coerceText({ nested: { '#text': 'deep value' } }), 'deep value');
  assert.equal(coerceText([{ '#text': 'first' }, { '#text': 'second' }]), 'first');
  assert.equal(coerceText([]), '');
  assert.equal(coerceText(42), '42');
  assert.deepEqual(firstLangEntry({ '@xml:lang': 'zh', '#text': '中文' }), {
    '@xml:lang': 'zh',
    '#text': '中文',
  });
  assert.deepEqual(firstLangEntry({ '#text': 'bare text' }), {
    '@xml:lang': 'en',
    '#text': 'bare text',
  });
  assert.deepEqual(firstLangEntry({ nested: [{ '@xml:lang': 'en', '#text': 'English' }] }), {
    '@xml:lang': 'en',
    '#text': 'English',
  });
  assert.deepEqual(firstLangEntry({ nested: { empty: true } }, 'fallback'), {
    '@xml:lang': 'en',
    '#text': 'fallback',
  });
  assert.deepEqual(firstLangEntry(undefined, 'fallback'), {
    '@xml:lang': 'en',
    '#text': 'fallback',
  });
  assert.equal(firstLangEntry(undefined), null);
  assert.equal(normalizeText(' Waste-water ; CN '), 'waste water cn');

  const row = {
    id: '11111111-1111-1111-1111-111111111111',
    version: '01.00.000',
    json_ordered: {
      flowDataSet: {
        flowInformation: {
          dataSetInformation: {
            'common:UUID': '11111111-1111-1111-1111-111111111111',
            name: {
              baseName: [{ '@xml:lang': 'en', '#text': 'Flow name' }],
            },
            'common:shortDescription': [{ '@xml:lang': 'en', '#text': 'Short desc' }],
          },
        },
        modellingAndValidation: {
          LCIMethodAndAllocation: {
            typeOfDataSet: 'Product flow',
          },
        },
        administrativeInformation: {
          publicationAndOwnership: {
            'common:dataSetVersion': '01.00.000',
          },
        },
      },
    },
  };

  assert.equal(datasetPayloadFromRow(row).flowDataSet !== undefined, true);
  assert.equal(flowDatasetFromRow(row).flowInformation !== undefined, true);
  assert.deepEqual(datasetPayloadFromRow({ json: { ok: true } }), { ok: true });
  assert.deepEqual(datasetPayloadFromRow({ raw: true }), { raw: true });
  assert.deepEqual(flowDatasetFromRow({ raw: true }), { raw: true });

  const record = extractFlowRecord(row);
  assert.equal(record.id, '11111111-1111-1111-1111-111111111111');
  assert.equal(record.version, '01.00.000');
  assert.equal(record.name, 'Flow name');
  assert.equal(record.flowType, 'Product flow');
  assert.deepEqual(record.shortDescription, {
    '@xml:lang': 'en',
    '#text': 'Short desc',
  });

  const fallbackRecord = extractFlowRecord({
    typeOfDataSet: 'Waste flow',
    json: {
      flowInformation: {
        dataSetInformation: {
          'common:UUID': '22222222-2222-2222-2222-222222222222',
          'common:shortDescription': [{ '#text': 'Fallback short description' }],
        },
      },
    },
  });
  assert.equal(fallbackRecord.id, '22222222-2222-2222-2222-222222222222');
  assert.equal(fallbackRecord.version, '01.00.000');
  assert.equal(fallbackRecord.name, 'Fallback short description');
  assert.equal(fallbackRecord.flowType, 'Waste flow');
});

test('flow-governance loadRowsFromFile supports json and jsonl inputs and rejects invalid shapes', () => {
  const dir = mkdtempSync(path.join(os.tmpdir(), 'tg-cli-flow-governance-'));

  try {
    const jsonPath = path.join(dir, 'rows.json');
    const jsonlPath = path.join(dir, 'rows.jsonl');
    const invalidJsonPath = path.join(dir, 'invalid.json');
    const invalidJsonlPath = path.join(dir, 'invalid.jsonl');

    writeFileSync(jsonPath, '[{"id":"row-1"},{"id":"row-2"}]\n', 'utf8');
    writeFileSync(jsonlPath, '{"id":"row-3"}\n{"id":"row-4"}\n', 'utf8');
    writeFileSync(invalidJsonPath, '{"id":"not-an-array"}\n', 'utf8');
    writeFileSync(invalidJsonlPath, '["not-an-object"]\n', 'utf8');

    assert.deepEqual(loadRowsFromFile(jsonPath), [{ id: 'row-1' }, { id: 'row-2' }]);
    assert.deepEqual(loadRowsFromFile(jsonlPath), [{ id: 'row-3' }, { id: 'row-4' }]);

    assert.throws(
      () => loadRowsFromFile(invalidJsonPath),
      (error: unknown) => {
        assert.ok(error instanceof CliError);
        assert.equal(error.code, 'FLOW_ROWS_INVALID_JSON');
        return true;
      },
    );

    assert.throws(
      () => loadRowsFromFile(invalidJsonlPath),
      (error: unknown) => {
        assert.ok(error instanceof CliError);
        assert.equal(error.code, 'FLOW_ROWS_INVALID_JSONL_ROW');
        return true;
      },
    );
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('flow-governance fallback helpers cover empty nested values and dataset-derived fields', () => {
  assert.equal(coerceText({ nested: { blank: '   ' } }), '');
  assert.equal(coerceText(['   ', { nested: { '#text': '   ' } }]), '');
  assert.deepEqual(firstLangEntry([{ empty: true }, { '@xml:lang': 'fr', '#text': 'Bonjour' }]), {
    '@xml:lang': 'fr',
    '#text': 'Bonjour',
  });
  assert.equal(firstLangEntry([{ empty: true }]), null);

  const datasetOnlyRecord = extractFlowRecord({
    flowDataSet: {
      flowInformation: {
        dataSetInformation: {
          'common:UUID': '33333333-3333-3333-3333-333333333333',
          name: 'Dataset fallback name',
        },
      },
      administrativeInformation: {
        publicationAndOwnership: {
          'common:dataSetVersion': '02.00.000',
        },
      },
    },
  });
  assert.equal(datasetOnlyRecord.id, '33333333-3333-3333-3333-333333333333');
  assert.equal(datasetOnlyRecord.version, '02.00.000');
  assert.equal(datasetOnlyRecord.name, 'Dataset fallback name');
  assert.equal(datasetOnlyRecord.flowType, '');
  assert.deepEqual(datasetOnlyRecord.shortDescription, {
    '@xml:lang': 'en',
    '#text': 'Dataset fallback name',
  });

  const nameFallsBackToId = extractFlowRecord({
    flowDataSet: {
      flowInformation: {
        dataSetInformation: {
          'common:UUID': '44444444-4444-4444-4444-444444444444',
        },
      },
    },
  });
  assert.equal(nameFallsBackToId.name, '44444444-4444-4444-4444-444444444444');

  const rowTypeFallbackRecord = extractFlowRecord({
    typeOfDataSet: 'Elementary flow',
    json: {
      flowInformation: {
        dataSetInformation: {
          name: {
            nested: {
              '#text': 'Nested dataset name',
            },
          },
        },
      },
    },
  });
  assert.equal(rowTypeFallbackRecord.id, '');
  assert.equal(rowTypeFallbackRecord.version, '01.00.000');
  assert.equal(rowTypeFallbackRecord.name, 'Nested dataset name');
  assert.equal(rowTypeFallbackRecord.flowType, 'Elementary flow');
  assert.deepEqual(rowTypeFallbackRecord.shortDescription, {
    '@xml:lang': 'en',
    '#text': 'Nested dataset name',
  });
});
