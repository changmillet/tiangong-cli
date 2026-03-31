import test from 'node:test';
import assert from 'node:assert/strict';
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { CliError } from '../src/lib/errors.js';
import { __testInternals, runFlowRegenProduct } from '../src/lib/flow-regen-product.js';

type JsonRecord = Record<string, unknown>;

function lang(text: string, langCode = 'en'): JsonRecord {
  return {
    '@xml:lang': langCode,
    '#text': text,
  };
}

function writeJson(filePath: string, value: unknown): void {
  mkdirSync(path.dirname(filePath), { recursive: true });
  writeFileSync(filePath, `${JSON.stringify(value, null, 2)}\n`, 'utf8');
}

function writeJsonl(filePath: string, rows: unknown[]): void {
  mkdirSync(path.dirname(filePath), { recursive: true });
  writeFileSync(
    filePath,
    `${rows.map((row) => JSON.stringify(row)).join('\n')}${rows.length ? '\n' : ''}`,
    'utf8',
  );
}

function readJson(filePath: string): unknown {
  return JSON.parse(readFileSync(filePath, 'utf8')) as unknown;
}

function makeFlowRow(options: {
  id: string;
  version?: string;
  name?: string;
  flowType?: string;
  shortDescription?: unknown;
  baseName?: unknown;
}): JsonRecord {
  const version = options.version ?? '01.00.000';
  const name = options.name ?? options.id;
  return {
    id: options.id,
    version,
    typeOfDataSet: options.flowType ?? 'Product flow',
    json_ordered: {
      flowDataSet: {
        flowInformation: {
          dataSetInformation: {
            'common:UUID': options.id,
            name: {
              baseName: options.baseName ?? [lang(name)],
            },
            'common:shortDescription': options.shortDescription ?? [lang(`${name} short`)],
          },
        },
        modellingAndValidation: {
          LCIMethodAndAllocation: {
            typeOfDataSet: options.flowType ?? 'Product flow',
          },
        },
        administrativeInformation: {
          publicationAndOwnership: {
            'common:dataSetVersion': version,
          },
        },
      },
    },
  };
}

function makeExchange(
  options: {
    internalId?: string;
    direction?: string;
    flowId?: string;
    flowVersion?: string;
    flowText?: unknown;
    flowRef?: unknown;
    exchangeName?: string;
  } = {},
): JsonRecord {
  const exchange: JsonRecord = {
    '@dataSetInternalID': options.internalId ?? '1',
    exchangeDirection: options.direction ?? 'Output',
  };

  if (options.exchangeName) {
    exchange.exchangeName = options.exchangeName;
  }

  if (options.flowRef !== null) {
    exchange.referenceToFlowDataSet = options.flowRef ?? {
      '@type': 'flow data set',
      '@refObjectId': options.flowId ?? '',
      '@version': options.flowVersion ?? '',
      '@uri': '../flows/example.xml',
      'common:shortDescription':
        options.flowText === undefined ? [lang('Flow text')] : options.flowText,
    };
  }

  return exchange;
}

function makeProcessDataset(options: {
  id?: string;
  version?: string;
  name?: string;
  exchanges?: JsonRecord[];
  qref?: string;
  functionalUnit?: unknown;
  baseName?: unknown;
  includeDatasetId?: boolean;
  includeVersion?: boolean;
}): JsonRecord {
  const exchanges = options.exchanges ?? [];
  const qref = options.qref ?? String(exchanges[0]?.['@dataSetInternalID'] ?? '1');
  const dataSetInformation: JsonRecord = {
    name: {
      baseName: options.baseName ?? [lang(options.name ?? options.id ?? 'process')],
    },
  };

  if (options.includeDatasetId !== false && options.id !== undefined) {
    dataSetInformation['common:UUID'] = options.id;
  }

  const processDataSet: JsonRecord = {
    processInformation: {
      dataSetInformation,
      quantitativeReference: {
        referenceToReferenceFlow: qref,
        functionalUnitOrOther: options.functionalUnit ?? [],
      },
    },
    exchanges: {
      exchange: exchanges,
    },
  };

  if (options.includeVersion !== false) {
    processDataSet.administrativeInformation = {
      publicationAndOwnership: {
        'common:dataSetVersion': options.version ?? '01.00.000',
      },
    };
  }

  return processDataSet;
}

function makeProcessRow(options: {
  id?: string;
  version?: string;
  name?: string;
  exchanges?: JsonRecord[];
  qref?: string;
  functionalUnit?: unknown;
  carrier?: 'json_ordered' | 'json' | 'row';
  includeOuterIdentity?: boolean;
  includeDatasetId?: boolean;
  includeVersion?: boolean;
  baseName?: unknown;
}): JsonRecord {
  const carrier = options.carrier ?? 'json_ordered';
  const processDataSet = makeProcessDataset({
    id: options.id,
    version: options.version,
    name: options.name,
    exchanges: options.exchanges,
    qref: options.qref,
    functionalUnit: options.functionalUnit,
    includeDatasetId: options.includeDatasetId,
    includeVersion: options.includeVersion,
    baseName: options.baseName,
  });

  if (carrier === 'row') {
    return processDataSet;
  }

  const row: JsonRecord = {};
  if (options.includeOuterIdentity !== false && options.id !== undefined) {
    row.id = options.id;
  }
  if (options.includeOuterIdentity !== false && options.version !== undefined) {
    row.version = options.version;
  }

  row[carrier] = {
    processDataSet,
  };
  return row;
}

function clone<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T;
}

test('flow regen-product helper assertions and sdk resolution cover local file and loader branches', () => {
  const dir = mkdtempSync(path.join(os.tmpdir(), 'tg-cli-flow-regen-product-helpers-'));
  const objectFile = path.join(dir, 'object.json');
  const arrayFile = path.join(dir, 'array.json');
  const invalidFile = path.join(dir, 'invalid.json');
  const jsonlFile = path.join(dir, 'rows.jsonl');
  const outDir = path.join(dir, 'out');

  writeJson(objectFile, { ok: true });
  writeJson(arrayFile, []);
  writeFileSync(invalidFile, '{\n', 'utf8');
  writeJsonl(jsonlFile, []);

  try {
    assert.equal(
      __testInternals.assertInputFile(objectFile, 'FLOW_REQ', 'FLOW_MISSING'),
      path.resolve(objectFile),
    );
    assert.deepEqual(
      __testInternals.assertInputFiles([objectFile, jsonlFile], 'FLOW_REQ', 'FLOW_MISSING'),
      [path.resolve(objectFile), path.resolve(jsonlFile)],
    );
    assert.equal(__testInternals.assertOutDir(outDir), path.resolve(outDir));
    assert.deepEqual(
      __testInternals.readJsonObjectFile(
        objectFile,
        'FLOW_REGEN_ALIAS_MAP_REQUIRED',
        'FLOW_REGEN_ALIAS_MAP_NOT_FOUND',
        'FLOW_REGEN_ALIAS_MAP_INVALID',
      ),
      { ok: true },
    );
    assert.match(__testInternals.resolveCliRepoRoot(), /tiangong-lca-cli/u);
    assert.equal(__testInternals.buildSdkCandidates()[0], '@tiangong-lca/tidas-sdk/core');

    assert.throws(
      () => __testInternals.assertInputFile('', 'FLOW_REQ', 'FLOW_MISSING'),
      (error: unknown) => error instanceof CliError && error.code === 'FLOW_REQ',
    );
    assert.throws(
      () =>
        __testInternals.assertInputFile(path.join(dir, 'missing.json'), 'FLOW_REQ', 'FLOW_MISSING'),
      (error: unknown) => error instanceof CliError && error.code === 'FLOW_MISSING',
    );
    assert.throws(
      () => __testInternals.assertInputFiles([], 'FLOW_REQ', 'FLOW_MISSING'),
      (error: unknown) => error instanceof CliError && error.code === 'FLOW_REQ',
    );
    assert.throws(
      () => __testInternals.assertOutDir(''),
      (error: unknown) => error instanceof CliError && error.code === 'FLOW_REGEN_OUT_DIR_REQUIRED',
    );
    assert.throws(
      () =>
        __testInternals.readJsonObjectFile(
          invalidFile,
          'FLOW_REGEN_ALIAS_MAP_REQUIRED',
          'FLOW_REGEN_ALIAS_MAP_NOT_FOUND',
          'FLOW_REGEN_ALIAS_MAP_INVALID',
        ),
      (error: unknown) =>
        error instanceof CliError && error.code === 'FLOW_REGEN_ALIAS_MAP_INVALID',
    );
    assert.throws(
      () =>
        __testInternals.readJsonObjectFile(
          arrayFile,
          'FLOW_REGEN_ALIAS_MAP_REQUIRED',
          'FLOW_REGEN_ALIAS_MAP_NOT_FOUND',
          'FLOW_REGEN_ALIAS_MAP_INVALID',
        ),
      (error: unknown) =>
        error instanceof CliError && error.code === 'FLOW_REGEN_ALIAS_MAP_INVALID',
    );

    const successRequire = ((candidate: string) => {
      if (candidate === 'bad') {
        throw new Error('missing module');
      }
      if (candidate === 'missing-create') {
        return {};
      }
      return {
        createProcess: () => ({
          validate: () => ({ success: true }),
        }),
      };
    }) as unknown as NodeJS.Require;

    const loaded = __testInternals.resolveSdkModuleFromCandidates(successRequire, [
      'bad',
      'missing-create',
      'good',
    ]);
    assert.equal(loaded.location, 'good');
    assert.equal(typeof loaded.createProcess, 'function');

    assert.throws(
      () =>
        __testInternals.resolveSdkModuleFromCandidates(
          ((candidate: string) => {
            throw new Error(`cannot load ${candidate}`);
          }) as unknown as NodeJS.Require,
          ['broken'],
        ),
      (error: unknown) =>
        error instanceof CliError && error.code === 'FLOW_REGEN_PROCESS_SDK_NOT_FOUND',
    );

    try {
      const ambientModule = __testInternals.resolveLocalSdkModule();
      assert.equal(typeof ambientModule.location, 'string');
    } catch (error) {
      assert.ok(error instanceof CliError);
      assert.equal(error.code, 'FLOW_REGEN_PROCESS_SDK_NOT_FOUND');
    }
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('runFlowRegenProduct rejects missing scope flow inputs before execution', async () => {
  const dir = mkdtempSync(path.join(os.tmpdir(), 'tg-cli-flow-regen-product-missing-scope-'));
  const processesFile = path.join(dir, 'processes.jsonl');

  writeJsonl(processesFile, []);

  try {
    await assert.rejects(
      () =>
        runFlowRegenProduct({
          processesFile,
          scopeFlowFiles: undefined as unknown as string[],
          outDir: path.join(dir, 'artifacts'),
        }),
      (error: unknown) =>
        error instanceof CliError && error.code === 'FLOW_REGEN_SCOPE_FLOW_FILES_REQUIRED',
    );
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('flow regen-product process and flow helper utilities cover identity extraction, references, names, and emergy detection', () => {
  const datasetRow = makeProcessRow({
    id: 'proc-1',
    version: '01.02.000',
    name: 'Process One',
    exchanges: [
      makeExchange({
        internalId: '2',
        direction: 'Input',
        flowId: 'flow-input',
        flowVersion: '01.00.000',
        flowText: [lang('Input flow')],
      }),
      makeExchange({
        internalId: '1',
        direction: 'Output',
        flowId: 'flow-output',
        flowVersion: '01.00.000',
        flowText: [lang('Output flow')],
      }),
    ],
    qref: '1',
    carrier: 'json_ordered',
  });
  const jsonRow = makeProcessRow({
    id: 'proc-2',
    version: '01.00.000',
    name: 'Process Two',
    carrier: 'json',
  });
  const rawRow = makeProcessRow({
    id: 'proc-3',
    version: '01.00.000',
    name: 'Process Three',
    carrier: 'row',
  });
  const fallbackIdentityRow = {
    json: {
      processDataSet: makeProcessDataset({
        id: 'proc-4',
        version: '02.00.000',
        name: 'Fallback Process',
        exchanges: [],
      }),
    },
  } satisfies JsonRecord;
  const defaultVersionRow = {
    json: {
      processDataSet: makeProcessDataset({
        id: 'proc-5',
        name: 'Default Version Process',
        exchanges: [],
        includeVersion: false,
      }),
    },
  } satisfies JsonRecord;
  const exactRefRow = clone(datasetRow);
  const outputFallbackRow = makeProcessRow({
    id: 'proc-output',
    version: '01.00.000',
    exchanges: [
      makeExchange({
        internalId: 'a',
        direction: 'Input',
        flowId: 'flow-a',
        flowVersion: '01.00.000',
      }),
      makeExchange({
        internalId: 'b',
        direction: 'Output',
        flowId: 'flow-b',
        flowVersion: '01.00.000',
      }),
    ],
    qref: 'missing',
  });
  const firstFallbackRow = makeProcessRow({
    id: 'proc-first',
    version: '01.00.000',
    exchanges: [
      makeExchange({
        internalId: 'x',
        direction: 'Input',
        flowId: 'flow-x',
        flowVersion: '01.00.000',
      }),
    ],
    qref: 'missing',
  });
  const nonRecordReferenceRow = makeProcessRow({
    id: 'proc-non-record-ref',
    version: '01.00.000',
    exchanges: [
      makeExchange({
        internalId: '1',
        direction: 'Output',
        flowRef: 'free-text-reference',
      }),
    ],
  });
  const noExchangeRow = makeProcessRow({
    id: 'proc-empty',
    version: '01.00.000',
    exchanges: [],
  });

  const emergyFlow = makeFlowRow({
    id: 'flow-emergy',
    version: '01.00.000',
    name: 'Solar emergy flow',
    shortDescription: [lang('Solar emergy flow')],
  });
  const plainFlow = makeFlowRow({
    id: 'flow-plain',
    version: '01.00.000',
    name: 'Normal grid electricity',
    shortDescription: [lang('Normal grid electricity')],
  });
  const sameUuidFallbackFlow = makeFlowRow({
    id: 'flow-same',
    version: '01.00.000',
    name: 'Solar emergy fallback flow',
    shortDescription: [lang('Solar emergy fallback flow')],
  });
  const index = __testInternals.buildFlowIndex([
    makeFlowRow({
      id: 'flow-grid',
      version: '01.00.000',
      name: 'Grid electricity',
      baseName: [lang('Grid electricity')],
    }),
    makeFlowRow({
      id: 'flow-grid',
      version: '01.01.000',
      name: 'Grid electricity',
      baseName: [lang('Grid electricity')],
    }),
    makeFlowRow({
      id: 'flow-steam',
      version: '01.00.000',
      name: 'Steam',
      baseName: [lang('Steam')],
    }),
    emergyFlow,
    plainFlow,
    sameUuidFallbackFlow,
  ]);

  assert.deepEqual(
    __testInternals.processDatasetFromRow(datasetRow),
    (datasetRow.json_ordered as JsonRecord).processDataSet,
  );
  assert.deepEqual(
    __testInternals.processDatasetFromRow(jsonRow),
    (jsonRow.json as JsonRecord).processDataSet,
  );
  assert.deepEqual(__testInternals.processDatasetFromRow(rawRow), rawRow);

  assert.deepEqual(__testInternals.extractProcessIdentity(datasetRow), {
    id: 'proc-1',
    version: '01.02.000',
    name: 'Process One',
  });
  assert.deepEqual(__testInternals.extractProcessIdentity(fallbackIdentityRow), {
    id: 'proc-4',
    version: '02.00.000',
    name: 'Fallback Process',
  });
  assert.deepEqual(__testInternals.extractProcessIdentity(defaultVersionRow), {
    id: 'proc-5',
    version: '01.00.000',
    name: 'Default Version Process',
  });

  assert.equal(__testInternals.exchangeRecords(datasetRow).length, 2);
  assert.equal(
    (__testInternals.extractProcessReferenceExchange(exactRefRow) as JsonRecord)[
      '@dataSetInternalID'
    ],
    '1',
  );
  assert.equal(
    (__testInternals.extractProcessReferenceExchange(outputFallbackRow) as JsonRecord)[
      '@dataSetInternalID'
    ],
    'b',
  );
  assert.equal(
    (__testInternals.extractProcessReferenceExchange(firstFallbackRow) as JsonRecord)[
      '@dataSetInternalID'
    ],
    'x',
  );
  assert.equal(__testInternals.extractProcessReferenceExchange(noExchangeRow), null);

  assert.equal(__testInternals.extractReferenceText(' raw text '), 'raw text');
  assert.equal(
    __testInternals.extractReferenceText({
      'common:shortDescription': [lang('Structured text')],
    }),
    'Structured text',
  );
  assert.deepEqual(__testInternals.extractProcessReferenceFlowRef(datasetRow), {
    flow_id: 'flow-output',
    flow_version: '01.00.000',
    flow_text: 'Output flow',
    exchange_internal_id: '1',
  });
  assert.deepEqual(__testInternals.extractProcessReferenceFlowRef(noExchangeRow), {
    flow_id: '',
    flow_version: '',
    flow_text: '',
    exchange_internal_id: '',
  });
  assert.deepEqual(__testInternals.extractProcessReferenceFlowRef(nonRecordReferenceRow), {
    flow_id: '',
    flow_version: '',
    flow_text: '',
    exchange_internal_id: '1',
  });
  assert.equal(__testInternals.processRowKey(datasetRow), 'proc-1@01.02.000');
  assert.equal(__testInternals.processRowKey({}), '');
  assert.deepEqual(__testInternals.versionKey('01.10.beta'), [1, 10, 0]);
  assert.equal(__testInternals.compareVersionKeys([1, 2], [1, 1]), 1);
  assert.equal(__testInternals.compareVersionKeys([1], [1, 0, 1]) < 0, true);
  assert.equal(__testInternals.compareVersionKeys([1, 1], [1]) > 0, true);
  assert.equal(
    __testInternals.processRowSortComparator(
      makeProcessRow({ id: 'proc-a', version: '01.00.000', name: 'A' }),
      makeProcessRow({ id: 'proc-b', version: '01.00.000', name: 'B' }),
    ) < 0,
    true,
  );
  assert.equal(
    __testInternals.processRowSortComparator(
      makeProcessRow({ id: 'proc-a', version: '01.00.000', name: 'A' }),
      makeProcessRow({ id: 'proc-a', version: '02.00.000', name: 'A' }),
    ) < 0,
    true,
  );
  assert.equal(
    __testInternals.processRowSortComparator(
      makeProcessRow({ id: 'proc-a', version: '01.00.000', name: 'Alpha' }),
      makeProcessRow({ id: 'proc-a', version: '01.00.000', name: 'Beta' }),
    ) < 0,
    true,
  );

  assert.equal(index.byUuid['flow-grid'].length, 2);
  assert.equal(index.byUuidVersion['flow-grid@01.01.000'].name, 'Grid electricity');
  assert.equal(index.byName.grid_electricity, undefined);
  assert.equal(index.byName['grid electricity'].length, 2);
  const aliasMap = {
    'old-flow@01.00.000': { id: 'flow-grid', version: '01.01.000' },
    'old-flow': { id: 'flow-steam', version: '01.00.000' },
  } satisfies JsonRecord;
  assert.deepEqual(
    __testInternals.aliasLookup(aliasMap, 'old-flow', '01.00.000'),
    aliasMap['old-flow@01.00.000'],
  );
  assert.deepEqual(
    __testInternals.aliasLookup(aliasMap, 'old-flow', '02.00.000'),
    aliasMap['old-flow'],
  );
  assert.equal(__testInternals.aliasLookup(aliasMap, 'missing-flow', null), null);

  assert.equal(
    __testInternals.buildLocalDatasetUri('flow data set', 'flow-grid', '01.01.000'),
    '../flows/flow-grid_01.01.000.xml',
  );
  assert.equal(
    __testInternals.buildLocalDatasetUri('dataset', 'item', ''),
    '../datasets/item_01.00.000.xml',
  );
  assert.equal(__testInternals.buildLocalDatasetUri('flow', '', '01.00.000'), '');

  assert.deepEqual(
    __testInternals.preserveShortDescriptionShape([lang('Old', 'zh')], lang('New')),
    [lang('New')],
  );
  const preservedArrayNoLang = __testInternals.preserveShortDescriptionShape(
    [{ '@xml:lang': 'zh', '#text': '旧' }],
    { '#text': 'No lang target' },
  ) as JsonRecord[];
  assert.equal(preservedArrayNoLang[0]['@xml:lang'], 'zh');
  const preservedArrayNoLangAnywhere = __testInternals.preserveShortDescriptionShape(
    [{ '#text': '旧' }],
    { '#text': 'No lang anywhere' },
  ) as JsonRecord[];
  assert.equal(preservedArrayNoLangAnywhere[0]['@xml:lang'], 'en');
  const preservedArrayDefaultLang = __testInternals.preserveShortDescriptionShape(
    [{ '#text': '旧' }],
    { '#text': 'Array target without lang' },
  ) as JsonRecord[];
  assert.equal(preservedArrayDefaultLang[0]['@xml:lang'], 'en');
  assert.deepEqual(__testInternals.preserveShortDescriptionShape([], lang('Next')), [lang('Next')]);
  assert.deepEqual(
    __testInternals.preserveShortDescriptionShape(
      { '@xml:lang': 'zh', '#text': '旧' },
      lang('Updated'),
    ),
    lang('Updated'),
  );
  const preservedRecordNoLang = __testInternals.preserveShortDescriptionShape(
    { '#text': '旧' },
    { '#text': 'Record target without lang' },
  ) as JsonRecord;
  assert.equal(preservedRecordNoLang['@xml:lang'], 'en');
  assert.deepEqual(
    __testInternals.preserveShortDescriptionShape(undefined, lang('Fallback')),
    lang('Fallback'),
  );

  const flowRecord = index.byUuidVersion['flow-steam@01.00.000'];
  assert.deepEqual(__testInternals.flowReferenceFromRecord(flowRecord), {
    '@type': 'flow data set',
    '@refObjectId': 'flow-steam',
    '@version': '01.00.000',
    '@uri': '../flows/flow-steam_01.00.000.xml',
    'common:shortDescription': lang('Steam short'),
  });
  assert.deepEqual(
    __testInternals.flowReferenceFromRecord({
      id: 'flow-no-short',
      version: '01.00.000',
      name: 'No short description',
      flowType: 'Product flow',
      shortDescription: null,
      row: {},
    }),
    {
      '@type': 'flow data set',
      '@refObjectId': 'flow-no-short',
      '@version': '01.00.000',
      '@uri': '../flows/flow-no-short_01.00.000.xml',
      'common:shortDescription': lang('No short description'),
    },
  );
  assert.deepEqual(
    __testInternals.patchedFlowReference(
      {
        '@type': '',
        'common:shortDescription': [],
      },
      flowRecord,
    ),
    {
      '@type': 'flow data set',
      '@refObjectId': 'flow-steam',
      '@version': '01.00.000',
      '@uri': '../flows/flow-steam_01.00.000.xml',
      'common:shortDescription': [lang('Steam short')],
    },
  );
  assert.deepEqual(__testInternals.patchedFlowReference(null, flowRecord), {
    '@type': 'flow data set',
    '@refObjectId': 'flow-steam',
    '@version': '01.00.000',
    '@uri': '../flows/flow-steam_01.00.000.xml',
    'common:shortDescription': lang('Steam short'),
  });

  assert.deepEqual(
    __testInternals.langEntries([lang('One'), { nested: { label: lang('Two', 'zh') } }]),
    [
      { lang: 'en', text: 'One' },
      { lang: 'zh', text: 'Two' },
    ],
  );
  assert.deepEqual(__testInternals.langEntries({ '#text': 'No language entry' }), [
    { lang: 'en', text: 'No language entry' },
  ]);
  assert.deepEqual(__testInternals.langEntries('plain-text'), []);
  assert.equal(__testInternals.textHasEmergyKeyword('Solar emergy accounting'), true);
  assert.equal(__testInternals.textHasEmergyKeyword(''), false);
  assert.deepEqual(__testInternals.uniqueNonEmptyTexts(['  Alpha ', 'alpha', '', 'Beta']), [
    'Alpha',
    'Beta',
  ]);

  const flowDecision = __testInternals.flowEmergyScopeDecision(emergyFlow);
  assert.equal(flowDecision.excluded, true);
  assert.match(String((flowDecision.signals as string[])[0]), /emergy_name/u);
  const plainDecision = __testInternals.flowEmergyScopeDecision(plainFlow);
  assert.equal(plainDecision.excluded, false);

  const processViaFlow = makeProcessRow({
    id: 'proc-emergy-flow',
    version: '01.00.000',
    exchanges: [
      makeExchange({
        internalId: '1',
        direction: 'Output',
        flowId: 'flow-emergy',
        flowVersion: '01.00.000',
        flowText: [lang('Solar emergy flow')],
      }),
    ],
  });
  const processViaSameUuidFallback = makeProcessRow({
    id: 'proc-emergy-same-uuid',
    version: '01.00.000',
    exchanges: [
      makeExchange({
        internalId: '1',
        direction: 'Output',
        flowId: 'flow-same',
        flowVersion: '',
        flowText: [lang('Unknown version')],
      }),
    ],
  });
  const processViaFunctionalUnit = makeProcessRow({
    id: 'proc-emergy-unit',
    version: '01.00.000',
    exchanges: [
      makeExchange({
        internalId: '1',
        direction: 'Output',
        flowId: 'missing-flow',
        flowVersion: '01.00.000',
        flowText: [lang('Regular flow')],
      }),
    ],
    functionalUnit: [lang('按能值核算')],
  });
  const safeProcess = makeProcessRow({
    id: 'proc-safe',
    version: '01.00.000',
    exchanges: [
      makeExchange({
        internalId: '1',
        direction: 'Output',
        flowId: 'flow-plain',
        flowVersion: '01.00.000',
        flowText: [lang('Normal grid electricity')],
      }),
    ],
  });

  assert.equal(__testInternals.processEmergyScopeDecision(processViaFlow, index).excluded, true);
  assert.equal(
    __testInternals.processEmergyScopeDecision(processViaSameUuidFallback, index).excluded,
    true,
  );
  assert.equal(
    __testInternals.processEmergyScopeDecision(processViaSameUuidFallback, {
      records: [],
      byUuidVersion: {},
      byUuid: {
        'flow-same': [undefined as unknown as never],
      },
      byName: {},
    }).excluded,
    false,
  );
  assert.equal(
    __testInternals.processEmergyScopeDecision(processViaFunctionalUnit, index).excluded,
    true,
  );
  assert.equal(__testInternals.processEmergyScopeDecision(safeProcess, index).excluded, false);

  const filtered = __testInternals.filterEmergyNamedProcesses([processViaFlow, safeProcess], index);
  assert.equal(filtered.keptRows.length, 1);
  assert.equal(filtered.excludedRows.length, 1);
});

test('flow regen-product scan and repair helpers cover classification, repair planning, diffing, and path utilities', () => {
  const scopeIndex = __testInternals.buildFlowIndex([
    makeFlowRow({ id: 'flow-target', version: '02.00.000', name: 'Target flow' }),
    makeFlowRow({ id: 'flow-broken', version: '02.00.000', name: 'Broken target flow' }),
    makeFlowRow({ id: 'flow-unique', version: '01.00.000', name: 'Unique exact name' }),
    makeFlowRow({ id: 'flow-amb-1', version: '01.00.000', name: 'Ambiguous exact name' }),
    makeFlowRow({ id: 'flow-amb-2', version: '02.00.000', name: 'Ambiguous exact name' }),
  ]);
  const catalogIndex = __testInternals.buildFlowIndex([
    makeFlowRow({ id: 'flow-catalog', version: '01.00.000', name: 'Catalog flow' }),
  ]);
  const aliasMap = {
    'flow-broken': { id: 'flow-target', version: '02.00.000' },
    'old-flow@01.00.000': { id: 'flow-target', version: '02.00.000' },
  } satisfies JsonRecord;
  const processRow = makeProcessRow({
    id: 'proc-scan',
    version: '01.00.000',
    name: 'Scan process',
  });

  const noReference = __testInternals.classifyExchangeRef(
    processRow,
    makeExchange({ internalId: '1', flowRef: null }),
    scopeIndex,
    catalogIndex,
    aliasMap,
  );
  assert.equal(noReference.issue_type, 'no_reference');

  const versionMissing = __testInternals.classifyExchangeRef(
    processRow,
    makeExchange({
      internalId: '2',
      flowId: 'flow-broken',
      flowVersion: '',
      flowText: [lang('Broken target flow')],
    }),
    scopeIndex,
    catalogIndex,
    aliasMap,
  );
  assert.equal(versionMissing.issue_type, 'version_missing');
  assert.deepEqual(versionMissing.evidence, {
    alias_target: { id: 'flow-target', version: '02.00.000' },
  });

  const existsOutsideTarget = __testInternals.classifyExchangeRef(
    processRow,
    makeExchange({
      internalId: '3',
      flowId: 'flow-catalog',
      flowVersion: '',
      flowText: [lang('Catalog flow')],
    }),
    scopeIndex,
    catalogIndex,
    aliasMap,
  );
  assert.equal(existsOutsideTarget.issue_type, 'exists_outside_target');

  const missingUuidWithoutVersion = __testInternals.classifyExchangeRef(
    processRow,
    makeExchange({
      internalId: '4',
      flowId: 'flow-missing',
      flowVersion: '',
    }),
    scopeIndex,
    catalogIndex,
    aliasMap,
  );
  assert.equal(missingUuidWithoutVersion.issue_type, 'missing_uuid');

  const existsInTarget = __testInternals.classifyExchangeRef(
    processRow,
    makeExchange({
      internalId: '5',
      flowId: 'flow-target',
      flowVersion: '02.00.000',
      flowText: [lang('Target flow')],
    }),
    scopeIndex,
    catalogIndex,
    aliasMap,
  );
  assert.equal(existsInTarget.issue_type, 'exists_in_target');
  assert.deepEqual(existsInTarget.evidence, {
    scope_group: '',
    flow_name: 'Target flow',
  });

  const brokenVersion = __testInternals.classifyExchangeRef(
    processRow,
    makeExchange({
      internalId: '6',
      flowId: 'flow-broken',
      flowVersion: '99.00.000',
      flowText: [lang('Broken target flow')],
    }),
    scopeIndex,
    catalogIndex,
    aliasMap,
  );
  assert.equal(brokenVersion.issue_type, 'broken_version');
  assert.deepEqual(brokenVersion.evidence, {
    available_versions_in_target: ['02.00.000'],
  });

  const versionedOutsideTarget = __testInternals.classifyExchangeRef(
    processRow,
    makeExchange({
      internalId: '7',
      flowId: 'flow-catalog',
      flowVersion: '01.00.000',
      flowText: [lang('Catalog flow')],
    }),
    scopeIndex,
    catalogIndex,
    aliasMap,
  );
  assert.equal(versionedOutsideTarget.issue_type, 'exists_outside_target');

  const versionedOutsideTargetByUuid = __testInternals.classifyExchangeRef(
    processRow,
    makeExchange({
      internalId: '7b',
      flowId: 'flow-catalog',
      flowVersion: '02.00.000',
      flowText: [lang('Catalog flow')],
    }),
    scopeIndex,
    catalogIndex,
    aliasMap,
  );
  assert.equal(versionedOutsideTargetByUuid.issue_type, 'exists_outside_target');

  const missingUuidWithVersion = __testInternals.classifyExchangeRef(
    processRow,
    makeExchange({
      internalId: '8',
      flowId: 'flow-nowhere',
      flowVersion: '01.00.000',
    }),
    scopeIndex,
    catalogIndex,
    aliasMap,
  );
  assert.equal(missingUuidWithVersion.issue_type, 'missing_uuid');

  const findingMap = __testInternals.buildFindingMap([noReference, existsInTarget]);
  assert.deepEqual(Object.keys(findingMap).sort(), [
    'proc-scan@01.00.000::1',
    'proc-scan@01.00.000::5',
  ]);

  const keepAction = __testInternals.planExchangeRepair(
    makeExchange({
      internalId: 'keep',
      flowId: 'flow-target',
      flowVersion: '02.00.000',
      flowText: [lang('Target flow')],
    }),
    scopeIndex,
    aliasMap,
    existsInTarget,
    'alias-only',
  );
  assert.equal(keepAction.decision, 'keep_as_is');
  assert.equal(keepAction.reason, 'already_in_target');

  const aliasAutoPatch = __testInternals.planExchangeRepair(
    makeExchange({
      internalId: 'alias-auto',
      flowId: 'old-flow',
      flowVersion: '01.00.000',
      flowText: [lang('Old flow')],
    }),
    scopeIndex,
    aliasMap,
    null,
    'alias-only',
  );
  assert.equal(aliasAutoPatch.decision, 'auto_patch');
  assert.equal(aliasAutoPatch.reason, 'direct_alias_map');

  const aliasManual = __testInternals.planExchangeRepair(
    makeExchange({
      internalId: 'alias-manual',
      flowId: 'old-flow',
      flowVersion: '01.00.000',
      flowText: [lang('Old flow')],
    }),
    scopeIndex,
    aliasMap,
    null,
    'disabled',
  );
  assert.equal(aliasManual.decision, 'manual_review');
  assert.equal(aliasManual.reason, 'alias_target_found_but_policy_disallows_auto_patch');

  const uniqueAuto = __testInternals.planExchangeRepair(
    makeExchange({
      internalId: 'unique-auto',
      flowId: 'missing-id',
      flowVersion: '01.00.000',
      flowText: [lang('Unique exact name')],
    }),
    scopeIndex,
    {},
    null,
    'alias-or-unique-name',
  );
  assert.equal(uniqueAuto.decision, 'auto_patch');
  assert.equal(uniqueAuto.reason, 'unique_exact_name_match');

  const uniqueManual = __testInternals.planExchangeRepair(
    makeExchange({
      internalId: 'unique-manual',
      flowId: 'missing-id',
      flowVersion: '01.00.000',
      flowText: [lang('Unique exact name')],
    }),
    scopeIndex,
    {},
    null,
    'alias-only',
  );
  assert.equal(uniqueManual.decision, 'manual_review');
  assert.equal(uniqueManual.reason, 'unique_exact_name_match_blocked_by_policy');

  const exchangeNameFallback = __testInternals.planExchangeRepair(
    makeExchange({
      internalId: 'exchange-name-fallback',
      flowRef: 'non-record-reference',
      exchangeName: 'Unique exact name',
    }),
    scopeIndex,
    {},
    null,
    'alias-or-unique-name',
  );
  assert.equal(exchangeNameFallback.decision, 'auto_patch');
  assert.equal(exchangeNameFallback.reason, 'unique_exact_name_match');

  const exchangeObjectNameFallback = makeExchange({
    internalId: 'exchange-object-name-fallback',
    flowRef: 'non-record-reference',
  });
  exchangeObjectNameFallback.name = 'Unique exact name';
  const exchangeObjectNameAction = __testInternals.planExchangeRepair(
    exchangeObjectNameFallback,
    scopeIndex,
    {},
    null,
    'alias-or-unique-name',
  );
  assert.equal(exchangeObjectNameAction.decision, 'auto_patch');
  assert.equal(exchangeObjectNameAction.reason, 'unique_exact_name_match');

  const exchangeObjectNameFromNameField = __testInternals.planExchangeRepair(
    {
      '@dataSetInternalID': 'exchange-object-name-fallback',
      exchangeDirection: 'Output',
      referenceToFlowDataSet: 'non-record-reference',
      name: { '#text': 'Unique exact name' },
    } as unknown as JsonRecord,
    scopeIndex,
    {},
    null,
    'alias-or-unique-name',
  );
  assert.equal(exchangeObjectNameFromNameField.decision, 'auto_patch');
  assert.equal(exchangeObjectNameFromNameField.reason, 'unique_exact_name_match');

  const ambiguous = __testInternals.planExchangeRepair(
    makeExchange({
      internalId: 'ambiguous',
      flowId: 'missing-id',
      flowVersion: '01.00.000',
      flowText: [lang('Ambiguous exact name')],
    }),
    scopeIndex,
    {},
    null,
    'alias-or-unique-name',
  );
  assert.equal(ambiguous.decision, 'manual_review');
  assert.equal(ambiguous.reason, 'ambiguous_exact_name_match');
  assert.equal(ambiguous.candidate_count, 2);

  const noMatch = __testInternals.planExchangeRepair(
    makeExchange({
      internalId: 'no-match',
      flowId: 'missing-id',
      flowVersion: '01.00.000',
      flowText: [lang('No deterministic match')],
    }),
    scopeIndex,
    {},
    null,
    'alias-only',
  );
  assert.equal(noMatch.decision, 'manual_review');
  assert.equal(noMatch.reason, 'no_deterministic_match');

  assert.equal(
    __testInternals.buildUnifiedJsonDiff({ a: 1 }, { a: 1 }),
    '--- before.json\n+++ after.json\n',
  );
  const changedDiff = __testInternals.buildUnifiedJsonDiff({ a: 1 }, { a: 2, b: true });
  assert.match(changedDiff, /@@ -2,1 \+2,2 @@/u);
  assert.match(changedDiff, /\+\s+"b": true/u);

  assert.deepEqual(__testInternals.deepDiffPaths({ a: 1 }, { a: 1 }), []);
  assert.deepEqual(__testInternals.deepDiffPaths({ a: 1 }, [1]), [[]]);
  assert.deepEqual(__testInternals.deepDiffPaths([1], [1, 2]), [[1]]);
  assert.deepEqual(__testInternals.deepDiffPaths({ a: 1 }, { a: 2 }), [['a']]);
  const objectDiff = __testInternals
    .deepDiffPaths({ a: { b: 1 } }, { a: { c: 1 } })
    .map((parts: Array<string | number>) => parts.join('.'));
  assert.deepEqual(objectDiff, ['a.b', 'a.c']);
  assert.equal(
    __testInternals.pathContainsReferenceToFlow(['a', 'referenceToFlowDataSet', 'b']),
    true,
  );
  assert.equal(__testInternals.pathContainsReferenceToFlow(['a', 'b']), false);
  assert.equal(__testInternals.safeProcessKey('proc:1', '01/00/000'), 'proc:1__01_00_000');
  assert.equal(__testInternals.safeProcessKey('proc', ''), 'proc__unknown');
});

test('flow regen-product merge, file-writing, and validator helper utilities cover remaining branches', () => {
  const dir = mkdtempSync(path.join(os.tmpdir(), 'tg-cli-flow-regen-product-merge-'));
  const jsonFile = path.join(dir, 'pool.json');
  const jsonlFile = path.join(dir, 'pool.jsonl');
  const existingRows = [
    makeProcessRow({
      id: 'proc-b',
      version: '01.00.000',
      name: 'Process B',
      exchanges: [
        makeExchange({ internalId: '1', flowId: 'flow-target', flowVersion: '02.00.000' }),
      ],
    }),
    makeProcessRow({
      id: 'proc-a',
      version: '01.01.000',
      name: 'Process A old',
      exchanges: [
        makeExchange({ internalId: '1', flowId: 'flow-target', flowVersion: '02.00.000' }),
      ],
    }),
  ];
  const incomingRows = [
    makeProcessRow({
      id: 'proc-a',
      version: '01.01.000',
      name: 'Process A new',
      exchanges: [
        makeExchange({ internalId: '1', flowId: 'flow-target', flowVersion: '02.00.000' }),
      ],
    }),
    makeProcessRow({
      id: 'proc-b',
      version: '01.00.000',
      name: 'Process B',
      exchanges: [
        makeExchange({ internalId: '1', flowId: 'flow-target', flowVersion: '02.00.000' }),
      ],
    }),
    makeProcessRow({
      id: 'proc-c',
      version: '01.00.000',
      name: 'Process C',
      exchanges: [
        makeExchange({ internalId: '1', flowId: 'flow-target', flowVersion: '02.00.000' }),
      ],
    }),
    {},
  ];

  writeJson(jsonFile, existingRows);

  try {
    const merged = __testInternals.mergeRowsByIdentity(existingRows, incomingRows);
    assert.equal(merged.counts.inserted, 1);
    assert.equal(merged.counts.updated, 1);
    assert.equal(merged.counts.unchanged, 1);
    assert.equal(merged.counts.skipped_invalid, 1);

    __testInternals.writeRowsFile(jsonFile, existingRows);
    __testInternals.writeRowsFile(jsonlFile, existingRows);
    assert.ok(existsSync(jsonFile));
    assert.ok(existsSync(jsonlFile));

    const syncedJson = __testInternals.syncProcessPoolFile(jsonFile, incomingRows.slice(0, 3));
    assert.equal(syncedJson.updated, 1);
    assert.equal(syncedJson.inserted, 1);
    assert.equal(syncedJson.pool_post_count, 3);
    assert.deepEqual(
      (readJson(jsonFile) as JsonRecord[]).map((row) => row.id),
      ['proc-a', 'proc-b', 'proc-c'],
    );

    const syncedJsonl = __testInternals.syncProcessPoolFile(
      path.join(dir, 'new-pool.jsonl'),
      incomingRows.slice(0, 1),
    );
    assert.equal(syncedJsonl.pool_pre_count, 0);
    assert.equal(syncedJsonl.inserted, 1);

    const skippedValidator = __testInternals.resolveProcessValidator('skip', {});
    assert.equal(skippedValidator.tidasValidation, false);

    const ambientValidator = __testInternals.resolveProcessValidator('auto', {});
    assert.equal(typeof ambientValidator.tidasValidation, 'boolean');

    const autoSuccess = __testInternals.resolveProcessValidator('auto', {
      loadSdkModule: () => ({
        createProcess: () => ({
          validate: () => ({ success: true }),
        }),
      }),
    });
    assert.equal(typeof autoSuccess.createProcess, 'function');
    assert.equal(autoSuccess.tidasValidation, true);

    const autoInvalid = __testInternals.resolveProcessValidator('auto', {
      loadSdkModule: () => ({
        location: 'mock-sdk',
      }),
    });
    assert.equal(autoInvalid.createProcess, null);
    assert.equal(autoInvalid.tidasValidation, false);

    const autoThrown = __testInternals.resolveProcessValidator('auto', {
      loadSdkModule: () => {
        throw new Error('boom');
      },
    });
    assert.equal(autoThrown.createProcess, null);
    assert.equal(autoThrown.tidasValidation, false);

    assert.throws(
      () =>
        __testInternals.resolveProcessValidator('required', {
          loadSdkModule: () => ({
            location: 'broken-sdk',
          }),
        }),
      (error: unknown) =>
        error instanceof CliError && error.code === 'FLOW_REGEN_PROCESS_SDK_INVALID',
    );
    assert.throws(
      () =>
        __testInternals.resolveProcessValidator('required', {
          loadSdkModule: () => ({}),
        }),
      (error: unknown) =>
        error instanceof CliError &&
        error.code === 'FLOW_REGEN_PROCESS_SDK_INVALID' &&
        error.details === null,
    );
    assert.throws(
      () =>
        __testInternals.resolveProcessValidator('required', {
          loadSdkModule: () => {
            throw new Error('sdk missing');
          },
        }),
      /sdk missing/u,
    );

    assert.equal(__testInternals.formatValidationDetails(null), 'Validator returned no result.');
    assert.equal(
      __testInternals.formatValidationDetails(new Error('validator failed')),
      'validator failed',
    );
    assert.equal(__testInternals.formatValidationDetails('plain failure'), 'plain failure');
    assert.equal(__testInternals.formatValidationDetails({ ok: false }), '{"ok":false}');
    const circular: JsonRecord = {};
    circular.self = circular;
    assert.match(__testInternals.formatValidationDetails(circular), /\[object Object\]/u);

    const payload = makeProcessDataset({
      id: 'proc-validate',
      version: '01.00.000',
      exchanges: [],
    });
    assert.equal(
      __testInternals.evaluateProcessSdkValidation(payload, () => ({
        validateEnhanced: () => ({ success: true }),
      })),
      null,
    );
    assert.equal(
      __testInternals.evaluateProcessSdkValidation(payload, () => ({
        validate: () => ({ success: false, reason: 'bad' }),
      })),
      '{"success":false,"reason":"bad"}',
    );
    assert.equal(
      __testInternals.evaluateProcessSdkValidation(payload, () => ({})),
      'Validator returned no result.',
    );
    assert.equal(
      __testInternals.evaluateProcessSdkValidation(payload, () => {
        throw new Error('kaboom');
      }),
      'kaboom',
    );

    const directValidate = __testInternals.runValidateStage({
      originalRows: [],
      patchedRows: [
        makeProcessRow({
          id: 'proc-missing',
          version: '01.00.000',
          exchanges: [
            makeExchange({
              internalId: '1',
              flowId: 'flow-target',
              flowVersion: '02.00.000',
            }),
          ],
        }),
        {},
      ],
      scopeIndex: __testInternals.buildFlowIndex([
        makeFlowRow({ id: 'flow-target', version: '02.00.000', name: 'Target flow' }),
      ]),
      outDir: path.join(dir, 'direct-validate'),
      tidasMode: 'skip',
      deps: {},
    });
    assert.equal(directValidate.summary.failed, 2);
    assert.equal(directValidate.summary.tidas_validation, false);

    const directRepair = __testInternals.runRepairStage({
      processes: [
        makeProcessRow({
          id: 'proc-no-finding',
          version: '01.00.000',
          exchanges: [
            makeExchange({
              internalId: '1',
              flowId: 'missing-flow',
              flowVersion: '01.00.000',
              flowText: [lang('Missing flow')],
            }),
          ],
        }),
      ],
      scopeIndex: __testInternals.buildFlowIndex([
        makeFlowRow({ id: 'flow-target', version: '02.00.000', name: 'Target flow' }),
      ]),
      aliasMap: {},
      scanFindings: [],
      autoPatchPolicy: 'alias-only',
      outDir: path.join(dir, 'direct-repair'),
      apply: false,
      processPoolFile: null,
    });
    assert.equal(directRepair.manualQueue.length, 1);
    assert.equal(directRepair.plan[0]?.current_issue_type, undefined);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('flow regen-product validateProcessPatch reports structural, scope, and tidas failures', () => {
  const scopeIndex = __testInternals.buildFlowIndex([
    makeFlowRow({ id: 'flow-target', version: '02.00.000', name: 'Target flow' }),
  ]);
  const originalRow = makeProcessRow({
    id: 'proc-validate',
    version: '01.00.000',
    name: 'Validate process',
    exchanges: [
      makeExchange({
        internalId: '1',
        direction: 'Output',
        flowId: 'flow-target',
        flowVersion: '02.00.000',
        flowText: [lang('Target flow')],
      }),
    ],
    qref: '1',
  });

  const okResult = __testInternals.validateProcessPatch(
    originalRow,
    clone(originalRow),
    scopeIndex,
    null,
  );
  assert.equal(okResult.ok, true);

  const missingOriginal = __testInternals.validateProcessPatch(
    null,
    clone(originalRow),
    scopeIndex,
    null,
  );
  assert.equal(missingOriginal.ok, false);
  assert.equal(missingOriginal.issues[0].type, 'missing_original_row');

  const changedNameRow = clone(originalRow);
  (
    ((changedNameRow.json_ordered as JsonRecord).processDataSet as JsonRecord)
      .processInformation as JsonRecord
  ).dataSetInformation = {
    'common:UUID': 'proc-validate',
    name: { baseName: [lang('Changed name')] },
  };
  const nonReferenceChanges = __testInternals.validateProcessPatch(
    originalRow,
    changedNameRow,
    scopeIndex,
    null,
  );
  assert.equal(nonReferenceChanges.ok, false);
  assert.equal(nonReferenceChanges.issues[0].type, 'non_reference_changes_detected');

  const changedQrefRow = clone(originalRow);
  (
    (
      ((changedQrefRow.json_ordered as JsonRecord).processDataSet as JsonRecord)
        .processInformation as JsonRecord
    ).quantitativeReference as JsonRecord
  ).referenceToReferenceFlow = '2';
  const changedQref = __testInternals.validateProcessPatch(
    originalRow,
    changedQrefRow,
    scopeIndex,
    null,
  );
  assert.equal(
    changedQref.issues.some((issue) => issue.type === 'quantitative_reference_changed'),
    true,
  );

  const changedExchangeCountRow = clone(originalRow);
  ((
    ((changedExchangeCountRow.json_ordered as JsonRecord).processDataSet as JsonRecord)
      .exchanges as JsonRecord
  ).exchange as JsonRecord[]) = [];
  const changedExchangeCount = __testInternals.validateProcessPatch(
    originalRow,
    changedExchangeCountRow,
    scopeIndex,
    null,
  );
  assert.equal(
    changedExchangeCount.issues.some((issue) => issue.type === 'exchange_count_changed'),
    true,
  );

  const missingFlowReferenceRow = clone(originalRow);
  (
    (
      (
        ((missingFlowReferenceRow.json_ordered as JsonRecord).processDataSet as JsonRecord)
          .exchanges as JsonRecord
      ).exchange as JsonRecord[]
    )[0].referenceToFlowDataSet as JsonRecord
  )['@version'] = '';
  const missingFlowReference = __testInternals.validateProcessPatch(
    originalRow,
    missingFlowReferenceRow,
    scopeIndex,
    null,
  );
  assert.equal(
    missingFlowReference.issues.some(
      (issue) => issue.type === 'missing_flow_reference_after_patch',
    ),
    true,
  );

  const nonRecordFlowReferenceRow = clone(originalRow);
  (
    (
      ((nonRecordFlowReferenceRow.json_ordered as JsonRecord).processDataSet as JsonRecord)
        .exchanges as JsonRecord
    ).exchange as JsonRecord[]
  )[0].referenceToFlowDataSet = 'not-a-record';
  const nonRecordFlowReference = __testInternals.validateProcessPatch(
    originalRow,
    nonRecordFlowReferenceRow,
    scopeIndex,
    null,
  );
  assert.equal(
    nonRecordFlowReference.issues.some(
      (issue) => issue.type === 'missing_flow_reference_after_patch',
    ),
    true,
  );

  const nonObjectReferenceRow = clone(originalRow);
  (
    (
      (
        ((nonObjectReferenceRow.json_ordered as JsonRecord).processDataSet as JsonRecord)
          .exchanges as JsonRecord
      ).exchange as JsonRecord[]
    )[0] as JsonRecord
  ).referenceToFlowDataSet = null;
  const nonObjectReference = __testInternals.validateProcessPatch(
    originalRow,
    nonObjectReferenceRow,
    scopeIndex,
    null,
  );
  assert.equal(
    nonObjectReference.issues.some((issue) => issue.type === 'missing_flow_reference_after_patch'),
    true,
  );

  const outsideScopeRow = clone(originalRow);
  (
    (
      (
        ((outsideScopeRow.json_ordered as JsonRecord).processDataSet as JsonRecord)
          .exchanges as JsonRecord
      ).exchange as JsonRecord[]
    )[0].referenceToFlowDataSet as JsonRecord
  )['@refObjectId'] = 'flow-elsewhere';
  const outsideScope = __testInternals.validateProcessPatch(
    originalRow,
    outsideScopeRow,
    scopeIndex,
    null,
  );
  assert.equal(
    outsideScope.issues.some((issue) => issue.type === 'patched_reference_not_in_scope_catalog'),
    true,
  );

  const tidasFailed = __testInternals.validateProcessPatch(
    originalRow,
    clone(originalRow),
    scopeIndex,
    () => ({
      validate: () => ({ success: false, reason: 'bad schema' }),
    }),
  );
  assert.equal(
    tidasFailed.issues.some((issue) => issue.type === 'tidas_validation_failed'),
    true,
  );
});

test('runFlowRegenProduct plan mode writes scan and repair artifacts with emergy exclusion', async () => {
  const dir = mkdtempSync(path.join(os.tmpdir(), 'tg-cli-flow-regen-product-plan-'));
  const processesFile = path.join(dir, 'processes.jsonl');
  const scopeFile = path.join(dir, 'scope.json');
  const catalogFile = path.join(dir, 'catalog.jsonl');
  const outDir = path.join(dir, 'artifacts');

  writeJson(scopeFile, [
    makeFlowRow({ id: 'flow-target', version: '02.00.000', name: 'Target flow' }),
  ]);
  writeJsonl(catalogFile, [
    makeFlowRow({ id: 'flow-target', version: '02.00.000', name: 'Target flow' }),
    makeFlowRow({ id: 'flow-emergy', version: '01.00.000', name: 'Solar emergy flow' }),
    makeFlowRow({ id: 'flow-catalog', version: '01.00.000', name: 'Catalog flow' }),
  ]);
  writeJsonl(processesFile, [
    makeProcessRow({
      id: 'proc-emergy',
      version: '01.00.000',
      name: 'Emergy process',
      exchanges: [
        makeExchange({
          internalId: '1',
          direction: 'Output',
          flowId: 'flow-emergy',
          flowVersion: '01.00.000',
          flowText: [lang('Solar emergy flow')],
        }),
      ],
    }),
    makeProcessRow({
      id: 'proc-plan',
      version: '01.00.000',
      name: 'Plannable process',
      exchanges: [
        makeExchange({
          internalId: '1',
          direction: 'Output',
          flowId: 'flow-target',
          flowVersion: '02.00.000',
          flowText: [lang('Target flow')],
        }),
        makeExchange({
          internalId: '2',
          direction: 'Input',
          flowId: 'flow-catalog',
          flowVersion: '',
          flowText: [lang('Catalog flow')],
        }),
      ],
      qref: '1',
    }),
  ]);

  try {
    const report = await runFlowRegenProduct(
      {
        processesFile,
        scopeFlowFiles: [scopeFile],
        catalogFlowFiles: [catalogFile],
        outDir,
        excludeEmergy: true,
      },
      {
        now: () => new Date('2026-03-30T12:00:00.000Z'),
      },
    );

    assert.equal(report.generated_at_utc, '2026-03-30T12:00:00.000Z');
    assert.equal(report.mode, 'plan');
    assert.equal(report.alias_map_file, null);
    assert.equal(report.catalog_flow_files[0], path.resolve(catalogFile));
    assert.equal(report.counts.process_count_before_emergy_exclusion, 2);
    assert.equal(report.counts.process_count, 1);
    assert.equal(report.counts.emergy_excluded_process_count, 1);
    assert.equal(report.counts.exchange_count, 2);
    assert.equal(report.counts.repair_item_count, 2);
    assert.equal(report.counts.decision_counts.keep_as_is, 1);
    assert.equal(report.counts.decision_counts.manual_review, 1);
    assert.equal(report.validation.enabled, false);
    assert.equal(report.validation.ok, null);
    assert.equal(report.files.apply, null);
    assert.equal(report.files.validate, null);
    assert.ok(existsSync(report.files.report));
    assert.ok(existsSync(report.files.scan.findings_jsonl));
    assert.ok(existsSync(report.files.repair.manual_review_queue));

    const scanSummary = readJson(report.files.scan.summary) as JsonRecord;
    assert.deepEqual(scanSummary.issue_counts, {
      exists_in_target: 1,
      exists_outside_target: 1,
    });

    const excluded = readJson(report.files.scan.emergy_excluded_processes) as JsonRecord[];
    assert.equal(excluded.length, 1);
    assert.equal(excluded[0].process_id, 'proc-emergy');

    const repairSummary = readJson(report.files.repair.summary) as JsonRecord;
    assert.equal(repairSummary.patched_process_count, 0);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('runFlowRegenProduct apply mode writes patches, pool sync, and validation artifacts', async () => {
  const dir = mkdtempSync(path.join(os.tmpdir(), 'tg-cli-flow-regen-product-apply-'));
  const processesFile = path.join(dir, 'processes.json');
  const scopeFile = path.join(dir, 'scope.jsonl');
  const aliasMapFile = path.join(dir, 'aliases.json');
  const processPoolFile = path.join(dir, 'process-pool.jsonl');
  const outDir = path.join(dir, 'artifacts');
  const originalProcess = makeProcessRow({
    id: 'proc-apply',
    version: '01.00.000',
    name: 'Apply process',
    exchanges: [
      makeExchange({
        internalId: '1',
        direction: 'Input',
        flowId: 'old-flow',
        flowVersion: '01.00.000',
        flowText: [lang('Legacy input flow')],
      }),
      makeExchange({
        internalId: '2',
        direction: 'Output',
        flowId: 'keep-flow',
        flowVersion: '01.00.000',
        flowText: [lang('Keep flow')],
      }),
    ],
    qref: '2',
  });

  writeJson(processesFile, [originalProcess]);
  writeJsonl(scopeFile, [
    makeFlowRow({ id: 'flow-target', version: '02.00.000', name: 'Target input flow' }),
    makeFlowRow({ id: 'keep-flow', version: '01.00.000', name: 'Keep flow' }),
  ]);
  writeJson(aliasMapFile, {
    'old-flow@01.00.000': {
      id: 'flow-target',
      version: '02.00.000',
    },
  });
  writeJsonl(processPoolFile, [
    originalProcess,
    makeProcessRow({
      id: 'proc-other',
      version: '01.00.000',
      name: 'Other process',
      exchanges: [
        makeExchange({
          internalId: '1',
          direction: 'Output',
          flowId: 'keep-flow',
          flowVersion: '01.00.000',
        }),
      ],
    }),
  ]);

  try {
    const report = await runFlowRegenProduct(
      {
        processesFile,
        scopeFlowFiles: [scopeFile],
        aliasMapFile,
        outDir,
        apply: true,
        processPoolFile,
        tidasMode: 'required',
        autoPatchPolicy: 'alias-only',
      },
      {
        loadSdkModule: () => ({
          createProcess: () => ({
            validateEnhanced: () => ({ success: true }),
          }),
        }),
        now: () => new Date('2026-03-30T13:00:00.000Z'),
      },
    );

    assert.equal(report.generated_at_utc, '2026-03-30T13:00:00.000Z');
    assert.equal(report.mode, 'apply');
    assert.equal(report.auto_patch_policy, 'alias-only');
    assert.equal(report.catalog_flow_files[0], path.resolve(scopeFile));
    assert.equal(report.counts.repair_item_count, 2);
    assert.equal(report.counts.decision_counts.auto_patch, 1);
    assert.equal(report.counts.decision_counts.keep_as_is, 1);
    assert.equal(report.counts.patched_process_count, 1);
    assert.equal(report.counts.validation_passed_count, 1);
    assert.equal(report.counts.validation_failed_count, 0);
    assert.equal(report.validation.enabled, true);
    assert.equal(report.validation.tidas_validation, true);
    assert.equal(report.validation.ok, true);
    assert.ok(report.files.apply);
    assert.ok(report.files.validate);
    assert.ok(existsSync(report.files.apply!.patched_processes));
    assert.ok(existsSync(report.files.validate!.report));

    const patchedRows = readJson(report.files.apply!.patched_processes) as JsonRecord[];
    assert.equal(patchedRows.length, 1);
    const patchedExchange = (
      (
        ((patchedRows[0].json_ordered as JsonRecord).processDataSet as JsonRecord)
          .exchanges as JsonRecord
      ).exchange as JsonRecord[]
    )[0].referenceToFlowDataSet as JsonRecord;
    assert.equal(patchedExchange['@refObjectId'], 'flow-target');
    assert.equal(patchedExchange['@version'], '02.00.000');

    const patchDir = path.join(
      report.files.apply!.patch_root,
      __testInternals.safeProcessKey('proc-apply', '01.00.000'),
    );
    assert.ok(existsSync(path.join(patchDir, 'before.json')));
    assert.ok(existsSync(path.join(patchDir, 'after.json')));
    assert.ok(existsSync(path.join(patchDir, 'evidence.json')));
    assert.ok(existsSync(path.join(patchDir, 'diff.patch')));

    const validateReport = readJson(report.files.validate!.report) as JsonRecord;
    assert.deepEqual(validateReport.summary, {
      patched_process_count: 1,
      passed: 1,
      failed: 0,
      tidas_validation: true,
    });

    const repairSummary = readJson(report.files.apply!.summary) as JsonRecord;
    assert.deepEqual(repairSummary.process_pool_sync, {
      pool_file: path.resolve(processPoolFile),
      pool_pre_count: 2,
      incoming_count: 1,
      pool_post_count: 2,
      inserted: 0,
      updated: 1,
      unchanged: 0,
      skipped_invalid: 0,
    });

    const poolRows = readFileSync(processPoolFile, 'utf8')
      .split(/\r?\n/u)
      .filter(Boolean)
      .map((line) => JSON.parse(line) as JsonRecord);
    assert.equal(poolRows.length, 2);
    const syncedProcess = poolRows.find((row) => row.id === 'proc-apply') as JsonRecord;
    const syncedRef = (
      (
        ((syncedProcess.json_ordered as JsonRecord).processDataSet as JsonRecord)
          .exchanges as JsonRecord
      ).exchange as JsonRecord[]
    )[0].referenceToFlowDataSet as JsonRecord;
    assert.equal(syncedRef['@refObjectId'], 'flow-target');
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('runFlowRegenProduct apply mode surfaces validation failures in the final report', async () => {
  const dir = mkdtempSync(path.join(os.tmpdir(), 'tg-cli-flow-regen-product-apply-fail-'));
  const processesFile = path.join(dir, 'processes.jsonl');
  const scopeFile = path.join(dir, 'scope.json');
  const aliasMapFile = path.join(dir, 'aliases.json');
  const outDir = path.join(dir, 'artifacts');

  writeJsonl(processesFile, [
    makeProcessRow({
      id: 'proc-fail',
      version: '01.00.000',
      name: 'Failing process',
      exchanges: [
        makeExchange({
          internalId: '1',
          direction: 'Output',
          flowId: 'old-flow',
          flowVersion: '01.00.000',
          flowText: [lang('Legacy flow')],
        }),
      ],
    }),
  ]);
  writeJson(scopeFile, [
    makeFlowRow({ id: 'flow-target', version: '02.00.000', name: 'Target flow' }),
  ]);
  writeJson(aliasMapFile, {
    'old-flow@01.00.000': {
      id: 'flow-target',
      version: '02.00.000',
    },
  });

  try {
    const report = await runFlowRegenProduct(
      {
        processesFile,
        scopeFlowFiles: [scopeFile],
        aliasMapFile,
        outDir,
        apply: true,
      },
      {
        loadSdkModule: () => ({
          createProcess: () => ({
            validate: () => ({ success: false, reason: 'schema mismatch' }),
          }),
        }),
      },
    );

    assert.equal(report.mode, 'apply');
    assert.equal(report.tidas_mode, 'auto');
    assert.equal(report.validation.enabled, true);
    assert.equal(report.validation.tidas_validation, true);
    assert.equal(report.validation.ok, false);
    assert.equal(report.counts.validation_passed_count, 0);
    assert.equal(report.counts.validation_failed_count, 1);

    const failures = readFileSync(report.files.validate!.failures, 'utf8')
      .split(/\r?\n/u)
      .filter(Boolean)
      .map((line) => JSON.parse(line) as JsonRecord);
    assert.equal(failures.length, 1);
    assert.equal(
      ((failures[0].issues as JsonRecord[])[0] as JsonRecord).type,
      'tidas_validation_failed',
    );
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
