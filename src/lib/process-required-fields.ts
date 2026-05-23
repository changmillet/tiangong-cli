import path from 'node:path';
import { writeJsonArtifact, writeJsonLinesArtifact } from './artifacts.js';
import { CliError } from './errors.js';
import {
  cloneJson,
  datasetRoot,
  firstNonEmpty,
  isRecord,
  materializeDatasetRows,
  type DatasetRowInput,
  type JsonObject,
} from './dataset-local.js';

const ANNUAL_SUPPLY_FIELD =
  'processDataSet.modellingAndValidation.dataSourcesTreatmentAndRepresentativeness.annualSupplyOrProductionVolume';
const ANNUAL_SUPPLY_ROOT_FIELD =
  'modellingAndValidation.dataSourcesTreatmentAndRepresentativeness.annualSupplyOrProductionVolume';
const ANNUAL_SUPPLY_PATTERN = /^[+-]?(\d+(\.\d*)?|\.\d+)([Ee][+-]?\d+)?\s+\S.*$/u;
const NET_CALORIFIC_VALUE_ID = '93a60a56-a3c8-11da-a746-0800200c9a66';

type AnnualSupplyStatus = 'existing' | 'completed' | 'blocked' | 'skipped';

export type ProcessRequiredFieldIssue = {
  code: string;
  message: string;
  path: string;
};

export type ProcessRequiredFieldCompletion = {
  field_path: string;
  source: 'existing' | 'evidence' | 'reference_flow_amount';
  value: Array<{ '#text': string; '@xml:lang': string }>;
  amount: string;
  unit: string;
  reference_exchange_internal_id: string | null;
  basis: string;
};

export type ProcessRequiredFieldsRowReport = {
  index: number;
  id: string | null;
  version: string | null;
  type: string | null;
  status: AnnualSupplyStatus;
  issues: ProcessRequiredFieldIssue[];
  completions: ProcessRequiredFieldCompletion[];
};

export type ProcessRequiredFieldsReport = {
  generated_at_utc: string;
  input_path: string;
  out_path: string;
  out_dir: string | null;
  status: 'completed' | 'completed_with_blockers';
  default_unit: string;
  counts: {
    total: number;
    processes: number;
    completed: number;
    existing: number;
    blocked: number;
    skipped: number;
  };
  files: {
    output_rows: string;
    report: string | null;
    evidence: string | null;
  };
  rows: ProcessRequiredFieldsRowReport[];
};

export type RunProcessRequiredFieldsCompleteOptions = {
  inputPath: string;
  outPath: string;
  outDir?: string | null;
  defaultUnit?: string | null;
  rawInput?: unknown;
  now?: Date;
};

type CompletionContext = {
  defaultUnit: string;
};

type AnnualSupplyEvidenceValue = {
  value: Array<{ '#text': string; '@xml:lang': string }>;
  amount: string;
  unit: string;
  basis: string;
};

function normalizeUnit(value: string | null | undefined): string {
  const token = value?.trim();
  return token || 'unit';
}

function textValue(value: unknown): string | null {
  if (typeof value === 'string') {
    const trimmed = value.trim();
    return trimmed || null;
  }
  if (typeof value === 'number' && Number.isFinite(value)) {
    return String(value);
  }
  return null;
}

function asList(value: unknown): unknown[] {
  if (Array.isArray(value)) {
    return value;
  }
  return value === undefined || value === null ? [] : [value];
}

function valueAtPath(root: unknown, pathExpression: string): unknown {
  let current = root;
  for (const segment of pathExpression.split('.')) {
    if (!isRecord(current)) {
      return undefined;
    }
    current = current[segment];
  }
  return current;
}

function annualSupplyItems(value: unknown): Array<{ '#text': string; '@xml:lang': string }> {
  return asList(value).filter((item): item is { '#text': string; '@xml:lang': string } => {
    return (
      isRecord(item) && typeof item['#text'] === 'string' && typeof item['@xml:lang'] === 'string'
    );
  });
}

function isValidAnnualSupplyVolume(value: unknown): boolean {
  const items = annualSupplyItems(value);
  return (
    items.length > 0 && items.every((item) => ANNUAL_SUPPLY_PATTERN.test(item['#text'].trim()))
  );
}

export function collectProcessRequiredFieldIssues(
  payload: JsonObject,
): ProcessRequiredFieldIssue[] {
  const root = datasetRoot(payload, 'process');
  const modelling = isRecord(root.modellingAndValidation) ? root.modellingAndValidation : {};
  const dataSources = isRecord(modelling.dataSourcesTreatmentAndRepresentativeness)
    ? modelling.dataSourcesTreatmentAndRepresentativeness
    : null;
  if (!dataSources) {
    return [
      {
        code: 'process_data_sources_treatment_missing',
        message: 'Process payload must include dataSourcesTreatmentAndRepresentativeness.',
        path: 'processDataSet.modellingAndValidation.dataSourcesTreatmentAndRepresentativeness',
      },
    ];
  }

  const annualSupply = dataSources.annualSupplyOrProductionVolume;
  if (isValidAnnualSupplyVolume(annualSupply)) {
    return [];
  }

  const items = annualSupplyItems(annualSupply);
  if (items.length === 0) {
    return [
      {
        code: 'annual_supply_or_production_volume_missing',
        message:
          'Process payload must include annualSupplyOrProductionVolume as numeric text with a unit or context suffix.',
        path: ANNUAL_SUPPLY_FIELD,
      },
    ];
  }

  return [
    {
      code: 'annual_supply_or_production_volume_invalid',
      message:
        'annualSupplyOrProductionVolume must start with a real number followed by a unit or context suffix.',
      path: ANNUAL_SUPPLY_FIELD,
    },
  ];
}

function processRootForMutation(payload: JsonObject): JsonObject {
  if (isRecord(payload.processDataSet)) {
    return payload.processDataSet;
  }
  return payload;
}

function selectReferenceExchange(root: JsonObject): JsonObject | null {
  const exchangesRoot = isRecord(root.exchanges) ? root.exchanges : {};
  const exchanges = asList(exchangesRoot.exchange).filter(isRecord);
  const processInfo = isRecord(root.processInformation) ? root.processInformation : {};
  const quantitativeReference = isRecord(processInfo.quantitativeReference)
    ? processInfo.quantitativeReference
    : {};
  const referenceId = firstNonEmpty(quantitativeReference.referenceToReferenceFlow);

  const byReferenceId = referenceId
    ? exchanges.find((exchange) => firstNonEmpty(exchange['@dataSetInternalID']) === referenceId)
    : null;
  if (byReferenceId) {
    return byReferenceId;
  }

  return (
    exchanges.find((exchange) => exchange.quantitativeReference === true) ??
    exchanges.find(
      (exchange) => String(exchange.exchangeDirection ?? '').toLowerCase() === 'output',
    ) ??
    null
  );
}

function localizedTextIncludes(value: unknown, pattern: RegExp): boolean {
  return annualSupplyItems(value).some((item) => pattern.test(item['#text']));
}

function inferUnitFromReferenceExchange(exchange: JsonObject, fallbackUnit: string): string {
  const direct = firstNonEmpty(exchange.unit, exchange.referenceUnit, exchange.flowUnit);
  if (direct) {
    return direct;
  }

  const flowRef = isRecord(exchange.referenceToFlowDataSet) ? exchange.referenceToFlowDataSet : {};
  const shortDescriptions = flowRef['common:shortDescription'];
  if (
    localizedTextIncludes(shortDescriptions, /MJ|net calorific|lower calorific|净热值|低位发热/iu)
  ) {
    return 'MJ';
  }

  const refObjectId = firstNonEmpty(flowRef['@refObjectId']);
  const flowProperty = isRecord(exchange.flowProperty) ? exchange.flowProperty : {};
  const flowPropertyRef = isRecord(flowProperty.referenceToFlowPropertyDataSet)
    ? flowProperty.referenceToFlowPropertyDataSet
    : {};
  const flowPropertyId = firstNonEmpty(flowPropertyRef['@refObjectId']);
  const flowPropertyText = flowPropertyRef['common:shortDescription'];
  if (
    refObjectId === NET_CALORIFIC_VALUE_ID ||
    flowPropertyId === NET_CALORIFIC_VALUE_ID ||
    localizedTextIncludes(flowPropertyText, /Net calorific|净热值/u)
  ) {
    return 'MJ';
  }

  return fallbackUnit;
}

function buildAnnualSupplyValue(
  amount: string,
  unit: string,
): Array<{
  '#text': string;
  '@xml:lang': string;
}> {
  return [
    { '@xml:lang': 'en', '#text': `${amount} ${unit}/year` },
    { '@xml:lang': 'zh', '#text': `${amount} ${unit}/年` },
  ];
}

function annualSupplyTextParts(value: string): { amount: string; unit: string } | null {
  const match = /^\s*([+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[Ee][+-]?\d+)?)\s+(\S.*)$/u.exec(value);
  return match ? { amount: match[1] as string, unit: (match[2] as string).trim() } : null;
}

function annualSupplyValueFromText(value: string): AnnualSupplyEvidenceValue | null {
  const text = value.trim();
  if (!ANNUAL_SUPPLY_PATTERN.test(text)) {
    return null;
  }
  const parts = annualSupplyTextParts(text) as { amount: string; unit: string };
  return {
    value: [
      { '@xml:lang': 'en', '#text': text },
      { '@xml:lang': 'zh', '#text': text },
    ],
    amount: parts.amount,
    unit: parts.unit,
    basis: 'Evidence provided a complete annual supply / production volume text value.',
  };
}

function normalizeAnnualSupplyEvidenceValue(
  candidate: unknown,
  context: CompletionContext,
): AnnualSupplyEvidenceValue | null {
  if (typeof candidate === 'string') {
    return annualSupplyValueFromText(candidate);
  }

  if (typeof candidate === 'number' && Number.isFinite(candidate)) {
    const amount = String(candidate);
    return {
      value: buildAnnualSupplyValue(amount, context.defaultUnit),
      amount,
      unit: context.defaultUnit,
      basis:
        'Evidence provided a numeric annual supply / production volume; the configured default unit was applied.',
    };
  }

  if (isValidAnnualSupplyVolume(candidate)) {
    const value = annualSupplyItems(candidate).map((item) => ({
      '@xml:lang': item['@xml:lang'],
      '#text': item['#text'].trim(),
    }));
    const parts = annualSupplyTextParts(value[0]!['#text']) as { amount: string; unit: string };
    return {
      value,
      amount: parts.amount,
      unit: parts.unit,
      basis: 'Evidence provided validated multilingual annual supply / production volume values.',
    };
  }

  if (!isRecord(candidate)) {
    return null;
  }

  for (const key of [
    'value',
    'text',
    'value_text',
    'valueText',
    'annualSupplyOrProductionVolume',
    'annual_supply_or_production_volume',
  ]) {
    const nested = normalizeAnnualSupplyEvidenceValue(candidate[key], context);
    if (nested) {
      return nested;
    }
  }

  const english = textValue(candidate.en ?? candidate.english);
  const chinese = textValue(candidate.zh ?? candidate.chinese ?? candidate['zh-CN']);
  const localized = [
    english ? { '@xml:lang': 'en', '#text': english } : null,
    chinese ? { '@xml:lang': 'zh', '#text': chinese } : null,
  ].filter((item): item is { '#text': string; '@xml:lang': string } => Boolean(item));
  if (localized.length > 0 && isValidAnnualSupplyVolume(localized)) {
    const parts = annualSupplyTextParts(localized[0]!['#text']) as {
      amount: string;
      unit: string;
    };
    return {
      value: localized,
      amount: parts.amount,
      unit: parts.unit,
      basis: 'Evidence provided language-specific annual supply / production volume text values.',
    };
  }

  const amount = textValue(candidate.amount ?? candidate.value_amount ?? candidate.valueAmount);
  const unit = normalizeUnit(
    textValue(
      candidate.unit ?? candidate.uom ?? candidate.reference_unit ?? candidate.referenceUnit,
    ) ?? context.defaultUnit,
  );
  if (amount) {
    return {
      value: buildAnnualSupplyValue(amount, unit),
      amount,
      unit,
      basis:
        'Evidence provided annual supply / production volume amount and unit as structured fields.',
    };
  }

  return null;
}

function isAnnualSupplyEvidencePath(pathExpression: string | null): boolean {
  if (!pathExpression) {
    return false;
  }
  return (
    pathExpression === ANNUAL_SUPPLY_FIELD ||
    pathExpression === ANNUAL_SUPPLY_ROOT_FIELD ||
    pathExpression.endsWith(`.${ANNUAL_SUPPLY_ROOT_FIELD}`) ||
    pathExpression.endsWith('.annualSupplyOrProductionVolume')
  );
}

function fieldPathFromEvidenceEntry(entry: JsonObject): string | null {
  return firstNonEmpty(
    entry.field_path,
    entry.fieldPath,
    entry.path,
    entry.field,
    entry.target_path,
    entry.targetPath,
  );
}

function findAnnualSupplyEvidenceEntry(
  container: unknown,
  context: CompletionContext,
): AnnualSupplyEvidenceValue | null {
  if (!isRecord(container)) {
    return null;
  }

  const directPath = fieldPathFromEvidenceEntry(container);
  if (isAnnualSupplyEvidencePath(directPath)) {
    const directValue = normalizeAnnualSupplyEvidenceValue(container, context);
    if (directValue) {
      return directValue;
    }
  }

  for (const key of [
    'field_values',
    'fieldValues',
    'field_bindings',
    'fieldBindings',
    'bindings',
  ]) {
    const entries = asList(container[key]).filter(isRecord);
    for (const entry of entries) {
      if (!isAnnualSupplyEvidencePath(fieldPathFromEvidenceEntry(entry))) {
        continue;
      }
      const value = normalizeAnnualSupplyEvidenceValue(entry, context);
      if (value) {
        return value;
      }
    }
  }

  return null;
}

function findAnnualSupplyEvidenceValue(
  row: JsonObject,
  payload: JsonObject,
  context: CompletionContext,
): AnnualSupplyEvidenceValue | null {
  const explicitPaths = [
    'annualSupplyOrProductionVolume',
    'annual_supply_or_production_volume',
    'required_fields.annualSupplyOrProductionVolume',
    'requiredFields.annualSupplyOrProductionVolume',
    'authoring.required_fields.annualSupplyOrProductionVolume',
    'authoring.requiredFields.annualSupplyOrProductionVolume',
    'process_authoring.required_fields.annualSupplyOrProductionVolume',
    'processAuthoring.requiredFields.annualSupplyOrProductionVolume',
  ];
  for (const source of [row, payload]) {
    for (const explicitPath of explicitPaths) {
      const value = normalizeAnnualSupplyEvidenceValue(valueAtPath(source, explicitPath), context);
      if (value) {
        return value;
      }
    }
  }

  const evidencePaths = [
    'evidence_manifest',
    'evidenceManifest',
    'authoring_evidence',
    'authoringEvidence',
    'process_authoring.evidence_manifest',
    'processAuthoring.evidenceManifest',
  ];
  for (const source of [row, payload]) {
    for (const evidencePath of evidencePaths) {
      const value = findAnnualSupplyEvidenceEntry(valueAtPath(source, evidencePath), context);
      if (value) {
        return value;
      }
    }
  }

  return null;
}

function ensureDataSources(root: JsonObject): JsonObject {
  if (!isRecord(root.modellingAndValidation)) {
    root.modellingAndValidation = {};
  }
  const modelling = root.modellingAndValidation as JsonObject;
  if (!isRecord(modelling.dataSourcesTreatmentAndRepresentativeness)) {
    modelling.dataSourcesTreatmentAndRepresentativeness = {};
  }
  return modelling.dataSourcesTreatmentAndRepresentativeness as JsonObject;
}

function cloneRowWithPayload(row: DatasetRowInput): { row: JsonObject; payload: JsonObject } {
  const clonedRow = cloneJson(row.row);
  if (isRecord(clonedRow.json_ordered)) {
    return { row: clonedRow, payload: clonedRow.json_ordered };
  }
  if (isRecord(clonedRow.jsonOrdered)) {
    return { row: clonedRow, payload: clonedRow.jsonOrdered };
  }
  if (isRecord(clonedRow.json)) {
    return { row: clonedRow, payload: clonedRow.json };
  }
  if (isRecord(clonedRow.payload)) {
    return { row: clonedRow, payload: clonedRow.payload };
  }
  if (isRecord(clonedRow.process)) {
    return { row: clonedRow, payload: clonedRow.process };
  }
  return { row: clonedRow, payload: clonedRow };
}

function completeProcessRow(
  row: DatasetRowInput,
  context: CompletionContext,
): { row: JsonObject; report: ProcessRequiredFieldsRowReport } {
  const { row: clonedRow, payload } = cloneRowWithPayload(row);
  if (row.kind !== 'process') {
    return {
      row: clonedRow,
      report: {
        index: row.index,
        id: row.id,
        version: row.version,
        type: row.kind,
        status: 'skipped',
        issues: [],
        completions: [],
      },
    };
  }

  const existingIssues = collectProcessRequiredFieldIssues(payload);
  if (existingIssues.length === 0) {
    return {
      row: clonedRow,
      report: {
        index: row.index,
        id: row.id,
        version: row.version,
        type: row.kind,
        status: 'existing',
        issues: [],
        completions: [],
      },
    };
  }

  const root = processRootForMutation(payload);
  const evidenceValue = findAnnualSupplyEvidenceValue(clonedRow, payload, context);
  if (evidenceValue) {
    const dataSources = ensureDataSources(root);
    dataSources.annualSupplyOrProductionVolume = evidenceValue.value;
    const completion: ProcessRequiredFieldCompletion = {
      field_path: ANNUAL_SUPPLY_FIELD,
      source: 'evidence',
      value: evidenceValue.value,
      amount: evidenceValue.amount,
      unit: evidenceValue.unit,
      reference_exchange_internal_id: null,
      basis: evidenceValue.basis,
    };
    return {
      row: clonedRow,
      report: {
        index: row.index,
        id: row.id,
        version: row.version,
        type: row.kind,
        status: 'completed',
        issues: [],
        completions: [completion],
      },
    };
  }

  const referenceExchange = selectReferenceExchange(root);
  const amount = referenceExchange
    ? firstNonEmpty(referenceExchange.meanAmount, referenceExchange.resultingAmount)
    : null;
  if (!referenceExchange || !amount) {
    return {
      row: clonedRow,
      report: {
        index: row.index,
        id: row.id,
        version: row.version,
        type: row.kind,
        status: 'blocked',
        issues: [
          ...existingIssues,
          {
            code: 'annual_supply_reference_amount_missing',
            message:
              'Could not derive annualSupplyOrProductionVolume because the reference flow meanAmount/resultingAmount is missing.',
            path: 'processDataSet.exchanges.exchange',
          },
        ],
        completions: [],
      },
    };
  }

  const unit = inferUnitFromReferenceExchange(referenceExchange, context.defaultUnit);
  const value = buildAnnualSupplyValue(amount, unit);
  const dataSources = ensureDataSources(root);
  dataSources.annualSupplyOrProductionVolume = value;

  const completion: ProcessRequiredFieldCompletion = {
    field_path: ANNUAL_SUPPLY_FIELD,
    source: 'reference_flow_amount',
    value,
    amount,
    unit,
    reference_exchange_internal_id: firstNonEmpty(referenceExchange['@dataSetInternalID']),
    basis:
      'No explicit annual supply evidence value was present in the payload; field completed from the quantitative reference flow meanAmount/resultingAmount per authoring policy.',
  };

  return {
    row: clonedRow,
    report: {
      index: row.index,
      id: row.id,
      version: row.version,
      type: row.kind,
      status: 'completed',
      issues: [],
      completions: [completion],
    },
  };
}

function buildFiles(outPath: string, outDir: string | null): ProcessRequiredFieldsReport['files'] {
  if (!outDir) {
    return {
      output_rows: path.resolve(outPath),
      report: null,
      evidence: null,
    };
  }
  const resolved = path.resolve(outDir);
  return {
    output_rows: path.resolve(outPath),
    report: path.join(resolved, 'outputs', 'process-required-fields-report.json'),
    evidence: path.join(resolved, 'outputs', 'process-required-fields-evidence.jsonl'),
  };
}

function requireOutputPath(value: string): string {
  const trimmed = value.trim();
  if (!trimmed) {
    throw new CliError('Missing required --out value.', {
      code: 'PROCESS_REQUIRED_FIELDS_OUT_REQUIRED',
      exitCode: 2,
    });
  }
  return trimmed;
}

export async function runProcessRequiredFieldsComplete(
  options: RunProcessRequiredFieldsCompleteOptions,
): Promise<ProcessRequiredFieldsReport> {
  const outPath = requireOutputPath(options.outPath);
  const outDir = options.outDir?.trim() ? options.outDir.trim() : null;
  const defaultUnit = normalizeUnit(options.defaultUnit);
  const rows = materializeDatasetRows(options.inputPath, options.rawInput);
  const completed = rows.map((row) => completeProcessRow(row, { defaultUnit }));
  const rowReports = completed.map((item) => item.report);
  const files = buildFiles(outPath, outDir);
  const blockers = rowReports.filter((row) => row.status === 'blocked');

  const report: ProcessRequiredFieldsReport = {
    generated_at_utc: (options.now ?? new Date()).toISOString(),
    input_path: path.resolve(options.inputPath),
    out_path: path.resolve(outPath),
    out_dir: outDir ? path.resolve(outDir) : null,
    status: blockers.length > 0 ? 'completed_with_blockers' : 'completed',
    default_unit: defaultUnit,
    counts: {
      total: rowReports.length,
      processes: rowReports.filter((row) => row.type === 'process').length,
      completed: rowReports.filter((row) => row.status === 'completed').length,
      existing: rowReports.filter((row) => row.status === 'existing').length,
      blocked: blockers.length,
      skipped: rowReports.filter((row) => row.status === 'skipped').length,
    },
    files,
    rows: rowReports,
  };

  writeJsonLinesArtifact(
    files.output_rows,
    completed.map((item) => item.row),
  );
  if (files.report) {
    writeJsonArtifact(files.report, report);
  }
  if (files.evidence) {
    writeJsonLinesArtifact(
      files.evidence,
      rowReports.flatMap((row) =>
        row.completions.map((completion) => ({
          row_index: row.index,
          dataset_id: row.id,
          dataset_version: row.version,
          ...completion,
        })),
      ),
    );
  }

  return report;
}

export const __testInternals = {
  ANNUAL_SUPPLY_ROOT_FIELD,
  annualSupplyTextParts,
  annualSupplyValueFromText,
  cloneRowWithPayload,
  collectProcessRequiredFieldIssues,
  completeProcessRow,
  fieldPathFromEvidenceEntry,
  findAnnualSupplyEvidenceValue,
  findAnnualSupplyEvidenceEntry,
  inferUnitFromReferenceExchange,
  isAnnualSupplyEvidencePath,
  isValidAnnualSupplyVolume,
  normalizeAnnualSupplyEvidenceValue,
  requireOutputPath,
  selectReferenceExchange,
  textValue,
  valueAtPath,
};
