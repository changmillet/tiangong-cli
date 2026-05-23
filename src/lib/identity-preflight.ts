import { existsSync, readdirSync, statSync } from 'node:fs';
import path from 'node:path';
import * as tidasSdk from '@tiangong-lca/tidas-sdk';
import { writeJsonArtifact, writeJsonLinesArtifact } from './artifacts.js';
import { CliError } from './errors.js';
import {
  datasetIdentity,
  detectDatasetKind,
  isRecord,
  readDatasetRowsInput,
  unwrapDatasetPayload,
  type DatasetKind,
  type JsonObject,
} from './dataset-local.js';
import { readJsonInput } from './io.js';
import {
  normalizeIssuePath,
  type SafeParseSchema,
  type SdkValidationFactory,
  validateSchemaWithDeepFallback,
} from './tidas-sdk-validation.js';

type IdentityPreflightKind = 'process' | 'flow';

export type IdentityPreflightDecision =
  | 'reuse'
  | 'update_same_row'
  | 'version_bump'
  | 'create_new'
  | 'block_duplicate'
  | 'manual_review';

export type IdentityPreflightStatus = 'passed' | 'blocked' | 'needs_review';

export type IdentityPreflightFinding = {
  code: string;
  severity: 'info' | 'warning' | 'blocker';
  message: string;
  candidate_index?: number;
};

type ValidationSummary = {
  status: 'passed' | 'failed' | 'not_applicable';
  validator: string | null;
  issue_count: number;
  issues: Array<{
    path: string;
    message: string;
    code: string;
  }>;
};

type IdentityProfile = {
  id: string | null;
  version: string | null;
  state_code: number | null;
  names: string[];
  normalized_names: string[];
  identity_key: string;
  exchange_signature: string[];
  fields: Record<string, string | null | string[]>;
};

export type IdentityPreflightCandidateReport = {
  index: number;
  id: string | null;
  version: string | null;
  state_code: number | null;
  identity_key: string;
  match_score: number;
  match_reasons: string[];
  decision_hint: IdentityPreflightDecision | null;
};

export type IdentityPreflightCandidateSourceReport = {
  path: string;
  kind: 'embedded_request' | 'file' | 'directory';
  row_count: number;
  scanned_files: string[];
};

export type IdentityPreflightReport = {
  schema_version: 1;
  generated_at_utc: string;
  kind: IdentityPreflightKind;
  status: IdentityPreflightStatus;
  decision: IdentityPreflightDecision;
  confidence: 'high' | 'medium' | 'low';
  input_path: string;
  out_dir: string | null;
  target: {
    id: string | null;
    version: string | null;
    identity_key: string;
    exchange_signature: string[];
    schema_validation: ValidationSummary;
  };
  candidates: IdentityPreflightCandidateReport[];
  candidate_sources: IdentityPreflightCandidateSourceReport[];
  findings: IdentityPreflightFinding[];
  blockers: IdentityPreflightFinding[];
  next_action:
    | 'reuse_existing'
    | 'repair_existing_draft'
    | 'prepare_version_update'
    | 'materialize_new_payload'
    | 'stop_duplicate'
    | 'queue_manual_review';
  files: {
    identity_decision: string | null;
    candidates: string | null;
    candidate_sources: string | null;
  };
};

export type RunIdentityPreflightOptions = {
  inputPath: string;
  outDir?: string | null;
  rawInput?: unknown;
  candidateInputPaths?: string[];
  now?: Date;
  schemas?: Partial<Record<IdentityPreflightKind, SafeParseSchema>>;
};

export type RunProcessIdentityPreflightOptions = RunIdentityPreflightOptions;
export type RunFlowIdentityPreflightOptions = RunIdentityPreflightOptions;
export type ProcessIdentityPreflightReport = IdentityPreflightReport & { kind: 'process' };
export type FlowIdentityPreflightReport = IdentityPreflightReport & { kind: 'flow' };

type NormalizedInput = {
  target: JsonObject;
  candidates: JsonObject[];
  candidateInputPaths: string[];
};

type CandidateEvaluation = {
  report: IdentityPreflightCandidateReport;
  findings: IdentityPreflightFinding[];
};

const SCHEMA_EXPORTS: Record<IdentityPreflightKind, keyof typeof tidasSdk> = {
  flow: 'FlowSchema' as keyof typeof tidasSdk,
  process: 'ProcessSchema' as keyof typeof tidasSdk,
};

const ENTITY_FACTORY_EXPORTS: Record<IdentityPreflightKind, keyof typeof tidasSdk> = {
  flow: 'createFlow' as keyof typeof tidasSdk,
  process: 'createProcess' as keyof typeof tidasSdk,
};

function requiredInputPath(inputPath: string): string {
  const normalized = inputPath.trim();
  if (!normalized) {
    throw new CliError('Missing required --input value.', {
      code: 'IDENTITY_PREFLIGHT_INPUT_REQUIRED',
      exitCode: 2,
    });
  }
  return normalized;
}

function normalizeToRecord(value: unknown, label: string): JsonObject {
  if (!isRecord(value)) {
    throw new CliError(`${label} must be a JSON object.`, {
      code: 'IDENTITY_PREFLIGHT_INVALID_INPUT',
      exitCode: 2,
    });
  }
  return value;
}

function normalizeRows(value: unknown, label: string): JsonObject[] {
  if (value === undefined || value === null) {
    return [];
  }
  const rows = isRecord(value) && Array.isArray(value.rows) ? value.rows : value;
  const normalizedRows = Array.isArray(rows) ? rows : [rows];
  return normalizedRows.map((row, index) => normalizeToRecord(row, `${label}[${index}]`));
}

function normalizePathList(value: unknown, label: string): string[] {
  if (value === undefined || value === null) {
    return [];
  }
  const values = Array.isArray(value) ? value : [value];
  return values.flatMap((entry, index) => {
    if (typeof entry !== 'string') {
      throw new CliError(`${label}[${index}] must be a string path.`, {
        code: 'IDENTITY_PREFLIGHT_INVALID_CANDIDATE_INPUT',
        exitCode: 2,
      });
    }
    return entry
      .split(',')
      .map((pathValue) => pathValue.trim())
      .filter(Boolean);
  });
}

function pickKindTarget(input: JsonObject, kind: IdentityPreflightKind): unknown {
  if (input.target !== undefined) {
    return input.target;
  }
  if (input.candidate !== undefined) {
    return input.candidate;
  }
  if (kind === 'process' && input.process !== undefined) {
    return input.process;
  }
  if (kind === 'flow' && input.flow !== undefined) {
    return input.flow;
  }
  return input;
}

function normalizePreflightInput(rawInput: unknown, kind: IdentityPreflightKind): NormalizedInput {
  const input = normalizeToRecord(rawInput, 'identity preflight input');
  const target = normalizeToRecord(pickKindTarget(input, kind), 'identity preflight target');
  const candidateGroups = [
    ...normalizeRows(input.candidates, 'candidates'),
    ...normalizeRows(input.existing, 'existing'),
    ...normalizeRows(input.existing_rows, 'existing_rows'),
    ...normalizeRows(input.rows, 'rows'),
  ];

  return {
    target,
    candidates: candidateGroups,
    candidateInputPaths: [
      ...normalizePathList(input.candidate_input, 'candidate_input'),
      ...normalizePathList(input.candidate_inputs, 'candidate_inputs'),
      ...normalizePathList(input.candidateInputPaths, 'candidateInputPaths'),
      ...normalizePathList(input.candidate_files, 'candidate_files'),
      ...normalizePathList(input.candidateFiles, 'candidateFiles'),
    ],
  };
}

function collectCandidateFiles(candidatePath: string): string[] {
  const resolved = path.resolve(candidatePath);
  if (!existsSync(resolved)) {
    throw new CliError(`Candidate input not found: ${resolved}`, {
      code: 'IDENTITY_PREFLIGHT_CANDIDATE_INPUT_NOT_FOUND',
      exitCode: 2,
    });
  }

  const stats = statSync(resolved);
  if (stats.isFile()) {
    return [resolved];
  }
  if (!stats.isDirectory()) {
    throw new CliError(`Candidate input must be a JSON/JSONL file or directory: ${resolved}`, {
      code: 'IDENTITY_PREFLIGHT_CANDIDATE_INPUT_UNSUPPORTED',
      exitCode: 2,
    });
  }

  const files: string[] = [];
  const visit = (directory: string): void => {
    for (const entry of readdirSync(directory, { withFileTypes: true })) {
      if (entry.name.startsWith('.')) {
        continue;
      }
      const entryPath = path.join(directory, entry.name);
      if (entry.isDirectory()) {
        visit(entryPath);
      } else if (entry.isFile() && /\.(?:json|jsonl)$/iu.test(entry.name)) {
        files.push(entryPath);
      }
    }
  };
  visit(resolved);
  return files.sort((left, right) => left.localeCompare(right));
}

function readCandidateSource(candidatePath: string): {
  rows: JsonObject[];
  source: IdentityPreflightCandidateSourceReport;
} {
  const resolved = path.resolve(candidatePath);
  const files = collectCandidateFiles(resolved);
  const rows = files.flatMap((file) => readDatasetRowsInput(file));
  const kind = statSync(resolved).isDirectory() ? 'directory' : 'file';
  return {
    rows,
    source: {
      path: resolved,
      kind,
      row_count: rows.length,
      scanned_files: files,
    },
  };
}

function schemaForKind(
  kind: IdentityPreflightKind,
  schemas: Partial<Record<IdentityPreflightKind, SafeParseSchema>> | undefined,
): { validator: string; schema: SafeParseSchema; createEntity: SdkValidationFactory | null } {
  if (schemas?.[kind]) {
    return {
      validator: 'injected',
      schema: schemas[kind],
      createEntity: null,
    };
  }

  const exportName = SCHEMA_EXPORTS[kind];
  const candidate = (tidasSdk as Record<string, unknown>)[exportName];
  if (
    !candidate ||
    typeof candidate !== 'object' ||
    typeof (candidate as SafeParseSchema).safeParse !== 'function'
  ) {
    throw new CliError(`${String(exportName)} is unavailable in @tiangong-lca/tidas-sdk.`, {
      code: 'IDENTITY_PREFLIGHT_SCHEMA_UNAVAILABLE',
      exitCode: 2,
      details: { kind },
    });
  }

  const factoryName = ENTITY_FACTORY_EXPORTS[kind];
  const createEntity = (tidasSdk as Record<string, unknown>)[factoryName];
  return {
    validator: `@tiangong-lca/tidas-sdk/${String(exportName)}`,
    schema: candidate as SafeParseSchema,
    createEntity:
      typeof createEntity === 'function' ? (createEntity as SdkValidationFactory) : null,
  };
}

function validateTargetSchema(
  target: JsonObject,
  kind: IdentityPreflightKind,
  schemas: Partial<Record<IdentityPreflightKind, SafeParseSchema>> | undefined,
): ValidationSummary {
  const detectedKind = detectDatasetKind(target);
  if (detectedKind && detectedKind !== kind) {
    return {
      status: 'failed',
      validator: null,
      issue_count: 1,
      issues: [
        {
          path: '<root>',
          message: `Expected ${kind} target but detected ${detectedKind}.`,
          code: 'dataset_kind_mismatch',
        },
      ],
    };
  }

  if (!detectedKind) {
    return {
      status: 'not_applicable',
      validator: null,
      issue_count: 0,
      issues: [],
    };
  }

  const { validator, schema, createEntity } = schemaForKind(kind, schemas);
  const payload = unwrapDatasetPayload(target);
  const outcome = validateSchemaWithDeepFallback(schema, payload, createEntity);
  if (outcome.success) {
    return {
      status: 'passed',
      validator,
      issue_count: 0,
      issues: [],
    };
  }

  return {
    status: 'failed',
    validator,
    issue_count: outcome.issues.length,
    issues: outcome.issues.map((issue) => ({
      path: normalizeIssuePath(issue.path),
      message: issue.message ?? 'Validation failed',
      code: issue.code ?? 'custom',
    })),
  };
}

function normalizeText(value: string): string {
  return value
    .normalize('NFKC')
    .trim()
    .toLowerCase()
    .replace(/[^\p{Letter}\p{Number}]+/gu, ' ')
    .replace(/\s+/gu, ' ')
    .trim();
}

function normalizeKey(key: string): string {
  return key
    .split(':')
    .pop()!
    .replace(/[^a-zA-Z0-9]+/gu, '')
    .toLowerCase();
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

function collectText(value: unknown, output: string[] = []): string[] {
  const direct = textValue(value);
  if (direct) {
    output.push(direct);
    return output;
  }
  if (Array.isArray(value)) {
    for (const entry of value) {
      collectText(entry, output);
    }
    return output;
  }
  if (isRecord(value)) {
    if (textValue(value['#text'])) {
      output.push(textValue(value['#text']) as string);
    }
    for (const [key, entry] of Object.entries(value)) {
      if (key === '#text') {
        continue;
      }
      collectText(entry, output);
    }
  }
  return output;
}

function collectValuesByKey(
  value: unknown,
  wantedKeys: Set<string>,
  output: unknown[] = [],
): unknown[] {
  if (Array.isArray(value)) {
    for (const entry of value) {
      collectValuesByKey(entry, wantedKeys, output);
    }
    return output;
  }
  if (!isRecord(value)) {
    return output;
  }
  for (const [key, entry] of Object.entries(value)) {
    if (wantedKeys.has(normalizeKey(key))) {
      output.push(entry);
    }
    collectValuesByKey(entry, wantedKeys, output);
  }
  return output;
}

function uniqueTexts(values: unknown[]): string[] {
  const normalized = new Map<string, string>();
  for (const value of values) {
    for (const text of collectText(value)) {
      const key = normalizeText(text);
      if (key && !normalized.has(key)) {
        normalized.set(key, text.trim());
      }
    }
  }
  return [...normalized.values()].sort((a, b) => normalizeText(a).localeCompare(normalizeText(b)));
}

function firstUniqueText(...values: unknown[]): string | null {
  return uniqueTexts(values)[0] ?? null;
}

function fieldFromKeys(row: JsonObject, payload: JsonObject, keys: string[]): string | null {
  const wanted = new Set(keys.map(normalizeKey));
  return firstUniqueText(
    ...collectValuesByKey(row, wanted),
    ...collectValuesByKey(payload, wanted),
  );
}

function textListFromKeys(row: JsonObject, payload: JsonObject, keys: string[]): string[] {
  const wanted = new Set(keys.map(normalizeKey));
  return uniqueTexts([...collectValuesByKey(row, wanted), ...collectValuesByKey(payload, wanted)]);
}

function normalizedList(values: string[]): string[] {
  return [...new Set(values.map(normalizeText).filter(Boolean))].sort();
}

function numberOrNull(value: unknown): number | null {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === 'string' && value.trim()) {
    const parsed = Number.parseInt(value.trim(), 10);
    return Number.isInteger(parsed) ? parsed : null;
  }
  return null;
}

function collectExchangeLikeRecords(value: unknown, output: JsonObject[] = []): JsonObject[] {
  if (Array.isArray(value)) {
    for (const entry of value) {
      collectExchangeLikeRecords(entry, output);
    }
    return output;
  }
  if (!isRecord(value)) {
    return output;
  }

  const keys = new Set(Object.keys(value).map(normalizeKey));
  if (
    keys.has('referencetoflowdataset') ||
    keys.has('flowid') ||
    keys.has('flowuuid') ||
    keys.has('flow') ||
    keys.has('exchangedirection')
  ) {
    output.push(value);
  }

  for (const entry of Object.values(value)) {
    collectExchangeLikeRecords(entry, output);
  }
  return output;
}

function exchangeRecordSignature(record: JsonObject): string | null {
  const flowId = fieldFromKeys(record, record, [
    '@refObjectId',
    'refObjectId',
    'flow_id',
    'flowId',
    'flow_uuid',
    'flowUuid',
    'referenceToFlowDataSet',
  ]);
  const direction = fieldFromKeys(record, record, [
    'exchangeDirection',
    'direction',
    'inputGroup',
    'outputGroup',
  ]);
  const amount = fieldFromKeys(record, record, [
    'meanAmount',
    'mean_amount',
    'resultingAmount',
    'resulting_amount',
    'amount',
    'meanValue',
  ]);

  const normalizedFlowId = flowId ? normalizeText(flowId) : '';
  if (!normalizedFlowId) {
    return null;
  }
  return [normalizedFlowId, normalizeText(direction ?? ''), normalizeText(amount ?? '')].join(':');
}

function processExchangeSignature(row: JsonObject, payload: JsonObject): string[] {
  return [
    ...new Set(
      [...collectExchangeLikeRecords(row), ...collectExchangeLikeRecords(payload)]
        .map(exchangeRecordSignature)
        .filter((entry): entry is string => Boolean(entry)),
    ),
  ].sort();
}

function profileDatasetIdentity(
  row: JsonObject,
  payload: JsonObject,
  kind: IdentityPreflightKind,
): { id: string | null; version: string | null } {
  const detectedKind = detectDatasetKind(row);
  const identity = datasetIdentity(
    row,
    payload,
    detectedKind === kind ? (kind as DatasetKind) : null,
  );
  return {
    id: firstUniqueText(row.id, row.process_id, row.flow_id, row.uuid, identity.id) ?? identity.id,
    version:
      firstUniqueText(row.version, row.dataset_version, identity.version) ?? identity.version,
  };
}

function processProfile(row: JsonObject): IdentityProfile {
  const payload = unwrapDatasetPayload(row);
  const identity = profileDatasetIdentity(row, payload, 'process');
  const names = textListFromKeys(row, payload, [
    'name',
    'baseName',
    'shortDescription',
    'name_en',
    'name_zh',
  ]);
  const referenceFlowIds = textListFromKeys(row, payload, [
    'reference_flow_id',
    'referenceFlowId',
    'reference_product_flow',
    'referenceProductFlow',
    'referenceToReferenceFlow',
    'referenceToFlowDataSet',
    '@refObjectId',
    'refObjectId',
  ]);
  const operation = fieldFromKeys(row, payload, ['operation', 'process_operation']);
  const quantitativeReference = fieldFromKeys(row, payload, [
    'quantitative_reference',
    'quantitativeReference',
    'qref',
    'referenceToReferenceFlow',
  ]);
  const geography = fieldFromKeys(row, payload, [
    'geography',
    'location',
    'locationOfOperationSupplyOrProduction',
  ]);
  const time = fieldFromKeys(row, payload, [
    'time',
    'reference_year',
    'referenceYear',
    'timePeriod',
  ]);
  const technologyRoute = fieldFromKeys(row, payload, [
    'technology_route',
    'technologyRoute',
    'technology',
    'treatmentStandardsRoutes',
  ]);
  const systemBoundary = fieldFromKeys(row, payload, [
    'system_boundary',
    'systemBoundary',
    'boundary',
  ]);
  const providerRole = fieldFromKeys(row, payload, ['provider_role', 'providerRole']);
  const exchangeSignature = processExchangeSignature(row, payload);
  const keyParts = [
    ...normalizedList(names).slice(0, 4),
    ...normalizedList(referenceFlowIds).slice(0, 4),
    normalizeText(operation ?? ''),
    normalizeText(quantitativeReference ?? ''),
    normalizeText(geography ?? ''),
    normalizeText(time ?? ''),
    normalizeText(technologyRoute ?? ''),
    normalizeText(systemBoundary ?? ''),
    normalizeText(providerRole ?? ''),
    exchangeSignature.join(','),
  ].filter(Boolean);

  return {
    id: identity.id,
    version: identity.version,
    state_code: numberOrNull(row.state_code ?? row.stateCode),
    names,
    normalized_names: normalizedList(names),
    identity_key: keyParts.join('|'),
    exchange_signature: exchangeSignature,
    fields: {
      reference_flow_ids: referenceFlowIds,
      operation,
      quantitative_reference: quantitativeReference,
      geography,
      time,
      technology_route: technologyRoute,
      system_boundary: systemBoundary,
      provider_role: providerRole,
    },
  };
}

function flowProfile(row: JsonObject): IdentityProfile {
  const payload = unwrapDatasetPayload(row);
  const identity = profileDatasetIdentity(row, payload, 'flow');
  const names = textListFromKeys(row, payload, [
    'name',
    'baseName',
    'shortDescription',
    'name_en',
    'name_zh',
    'synonyms',
  ]);
  const typeOfDataset = fieldFromKeys(row, payload, [
    'type_of_dataset',
    'typeOfDataSet',
    'flow_type',
    'flowType',
  ]);
  const cas = fieldFromKeys(row, payload, ['CASNumber', 'cas_number', 'cas']);
  const flowProperty = fieldFromKeys(row, payload, [
    'flow_property',
    'flowProperty',
    'referenceToFlowPropertyDataSet',
    'reference_property',
    'referenceProperty',
  ]);
  const referenceUnit = fieldFromKeys(row, payload, ['reference_unit', 'referenceUnit', 'unit']);
  const categories = textListFromKeys(row, payload, ['category', 'compartment']);
  const geography = fieldFromKeys(row, payload, [
    'geography',
    'location',
    'market',
    'mixAndLocationTypes',
  ]);
  const keyParts = [
    normalizeText(typeOfDataset ?? ''),
    ...normalizedList(names).slice(0, 4),
    normalizeText(cas ?? ''),
    normalizeText(flowProperty ?? ''),
    normalizeText(referenceUnit ?? ''),
    ...normalizedList(categories).slice(0, 4),
    normalizeText(geography ?? ''),
  ].filter(Boolean);

  return {
    id: identity.id,
    version: identity.version,
    state_code: numberOrNull(row.state_code ?? row.stateCode),
    names,
    normalized_names: normalizedList(names),
    identity_key: keyParts.join('|'),
    exchange_signature: [],
    fields: {
      type_of_dataset: typeOfDataset,
      cas,
      flow_property: flowProperty,
      reference_unit: referenceUnit,
      categories,
      geography,
    },
  };
}

function profileForKind(row: JsonObject, kind: IdentityPreflightKind): IdentityProfile {
  return kind === 'process' ? processProfile(row) : flowProfile(row);
}

function intersects(left: string[], right: string[]): boolean {
  const rightSet = new Set(right);
  return left.some((entry) => rightSet.has(entry));
}

function normalizedFieldValues(value: string | null | string[]): string[] {
  return (Array.isArray(value) ? value : [value])
    .filter((entry): entry is string => typeof entry === 'string')
    .map(normalizeText)
    .filter(Boolean);
}

function sameNonEmptyField(
  left: string | null | string[],
  right: string | null | string[],
): boolean {
  const leftValues = normalizedFieldValues(left);
  const rightValues = normalizedFieldValues(right);
  return leftValues.length > 0 && rightValues.length > 0 && intersects(leftValues, rightValues);
}

function sameExchangeSignature(left: string[], right: string[]): boolean {
  return left.length > 0 && right.length > 0 && left.join('|') === right.join('|');
}

function hasEquivalentFlowCore(target: IdentityProfile, candidate: IdentityProfile): boolean {
  const hasSameType = sameNonEmptyField(
    target.fields.type_of_dataset,
    candidate.fields.type_of_dataset,
  );
  const hasSameProperty = sameNonEmptyField(
    target.fields.flow_property,
    candidate.fields.flow_property,
  );
  const hasSameUnit = sameNonEmptyField(
    target.fields.reference_unit,
    candidate.fields.reference_unit,
  );
  const hasSameCas = sameNonEmptyField(target.fields.cas, candidate.fields.cas);
  const hasSameCategory = sameNonEmptyField(target.fields.categories, candidate.fields.categories);

  return (
    hasSameType &&
    hasSameProperty &&
    hasSameUnit &&
    intersects(target.normalized_names, candidate.normalized_names) &&
    (hasSameCas || hasSameCategory)
  );
}

function candidateEvaluation(
  target: IdentityProfile,
  candidate: IdentityProfile,
  kind: IdentityPreflightKind,
  index: number,
): CandidateEvaluation {
  const matchReasons: string[] = [];
  let matchScore = 0;
  let decisionHint: IdentityPreflightDecision | null = null;

  if (target.id && candidate.id && target.id === candidate.id) {
    matchScore += 100;
    matchReasons.push('same_dataset_id');
    if (candidate.state_code === 0) {
      decisionHint = 'update_same_row';
    } else if (target.version && candidate.version && target.version !== candidate.version) {
      decisionHint = 'version_bump';
    } else {
      decisionHint = 'reuse';
    }
  }

  if (
    target.identity_key &&
    candidate.identity_key &&
    target.identity_key === candidate.identity_key
  ) {
    matchScore += 90;
    matchReasons.push('same_identity_key');
    if (!decisionHint) {
      decisionHint = 'block_duplicate';
    }
  }

  if (
    kind === 'process' &&
    sameExchangeSignature(target.exchange_signature, candidate.exchange_signature)
  ) {
    matchScore += 40;
    matchReasons.push('same_exchange_signature');
    if (!decisionHint) {
      decisionHint = 'manual_review';
    }
  }

  const hasOverlappingName = intersects(target.normalized_names, candidate.normalized_names);
  if (hasOverlappingName) {
    matchScore += 20;
    matchReasons.push('overlapping_name');
    if (!decisionHint) {
      decisionHint = 'manual_review';
    }
  }

  const targetReferenceFields = Object.values(target.fields)
    .flat()
    .filter((value): value is string => typeof value === 'string')
    .map(normalizeText)
    .filter(Boolean);
  const candidateReferenceFields = Object.values(candidate.fields)
    .flat()
    .filter((value): value is string => typeof value === 'string')
    .map(normalizeText)
    .filter(Boolean);
  const hasOverlappingIdentityField = intersects(targetReferenceFields, candidateReferenceFields);
  if (hasOverlappingIdentityField) {
    matchScore += 10;
    matchReasons.push('overlapping_identity_field');
  }

  if (
    kind === 'process' &&
    decisionHint === 'manual_review' &&
    matchReasons.includes('same_exchange_signature') &&
    hasOverlappingIdentityField
  ) {
    matchScore += 20;
    matchReasons.push('same_exchange_fingerprint');
    decisionHint = 'block_duplicate';
  }

  if (
    kind === 'flow' &&
    (decisionHint === null || decisionHint === 'manual_review') &&
    hasEquivalentFlowCore(target, candidate)
  ) {
    matchScore += 70;
    matchReasons.push('equivalent_flow_core_fields');
    decisionHint = 'block_duplicate';
  }

  const findings: IdentityPreflightFinding[] = [];
  if (decisionHint === 'block_duplicate') {
    findings.push({
      code: `${kind}_duplicate_candidate`,
      severity: 'blocker',
      message: `Candidate ${index} matches the target identity and should block new ${kind} creation.`,
      candidate_index: index,
    });
  } else if (decisionHint === 'manual_review') {
    findings.push({
      code: `${kind}_manual_review_candidate`,
      severity: 'warning',
      message: `Candidate ${index} is similar enough to require manual review before new ${kind} creation.`,
      candidate_index: index,
    });
  } else if (decisionHint) {
    findings.push({
      code: `${kind}_${decisionHint}`,
      severity: 'info',
      message: `Candidate ${index} supports decision ${decisionHint}.`,
      candidate_index: index,
    });
  }

  return {
    report: {
      index,
      id: candidate.id,
      version: candidate.version,
      state_code: candidate.state_code,
      identity_key: candidate.identity_key,
      match_score: matchScore,
      match_reasons: matchReasons,
      decision_hint: decisionHint,
    },
    findings,
  };
}

function chooseDecision(
  evaluations: CandidateEvaluation[],
  validation: ValidationSummary,
  kind: IdentityPreflightKind,
): {
  decision: IdentityPreflightDecision;
  confidence: 'high' | 'medium' | 'low';
  findings: IdentityPreflightFinding[];
} {
  const findings = evaluations.flatMap((evaluation) => evaluation.findings);
  if (validation.status === 'failed') {
    return {
      decision: 'manual_review',
      confidence: 'high',
      findings: [
        {
          code: `${kind}_schema_invalid`,
          severity: 'blocker',
          message: `Target ${kind} payload failed schema validation.`,
        },
        ...findings,
      ],
    };
  }

  const sorted = [...evaluations].sort(
    (left, right) => right.report.match_score - left.report.match_score,
  );
  const top = sorted[0]?.report;
  if (!top || top.match_score === 0) {
    return {
      decision: 'create_new',
      confidence: 'medium',
      findings: [
        {
          code: `${kind}_no_duplicate_candidate`,
          severity: 'info',
          message: `No duplicate ${kind} candidate matched the target identity.`,
        },
        ...findings,
      ],
    };
  }

  if (top.decision_hint === 'block_duplicate') {
    return {
      decision: 'block_duplicate',
      confidence: top.match_score >= 90 ? 'high' : 'medium',
      findings,
    };
  }
  if (
    top.decision_hint === 'reuse' ||
    top.decision_hint === 'update_same_row' ||
    top.decision_hint === 'version_bump'
  ) {
    return {
      decision: top.decision_hint,
      confidence: top.match_score >= 90 ? 'high' : 'medium',
      findings,
    };
  }

  return {
    decision: 'manual_review',
    confidence: top.match_score >= 60 ? 'medium' : 'low',
    findings,
  };
}

function statusForDecision(
  decision: IdentityPreflightDecision,
  blockers: IdentityPreflightFinding[],
): IdentityPreflightStatus {
  if (blockers.length > 0 || decision === 'block_duplicate') {
    return 'blocked';
  }
  if (decision === 'manual_review') {
    return 'needs_review';
  }
  return 'passed';
}

function nextActionForDecision(
  decision: IdentityPreflightDecision,
): IdentityPreflightReport['next_action'] {
  if (decision === 'reuse') {
    return 'reuse_existing';
  }
  if (decision === 'update_same_row') {
    return 'repair_existing_draft';
  }
  if (decision === 'version_bump') {
    return 'prepare_version_update';
  }
  if (decision === 'block_duplicate') {
    return 'stop_duplicate';
  }
  if (decision === 'manual_review') {
    return 'queue_manual_review';
  }
  return 'materialize_new_payload';
}

function writeArtifacts(
  report: IdentityPreflightReport,
  outDir: string | null | undefined,
): IdentityPreflightReport['files'] {
  if (!outDir) {
    return {
      identity_decision: null,
      candidates: null,
      candidate_sources: null,
    };
  }

  const resolved = path.resolve(outDir);
  const files = {
    identity_decision: path.join(resolved, 'outputs', 'identity-decision.json'),
    candidates: path.join(resolved, 'outputs', 'identity-candidates.jsonl'),
    candidate_sources: path.join(resolved, 'outputs', 'identity-candidate-sources.json'),
  };

  writeJsonArtifact(files.identity_decision, { ...report, files });
  writeJsonLinesArtifact(files.candidates, report.candidates);
  writeJsonArtifact(files.candidate_sources, report.candidate_sources);
  return files;
}

export async function runIdentityPreflight(
  kind: IdentityPreflightKind,
  options: RunIdentityPreflightOptions,
): Promise<IdentityPreflightReport> {
  const inputPath = requiredInputPath(options.inputPath);
  const normalizedInput = normalizePreflightInput(
    options.rawInput ?? readJsonInput(inputPath),
    kind,
  );
  const candidateSourceReads = [
    ...normalizedInput.candidateInputPaths,
    ...(options.candidateInputPaths ?? []),
  ].map(readCandidateSource);
  const candidateSources: IdentityPreflightCandidateSourceReport[] = [
    ...(normalizedInput.candidates.length > 0
      ? [
          {
            path: path.resolve(inputPath),
            kind: 'embedded_request' as const,
            row_count: normalizedInput.candidates.length,
            scanned_files: [],
          },
        ]
      : []),
    ...candidateSourceReads.map((entry) => entry.source),
  ];
  const candidates = [
    ...normalizedInput.candidates,
    ...candidateSourceReads.flatMap((entry) => entry.rows),
  ];
  const targetProfile = profileForKind(normalizedInput.target, kind);
  const validation = validateTargetSchema(normalizedInput.target, kind, options.schemas);
  const evaluations = candidates.map((candidate, index) =>
    candidateEvaluation(targetProfile, profileForKind(candidate, kind), kind, index),
  );
  const decision = chooseDecision(evaluations, validation, kind);
  const blockers = decision.findings.filter((finding) => finding.severity === 'blocker');

  const baseReport: IdentityPreflightReport = {
    schema_version: 1,
    generated_at_utc: (options.now ?? new Date()).toISOString(),
    kind,
    status: statusForDecision(decision.decision, blockers),
    decision: decision.decision,
    confidence: decision.confidence,
    input_path: path.resolve(inputPath),
    out_dir: options.outDir ? path.resolve(options.outDir) : null,
    target: {
      id: targetProfile.id,
      version: targetProfile.version,
      identity_key: targetProfile.identity_key,
      exchange_signature: targetProfile.exchange_signature,
      schema_validation: validation,
    },
    candidates: evaluations.map((evaluation) => evaluation.report),
    candidate_sources: candidateSources,
    findings: decision.findings,
    blockers,
    next_action: nextActionForDecision(decision.decision),
    files: {
      identity_decision: null,
      candidates: null,
      candidate_sources: null,
    },
  };

  const files = writeArtifacts(baseReport, options.outDir);
  return {
    ...baseReport,
    files,
  };
}

export async function runProcessIdentityPreflight(
  options: RunProcessIdentityPreflightOptions,
): Promise<ProcessIdentityPreflightReport> {
  return (await runIdentityPreflight('process', options)) as ProcessIdentityPreflightReport;
}

export async function runFlowIdentityPreflight(
  options: RunFlowIdentityPreflightOptions,
): Promise<FlowIdentityPreflightReport> {
  return (await runIdentityPreflight('flow', options)) as FlowIdentityPreflightReport;
}

export const __testInternals = {
  normalizePreflightInput,
  schemaForKind,
  entityFactoryExports: ENTITY_FACTORY_EXPORTS,
  processProfile,
  flowProfile,
  candidateEvaluation,
  chooseDecision,
  readCandidateSource,
};
