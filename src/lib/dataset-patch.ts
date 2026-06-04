import crypto from 'node:crypto';
import { existsSync, readFileSync } from 'node:fs';
import path from 'node:path';
import { isDeepStrictEqual } from 'node:util';
import { writeJsonArtifact, writeJsonLinesArtifact } from './artifacts.js';
import {
  cloneJson,
  isRecord,
  materializeDatasetRows,
  trimToken,
  type JsonObject,
} from './dataset-local.js';
import { CliError } from './errors.js';
import { readJsonInput } from './io.js';

type PatchApplyStatus = 'completed' | 'blocked';
type PatchOperationName = 'add' | 'remove' | 'replace' | 'test';

export type DatasetPatchEvidence =
  | string
  | {
      source?: string | null;
      quote?: string | null;
      path?: string | null;
      package?: string | null;
      reasoning?: string | null;
      trace_hash?: string | null;
      [key: string]: unknown;
    }
  | unknown[]
  | null;

export type DatasetPatchOperation = {
  op: PatchOperationName | string;
  path: string;
  value?: unknown;
  evidence?: DatasetPatchEvidence;
  basis?: string | null;
  resolution?: unknown;
  closes?: unknown;
  closes_action_items?: unknown;
  closesActionItems?: unknown;
  action_items?: unknown;
  actionItems?: unknown;
};

export type DatasetPatchSet = {
  row_index?: number | string | null;
  dataset_id?: string | null;
  id?: string | null;
  uuid?: string | null;
  entity_id?: string | null;
  version?: string | null;
  dataset_version?: string | null;
  authoring_package?: string | null;
  operations?: DatasetPatchOperation[];
  patches?: DatasetPatchOperation[];
};

export type DatasetPatchApplyBlocker = {
  code: string;
  message: string;
  row_index?: number;
  dataset_id?: string | null;
  dataset_version?: string | null;
  patch_index?: number;
  operation_index?: number;
  op?: string;
  path?: string;
};

export type DatasetPatchEvidenceEntry = {
  row_index: number;
  dataset_id: string | null;
  dataset_version: string | null;
  patch_index: number;
  operation_index: number;
  op: PatchOperationName;
  path: string;
  basis: string | null;
  evidence: DatasetPatchEvidence;
  resolution?: unknown;
  authoring_package: string | null;
  authoring_package_sha256?: string | null;
  closes_action_items?: ActionItemClosure[];
};

export type DatasetPatchApplyReport = {
  schema_version: 1;
  generated_at_utc: string;
  input_path: string;
  patch_path: string;
  out_path: string;
  status: PatchApplyStatus;
  row_count: number;
  patch_count: number;
  operation_count: number;
  applied_operation_count: number;
  evidence_count: number;
  closed_action_item_count?: number;
  blockers: DatasetPatchApplyBlocker[];
  files: {
    patched_rows: string;
    patch_evidence: string | null;
    report: string | null;
  };
};

export type RunDatasetPatchApplyOptions = {
  inputPath: string;
  patchPath: string;
  outPath: string;
  outDir?: string | null;
  rawInput?: unknown;
  rawPatch?: unknown;
  authoringPackageDir?: string | null;
  requireAuthoringPackage?: boolean;
  requireActionItemClosure?: boolean;
  now?: Date;
};

type NormalizedPatchSet = {
  rowIndex: number | null;
  datasetId: string | null;
  datasetVersion: string | null;
  authoringPackage: string | null;
  operations: DatasetPatchOperation[];
};

type AuthoringPackageContext = {
  path: string;
  sha256: string;
  payload: JsonObject;
  actionItems: ActionItemClosure[];
};

type ActionItemClosure = {
  code: string;
  path: string | null;
};

type NormalizePatchResult = {
  patches: NormalizedPatchSet[];
  blockers: DatasetPatchApplyBlocker[];
};

type PatchTarget = {
  container: JsonObject | unknown[];
  key: string | number;
};

const decisionOnlyActionKinds = new Set([
  'identity_decision_authoring',
  'classification_decision_authoring',
  'location_decision_authoring',
]);

function nowIso(now?: Date): string {
  return (now ?? new Date()).toISOString();
}

function hasOwn(value: object, key: string): boolean {
  return Object.prototype.hasOwnProperty.call(value, key);
}

function parseRowIndex(value: unknown): number | null {
  if (typeof value === 'number' && Number.isInteger(value) && value >= 0) {
    return value;
  }
  if (typeof value === 'string' && /^\d+$/u.test(value)) {
    return Number.parseInt(value, 10);
  }
  return null;
}

function nonEmptyString(value: unknown): string | null {
  return trimToken(value);
}

function sha256Text(value: string): string {
  return crypto.createHash('sha256').update(value).digest('hex');
}

function normalizeClosure(value: unknown): ActionItemClosure | null {
  if (typeof value === 'string') {
    const code = value.trim();
    return code ? { code, path: null } : null;
  }
  if (!isRecord(value)) {
    return null;
  }
  const code =
    nonEmptyString(value.code) ??
    nonEmptyString(value.action_item_code) ??
    nonEmptyString(value.actionItemCode) ??
    nonEmptyString(value.rule_id) ??
    nonEmptyString(value.ruleId);
  if (!code) {
    return null;
  }
  return {
    code,
    path:
      nonEmptyString(value.path) ??
      nonEmptyString(value.json_path) ??
      nonEmptyString(value.jsonPath) ??
      null,
  };
}

function actionItemClosureKey(value: ActionItemClosure): string {
  return `${value.code}\u0000${value.path ?? ''}`;
}

function normalizeClosureList(value: unknown): ActionItemClosure[] {
  const values = Array.isArray(value) ? value : value === undefined || value === null ? [] : [value];
  const closures = values
    .map(normalizeClosure)
    .filter((closure): closure is ActionItemClosure => closure !== null);
  return [...new Map(closures.map((closure) => [actionItemClosureKey(closure), closure])).values()];
}

function operationClosures(operation: DatasetPatchOperation): ActionItemClosure[] {
  return normalizeClosureList(
    operation.closes ??
      operation.closes_action_items ??
      operation.closesActionItems ??
      operation.action_items ??
      operation.actionItems,
  );
}

function actionItemFromPackage(value: unknown): ActionItemClosure | null {
  if (!isRecord(value) || value.ai_required === false) {
    return null;
  }
  const actionKind = nonEmptyString(value.action_kind) ?? nonEmptyString(value.actionKind);
  if (actionKind && decisionOnlyActionKinds.has(actionKind)) {
    return null;
  }
  const code =
    nonEmptyString(value.code) ??
    nonEmptyString(value.rule_id) ??
    nonEmptyString(value.ruleId);
  if (!code) {
    return null;
  }
  return {
    code,
    path: nonEmptyString(value.path) ?? null,
  };
}

function closureMatchesActionItem(closure: ActionItemClosure, actionItem: ActionItemClosure): boolean {
  return (
    closure.code === actionItem.code &&
    (!closure.path || !actionItem.path || closure.path === actionItem.path)
  );
}

function normalizeOperationArray(value: unknown): DatasetPatchOperation[] | null {
  if (!Array.isArray(value)) {
    return null;
  }
  if (!value.every(looksLikeOperation)) {
    return null;
  }
  return value as DatasetPatchOperation[];
}

function looksLikeOperation(value: unknown): boolean {
  return isRecord(value) && typeof value.op === 'string' && typeof value.path === 'string';
}

function normalizePatchSet(value: unknown): NormalizedPatchSet | null {
  if (!isRecord(value)) {
    return null;
  }
  const operations =
    normalizeOperationArray(value.operations) ?? normalizeOperationArray(value.patches);
  if (!operations) {
    return null;
  }
  return {
    rowIndex: parseRowIndex(value.row_index ?? value.rowIndex),
    datasetId:
      nonEmptyString(value.dataset_id) ??
      nonEmptyString(value.id) ??
      nonEmptyString(value.uuid) ??
      nonEmptyString(value.entity_id),
    datasetVersion: nonEmptyString(value.dataset_version) ?? nonEmptyString(value.version),
    authoringPackage: nonEmptyString(value.authoring_package) ?? nonEmptyString(value.authoringPackage),
    operations,
  };
}

function patchPayloadCompletionStatus(rawPatch: unknown): string | null {
  if (!isRecord(rawPatch)) {
    return null;
  }
  return (
    nonEmptyString(rawPatch.patch_status) ??
    nonEmptyString(rawPatch.patchStatus) ??
    nonEmptyString(rawPatch.status)
  );
}

function normalizePatchPayload(rawPatch: unknown): NormalizePatchResult {
  const blockers: DatasetPatchApplyBlocker[] = [];
  const patches: NormalizedPatchSet[] = [];
  const addPatchSet = (value: unknown, patchIndex: number): void => {
    const patch = normalizePatchSet(value);
    if (patch) {
      patches.push(patch);
      return;
    }
    blockers.push({
      code: 'patch_set_invalid',
      message: 'Patch set must be an object with row_index or dataset_id and operations[].',
      patch_index: patchIndex,
    });
  };

  const patchStatus = patchPayloadCompletionStatus(rawPatch);
  if (patchStatus !== 'completed') {
    blockers.push({
      code: 'ai_patch_status_not_completed',
      message: 'Patch payload must declare patch_status=completed before deterministic apply.',
    });
    return { patches, blockers };
  }

  if (Array.isArray(rawPatch)) {
    if (rawPatch.every(looksLikeOperation)) {
      blockers.push({
        code: 'patch_row_required',
        message: 'Top-level operation arrays are not supported because they do not identify a row.',
      });
      return { patches, blockers };
    }
    rawPatch.forEach(addPatchSet);
    return { patches, blockers };
  }

  if (!isRecord(rawPatch)) {
    blockers.push({
      code: 'patch_payload_invalid',
      message: 'Patch payload must be an object or an array of patch sets.',
    });
    return { patches, blockers };
  }

  const directPatch = normalizePatchSet(rawPatch);
  if (directPatch) {
    patches.push(directPatch);
    return { patches, blockers };
  }

  const candidateList =
    (Array.isArray(rawPatch.patch_sets) ? rawPatch.patch_sets : null) ??
    (Array.isArray(rawPatch.patches) ? rawPatch.patches : null) ??
    (Array.isArray(rawPatch.suggestions) ? rawPatch.suggestions : null) ??
    (Array.isArray(rawPatch.items) ? rawPatch.items : null);

  if (candidateList) {
    if (candidateList.every(looksLikeOperation)) {
      blockers.push({
        code: 'patch_row_required',
        message: 'Top-level operation arrays are not supported because they do not identify a row.',
      });
      return { patches, blockers };
    }
    candidateList.forEach(addPatchSet);
    return { patches, blockers };
  }

  blockers.push({
    code: 'patch_payload_invalid',
    message: 'Patch payload must contain operations[], patches[], patch_sets[], suggestions[], or items[].',
  });
  return { patches, blockers };
}

function decodePointerToken(token: string): string {
  if (/(^|[^~])~([^01]|$)/u.test(token)) {
    throw new Error(`Invalid JSON Pointer escape in token: ${token}`);
  }
  return token.replace(/~1/gu, '/').replace(/~0/gu, '~');
}

function parsePointer(pointer: unknown): string[] {
  if (typeof pointer !== 'string' || !pointer.startsWith('/')) {
    throw new Error('Patch path must be a JSON Pointer starting with /.');
  }
  return pointer
    .slice(1)
    .split('/')
    .map((token) => decodePointerToken(token));
}

function parseArrayIndex(token: string, length: number, allowAppend: boolean): number {
  if (allowAppend && token === '-') {
    return length;
  }
  if (!/^(0|[1-9]\d*)$/u.test(token)) {
    throw new Error(`Expected array index, got ${token}.`);
  }
  const index = Number.parseInt(token, 10);
  if (index < 0 || index > length || (!allowAppend && index === length)) {
    throw new Error(`Array index out of bounds: ${token}.`);
  }
  return index;
}

function resolvePatchTarget(root: JsonObject, pointer: string, allowAdd: boolean): PatchTarget {
  const tokens = parsePointer(pointer);
  if (tokens.length === 0) {
    throw new Error('Root-level patches are not supported for dataset rows.');
  }

  let current: unknown = root;
  for (const token of tokens.slice(0, -1)) {
    if (Array.isArray(current)) {
      current = current[parseArrayIndex(token, current.length, false)];
      continue;
    }
    if (isRecord(current)) {
      if (!hasOwn(current, token)) {
        throw new Error(`Patch parent path does not exist at ${token}.`);
      }
      current = current[token];
      continue;
    }
    throw new Error(`Patch parent is not an object or array at ${token}.`);
  }

  const keyToken = tokens[tokens.length - 1] ?? '';
  if (Array.isArray(current)) {
    return {
      container: current,
      key: parseArrayIndex(keyToken, current.length, allowAdd),
    };
  }
  if (isRecord(current)) {
    return { container: current, key: keyToken };
  }
  throw new Error('Patch parent is not an object or array.');
}

function targetExists(target: PatchTarget): boolean {
  if (Array.isArray(target.container)) {
    return typeof target.key === 'number' && target.key >= 0 && target.key < target.container.length;
  }
  return typeof target.key === 'string' && hasOwn(target.container, target.key);
}

function getTargetValue(target: PatchTarget): unknown {
  if (!targetExists(target)) {
    throw new Error('Patch target path does not exist.');
  }
  return target.container[target.key as never];
}

function setTargetValue(target: PatchTarget, value: unknown, insert: boolean): void {
  if (Array.isArray(target.container)) {
    const index = target.key as number;
    if (insert) {
      target.container.splice(index, 0, value);
      return;
    }
    target.container[index] = value;
    return;
  }
  target.container[target.key as string] = value;
}

function removeTargetValue(target: PatchTarget): void {
  if (!targetExists(target)) {
    throw new Error('Patch target path does not exist.');
  }
  if (Array.isArray(target.container)) {
    target.container.splice(target.key as number, 1);
    return;
  }
  delete target.container[target.key as string];
}

function evidenceIsPresent(evidence: unknown): boolean {
  if (typeof evidence === 'string') {
    return evidence.trim().length > 0;
  }
  if (Array.isArray(evidence)) {
    return evidence.length > 0;
  }
  if (isRecord(evidence)) {
    return Object.keys(evidence).length > 0;
  }
  return false;
}

function operationBasis(operation: DatasetPatchOperation): string | null {
  return nonEmptyString(operation.basis);
}

function requiresEvidence(operation: DatasetPatchOperation): boolean {
  return operation.op !== 'test';
}

function validateOperationShape(
  operation: DatasetPatchOperation,
  patchIndex: number,
  operationIndex: number,
  rowIndex: number,
  datasetId: string | null,
  datasetVersion: string | null,
): DatasetPatchApplyBlocker | null {
  if (!['add', 'remove', 'replace', 'test'].includes(operation.op)) {
    return {
      code: 'patch_operation_unsupported',
      message: `Unsupported patch operation: ${operation.op}`,
      row_index: rowIndex,
      dataset_id: datasetId,
      dataset_version: datasetVersion,
      patch_index: patchIndex,
      operation_index: operationIndex,
      op: operation.op,
      path: operation.path,
    };
  }
  if (typeof operation.path !== 'string' || !operation.path.startsWith('/')) {
    return {
      code: 'patch_path_invalid',
      message: 'Patch operation path must be a JSON Pointer starting with /.',
      row_index: rowIndex,
      dataset_id: datasetId,
      dataset_version: datasetVersion,
      patch_index: patchIndex,
      operation_index: operationIndex,
      op: operation.op,
      path: typeof operation.path === 'string' ? operation.path : undefined,
    };
  }
  if (['add', 'replace', 'test'].includes(operation.op) && !hasOwn(operation, 'value')) {
    return {
      code: 'patch_value_required',
      message: `${operation.op} operation requires value.`,
      row_index: rowIndex,
      dataset_id: datasetId,
      dataset_version: datasetVersion,
      patch_index: patchIndex,
      operation_index: operationIndex,
      op: operation.op,
      path: operation.path,
    };
  }
  if (
    requiresEvidence(operation) &&
    !operationBasis(operation) &&
    !evidenceIsPresent(operation.evidence)
  ) {
    return {
      code: 'patch_evidence_required',
      message: 'Non-test patch operations require basis or evidence.',
      row_index: rowIndex,
      dataset_id: datasetId,
      dataset_version: datasetVersion,
      patch_index: patchIndex,
      operation_index: operationIndex,
      op: operation.op,
      path: operation.path,
    };
  }
  return null;
}

function applyOperation(root: JsonObject, operation: DatasetPatchOperation): void {
  const op = operation.op as PatchOperationName;
  const target = resolvePatchTarget(root, operation.path, op === 'add');
  if (op === 'test') {
    const actual = getTargetValue(target);
    if (!isDeepStrictEqual(actual, operation.value)) {
      throw new Error('Patch test failed.');
    }
    return;
  }
  if (op === 'remove') {
    removeTargetValue(target);
    return;
  }
  if (op === 'replace') {
    if (!targetExists(target)) {
      throw new Error('Patch target path does not exist.');
    }
    setTargetValue(target, operation.value, false);
    return;
  }
  setTargetValue(target, operation.value, Array.isArray(target.container));
}

function findTargetRow(
  patch: NormalizedPatchSet,
  rows: ReturnType<typeof materializeDatasetRows>,
  patchIndex: number,
): { rowIndex: number | null; blocker: DatasetPatchApplyBlocker | null } {
  if (patch.rowIndex !== null) {
    if (patch.rowIndex >= rows.length) {
      return {
        rowIndex: null,
        blocker: {
          code: 'patch_row_index_invalid',
          message: `Patch row_index ${patch.rowIndex} is outside input row count ${rows.length}.`,
          row_index: patch.rowIndex,
          dataset_id: patch.datasetId,
          dataset_version: patch.datasetVersion,
          patch_index: patchIndex,
        },
      };
    }
    const row = rows[patch.rowIndex];
    if (patch.datasetId && row?.id !== patch.datasetId) {
      return {
        rowIndex: null,
        blocker: {
          code: 'patch_dataset_id_mismatch',
          message: `Patch dataset_id ${patch.datasetId} does not match row_index ${patch.rowIndex}.`,
          row_index: patch.rowIndex,
          dataset_id: patch.datasetId,
          dataset_version: patch.datasetVersion,
          patch_index: patchIndex,
        },
      };
    }
    if (patch.datasetVersion && row?.version !== patch.datasetVersion) {
      return {
        rowIndex: null,
        blocker: {
          code: 'patch_dataset_version_mismatch',
          message: `Patch dataset_version ${patch.datasetVersion} does not match row_index ${patch.rowIndex}.`,
          row_index: patch.rowIndex,
          dataset_id: patch.datasetId,
          dataset_version: patch.datasetVersion,
          patch_index: patchIndex,
        },
      };
    }
    return { rowIndex: patch.rowIndex, blocker: null };
  }

  if (!patch.datasetId) {
    return {
      rowIndex: null,
      blocker: {
        code: 'patch_row_required',
        message: 'Patch set must identify a target row by row_index or dataset_id.',
        dataset_id: patch.datasetId,
        dataset_version: patch.datasetVersion,
        patch_index: patchIndex,
      },
    };
  }

  const matches = rows.filter(
    (row) => row.id === patch.datasetId && (!patch.datasetVersion || row.version === patch.datasetVersion),
  );
  if (matches.length === 0) {
    return {
      rowIndex: null,
      blocker: {
        code: 'patch_dataset_not_found',
        message: `No input row matched dataset_id ${patch.datasetId}.`,
        dataset_id: patch.datasetId,
        dataset_version: patch.datasetVersion,
        patch_index: patchIndex,
      },
    };
  }
  if (matches.length > 1) {
    return {
      rowIndex: null,
      blocker: {
        code: 'patch_dataset_ambiguous',
        message: `Multiple input rows matched dataset_id ${patch.datasetId}; use row_index or dataset_version.`,
        dataset_id: patch.datasetId,
        dataset_version: patch.datasetVersion,
        patch_index: patchIndex,
      },
    };
  }
  return { rowIndex: matches[0]?.index ?? null, blocker: null };
}

function resolveAuthoringPackagePath(
  authoringPackage: string | null,
  authoringPackageDir: string | null | undefined,
): string | null {
  if (!authoringPackage) {
    return null;
  }
  const directPath = path.resolve(authoringPackage);
  if (existsSync(directPath)) {
    return directPath;
  }
  if (authoringPackageDir) {
    const fromDir = path.resolve(authoringPackageDir, authoringPackage);
    if (existsSync(fromDir)) {
      return fromDir;
    }
    const byBasename = path.resolve(authoringPackageDir, path.basename(authoringPackage));
    if (existsSync(byBasename)) {
      return byBasename;
    }
  }
  return directPath;
}

function readAuthoringPackageContext(options: {
  patch: NormalizedPatchSet;
  rowIndex: number;
  rowId: string | null | undefined;
  rowVersion: string | null | undefined;
  patchIndex: number;
  authoringPackageDir?: string | null;
  requireAuthoringPackage?: boolean;
}): { context: AuthoringPackageContext | null; blockers: DatasetPatchApplyBlocker[] } {
  const blockers: DatasetPatchApplyBlocker[] = [];
  const packagePath = resolveAuthoringPackagePath(
    options.patch.authoringPackage,
    options.authoringPackageDir,
  );
  if (!packagePath) {
    if (options.requireAuthoringPackage) {
      blockers.push({
        code: 'authoring_package_required',
        message: 'Strict patch apply requires each patch set to identify an authoring_package.',
        row_index: options.rowIndex,
        dataset_id: options.rowId ?? options.patch.datasetId,
        dataset_version: options.rowVersion ?? options.patch.datasetVersion,
        patch_index: options.patchIndex,
      });
    }
    return { context: null, blockers };
  }
  if (!existsSync(packagePath)) {
    blockers.push({
      code: 'authoring_package_not_found',
      message: `Authoring package was not found: ${packagePath}`,
      row_index: options.rowIndex,
      dataset_id: options.rowId ?? options.patch.datasetId,
      dataset_version: options.rowVersion ?? options.patch.datasetVersion,
      patch_index: options.patchIndex,
    });
    return { context: null, blockers };
  }

  let rawText = '';
  let payload: unknown = null;
  try {
    rawText = readFileSync(packagePath, 'utf8');
    payload = JSON.parse(rawText);
  } catch (error) {
    blockers.push({
      code: 'authoring_package_invalid',
      message: error instanceof Error ? error.message : String(error),
      row_index: options.rowIndex,
      dataset_id: options.rowId ?? options.patch.datasetId,
      dataset_version: options.rowVersion ?? options.patch.datasetVersion,
      patch_index: options.patchIndex,
    });
    return { context: null, blockers };
  }
  if (!isRecord(payload)) {
    blockers.push({
      code: 'authoring_package_invalid',
      message: 'Authoring package must be a JSON object.',
      row_index: options.rowIndex,
      dataset_id: options.rowId ?? options.patch.datasetId,
      dataset_version: options.rowVersion ?? options.patch.datasetVersion,
      patch_index: options.patchIndex,
    });
    return { context: null, blockers };
  }

  const packageEntityId = nonEmptyString(payload.entity_id) ?? nonEmptyString(payload.process_id);
  const packageVersion = nonEmptyString(payload.version);
  const rowId = nonEmptyString(options.rowId) ?? options.patch.datasetId;
  const rowVersion = nonEmptyString(options.rowVersion) ?? options.patch.datasetVersion;
  if (packageEntityId && rowId && packageEntityId !== rowId) {
    blockers.push({
      code: 'authoring_package_entity_mismatch',
      message: `Authoring package entity_id ${packageEntityId} does not match target row ${rowId}.`,
      row_index: options.rowIndex,
      dataset_id: rowId,
      dataset_version: rowVersion,
      patch_index: options.patchIndex,
    });
  }
  if (packageVersion && rowVersion && packageVersion !== rowVersion) {
    blockers.push({
      code: 'authoring_package_version_mismatch',
      message: `Authoring package version ${packageVersion} does not match target row ${rowVersion}.`,
      row_index: options.rowIndex,
      dataset_id: rowId,
      dataset_version: rowVersion,
      patch_index: options.patchIndex,
    });
  }

  const actionItems = normalizeClosureList(
    (Array.isArray(payload.action_items) ? payload.action_items : [])
      .map(actionItemFromPackage)
      .filter((item): item is ActionItemClosure => item !== null),
  );
  return {
    context: {
      path: packagePath,
      sha256: sha256Text(rawText),
      payload,
      actionItems,
    },
    blockers,
  };
}

export async function runDatasetPatchApply(
  options: RunDatasetPatchApplyOptions,
): Promise<DatasetPatchApplyReport> {
  if (!options.inputPath) {
    throw new CliError('Missing required --input value.', {
      code: 'DATASET_PATCH_INPUT_REQUIRED',
      exitCode: 2,
    });
  }
  if (!options.patchPath) {
    throw new CliError('Missing required --patch value.', {
      code: 'DATASET_PATCH_PATCH_REQUIRED',
      exitCode: 2,
    });
  }
  if (!options.outPath) {
    throw new CliError('Missing required --out value.', {
      code: 'DATASET_PATCH_OUT_REQUIRED',
      exitCode: 2,
    });
  }

  const rows = materializeDatasetRows(options.inputPath, options.rawInput);
  const originalRows = rows.map((row) => cloneJson(row.row));
  const candidateRows = rows.map((row) => cloneJson(row.row));
  const rawPatch = options.rawPatch ?? readJsonInput(path.resolve(options.patchPath));
  const normalized = normalizePatchPayload(rawPatch);
  const blockers = [...normalized.blockers];
  const evidenceEntries: DatasetPatchEvidenceEntry[] = [];
  let operationCount = 0;
  let tentativeAppliedCount = 0;

  normalized.patches.forEach((patch, patchIndex) => {
    operationCount += patch.operations.length;
    const target = findTargetRow(patch, rows, patchIndex);
    if (target.blocker || target.rowIndex === null) {
      blockers.push(target.blocker ?? {
        code: 'patch_row_required',
        message: 'Patch set could not be matched to an input row.',
        patch_index: patchIndex,
      });
      return;
    }
    if (patch.operations.length === 0) {
      blockers.push({
        code: 'patch_operations_missing',
        message: 'Patch set operations[] must not be empty.',
        row_index: target.rowIndex,
        dataset_id: rows[target.rowIndex]?.id ?? patch.datasetId,
        dataset_version: rows[target.rowIndex]?.version ?? patch.datasetVersion,
        patch_index: patchIndex,
      });
      return;
    }

    const row = rows[target.rowIndex];
    const candidateRow = candidateRows[target.rowIndex];
    if (!isRecord(candidateRow)) {
      blockers.push({
        code: 'patch_row_invalid',
        message: 'Patch target row is not a JSON object.',
        row_index: target.rowIndex,
        dataset_id: row?.id ?? patch.datasetId,
        dataset_version: row?.version ?? patch.datasetVersion,
        patch_index: patchIndex,
      });
      return;
    }
    const shouldValidateAuthoringPackage =
      Boolean(options.authoringPackageDir) ||
      options.requireAuthoringPackage === true ||
      options.requireActionItemClosure === true;
    const packageResult = shouldValidateAuthoringPackage
      ? readAuthoringPackageContext({
          patch,
          rowIndex: target.rowIndex,
          rowId: row?.id,
          rowVersion: row?.version,
          patchIndex,
          authoringPackageDir: options.authoringPackageDir,
          requireAuthoringPackage:
            options.requireAuthoringPackage === true || options.requireActionItemClosure === true,
        })
      : { context: null, blockers: [] };
    blockers.push(...packageResult.blockers);
    if (packageResult.blockers.length > 0) {
      return;
    }
    const packageContext = packageResult.context;
    const closedActionItems: ActionItemClosure[] = [];

    patch.operations.forEach((operation, operationIndex) => {
      const shapeBlocker = validateOperationShape(
        operation,
        patchIndex,
        operationIndex,
        target.rowIndex ?? -1,
        row?.id ?? patch.datasetId,
        row?.version ?? patch.datasetVersion,
      );
      if (shapeBlocker) {
        blockers.push(shapeBlocker);
        return;
      }
      const closesActionItems = operationClosures(operation);
      if (options.requireActionItemClosure === true && packageContext) {
        for (const closure of closesActionItems) {
          if (!packageContext.actionItems.some((item) => closureMatchesActionItem(closure, item))) {
            blockers.push({
              code: 'authoring_action_item_unknown',
              message: `Patch operation closes unknown authoring action item ${closure.code}.`,
              row_index: target.rowIndex ?? -1,
              dataset_id: row?.id ?? patch.datasetId,
              dataset_version: row?.version ?? patch.datasetVersion,
              patch_index: patchIndex,
              operation_index: operationIndex,
              op: operation.op,
              path: operation.path,
            });
          }
        }
      }
      closedActionItems.push(...closesActionItems);

      try {
        applyOperation(candidateRow, operation);
        tentativeAppliedCount += operation.op === 'test' ? 0 : 1;
        if (operation.op !== 'test') {
          evidenceEntries.push({
            row_index: target.rowIndex ?? -1,
            dataset_id: row?.id ?? patch.datasetId,
            dataset_version: row?.version ?? patch.datasetVersion,
            patch_index: patchIndex,
            operation_index: operationIndex,
            op: operation.op as PatchOperationName,
            path: operation.path,
            basis: operationBasis(operation),
            evidence: operation.evidence ?? null,
            resolution: operation.resolution ?? null,
            authoring_package: patch.authoringPackage,
            authoring_package_sha256: packageContext?.sha256 ?? null,
            closes_action_items: closesActionItems,
          });
        }
      } catch (error) {
        blockers.push({
          code: operation.op === 'test' ? 'patch_test_failed' : 'patch_apply_failed',
          message: error instanceof Error ? error.message : String(error),
          row_index: target.rowIndex ?? undefined,
          dataset_id: row?.id ?? patch.datasetId,
          dataset_version: row?.version ?? patch.datasetVersion,
          patch_index: patchIndex,
          operation_index: operationIndex,
          op: operation.op,
          path: operation.path,
        });
      }
    });
    if (options.requireActionItemClosure === true && packageContext) {
      for (const actionItem of packageContext.actionItems) {
        if (!closedActionItems.some((closure) => closureMatchesActionItem(closure, actionItem))) {
          blockers.push({
            code: 'authoring_action_item_unclosed',
            message: `Authoring action item ${actionItem.code} was not closed by the AI patch set.`,
            row_index: target.rowIndex,
            dataset_id: row?.id ?? patch.datasetId,
            dataset_version: row?.version ?? patch.datasetVersion,
            patch_index: patchIndex,
            path: actionItem.path ?? undefined,
          });
        }
      }
    }
  });

  const resolvedOut = path.resolve(options.outPath);
  const resolvedOutDir = options.outDir ? path.resolve(options.outDir) : path.dirname(resolvedOut);
  const evidenceFile = path.join(resolvedOutDir, 'outputs', 'patch-evidence.jsonl');
  const reportFile = path.join(resolvedOutDir, 'outputs', 'dataset-patch-apply-report.json');
  const status: PatchApplyStatus = blockers.length > 0 ? 'blocked' : 'completed';
  const outputRows = status === 'completed' ? candidateRows : originalRows;

  writeJsonLinesArtifact(resolvedOut, outputRows);
  writeJsonLinesArtifact(evidenceFile, status === 'completed' ? evidenceEntries : []);

  const report: DatasetPatchApplyReport = {
    schema_version: 1,
    generated_at_utc: nowIso(options.now),
    input_path: options.inputPath,
    patch_path: options.patchPath,
    out_path: resolvedOut,
    status,
    row_count: rows.length,
    patch_count: normalized.patches.length,
    operation_count: operationCount,
    applied_operation_count: status === 'completed' ? tentativeAppliedCount : 0,
    evidence_count: status === 'completed' ? evidenceEntries.length : 0,
    closed_action_item_count: status === 'completed'
      ? evidenceEntries.reduce(
          (total, entry) => total + (entry.closes_action_items?.length ?? 0),
          0,
        )
      : 0,
    blockers,
    files: {
      patched_rows: resolvedOut,
      patch_evidence: evidenceFile,
      report: reportFile,
    },
  };

  writeJsonArtifact(reportFile, report);
  return report;
}

export const __testInternals = {
  normalizePatchPayload,
  parsePointer,
};
