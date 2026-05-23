import path from 'node:path';
import { writeJsonArtifact, writeJsonLinesArtifact } from './artifacts.js';
import { CliError } from './errors.js';
import type { FetchLike } from './http.js';
import {
  createSupabaseDataClient,
  requireSupabaseRestRuntime,
  runSupabaseArrayQuery,
  type SupabaseDataRuntime,
} from './supabase-client.js';
import { createSupabaseDataRuntime } from './supabase-session.js';
import {
  firstNonEmpty,
  isRecord,
  readDatasetRowsInput,
  trimToken,
  unwrapDatasetPayload,
  type JsonObject,
} from './dataset-local.js';

const DEFAULT_TIMEOUT_MS = 10_000;

export type RemoteDatasetTable =
  | 'contacts'
  | 'flowproperties'
  | 'flows'
  | 'lciamethods'
  | 'lifecyclemodels'
  | 'processes'
  | 'sources'
  | 'unitgroups';

export type RemoteVerificationReferenceRole = 'root' | 'reference';
export type RemoteVerificationRootPolicy = 'existing' | 'candidate';

export type RemoteVerificationStatus =
  | 'ok'
  | 'lookup_failed'
  | 'missing_dataset'
  | 'missing_version'
  | 'unsupported_type'
  | 'version_missing'
  | 'version_outdated';

export type RemoteDatasetReference = {
  row_index: number;
  role: RemoteVerificationReferenceRole;
  table: RemoteDatasetTable | null;
  type: string | null;
  id: string | null;
  version: string | null;
  path: string;
  short_description: string | null;
};

export type RemoteDatasetLookupRow = {
  id: string;
  version: string | null;
};

export type RemoteDatasetLookup = {
  exact: RemoteDatasetLookupRow | null;
  latest: RemoteDatasetLookupRow | null;
  exact_source_url: string | null;
  latest_source_url: string | null;
};

export type RemoteDatasetLookupRequest = {
  table: RemoteDatasetTable;
  id: string;
  version: string | null;
};

export type RemoteVerificationCheck = {
  row_index: number;
  role: RemoteVerificationReferenceRole;
  table: RemoteDatasetTable | null;
  type: string | null;
  id: string | null;
  version: string | null;
  path: string;
  short_description: string | null;
  status: RemoteVerificationStatus;
  latest_version: string | null;
  exact_source_url: string | null;
  latest_source_url: string | null;
  message: string;
};

export type RemoteVerificationBlocker = {
  code: RemoteVerificationStatus;
  severity: 'error';
  message: string;
  row_index: number;
  role: RemoteVerificationReferenceRole;
  table: RemoteDatasetTable | null;
  id: string | null;
  version: string | null;
  latest_version: string | null;
  path: string;
};

export type DatasetRemoteVerificationReport = {
  schema_version: 1;
  generated_at_utc: string;
  status: 'passed_remote_verification' | 'blocked_remote_verification';
  root_policy: RemoteVerificationRootPolicy;
  input_path: string;
  out_dir: string;
  counts: {
    rows: number;
    references: number;
    checked: number;
    blockers: number;
    by_status: Record<RemoteVerificationStatus, number>;
    by_table: Record<RemoteDatasetTable, number>;
  };
  blockers: RemoteVerificationBlocker[];
  files: {
    report: string;
    checks: string;
    blockers: string;
  };
};

export type RunDatasetRemoteVerifyOptions = {
  inputPath: string;
  outDir: string;
  rootPolicy?: RemoteVerificationRootPolicy;
  rawInput?: unknown;
  env?: NodeJS.ProcessEnv;
  fetchImpl?: FetchLike;
  timeoutMs?: number;
  now?: Date;
  lookupDatasetImpl?: (request: RemoteDatasetLookupRequest) => Promise<RemoteDatasetLookup>;
};

type RootDatasetDescriptor = {
  wrapper: string;
  table: RemoteDatasetTable;
  type: string;
  information_key: string;
};

const ROOT_DATASET_DESCRIPTORS: RootDatasetDescriptor[] = [
  {
    wrapper: 'processDataSet',
    table: 'processes',
    type: 'process data set',
    information_key: 'processInformation',
  },
  {
    wrapper: 'flowDataSet',
    table: 'flows',
    type: 'flow data set',
    information_key: 'flowInformation',
  },
  {
    wrapper: 'lifeCycleModelDataSet',
    table: 'lifecyclemodels',
    type: 'life cycle model data set',
    information_key: 'lifeCycleModelInformation',
  },
  {
    wrapper: 'sourceDataSet',
    table: 'sources',
    type: 'source data set',
    information_key: 'sourceInformation',
  },
  {
    wrapper: 'contactDataSet',
    table: 'contacts',
    type: 'contact data set',
    information_key: 'contactInformation',
  },
  {
    wrapper: 'flowPropertyDataSet',
    table: 'flowproperties',
    type: 'flow property data set',
    information_key: 'flowPropertyInformation',
  },
  {
    wrapper: 'unitGroupDataSet',
    table: 'unitgroups',
    type: 'unit group data set',
    information_key: 'unitGroupInformation',
  },
  {
    wrapper: 'LCIAMethodDataSet',
    table: 'lciamethods',
    type: 'LCIA method data set',
    information_key: 'LCIAMethodInformation',
  },
];

const TABLE_ALIASES = new Map<string, RemoteDatasetTable>([
  ['contact', 'contacts'],
  ['contact data set', 'contacts'],
  ['contacts', 'contacts'],
  ['flow', 'flows'],
  ['flow data set', 'flows'],
  ['flows', 'flows'],
  ['flow property', 'flowproperties'],
  ['flow property data set', 'flowproperties'],
  ['flowproperties', 'flowproperties'],
  ['lcia method', 'lciamethods'],
  ['lcia method data set', 'lciamethods'],
  ['lciamethod', 'lciamethods'],
  ['lciamethods', 'lciamethods'],
  ['life cycle model', 'lifecyclemodels'],
  ['life cycle model data set', 'lifecyclemodels'],
  ['lifecycle model', 'lifecyclemodels'],
  ['lifecycle model data set', 'lifecyclemodels'],
  ['lifecyclemodel', 'lifecyclemodels'],
  ['lifecyclemodels', 'lifecyclemodels'],
  ['process', 'processes'],
  ['process data set', 'processes'],
  ['processes', 'processes'],
  ['source', 'sources'],
  ['source data set', 'sources'],
  ['sources', 'sources'],
  ['unit group', 'unitgroups'],
  ['unit group data set', 'unitgroups'],
  ['unitgroup', 'unitgroups'],
  ['unitgroups', 'unitgroups'],
]);

const PATH_TABLE_HINTS: Array<[RegExp, RemoteDatasetTable]> = [
  [/referenceToFlowDataSet$/u, 'flows'],
  [/referenceToProcess$/u, 'processes'],
  [/referenceToResultingProcess$/u, 'processes'],
  [/referenceToFlowPropertyDataSet$/u, 'flowproperties'],
  [/referenceToReferenceUnitGroup$/u, 'unitgroups'],
  [/referenceToLCIAMethodDataSet$/u, 'lciamethods'],
  [/referenceToDataSource$/u, 'sources'],
  [/referenceToExternalDocumentation$/u, 'sources'],
  [/referenceToDataSetFormat$/u, 'sources'],
  [/referenceToComplianceSystem$/u, 'sources'],
  [/referenceToDataSetUseApproval$/u, 'sources'],
  [/referenceToContact$/u, 'contacts'],
  [/referenceToCommissioner$/u, 'contacts'],
  [/referenceToPersonOrEntity/u, 'contacts'],
  [/referenceToNameOfReviewerAndInstitution$/u, 'contacts'],
];

const EMPTY_STATUS_COUNTS: Record<RemoteVerificationStatus, number> = {
  ok: 0,
  lookup_failed: 0,
  missing_dataset: 0,
  missing_version: 0,
  unsupported_type: 0,
  version_missing: 0,
  version_outdated: 0,
};

const EMPTY_TABLE_COUNTS: Record<RemoteDatasetTable, number> = {
  contacts: 0,
  flowproperties: 0,
  flows: 0,
  lciamethods: 0,
  lifecyclemodels: 0,
  processes: 0,
  sources: 0,
  unitgroups: 0,
};

function requireNonEmpty(value: string, label: string, code: string): string {
  const normalized = value.trim();
  if (!normalized) {
    throw new CliError(`Missing required ${label} value.`, {
      code,
      exitCode: 2,
    });
  }
  return normalized;
}

function nowIso(now: Date = new Date()): string {
  return now.toISOString();
}

function textValue(value: unknown): string | null {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return String(value);
  }
  return trimToken(value);
}

function normalizeTableToken(value: string | null): string | null {
  return value ? value.trim().replace(/\s+/gu, ' ').toLowerCase() : null;
}

function tableFromType(value: string | null): RemoteDatasetTable | null {
  const normalized = normalizeTableToken(value);
  return normalized ? (TABLE_ALIASES.get(normalized) ?? null) : null;
}

function tableFromPath(pathExpression: string): RemoteDatasetTable | null {
  const pathEnd = pathExpression.split('/').filter(Boolean).at(-1) ?? '';
  return PATH_TABLE_HINTS.find(([pattern]) => pattern.test(pathEnd))?.[1] ?? null;
}

function shortDescription(value: unknown): string | null {
  if (typeof value === 'string') {
    return textValue(value);
  }
  if (Array.isArray(value)) {
    for (const entry of value) {
      const text = shortDescription(entry);
      if (text) {
        return text;
      }
    }
    return null;
  }
  if (isRecord(value)) {
    return textValue(value['#text'] ?? value.text ?? value.value);
  }
  return null;
}

function pointerPath(basePath: string, segment: string | number): string {
  const encoded = String(segment).replace(/~/gu, '~0').replace(/\//gu, '~1');
  return `${basePath}/${encoded}`;
}

function rootOf(
  payload: JsonObject,
): { descriptor: RootDatasetDescriptor; root: JsonObject } | null {
  for (const descriptor of ROOT_DATASET_DESCRIPTORS) {
    if (isRecord(payload[descriptor.wrapper])) {
      return { descriptor, root: payload[descriptor.wrapper] as JsonObject };
    }
  }
  return null;
}

function rootIdentity(row: JsonObject, payload: JsonObject): RemoteDatasetReference | null {
  const root = rootOf(payload);
  if (!root) {
    return null;
  }
  const information = isRecord(root.root[root.descriptor.information_key])
    ? (root.root[root.descriptor.information_key] as JsonObject)
    : {};
  const dataSetInformation = isRecord(information.dataSetInformation)
    ? information.dataSetInformation
    : {};
  const administrativeInformation = isRecord(root.root.administrativeInformation)
    ? root.root.administrativeInformation
    : {};
  const publicationAndOwnership = isRecord(administrativeInformation.publicationAndOwnership)
    ? administrativeInformation.publicationAndOwnership
    : {};

  return {
    row_index: -1,
    role: 'root',
    table: root.descriptor.table,
    type: root.descriptor.type,
    id: firstNonEmpty(row.id, row.uuid, dataSetInformation['common:UUID']),
    version: firstNonEmpty(row.version, publicationAndOwnership['common:dataSetVersion']),
    path: `/${root.descriptor.wrapper}`,
    short_description: shortDescription(dataSetInformation.name ?? dataSetInformation.shortName),
  };
}

function referenceFromRecord(
  rowIndex: number,
  value: JsonObject,
  pathExpression: string,
): RemoteDatasetReference | null {
  const id = firstNonEmpty(value['@refObjectId'], value.refObjectId, value.id);
  if (!id) {
    return null;
  }
  const type = firstNonEmpty(value['@type'], value.type);
  const table = tableFromType(type) ?? tableFromPath(pathExpression);
  return {
    row_index: rowIndex,
    role: 'reference',
    table,
    type,
    id,
    version: firstNonEmpty(value['@version'], value.version),
    path: pathExpression,
    short_description: shortDescription(value['common:shortDescription']),
  };
}

function flowReferenceFromRecord(
  rowIndex: number,
  value: JsonObject,
  pathExpression: string,
): RemoteDatasetReference | null {
  const id = firstNonEmpty(value['@flowUUID']);
  return id
    ? {
        row_index: rowIndex,
        role: 'reference',
        table: 'flows',
        type: 'flow data set',
        id,
        version: firstNonEmpty(value['@version'], value.version),
        path: pointerPath(pathExpression, '@flowUUID'),
        short_description: null,
      }
    : null;
}

function collectReferencesFromValue(
  rowIndex: number,
  value: unknown,
  pathExpression: string,
  references: RemoteDatasetReference[],
): void {
  if (Array.isArray(value)) {
    value.forEach((entry, index) =>
      collectReferencesFromValue(rowIndex, entry, pointerPath(pathExpression, index), references),
    );
    return;
  }

  if (!isRecord(value)) {
    return;
  }

  const ref = referenceFromRecord(rowIndex, value, pathExpression);
  if (ref) {
    references.push(ref);
  }
  const flowRef = flowReferenceFromRecord(rowIndex, value, pathExpression);
  if (flowRef) {
    references.push(flowRef);
  }
  for (const [key, nested] of Object.entries(value)) {
    collectReferencesFromValue(rowIndex, nested, pointerPath(pathExpression, key), references);
  }
}

function collectRemoteReferences(rows: JsonObject[]): RemoteDatasetReference[] {
  const references: RemoteDatasetReference[] = [];
  rows.forEach((row, index) => {
    const payload = unwrapDatasetPayload(row);
    const root = rootIdentity(row, payload);
    if (root) {
      references.push({ ...root, row_index: index });
    }
    collectReferencesFromValue(index, payload, '', references);
  });
  return references;
}

function compareVersions(left: string | null, right: string | null): number {
  if (!left && !right) {
    return 0;
  }
  if (!left) {
    return -1;
  }
  if (!right) {
    return 1;
  }
  const leftParts = left.split(/[._-]/u);
  const rightParts = right.split(/[._-]/u);
  const length = Math.max(leftParts.length, rightParts.length);
  for (let index = 0; index < length; index += 1) {
    const leftPart = leftParts[index] ?? '0';
    const rightPart = rightParts[index] ?? '0';
    const leftNumber = Number(leftPart);
    const rightNumber = Number(rightPart);
    if (Number.isFinite(leftNumber) && Number.isFinite(rightNumber)) {
      if (leftNumber !== rightNumber) {
        return leftNumber > rightNumber ? 1 : -1;
      }
    } else {
      const compared = leftPart.localeCompare(rightPart);
      if (compared !== 0) {
        return compared > 0 ? 1 : -1;
      }
    }
  }
  return 0;
}

function buildRemoteUrl(
  restBaseUrl: string,
  table: RemoteDatasetTable,
  id: string,
  version: string | null,
): string {
  const url = new URL(`${restBaseUrl.replace(/\/+$/u, '')}/${table}`);
  url.searchParams.set('id', `eq.${id}`);
  if (version) {
    url.searchParams.set('version', `eq.${version}`);
  } else {
    url.searchParams.set('order', 'version.desc');
    url.searchParams.set('limit', '1');
  }
  return url.toString();
}

function normalizeRows(value: unknown): RemoteDatasetLookupRow[] {
  return Array.isArray(value)
    ? value
        .filter(isRecord)
        .map((row) => ({
          id: firstNonEmpty(row.id) ?? '',
          version: firstNonEmpty(row.version),
        }))
        .filter((row) => Boolean(row.id))
    : [];
}

async function lookupRemoteDataset(options: {
  runtime: SupabaseDataRuntime;
  fetchImpl: FetchLike;
  timeoutMs: number;
  request: RemoteDatasetLookupRequest;
}): Promise<RemoteDatasetLookup> {
  const { client, restBaseUrl } = createSupabaseDataClient(
    options.runtime,
    options.fetchImpl,
    options.timeoutMs,
  );
  const exactSourceUrl = options.request.version
    ? buildRemoteUrl(
        restBaseUrl,
        options.request.table,
        options.request.id,
        options.request.version,
      )
    : null;
  const latestSourceUrl = buildRemoteUrl(
    restBaseUrl,
    options.request.table,
    options.request.id,
    null,
  );
  const exactRows = options.request.version
    ? normalizeRows(
        await runSupabaseArrayQuery(
          client
            .from(options.request.table)
            .select('id,version')
            .eq('id', options.request.id)
            .eq('version', options.request.version),
          exactSourceUrl as string,
        ),
      )
    : [];
  const latestRows = normalizeRows(
    await runSupabaseArrayQuery(
      client
        .from(options.request.table)
        .select('id,version')
        .eq('id', options.request.id)
        .order('version', { ascending: false })
        .limit(1),
      latestSourceUrl,
    ),
  );
  return {
    exact: exactRows[0] ?? null,
    latest: latestRows[0] ?? null,
    exact_source_url: exactSourceUrl,
    latest_source_url: latestSourceUrl,
  };
}

function checkMessage(reference: RemoteDatasetReference, status: RemoteVerificationStatus): string {
  const label = `${reference.table ?? reference.type ?? 'unknown'}:${reference.id ?? '-'}@${
    reference.version ?? '-'
  }`;
  switch (status) {
    case 'ok':
      return `Remote dataset reference is resolvable: ${label}.`;
    case 'lookup_failed':
      return `Remote lookup failed for dataset reference: ${label}.`;
    case 'missing_dataset':
      return `Remote dataset id does not exist: ${label}.`;
    case 'missing_version':
      return `Remote dataset id exists, but the requested version does not exist: ${label}.`;
    case 'unsupported_type':
      return `Dataset reference type cannot be mapped to a TianGong table: ${label}.`;
    case 'version_missing':
      return `Dataset reference is missing @version and cannot pass publish-readiness verification: ${label}.`;
    case 'version_outdated':
      return `Requested dataset version is lower than the latest published version: ${label}.`;
  }
}

function classifyCheck(
  reference: RemoteDatasetReference,
  lookup: RemoteDatasetLookup | null,
  lookupFailed: boolean,
  rootPolicy: RemoteVerificationRootPolicy,
): RemoteVerificationCheck {
  let status: RemoteVerificationStatus = 'ok';
  if (!reference.table || !reference.id) {
    status = 'unsupported_type';
  } else if (lookupFailed) {
    status = 'lookup_failed';
  } else if (!reference.version) {
    status = 'version_missing';
  } else if (reference.role === 'root' && rootPolicy === 'candidate') {
    if (!lookup?.latest) {
      status = 'ok';
    } else if (!lookup.exact) {
      status =
        compareVersions(reference.version, lookup.latest.version) > 0
          ? 'ok'
          : compareVersions(lookup.latest.version, reference.version) > 0
            ? 'version_outdated'
            : 'missing_version';
    } else if (compareVersions(lookup.latest.version, reference.version) > 0) {
      status = 'version_outdated';
    }
  } else if (!lookup?.latest) {
    status = 'missing_dataset';
  } else if (!lookup.exact) {
    status = 'missing_version';
  } else if (compareVersions(lookup.latest.version, reference.version) > 0) {
    status = 'version_outdated';
  }
  return {
    ...reference,
    status,
    latest_version: lookup?.latest?.version ?? null,
    exact_source_url: lookup?.exact_source_url ?? null,
    latest_source_url: lookup?.latest_source_url ?? null,
    message: checkMessage(reference, status),
  };
}

function blockerFromCheck(check: RemoteVerificationCheck): RemoteVerificationBlocker | null {
  return check.status === 'ok'
    ? null
    : {
        code: check.status,
        severity: 'error',
        message: check.message,
        row_index: check.row_index,
        role: check.role,
        table: check.table,
        id: check.id,
        version: check.version,
        latest_version: check.latest_version,
        path: check.path,
      };
}

function buildFiles(outDir: string): DatasetRemoteVerificationReport['files'] {
  const resolved = path.resolve(outDir);
  return {
    report: path.join(resolved, 'outputs', 'remote-verification-report.json'),
    checks: path.join(resolved, 'outputs', 'remote-verification.jsonl'),
    blockers: path.join(resolved, 'outputs', 'blockers.jsonl'),
  };
}

function uniqueLookupKey(reference: RemoteDatasetReference): string | null {
  return reference.table && reference.id
    ? `${reference.table}:${reference.id}:${reference.version ?? ''}`
    : null;
}

export async function runDatasetRemoteVerify(
  options: RunDatasetRemoteVerifyOptions,
): Promise<DatasetRemoteVerificationReport> {
  const inputPath = path.resolve(
    requireNonEmpty(options.inputPath, '--input', 'DATASET_REMOTE_VERIFY_INPUT_REQUIRED'),
  );
  const outDir = path.resolve(
    requireNonEmpty(options.outDir, '--out-dir', 'DATASET_REMOTE_VERIFY_OUT_DIR_REQUIRED'),
  );
  const rows = readDatasetRowsInput(inputPath, options.rawInput);
  const references = collectRemoteReferences(rows);
  const rootPolicy = options.rootPolicy ?? 'existing';
  const fetchImpl = options.fetchImpl ?? (fetch as FetchLike);
  const timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS;
  const runtime =
    options.lookupDatasetImpl === undefined
      ? createSupabaseDataRuntime({
          runtime: requireSupabaseRestRuntime(options.env ?? process.env),
          fetchImpl,
          timeoutMs,
          now: options.now,
        })
      : null;
  const lookupImpl =
    options.lookupDatasetImpl ??
    ((request: RemoteDatasetLookupRequest) =>
      lookupRemoteDataset({
        runtime: runtime as SupabaseDataRuntime,
        fetchImpl,
        timeoutMs,
        request,
      }));
  const lookupCache = new Map<string, Promise<RemoteDatasetLookup>>();
  const checks: RemoteVerificationCheck[] = [];

  for (const reference of references) {
    let lookup: RemoteDatasetLookup | null = null;
    let lookupFailed = false;
    const lookupKey = uniqueLookupKey(reference);
    if (reference.table && reference.id && lookupKey) {
      try {
        if (!lookupCache.has(lookupKey)) {
          lookupCache.set(
            lookupKey,
            lookupImpl({
              table: reference.table,
              id: reference.id,
              version: reference.version,
            }),
          );
        }
        lookup = await lookupCache.get(lookupKey)!;
      } catch {
        lookupFailed = true;
      }
    }
    checks.push(classifyCheck(reference, lookup, lookupFailed, rootPolicy));
  }

  const blockers = checks
    .map(blockerFromCheck)
    .filter((blocker): blocker is RemoteVerificationBlocker => blocker !== null);
  const byStatus = { ...EMPTY_STATUS_COUNTS };
  const byTable = { ...EMPTY_TABLE_COUNTS };
  checks.forEach((check) => {
    byStatus[check.status] += 1;
    if (check.table) {
      byTable[check.table] += 1;
    }
  });

  const files = buildFiles(outDir);
  const report: DatasetRemoteVerificationReport = {
    schema_version: 1,
    generated_at_utc: nowIso(options.now),
    status: blockers.length > 0 ? 'blocked_remote_verification' : 'passed_remote_verification',
    root_policy: rootPolicy,
    input_path: inputPath,
    out_dir: outDir,
    counts: {
      rows: rows.length,
      references: references.length,
      checked: checks.length,
      blockers: blockers.length,
      by_status: byStatus,
      by_table: byTable,
    },
    blockers,
    files,
  };

  writeJsonArtifact(files.report, report);
  writeJsonLinesArtifact(files.checks, checks);
  writeJsonLinesArtifact(files.blockers, blockers);

  return report;
}

export const __testInternals = {
  buildRemoteUrl,
  collectRemoteReferences,
  compareVersions,
  lookupRemoteDataset,
  normalizeRows,
  shortDescription,
  tableFromPath,
  tableFromType,
};
