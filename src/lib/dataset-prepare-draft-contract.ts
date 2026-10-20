import { CliError } from './errors.js';
import { sha256Json } from './dataset-maintenance-contract.js';

export type PrepareJson = Record<string, unknown>;
export const PREPARE_TABLES = ['contacts', 'sources', 'flows', 'processes'] as const;
export type PrepareTable = (typeof PREPARE_TABLES)[number];
export const PREPARE_TYPES = {
  contact: 'contacts',
  source: 'sources',
  flow: 'flows',
  process: 'processes',
} as const;
export type PrepareSnapshot = { version: string; jsonOrdered: PrepareJson };
export type PrepareRow = PrepareSnapshot & {
  userId: string;
  stateCode: number;
  modelId: string | null;
};
export type PrepareRequest = {
  table: PrepareTable;
  id: string;
  expectedActorId: string;
  expectedDrafts: PrepareSnapshot[];
  selectedVersion?: string | null;
  source: PrepareSnapshot & { userId: string; stateCode: 100 | 200 };
};
export type PrepareEnvelope = {
  schema_version: 'dataset-prepare-draft-request.v1';
  project_ref: string;
  requestId: string;
  request: PrepareRequest;
};
export type PrepareResult = {
  requestId: string;
  table: PrepareTable;
  id: string;
  actorId: string;
  outcome: 'created' | 'reused' | 'selected';
  version: string;
  sourceVersion: string;
  draftVersions: string[];
  selectedDraft: Omit<PrepareRow, 'modelId'>;
  replayed: boolean;
};
const UUID = /^[a-f0-9]{8}-[a-f0-9]{4}-[1-5][a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/u;
const VERSION = /^\d{2}\.\d{2}\.\d{3}$/u;

export function prepareFail(
  message: string,
  code = 'DATASET_PREPARE_DRAFT_INVALID',
  details?: unknown,
): never {
  throw new CliError(message, { code, exitCode: 1, ...(details === undefined ? {} : { details }) });
}
export function prepareObject(value: unknown): value is PrepareJson {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}
function shape(
  value: unknown,
  required: string[],
  optional: string[] = [],
): asserts value is PrepareJson {
  if (
    !prepareObject(value) ||
    required.some((key) => !Object.hasOwn(value, key)) ||
    Object.keys(value).some((key) => ![...required, ...optional].includes(key))
  )
    prepareFail('Preparation object has missing or unexpected fields.');
}
export function prepareUuid(value: unknown): string {
  if (typeof value !== 'string' || !UUID.test(value))
    prepareFail('Preparation requires a canonical UUID.');
  return value;
}
export function prepareVersion(value: unknown): string {
  if (typeof value !== 'string' || !VERSION.test(value))
    prepareFail('Preparation versions must use NN.NN.NNN.');
  return value;
}
export function prepareTable(value: unknown): PrepareTable {
  if (!PREPARE_TABLES.includes(value as PrepareTable))
    prepareFail(
      'Preparation supports contact, source, flow and standalone process only.',
      'INVALID_DATASET_TABLE',
    );
  return value as PrepareTable;
}
export function assertPrepareMutablePayload(table: PrepareTable, payload: PrepareJson): void {
  if (table !== 'flows') return;
  const root = payload.flowDataSet;
  const modelling = prepareObject(root) ? root.modellingAndValidation : null;
  const method = prepareObject(modelling) ? modelling.LCIMethod : null;
  if (
    !prepareObject(method) ||
    !['Product flow', 'Waste flow'].includes(String(method.typeOfDataSet))
  )
    prepareFail(
      'Only Product flow and Waste flow may prepare ordinary owner drafts.',
      'DATASET_REFERENCE_ONLY',
    );
}
function snapshot(value: unknown): PrepareSnapshot {
  shape(value, ['version', 'jsonOrdered']);
  if (!prepareObject(value.jsonOrdered))
    prepareFail('Preparation before image must be a JSON object.');
  return { version: prepareVersion(value.version), jsonOrdered: value.jsonOrdered };
}
export function parsePrepareEnvelope(value: unknown): PrepareEnvelope {
  shape(value, ['schema_version', 'project_ref', 'requestId', 'request']);
  if (
    value.schema_version !== 'dataset-prepare-draft-request.v1' ||
    typeof value.project_ref !== 'string' ||
    !value.project_ref.trim() ||
    value.project_ref.trim() !== value.project_ref
  )
    prepareFail('Preparation header is invalid.');
  const raw = value.request;
  shape(raw, ['table', 'id', 'expectedActorId', 'expectedDrafts', 'source'], ['selectedVersion']);
  if (raw.source === null)
    prepareFail(
      'Initial creation requires a separately settled policy.',
      'DATASET_INITIAL_CREATION_UNSUPPORTED',
    );
  shape(raw.source, ['version', 'jsonOrdered', 'userId', 'stateCode']);
  if (
    !prepareObject(raw.source.jsonOrdered) ||
    ![100, 200].includes(raw.source.stateCode as number)
  )
    prepareFail('Preparation source must be an exact public row.');
  if (!Array.isArray(raw.expectedDrafts))
    prepareFail('Preparation requires the complete expected draft set.');
  const drafts = raw.expectedDrafts
    .map(snapshot)
    .sort((a, b) => a.version.localeCompare(b.version));
  if (new Set(drafts.map((row) => row.version)).size !== drafts.length)
    prepareFail('Preparation draft versions must be unique.');
  const selectedVersion = raw.selectedVersion == null ? null : prepareVersion(raw.selectedVersion);
  if (selectedVersion !== null && !drafts.some((row) => row.version === selectedVersion))
    prepareFail(
      'Selected version is absent from the complete expected draft set.',
      'DATASET_DRAFT_SELECTION_INVALID',
    );
  const table = prepareTable(raw.table);
  assertPrepareMutablePayload(table, raw.source.jsonOrdered);
  const selected = prepareSelection(drafts, selectedVersion);
  if (selected) assertPrepareMutablePayload(table, selected.jsonOrdered);
  return {
    schema_version: 'dataset-prepare-draft-request.v1',
    project_ref: value.project_ref,
    requestId: prepareUuid(value.requestId),
    request: {
      table,
      id: prepareUuid(raw.id),
      expectedActorId: prepareUuid(raw.expectedActorId),
      expectedDrafts: drafts,
      selectedVersion,
      source: {
        version: prepareVersion(raw.source.version),
        userId: prepareUuid(raw.source.userId),
        stateCode: raw.source.stateCode as 100 | 200,
        jsonOrdered: raw.source.jsonOrdered,
      },
    },
  };
}
export function prepareSame(left: unknown, right: unknown): boolean {
  return sha256Json(left) === sha256Json(right);
}
export function prepareSelection(
  drafts: PrepareSnapshot[],
  selectedVersion: string | null,
): PrepareSnapshot | null {
  if (selectedVersion !== null)
    return (
      drafts.find((row) => row.version === selectedVersion) ??
      prepareFail('Selected version is not a fresh owner draft.', 'DATASET_DRAFT_SELECTION_INVALID')
    );
  return drafts.length === 1 ? drafts[0]! : null;
}

export function createdPreparePayload(request: PrepareRequest, version: string): PrepareJson {
  const payload = structuredClone(request.source.jsonOrdered);
  const roots = {
    contacts: ['contactDataSet', 'contact'],
    sources: ['sourceDataSet', 'source'],
    flows: ['flowDataSet', 'productFlow'],
    processes: ['processDataSet', 'process'],
  } as const;
  const [key, slug] = roots[request.table];
  const root = prepareObject(payload[key]) ? payload[key] : {};
  const admin = prepareObject(root.administrativeInformation) ? root.administrativeInformation : {};
  const publication = prepareObject(admin.publicationAndOwnership)
    ? admin.publicationAndOwnership
    : {};
  publication['common:dataSetVersion'] = version;
  publication['common:permanentDataSetURI'] =
    `https://lcdn.tiangong.earth/datasetdetail/${slug}.xhtml?uuid=${request.id}&version=${version}`;
  admin.publicationAndOwnership = publication;
  root.administrativeInformation = admin;
  payload[key] = root;
  return payload;
}
export function parsePrepareResult(value: unknown, envelope: PrepareEnvelope): PrepareResult {
  shape(value, [
    'requestId',
    'table',
    'id',
    'actorId',
    'outcome',
    'version',
    'sourceVersion',
    'draftVersions',
    'selectedDraft',
    'replayed',
  ]);
  const req = envelope.request;
  shape(value.selectedDraft, ['version', 'userId', 'stateCode', 'jsonOrdered']);
  const selected = value.selectedDraft;
  const expected = prepareSelection(req.expectedDrafts, req.selectedVersion ?? null);
  const created = req.expectedDrafts.length === 0;
  const expectedOutcome = created
    ? 'created'
    : req.expectedDrafts.length === 1
      ? 'reused'
      : 'selected';
  const version = prepareVersion(value.version);
  const expectedPayload = created
    ? createdPreparePayload(req, version)
    : (expected?.jsonOrdered ?? null);
  const versions = created ? [version] : req.expectedDrafts.map((row) => row.version);
  if (
    value.requestId !== envelope.requestId ||
    value.table !== req.table ||
    value.id !== req.id ||
    value.actorId !== req.expectedActorId ||
    value.sourceVersion !== req.source.version ||
    value.outcome !== expectedOutcome ||
    typeof value.replayed !== 'boolean' ||
    (created ? version <= req.source.version : version !== expected?.version) ||
    !prepareSame(value.draftVersions, versions) ||
    selected.version !== version ||
    selected.userId !== req.expectedActorId ||
    selected.stateCode !== 0 ||
    !prepareSame(selected.jsonOrdered, expectedPayload)
  )
    prepareFail(
      'Server preparation receipt does not match the exact original request and source.',
      'DATASET_PREPARE_DRAFT_RECEIPT_INVALID',
    );
  return value as PrepareResult;
}
