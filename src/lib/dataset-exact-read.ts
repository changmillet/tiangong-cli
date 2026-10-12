// data-api-relations: contacts, flowproperties, flows, lifecyclemodels, processes, sources, unitgroups
// data-api-dynamic-relation-expression: options.table
import { readFileSync, realpathSync } from 'node:fs';
import path from 'node:path';
import type { AuthIdentityReceipt } from './auth-identity-receipt.js';
import type { DatasetCommandTable } from './dataset-command.js';
import {
  assertReconciliationFiles,
  reconciliationFileFact,
  reconciliationOutputDirectory,
  writeDraftReconciliation,
  type ReconciliationFileFact,
} from './dataset-draft-reconciliation.js';
import { sha256Json } from './dataset-maintenance-contract.js';
import { resolveFlowIdentityApprovalClaimRoot } from './dataset-maintenance-flow-identity-approval-claim.js';
import { CliError } from './errors.js';
import type { FetchLike } from './http.js';
import { loadCliPackageVersion } from './package-version.js';
import { createDatasetReadSession } from './dataset-read-session.js';
import { createSupabaseDataClient, runSupabaseQuery } from './supabase-client.js';

type JsonObject = Record<string, unknown>;
type ExactIdentity = { id: string; version: string };
export type DatasetExactReadSelection =
  | (ExactIdentity & { table: 'processes'; expected_model?: ExactIdentity | null })
  | (ExactIdentity & { table: 'lifecyclemodels'; referenced_by: ExactIdentity[] });
export type DatasetExactReadScope = {
  schema_version: 'dataset-exact-read-scope.v1';
  project_ref: string;
  actor: { user_id: string; email?: string };
  sources: Array<{ path: string; sha256: string }>;
  rows: DatasetExactReadSelection[];
  read_scope?: 'public_100_199_or_current_owner_state_0';
};
export type DatasetExactReadRow = {
  selection: DatasetExactReadSelection;
  status: 'found' | 'absent' | 'indeterminate' | 'drift';
  reason:
    | 'exact_row'
    | 'not_visible'
    | 'read_failed'
    | 'protocol_scope_violation'
    | 'ambiguous_rows'
    | 'invalid_or_incomplete_row'
    | 'model_association_changed'
    | 'referenced_process_drift';
  observed_at_utc: string;
  actual_row: JsonObject | null;
  actual_row_sha256: string | null;
  actual_payload_sha256: string | null;
  model_association: {
    model_id: string;
    resolved_version: string;
    version_source: 'model_version' | 'process_version_fallback';
  } | null;
};
export type DatasetReadExactReport = {
  schema_version: 'dataset-exact-read-report.v1';
  mode: 'read_only';
  status: 'observed' | 'indeterminate' | 'drift';
  authorization_granted: false;
  adoption_performed: false;
  read_scope: 'public_100_199_or_current_owner_state_0';
  identity: AuthIdentityReceipt;
  scope: ReconciliationFileFact;
  sources: ReconciliationFileFact[];
  rows: DatasetExactReadRow[];
};
export type RunDatasetReadExactOptions = {
  scopeFile: string;
  scopeSha256: string;
  outDir: string;
  env: NodeJS.ProcessEnv;
  fetchImpl: FetchLike;
  timeoutMs?: number;
  now?: Date;
};
export type SupabaseDataClient = ReturnType<typeof createSupabaseDataClient>['client'];
export type DatasetBodyReadScope = {
  mode: 'owner_draft' | 'public_or_owner_draft';
  actorUserId: string;
};

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/u;
const VERSION = /^\d{2}\.\d{2}\.\d{3}$/u;
const SHA = /^[0-9a-f]{64}$/u;
const SOURCE_MAX_BYTES = 64 * 1024 * 1024;
const COMMON_COLUMNS = 'id,version,user_id,state_code,modified_at,json_ordered,team_id';
const PROCESS_COLUMNS = COMMON_COLUMNS + ',model_id,model_version,review_id';

function fail(message: string): never {
  throw new CliError(message, { code: 'DATASET_EXACT_READ_INVALID', exitCode: 2 });
}
function isObject(value: unknown): value is JsonObject {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}
function keys(
  value: unknown,
  required: string[],
  optional: string[] = [],
): asserts value is JsonObject {
  if (
    !isObject(value) ||
    required.some((key) => !Object.hasOwn(value, key)) ||
    Object.keys(value).some((key) => !required.includes(key) && !optional.includes(key))
  )
    fail('Exact-read scope contains missing or unsupported fields.');
}
function identity(value: unknown): ExactIdentity {
  keys(value, ['id', 'version']);
  if (
    typeof value.id !== 'string' ||
    !UUID.test(value.id) ||
    typeof value.version !== 'string' ||
    !VERSION.test(value.version)
  )
    fail('Exact-read identities require a canonical UUID and exact version.');
  return { id: value.id, version: value.version };
}
function identityKey(value: ExactIdentity): string {
  return `${value.id}@${value.version}`;
}
function parseScope(value: unknown): DatasetExactReadScope {
  keys(value, ['schema_version', 'project_ref', 'actor', 'sources', 'rows'], ['read_scope']);
  if (
    Object.hasOwn(value, 'read_scope') &&
    value.read_scope !== 'public_100_199_or_current_owner_state_0'
  )
    fail('Exact-read scope cannot broaden the fixed read boundary.');
  if (
    value.schema_version !== 'dataset-exact-read-scope.v1' ||
    typeof value.project_ref !== 'string' ||
    !/^[a-z0-9-]+$/u.test(value.project_ref)
  )
    fail('Exact-read scope schema or project identity is invalid.');
  keys(value.actor, ['user_id'], ['email']);
  if (
    typeof value.actor.user_id !== 'string' ||
    !UUID.test(value.actor.user_id) ||
    (Object.hasOwn(value.actor, 'email') &&
      (typeof value.actor.email !== 'string' ||
        value.actor.email.length > 254 ||
        !/^[^@\s]+@[^@\s]+\.[^@\s]+$/u.test(value.actor.email) ||
        value.actor.email !== value.actor.email.toLowerCase()))
  )
    fail('Exact-read actor identity is invalid.');
  if (!Array.isArray(value.sources) || value.sources.length < 1 || value.sources.length > 16)
    fail('Exact-read scope requires between 1 and 16 provenance files.');
  const sourcePaths = new Set<string>();
  const sources = value.sources.map((source: unknown) => {
    keys(source, ['path', 'sha256']);
    if (
      typeof source.path !== 'string' ||
      !path.isAbsolute(source.path) ||
      source.path !== path.resolve(source.path) ||
      typeof source.sha256 !== 'string' ||
      !SHA.test(source.sha256) ||
      sourcePaths.has(source.path)
    )
      fail('Exact-read provenance paths and hashes must be explicit and unique.');
    sourcePaths.add(source.path);
    return { path: source.path, sha256: source.sha256 };
  });
  if (!Array.isArray(value.rows) || value.rows.length < 1 || value.rows.length > 256)
    fail('Exact-read scope requires between 1 and 256 selections.');
  const selected = new Set<string>();
  const rows: DatasetExactReadSelection[] = value.rows.map((selection: unknown) => {
    if (!isObject(selection)) fail('Exact-read selection must be an object.');
    const process = selection.table === 'processes';
    if (!process && selection.table !== 'lifecyclemodels')
      fail('Exact-read supports Process and LifecycleModel only.');
    keys(
      selection,
      process ? ['table', 'id', 'version'] : ['table', 'id', 'version', 'referenced_by'],
      process ? ['expected_model'] : [],
    );
    const exact = identity({ id: selection.id, version: selection.version });
    const key = `${selection.table}:${identityKey(exact)}`;
    if (selected.has(key)) fail('Exact-read selections must be unique.');
    selected.add(key);
    if (process) {
      return {
        table: 'processes',
        ...exact,
        ...(Object.hasOwn(selection, 'expected_model')
          ? {
              expected_model:
                selection.expected_model === null ? null : identity(selection.expected_model),
            }
          : {}),
      };
    }
    if (
      !Array.isArray(selection.referenced_by) ||
      selection.referenced_by.length < 1 ||
      selection.referenced_by.length > 256
    )
      fail('A Model selection requires explicit referencing Processes.');
    const references = selection.referenced_by.map(identity);
    if (new Set(references.map(identityKey)).size !== references.length)
      fail('Model referencing Processes must be unique.');
    return { table: 'lifecyclemodels', ...exact, referenced_by: references };
  });
  for (const selection of rows) {
    if (
      selection.table === 'lifecyclemodels' &&
      selection.referenced_by.some(
        (reference) => !selected.has(`processes:${identityKey(reference)}`),
      )
    )
      fail('Model references must name exact Process selections in this scope.');
  }
  return {
    schema_version: 'dataset-exact-read-scope.v1',
    project_ref: value.project_ref,
    actor: {
      user_id: value.actor.user_id,
      ...(typeof value.actor.email === 'string' ? { email: value.actor.email } : {}),
    },
    sources,
    rows,
    ...(Object.hasOwn(value, 'read_scope')
      ? { read_scope: 'public_100_199_or_current_owner_state_0' as const }
      : {}),
  };
}

export function isReadScopeViolation(error: unknown): boolean {
  return error instanceof CliError && error.code === 'DATASET_READ_SCOPE_VIOLATION';
}

export async function readExactRows(options: {
  client: SupabaseDataClient;
  restBaseUrl: string;
  table: DatasetCommandTable | 'lifecyclemodels';
  id: string;
  version: string;
  columns: string;
  readScope: DatasetBodyReadScope;
}): Promise<unknown[]> {
  keys(options.readScope, ['mode', 'actorUserId']);
  if (
    (options.readScope.mode !== 'owner_draft' &&
      options.readScope.mode !== 'public_or_owner_draft') ||
    typeof options.readScope.actorUserId !== 'string' ||
    !UUID.test(options.readScope.actorUserId)
  )
    fail('Exact body reads require a fixed scope and canonical verified actor.');
  const url = new URL(`${options.restBaseUrl.replace(/\/+$/u, '')}/${options.table}`);
  url.searchParams.set('select', options.columns);
  url.searchParams.set('id', `eq.${options.id}`);
  url.searchParams.set('version', `eq.${options.version}`);
  url.searchParams.set('limit', '2');
  let query = options.client
    .from(options.table)
    .select(options.columns)
    .eq('id', options.id)
    .eq('version', options.version)
    .limit(2);
  if (options.readScope.mode === 'owner_draft') {
    url.searchParams.set('user_id', `eq.${options.readScope.actorUserId}`);
    url.searchParams.set('state_code', 'eq.0');
    query = query.eq('user_id', options.readScope.actorUserId).eq('state_code', 0);
  } else {
    const predicate = `and(state_code.gte.100,state_code.lt.200),and(state_code.eq.0,user_id.eq.${options.readScope.actorUserId})`;
    url.searchParams.set('or', `(${predicate})`);
    query = query.or(predicate);
  }
  const rows = await runSupabaseQuery(query, url.toString());
  if (!Array.isArray(rows)) fail('Exact read did not return a row array.');
  for (const row of rows) {
    if (
      !isObject(row) ||
      row.id !== options.id ||
      row.version !== options.version ||
      !Object.hasOwn(row, 'user_id') ||
      !nullable(row.user_id, UUID) ||
      typeof row.state_code !== 'number' ||
      !Number.isSafeInteger(row.state_code) ||
      !(
        (row.state_code === 0 && row.user_id === options.readScope.actorUserId) ||
        (options.readScope.mode === 'public_or_owner_draft' &&
          row.state_code >= 100 &&
          row.state_code < 200)
      )
    )
      throw new CliError('Exact body read returned a row outside the permitted read scope.', {
        code: 'DATASET_READ_SCOPE_VIOLATION',
        exitCode: 1,
      });
  }
  return rows;
}

function empty(
  selection: DatasetExactReadSelection,
  status: DatasetExactReadRow['status'],
  reason: DatasetExactReadRow['reason'],
  observed: string,
): DatasetExactReadRow {
  return {
    selection,
    status,
    reason,
    observed_at_utc: observed,
    actual_row: null,
    actual_row_sha256: null,
    actual_payload_sha256: null,
    model_association: null,
  };
}
function nullable(value: unknown, pattern: RegExp): boolean {
  return value === null || (typeof value === 'string' && pattern.test(value));
}
function observe(
  rows: unknown[],
  selection: DatasetExactReadSelection,
  observed: string,
): DatasetExactReadRow {
  if (!rows.length) return empty(selection, 'absent', 'not_visible', observed);
  if (rows.length !== 1) return empty(selection, 'indeterminate', 'ambiguous_rows', observed);
  const row = rows[0];
  const fields = (selection.table === 'processes' ? PROCESS_COLUMNS : COMMON_COLUMNS).split(',');
  if (
    !isObject(row) ||
    fields.some((field) => !Object.hasOwn(row, field)) ||
    row.id !== selection.id ||
    row.version !== selection.version ||
    !nullable(row.user_id, UUID) ||
    !nullable(row.team_id, UUID) ||
    typeof row.state_code !== 'number' ||
    !Number.isSafeInteger(row.state_code) ||
    !isObject(row.json_ordered) ||
    (row.modified_at !== null &&
      (typeof row.modified_at !== 'string' || !Number.isFinite(Date.parse(row.modified_at)))) ||
    (selection.table === 'processes' &&
      (!nullable(row.model_id, UUID) ||
        !nullable(row.model_version, VERSION) ||
        !nullable(row.review_id, UUID) ||
        (row.model_id === null && row.model_version !== null)))
  )
    return empty(selection, 'indeterminate', 'invalid_or_incomplete_row', observed);
  const actual = Object.fromEntries(fields.map((field) => [field, row[field]]));
  const association: DatasetExactReadRow['model_association'] =
    selection.table === 'processes' && typeof actual.model_id === 'string'
      ? {
          model_id: actual.model_id,
          resolved_version:
            typeof actual.model_version === 'string' ? actual.model_version : selection.version,
          version_source:
            actual.model_version === null ? 'process_version_fallback' : 'model_version',
        }
      : null;
  const drift =
    selection.table === 'processes' &&
    Object.hasOwn(selection, 'expected_model') &&
    ((association?.model_id ?? null) !== (selection.expected_model?.id ?? null) ||
      (association?.resolved_version ?? null) !== (selection.expected_model?.version ?? null));
  return {
    selection,
    status: drift ? 'drift' : 'found',
    reason: drift ? 'model_association_changed' : 'exact_row',
    observed_at_utc: observed,
    actual_row: actual,
    actual_row_sha256: sha256Json(actual),
    actual_payload_sha256: sha256Json(actual.json_ordered),
    model_association: association,
  };
}

async function execute(options: RunDatasetReadExactOptions): Promise<DatasetReadExactReport> {
  if (typeof options.scopeSha256 !== 'string' || !SHA.test(options.scopeSha256))
    fail('Exact-read scope SHA-256 is invalid.');
  const timeout = options.timeoutMs ?? 10_000;
  if (!Number.isSafeInteger(timeout) || timeout < 1 || timeout > 2_147_483_647)
    fail('Exact-read timeout is invalid.');
  if (options.now && !Number.isFinite(options.now.getTime())) fail('Exact-read time is invalid.');
  const scopePath = path.resolve(options.scopeFile);
  const scopeFact = reconciliationFileFact(scopePath);
  if (scopeFact.sha256 !== options.scopeSha256) fail('Exact-read scope file hash does not match.');
  const scope = parseScope(JSON.parse(readFileSync(scopePath, 'utf8')));
  const scopePhysical = { ...scopeFact, path: realpathSync(scopePath) };
  const sources = scope.sources.map((source) => {
    const fact = reconciliationFileFact(source.path, false, SOURCE_MAX_BYTES);
    if (fact.sha256 !== source.sha256) fail('Exact-read provenance file hash does not match.');
    return { ...fact, path: realpathSync(source.path) };
  });
  if (new Set(sources.map((source) => source.path)).size !== sources.length)
    fail('Exact-read provenance files must be physically unique.');
  const facts = [
    scopeFact,
    scopePhysical,
    ...sources,
    ...scope.sources.map((source, index) => ({ ...sources[index]!, path: source.path })),
  ];
  assertReconciliationFiles(facts, SOURCE_MAX_BYTES);
  const output = reconciliationOutputDirectory(
    options.outDir,
    resolveFlowIdentityApprovalClaimRoot({ env: options.env }),
  );
  const {
    identity: receipt,
    runtime,
    assertCurrentActor,
  } = await createDatasetReadSession({
    env: options.env,
    fetchImpl: options.fetchImpl,
    cliVersion: loadCliPackageVersion(new URL('../cli.js', import.meta.url).href),
    expectedProjectRef: scope.project_ref,
    expectedUserId: scope.actor.user_id,
    ...(scope.actor.email !== undefined ? { expectedEmail: scope.actor.email } : {}),
    timeoutMs: timeout,
    now: options.now,
  });
  await assertCurrentActor();
  const { client, restBaseUrl } = createSupabaseDataClient(runtime, options.fetchImpl, timeout);
  const observations = new Map<string, DatasetExactReadRow>();
  const timestamp = () => (options.now ?? new Date()).toISOString();
  for (const table of ['processes', 'lifecyclemodels'] as const) {
    for (const selection of scope.rows.filter((row) => row.table === table)) {
      let observation: DatasetExactReadRow;
      if (
        selection.table === 'lifecyclemodels' &&
        selection.referenced_by.some((reference) => {
          const process = observations.get(`processes:${identityKey(reference)}`);
          return (
            process?.status !== 'found' ||
            process.model_association?.model_id !== selection.id ||
            process.model_association.resolved_version !== selection.version
          );
        })
      )
        observation = empty(selection, 'drift', 'referenced_process_drift', timestamp());
      else {
        try {
          observation = observe(
            await readExactRows({
              client,
              restBaseUrl,
              table: selection.table,
              id: selection.id,
              version: selection.version,
              columns: selection.table === 'processes' ? PROCESS_COLUMNS : COMMON_COLUMNS,
              readScope: { mode: 'public_or_owner_draft', actorUserId: scope.actor.user_id },
            }),
            selection,
            timestamp(),
          );
        } catch (error) {
          observation = empty(
            selection,
            'indeterminate',
            isReadScopeViolation(error) ? 'protocol_scope_violation' : 'read_failed',
            timestamp(),
          );
        }
      }
      observations.set(`${selection.table}:${identityKey(selection)}`, observation);
    }
  }
  await assertCurrentActor();
  assertReconciliationFiles(facts, SOURCE_MAX_BYTES);
  const rows = scope.rows.map((selection) =>
    observations.get(`${selection.table}:${identityKey(selection)}`)!,
  );
  const report: DatasetReadExactReport = {
    schema_version: 'dataset-exact-read-report.v1',
    mode: 'read_only',
    status: rows.some((row) => row.status === 'indeterminate')
      ? 'indeterminate'
      : rows.some((row) => row.status === 'drift')
        ? 'drift'
        : 'observed',
    authorization_granted: false,
    adoption_performed: false,
    read_scope: 'public_100_199_or_current_owner_state_0',
    identity: receipt,
    scope: scopePhysical,
    sources,
    rows,
  };
  writeDraftReconciliation(output, report, 'read-exact.json');
  return report;
}

export async function runDatasetReadExact(
  options: RunDatasetReadExactOptions,
): Promise<DatasetReadExactReport> {
  try {
    return await execute(options);
  } catch (error) {
    if (error instanceof CliError && error.code === 'DATASET_EXACT_READ_INVALID') throw error;
    throw new CliError('Exact-read capture failed; no completed observation was published.', {
      code: 'DATASET_EXACT_READ_FAILED',
      exitCode: 1,
    });
  }
}
