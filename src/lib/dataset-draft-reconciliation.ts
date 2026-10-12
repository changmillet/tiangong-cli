import { createHash } from 'node:crypto';
import {
  constants,
  existsSync,
  closeSync,
  fstatSync,
  fsyncSync,
  lstatSync,
  mkdirSync,
  openSync,
  readFileSync,
  realpathSync,
  statSync,
  writeFileSync,
  type BigIntStats,
} from 'node:fs';
import path from 'node:path';
import { sha256Json } from './dataset-maintenance-contract.js';
import { CliError } from './errors.js';

type JsonObject = Record<string, unknown>;
export type DraftObservationStatus =
  'equal_eligible' | 'different' | 'foreign' | 'ineligible' | 'absent' | 'indeterminate';
export type DraftObservedRow = {
  id: string;
  version: string;
  user_id: string;
  state_code: number;
  json_ordered: JsonObject;
  model_id?: string | null;
  model_version?: string | null;
  team_id?: string | null;
  review_id?: string | null;
};
export type DraftObservation = {
  status: DraftObservationStatus;
  reason:
    | 'equal_current_owner_draft'
    | 'different_body'
    | 'foreign_owner'
    | 'ineligible_state_or_metadata'
    | 'not_visible'
    | 'read_failed'
    | 'protocol_scope_violation'
    | 'invalid_or_incomplete_row'
    | 'ambiguous_rows';
  actual_row: DraftObservedRow | null;
  actual_row_sha256: string | null;
  actual_payload_sha256: string | null;
};
export type ReconciliationFileFact = { path: string; sha256: string | null; bytes: number | null };

export function reconciliationError(message: string): never {
  throw new CliError(message, { code: 'DATASET_DRAFT_RECONCILIATION_INVALID', exitCode: 2 });
}

export function reconciliationFileFact(
  file: string,
  optional = false,
  maxBytes = 32 * 1024 * 1024,
): ReconciliationFileFact {
  let descriptor: number;
  let entry: BigIntStats;
  try {
    entry = lstatSync(file, { bigint: true });
    if (!entry.isFile())
      reconciliationError('Reconciliation input must be a bounded regular file.');
    descriptor = openSync(file, constants.O_RDONLY | constants.O_NOFOLLOW);
  } catch (error) {
    if (optional && (error as NodeJS.ErrnoException).code === 'ENOENT')
      return { path: file, sha256: null, bytes: null };
    throw error;
  }
  try {
    const stat = fstatSync(descriptor, { bigint: true });
    if (
      !stat.isFile() ||
      stat.dev !== entry.dev ||
      stat.ino !== entry.ino ||
      stat.size > BigInt(maxBytes)
    )
      reconciliationError('Reconciliation input must be a bounded regular file.');
    const bytes = readFileSync(descriptor);
    if (
      BigInt(bytes.length) !== stat.size ||
      fstatSync(descriptor, { bigint: true }).mtimeNs !== stat.mtimeNs
    )
      reconciliationError('Reconciliation input changed during observation.');
    return {
      path: file,
      sha256: createHash('sha256').update(bytes).digest('hex'),
      bytes: bytes.length,
    };
  } finally {
    closeSync(descriptor);
  }
}

export function assertReconciliationFiles(
  facts: readonly ReconciliationFileFact[],
  maxBytes = 32 * 1024 * 1024,
): void {
  for (const fact of facts) {
    if (
      sha256Json(reconciliationFileFact(fact.path, fact.sha256 === null, maxBytes)) !==
      sha256Json(fact)
    )
      reconciliationError(
        'Original input, contract, intent or execution evidence changed during reconciliation.',
      );
  }
}

export function reconciliationOutputDirectory(output: string, ledgerRoot: string): string {
  const resolved = path.resolve(output);
  const physical = path.join(realpathSync(path.dirname(resolved)), path.basename(resolved));
  let ancestor = path.resolve(ledgerRoot);
  const missing: string[] = [];
  let physicalLedger: string;
  for (;;) {
    try {
      const physicalAncestor = realpathSync(ancestor);
      if (!statSync(physicalAncestor).isDirectory())
        reconciliationError('Reconciliation ledger ancestor must be a directory.');
      physicalLedger = path.join(physicalAncestor, ...missing.reverse());
      break;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
      missing.push(path.basename(ancestor));
      ancestor = path.dirname(ancestor);
    }
  }
  // A read-only observation must never create anything inside the existing execution registry.
  // Reserve normalized case aliases even when the current volume distinguishes them.
  const outputKey = physical.normalize('NFC').toUpperCase();
  const ledgerKey = physicalLedger.normalize('NFC').toUpperCase();
  if (outputKey === ledgerKey || outputKey.startsWith(ledgerKey + path.sep))
    reconciliationError('Reconciliation output must be outside the execution ledger.');
  if (existsSync(physical))
    reconciliationError('Reconciliation output already exists; select a new evidence directory.');
  return physical;
}

function isObject(value: unknown): value is JsonObject {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/iu;
const VERSION = /^\d{2}\.\d{2}\.\d{3}$/u;

export function observeDraftRows(
  rows: unknown[],
  expected: {
    id: string;
    version: string;
    user_id: string;
    desired_sha256: string;
    process: boolean;
  },
): DraftObservation {
  const empty = (
    status: DraftObservationStatus,
    reason: DraftObservation['reason'],
  ): DraftObservation => ({
    status,
    reason,
    actual_row: null,
    actual_row_sha256: null,
    actual_payload_sha256: null,
  });
  if (rows.length === 0) return empty('absent', 'not_visible');
  if (rows.length !== 1) return empty('indeterminate', 'ambiguous_rows');
  const row = rows[0];
  if (
    !isObject(row) ||
    row.id !== expected.id ||
    row.version !== expected.version ||
    typeof row.user_id !== 'string' ||
    !UUID.test(row.user_id) ||
    typeof row.state_code !== 'number' ||
    !Number.isSafeInteger(row.state_code) ||
    !isObject(row.json_ordered)
  )
    return empty('indeterminate', 'invalid_or_incomplete_row');
  if (expected.process) {
    for (const key of ['model_id', 'model_version', 'team_id', 'review_id']) {
      const value = row[key];
      if (
        value !== null &&
        (typeof value !== 'string' || !(key === 'model_version' ? VERSION : UUID).test(value))
      )
        return empty('indeterminate', 'invalid_or_incomplete_row');
    }
  }
  const actual: DraftObservedRow = {
    id: expected.id,
    version: expected.version,
    user_id: row.user_id,
    state_code: row.state_code,
    json_ordered: row.json_ordered,
    ...(expected.process
      ? {
          model_id: row.model_id as string | null,
          model_version: row.model_version as string | null,
          team_id: row.team_id as string | null,
          review_id: row.review_id as string | null,
        }
      : {}),
  };
  const bodyHash = sha256Json(actual.json_ordered);
  const status: DraftObservationStatus =
    actual.user_id !== expected.user_id
      ? 'foreign'
      : actual.state_code !== 0 ||
          (expected.process &&
            [actual.model_id, actual.model_version, actual.team_id, actual.review_id].some(
              (value) => value !== null,
            ))
        ? 'ineligible'
        : bodyHash !== expected.desired_sha256
          ? 'different'
          : 'equal_eligible';
  const reason = {
    foreign: 'foreign_owner',
    ineligible: 'ineligible_state_or_metadata',
    different: 'different_body',
    equal_eligible: 'equal_current_owner_draft',
  } as const;
  return {
    status,
    reason: reason[status],
    actual_row: actual,
    actual_row_sha256: sha256Json(actual),
    actual_payload_sha256: bodyHash,
  };
}

export function writeDraftReconciliation(
  output: string,
  report: unknown,
  filename: 'reconciliation.json' | 'read-exact.json' = 'reconciliation.json',
): void {
  const text = JSON.stringify(report, null, 2) + '\n';
  if (Buffer.byteLength(text) > 32 * 1024 * 1024)
    reconciliationError('Reconciliation evidence exceeds its size bound.');
  // Each invocation publishes a new immutable observation; never overwrite a prior observation.
  mkdirSync(output, { mode: 0o700 });
  const descriptor = openSync(path.join(output, filename), 'wx', 0o600);
  try {
    writeFileSync(descriptor, text, 'utf8');
    fsyncSync(descriptor);
  } finally {
    closeSync(descriptor);
  }
}
