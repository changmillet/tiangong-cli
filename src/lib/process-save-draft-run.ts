import { existsSync, readFileSync } from 'node:fs';
import path from 'node:path';
import { writeJsonArtifact, writeJsonLinesArtifact } from './artifacts.js';
import { CliError } from './errors.js';
import type { FetchLike } from './http.js';
import { readJsonInput } from './io.js';
import {
  summarizeProcessPayloadValidation,
  validateProcessPayload,
  type ProcessPayloadValidationResult,
} from './process-payload-validation.js';
import {
  collectPublishInputs,
  normalizePublishRequest,
  type PublishCollectedDatasetEntry,
  type PublishCollectedOrigin,
} from './publish.js';
import {
  syncStateAwareProcessRecord,
  type ProcessStateAwareWriteResult,
} from './process-save-draft.js';
import { buildRunId, resolveRunLayout } from './run.js';

type JsonObject = Record<string, unknown>;

function isRecord(value: unknown): value is JsonObject {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function first_non_empty(...values: unknown[]): string | null {
  for (const value of values) {
    if (typeof value === 'string' && value.trim()) {
      return value.trim();
    }
  }
  return null;
}

function resolve_path(baseDir: string, value: string): string {
  return path.resolve(baseDir, value);
}

function serialize_error(error: unknown): { message: string } {
  if (error instanceof Error) {
    return { message: error.message };
  }
  return { message: String(error) };
}

function readJsonLinesInput(inputPath: string): JsonObject[] {
  const resolved = path.resolve(inputPath);
  if (!existsSync(resolved)) {
    throw new CliError(`Input file not found: ${resolved}`, {
      code: 'INPUT_NOT_FOUND',
      exitCode: 2,
    });
  }

  return readFileSync(resolved, 'utf8')
    .split(/\r?\n/u)
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line, index) => {
      try {
        const parsed = JSON.parse(line) as unknown;
        if (!isRecord(parsed)) {
          throw new CliError(
            `Expected JSON object rows in JSONL input: ${resolved} (line ${index + 1})`,
            {
              code: 'INPUT_INVALID_JSONL_ROW',
              exitCode: 2,
            },
          );
        }
        return parsed;
      } catch (error) {
        if (error instanceof CliError) {
          throw error;
        }
        throw new CliError(`Input file contains invalid JSONL at line ${index + 1}: ${resolved}`, {
          code: 'INPUT_INVALID_JSONL',
          exitCode: 2,
          details: String(error),
        });
      }
    });
}

function readStructuredInput(inputPath: string): unknown {
  return inputPath.toLowerCase().endsWith('.jsonl')
    ? readJsonLinesInput(inputPath)
    : readJsonInput(inputPath);
}

function looksLikePublishRequest(value: unknown): boolean {
  return (
    isRecord(value) &&
    ('inputs' in value || 'publish' in value || 'out_dir' in value || 'output_dir' in value)
  );
}

function loadProcessPayloadFromDatasetEntry(entry: unknown, baseDir: string): JsonObject {
  if (isRecord(entry)) {
    for (const key of ['json_ordered', 'jsonOrdered', 'payload', 'process'] as const) {
      const candidate = entry[key];
      if (isRecord(candidate)) {
        return candidate;
      }
    }

    const fileValue = first_non_empty(entry.file, entry.path);
    if (fileValue) {
      const filePath = resolve_path(baseDir, fileValue);
      const loaded = readJsonInput(filePath);
      if (!isRecord(loaded)) {
        throw new CliError(`Expected JSON object input: ${filePath}`, {
          code: 'PROCESS_SAVE_DRAFT_INPUT_NOT_OBJECT',
          exitCode: 2,
        });
      }
      return loaded;
    }

    return entry;
  }

  if (typeof entry === 'string' && entry.trim()) {
    const filePath = resolve_path(baseDir, entry);
    const loaded = readJsonInput(filePath);
    if (!isRecord(loaded)) {
      throw new CliError(`Expected JSON object input: ${filePath}`, {
        code: 'PROCESS_SAVE_DRAFT_INPUT_NOT_OBJECT',
        exitCode: 2,
      });
    }
    return loaded;
  }

  throw new CliError('Unsupported process save-draft dataset entry.', {
    code: 'PROCESS_SAVE_DRAFT_UNSUPPORTED_ENTRY',
    exitCode: 2,
    details: entry,
  });
}

function extractProcessIdentity(payload: JsonObject): [string, string] {
  const root = isRecord(payload.processDataSet) ? payload.processDataSet : payload;
  const processInformation = isRecord(root.processInformation) ? root.processInformation : {};
  const dataSetInformation = isRecord(processInformation.dataSetInformation)
    ? processInformation.dataSetInformation
    : {};
  const administrativeInformation = isRecord(root.administrativeInformation)
    ? root.administrativeInformation
    : {};
  const publicationAndOwnership = isRecord(administrativeInformation.publicationAndOwnership)
    ? administrativeInformation.publicationAndOwnership
    : {};
  const datasetId = first_non_empty(dataSetInformation['common:UUID']);
  const version = first_non_empty(publicationAndOwnership['common:dataSetVersion'], '01.01.000')!;

  if (!datasetId) {
    throw new CliError(
      'Process payload missing processInformation.dataSetInformation.common:UUID.',
      {
        code: 'PUBLISH_PROCESS_ID_MISSING',
        exitCode: 2,
      },
    );
  }

  return [datasetId, version];
}

function processPayloadFromRow(row: JsonObject): JsonObject {
  if (isRecord(row.json_ordered)) {
    return row.json_ordered;
  }

  if (isRecord(row.json)) {
    return row.json;
  }

  if (isRecord(row.process)) {
    return row.process;
  }

  return row;
}

export type ProcessSaveDraftSource = 'rows_file' | 'bundle' | 'input';

export type ProcessSaveDraftCandidate = {
  id: string | null;
  version: string | null;
  source: ProcessSaveDraftSource;
  bundle_path: string | null;
  payload: JsonObject;
  validation?: ProcessPayloadValidationResult;
  error?: { message: string };
};

export type ProcessSaveDraftProcessReport = {
  id: string | null;
  version: string | null;
  source: ProcessSaveDraftSource;
  bundle_path: string | null;
  status: 'prepared' | 'executed' | 'failed';
  validation?: ProcessPayloadValidationResult;
  execution?: ProcessStateAwareWriteResult;
  error?: { message: string };
};

export type ProcessSaveDraftReport = {
  generated_at_utc: string;
  input_path: string;
  input_kind: 'rows_file' | 'publish_request';
  out_dir: string;
  commit: boolean;
  mode: 'dry_run' | 'commit';
  target_user_id: string | null;
  account_guard: {
    target_user_id_required: boolean;
    target_user_id: string | null;
    commit_account_binding: 'current_cli_auth_session';
    post_write_verify_required: boolean;
  };
  status: 'completed' | 'completed_with_failures';
  counts: {
    selected: number;
    prepared: number;
    executed: number;
    failed: number;
  };
  files: {
    normalized_input: string;
    selected_processes: string;
    progress_jsonl: string;
    failures_jsonl: string;
    summary_json: string;
  };
  processes: ProcessSaveDraftProcessReport[];
};

export type RunProcessSaveDraftOptions = {
  inputPath: string;
  outDir?: string | null;
  commit?: boolean | null;
  rawInput?: unknown;
  env?: NodeJS.ProcessEnv;
  fetchImpl?: FetchLike;
  timeoutMs?: number;
  now?: Date;
  targetUserId?: string | null;
  validateProcessPayloadImpl?: (payload: JsonObject) => ProcessPayloadValidationResult;
};

type PreparedProcessSaveDraftInput = {
  inputKind: 'rows_file' | 'publish_request';
  normalizedInput: unknown;
  processes: ProcessSaveDraftCandidate[];
};

function candidateFromPayload(
  payload: JsonObject,
  source: ProcessSaveDraftSource,
  bundlePath: string | null,
  validateProcessPayloadImpl: (payload: JsonObject) => ProcessPayloadValidationResult,
): ProcessSaveDraftCandidate {
  try {
    const [id, version] = extractProcessIdentity(payload);
    const validation = validateProcessPayloadImpl(payload);
    if (!validation.ok) {
      return {
        id,
        version,
        source,
        bundle_path: bundlePath,
        payload,
        validation,
        error: {
          message: summarizeProcessPayloadValidation(validation),
        },
      };
    }
    return {
      id,
      version,
      source,
      bundle_path: bundlePath,
      payload,
      validation,
    };
  } catch (error) {
    return {
      id: first_non_empty(payload['@id'], payload.id),
      version: first_non_empty(payload['@version'], payload.version, '01.01.000'),
      source,
      bundle_path: bundlePath,
      payload,
      error:
        error instanceof CliError && error.code === 'PUBLISH_PROCESS_ID_MISSING'
          ? {
              message:
                'Payload is not a canonical processDataSet wrapper; process save-draft only supports canonical process datasets.',
            }
          : serialize_error(error),
    };
  }
}

function sourceFromPublishOrigin(origin: PublishCollectedOrigin): ProcessSaveDraftSource {
  return origin.source === 'bundle' ? 'bundle' : 'input';
}

function candidateFromPublishEntry(
  entry: PublishCollectedDatasetEntry,
  validateProcessPayloadImpl: (payload: JsonObject) => ProcessPayloadValidationResult,
): ProcessSaveDraftCandidate {
  return candidateFromPayload(
    loadProcessPayloadFromDatasetEntry(entry.entry, entry.origin.base_dir),
    sourceFromPublishOrigin(entry.origin),
    entry.origin.bundle_path,
    validateProcessPayloadImpl,
  );
}

function prepareRowsFileInput(
  inputPath: string,
  rawInput: unknown,
  validateProcessPayloadImpl: (payload: JsonObject) => ProcessPayloadValidationResult,
): PreparedProcessSaveDraftInput {
  const rows = Array.isArray(rawInput) ? rawInput : [rawInput];
  const normalizedRows = rows.map((row, index) => {
    if (!isRecord(row)) {
      throw new CliError(
        `Expected JSON object rows in process save-draft input: ${inputPath} (index ${index})`,
        {
          code: 'PROCESS_SAVE_DRAFT_INVALID_ROW',
          exitCode: 2,
        },
      );
    }
    return row;
  });

  return {
    inputKind: 'rows_file',
    normalizedInput: {
      input_kind: 'rows_file',
      input_path: inputPath,
      row_count: normalizedRows.length,
    },
    processes: normalizedRows.map((row) =>
      candidateFromPayload(
        processPayloadFromRow(row),
        'rows_file',
        null,
        validateProcessPayloadImpl,
      ),
    ),
  };
}

function preparePublishRequestInput(
  inputPath: string,
  rawInput: unknown,
  commit: boolean,
  now: Date,
  validateProcessPayloadImpl: (payload: JsonObject) => ProcessPayloadValidationResult,
): PreparedProcessSaveDraftInput {
  const normalized = normalizePublishRequest(rawInput, {
    requestPath: inputPath,
    commitOverride: commit,
    now,
  });
  const collected = collectPublishInputs(normalized, path.dirname(inputPath));

  return {
    inputKind: 'publish_request',
    normalizedInput: normalized,
    processes: collected.processes.map((entry) =>
      candidateFromPublishEntry(entry, validateProcessPayloadImpl),
    ),
  };
}

function prepareProcessSaveDraftInput(
  inputPath: string,
  rawInput: unknown,
  commit: boolean,
  now: Date,
  validateProcessPayloadImpl: (payload: JsonObject) => ProcessPayloadValidationResult,
): PreparedProcessSaveDraftInput {
  return looksLikePublishRequest(rawInput)
    ? preparePublishRequestInput(inputPath, rawInput, commit, now, validateProcessPayloadImpl)
    : prepareRowsFileInput(inputPath, rawInput, validateProcessPayloadImpl);
}

function defaultOutDir(inputPath: string, commit: boolean, now: Date): string {
  const runId = buildRunId({
    namespace: 'process_save_draft',
    operation: commit ? 'commit' : 'dry_run',
    now,
  });
  return resolveRunLayout(
    path.join(path.dirname(inputPath), 'artifacts'),
    'process_save_draft',
    runId,
  ).runRoot;
}

function resolveOutDir(
  inputPath: string,
  outDir: string | null | undefined,
  commit: boolean,
  now: Date,
): string {
  return outDir ? path.resolve(outDir) : defaultOutDir(inputPath, commit, now);
}

function buildFiles(outDir: string): ProcessSaveDraftReport['files'] {
  const outputDir = path.join(outDir, 'outputs', 'save-draft-rpc');
  return {
    normalized_input: path.join(outDir, 'inputs', 'normalized-input.json'),
    selected_processes: path.join(outputDir, 'selected-processes.jsonl'),
    progress_jsonl: path.join(outputDir, 'progress.jsonl'),
    failures_jsonl: path.join(outputDir, 'failures.jsonl'),
    summary_json: path.join(outputDir, 'summary.json'),
  };
}

function compactCandidate(candidate: ProcessSaveDraftCandidate): JsonObject {
  return {
    id: candidate.id,
    version: candidate.version,
    source: candidate.source,
    bundle_path: candidate.bundle_path,
    payload: candidate.payload,
    ...(candidate.validation ? { validation: candidate.validation } : {}),
    ...(candidate.error ? { error: candidate.error } : {}),
  };
}

export async function runProcessSaveDraft(
  options: RunProcessSaveDraftOptions,
): Promise<ProcessSaveDraftReport> {
  const now = options.now ?? new Date();
  const inputPath = path.resolve(options.inputPath);
  const commit = options.commit === true;
  const targetUserId = first_non_empty(options.targetUserId);
  const rawInput = options.rawInput ?? readStructuredInput(inputPath);
  const validateProcessPayloadImpl = options.validateProcessPayloadImpl ?? validateProcessPayload;
  const prepared = prepareProcessSaveDraftInput(
    inputPath,
    rawInput,
    commit,
    now,
    validateProcessPayloadImpl,
  );
  const outDir = resolveOutDir(inputPath, options.outDir, commit, now);
  const files = buildFiles(outDir);

  if (commit && (!options.env || !options.fetchImpl)) {
    throw new CliError('Process save-draft commit requires env and fetch runtime bindings.', {
      code: 'PROCESS_SAVE_DRAFT_RUNTIME_REQUIRED',
      exitCode: 2,
    });
  }

  writeJsonArtifact(files.normalized_input, prepared.normalizedInput);
  writeJsonLinesArtifact(files.selected_processes, prepared.processes.map(compactCandidate));

  const reports: ProcessSaveDraftProcessReport[] = [];
  for (const candidate of prepared.processes) {
    const report: ProcessSaveDraftProcessReport = {
      id: candidate.id,
      version: candidate.version,
      source: candidate.source,
      bundle_path: candidate.bundle_path,
      status: 'prepared',
      ...(candidate.validation ? { validation: candidate.validation } : {}),
    };

    if (candidate.error) {
      report.status = 'failed';
      report.error = candidate.error;
      reports.push(report);
      continue;
    }

    if (!commit) {
      reports.push(report);
      continue;
    }

    try {
      report.execution = await syncStateAwareProcessRecord({
        id: candidate.id!,
        version: candidate.version!,
        payload: candidate.payload,
        env: options.env!,
        fetchImpl: options.fetchImpl!,
        timeoutMs: options.timeoutMs,
        targetUserId,
        audit: {
          command: 'tiangong-lca process save-draft',
          source: candidate.source,
          bundle_path: candidate.bundle_path,
          target_user_id: targetUserId,
        },
      });
      report.status = 'executed';
    } catch (error) {
      report.status = 'failed';
      report.error = serialize_error(error);
    }

    reports.push(report);
  }

  const failedReports = reports.filter((report) => report.status === 'failed');
  writeJsonLinesArtifact(files.progress_jsonl, reports);
  writeJsonLinesArtifact(files.failures_jsonl, failedReports);

  const report: ProcessSaveDraftReport = {
    generated_at_utc: now.toISOString(),
    input_path: inputPath,
    input_kind: prepared.inputKind,
    out_dir: outDir,
    commit,
    mode: commit ? 'commit' : 'dry_run',
    target_user_id: targetUserId,
    account_guard: {
      target_user_id_required: Boolean(targetUserId),
      target_user_id: targetUserId,
      commit_account_binding: 'current_cli_auth_session',
      post_write_verify_required: Boolean(targetUserId),
    },
    status: failedReports.length > 0 ? 'completed_with_failures' : 'completed',
    counts: {
      selected: prepared.processes.length,
      prepared: reports.filter((entry) => entry.status === 'prepared').length,
      executed: reports.filter((entry) => entry.status === 'executed').length,
      failed: failedReports.length,
    },
    files,
    processes: reports,
  };

  writeJsonArtifact(files.summary_json, report);
  return report;
}
