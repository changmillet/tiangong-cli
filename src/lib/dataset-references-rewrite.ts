import path from 'node:path';
import { writeJsonArtifact, writeJsonLinesArtifact } from './artifacts.js';
import { CliError } from './errors.js';
import type { FetchLike } from './http.js';
import {
  cloneJson,
  firstNonEmpty,
  isRecord,
  materializeDatasetRows,
  unwrapDatasetPayload,
  type DatasetKind,
  type JsonObject,
} from './dataset-local.js';
import {
  runLifecyclemodelSaveDraft,
  type LifecyclemodelSaveDraftReport,
} from './lifecyclemodel-save-draft-run.js';
import { runProcessSaveDraft, type ProcessSaveDraftReport } from './process-save-draft-run.js';

type ReferenceKind = 'flow';

type ParsedReference = {
  kind: ReferenceKind;
  id: string;
  version: string | null;
};

export type DatasetReferencesRewriteChange = {
  row_index: number;
  dataset_type: DatasetKind | null;
  dataset_id: string | null;
  dataset_version: string | null;
  path: string;
  field: '@refObjectId' | '@version' | '@flowUUID';
  before: string | null;
  after: string | null;
};

export type DatasetReferencesRewriteReport = {
  generated_at_utc: string;
  input_path: string;
  out_dir: string;
  mode: 'dry_run' | 'commit';
  status: 'completed' | 'completed_with_failures';
  from: ParsedReference;
  to: ParsedReference;
  filters: {
    types: DatasetKind[];
    scope: string | null;
  };
  counts: {
    input_rows: number;
    patched_rows: number;
    changes: number;
    process_rows: number;
    lifecyclemodel_rows: number;
  };
  files: {
    patched_rows: string;
    rewrite_plan: string;
    summary: string;
  };
  changes: DatasetReferencesRewriteChange[];
  commit_reports: {
    processes: ProcessSaveDraftReport | null;
    lifecyclemodels: LifecyclemodelSaveDraftReport | null;
  };
};

export type RunDatasetReferencesRewriteOptions = {
  inputPath: string;
  outDir: string;
  from: string;
  to: string;
  types?: string[] | null;
  scope?: string | null;
  commit?: boolean | null;
  rawInput?: unknown;
  env?: NodeJS.ProcessEnv;
  fetchImpl?: FetchLike;
  now?: Date;
  runProcessSaveDraftImpl?: typeof runProcessSaveDraft;
  runLifecyclemodelSaveDraftImpl?: typeof runLifecyclemodelSaveDraft;
};

function parseReference(value: string, label: string): ParsedReference {
  const match = /^(flow):([^@\s]+)(?:@([^@\s]+))?$/u.exec(value.trim());
  if (!match) {
    throw new CliError(`Expected ${label} reference like flow:<id>@<version>.`, {
      code: 'DATASET_REFERENCE_INVALID',
      exitCode: 2,
      details: value,
    });
  }
  return {
    kind: 'flow',
    id: match[2],
    version: match[3] ?? null,
  };
}

function normalizeTypes(values: string[] | null | undefined): DatasetKind[] {
  const raw = values && values.length > 0 ? values.flatMap((value) => value.split(',')) : [];
  const normalized = raw
    .map((value) => value.trim().toLowerCase())
    .filter(Boolean)
    .map((value) => {
      if (value === 'process' || value === 'processes') {
        return 'process' as const;
      }
      if (
        value === 'lifecyclemodel' ||
        value === 'lifecyclemodels' ||
        value === 'model' ||
        value === 'models'
      ) {
        return 'lifecyclemodel' as const;
      }
      throw new CliError('Expected --type or --types to contain process and/or lifecyclemodel.', {
        code: 'DATASET_REFERENCE_TYPES_INVALID',
        exitCode: 2,
        details: value,
      });
    });
  return normalized.length > 0 ? [...new Set(normalized)] : ['process', 'lifecyclemodel'];
}

function joinPath(base: string, key: string | number): string {
  return base ? `${base}.${String(key)}` : String(key);
}

function rewriteProcessReferences(
  value: unknown,
  from: ParsedReference,
  to: ParsedReference,
  pathPrefix: string,
  change: (
    path: string,
    field: '@refObjectId' | '@version',
    before: string | null,
    after: string | null,
  ) => void,
): void {
  if (Array.isArray(value)) {
    value.forEach((item, index) =>
      rewriteProcessReferences(item, from, to, joinPath(pathPrefix, index), change),
    );
    return;
  }
  if (!isRecord(value)) {
    return;
  }

  const reference = value.referenceToFlowDataSet;
  if (isRecord(reference) && reference['@refObjectId'] === from.id) {
    const versionMatches = !from.version || reference['@version'] === from.version;
    if (versionMatches) {
      if (reference['@refObjectId'] !== to.id) {
        change(
          joinPath(pathPrefix, 'referenceToFlowDataSet.@refObjectId'),
          '@refObjectId',
          reference['@refObjectId'],
          to.id,
        );
        reference['@refObjectId'] = to.id;
      }
      if (to.version && reference['@version'] !== to.version) {
        change(
          joinPath(pathPrefix, 'referenceToFlowDataSet.@version'),
          '@version',
          typeof reference['@version'] === 'string' ? reference['@version'] : null,
          to.version,
        );
        reference['@version'] = to.version;
      }
    }
  }

  Object.entries(value).forEach(([key, child]) =>
    rewriteProcessReferences(child, from, to, joinPath(pathPrefix, key), change),
  );
}

function rewriteLifecyclemodelReferences(
  value: unknown,
  from: ParsedReference,
  to: ParsedReference,
  pathPrefix: string,
  change: (path: string, field: '@flowUUID', before: string | null, after: string | null) => void,
): void {
  if (Array.isArray(value)) {
    value.forEach((item, index) =>
      rewriteLifecyclemodelReferences(item, from, to, joinPath(pathPrefix, index), change),
    );
    return;
  }
  if (!isRecord(value)) {
    return;
  }

  if (value['@flowUUID'] === from.id && value['@flowUUID'] !== to.id) {
    change(joinPath(pathPrefix, '@flowUUID'), '@flowUUID', value['@flowUUID'], to.id);
    value['@flowUUID'] = to.id;
  }

  Object.entries(value).forEach(([key, child]) =>
    rewriteLifecyclemodelReferences(child, from, to, joinPath(pathPrefix, key), change),
  );
}

function buildFiles(outDir: string): DatasetReferencesRewriteReport['files'] {
  return {
    patched_rows: path.join(outDir, 'outputs', 'patched-rows.jsonl'),
    rewrite_plan: path.join(outDir, 'outputs', 'rewrite-plan.json'),
    summary: path.join(outDir, 'outputs', 'summary.json'),
  };
}

export async function runDatasetReferencesRewrite(
  options: RunDatasetReferencesRewriteOptions,
): Promise<DatasetReferencesRewriteReport> {
  if (!options.outDir) {
    throw new CliError('Missing required --out-dir value.', {
      code: 'DATASET_REFERENCES_REWRITE_OUT_DIR_REQUIRED',
      exitCode: 2,
    });
  }

  const from = parseReference(options.from, '--from');
  const to = parseReference(options.to, '--to');

  const types = normalizeTypes(options.types);
  const rows = materializeDatasetRows(options.inputPath, options.rawInput);
  const outDir = path.resolve(options.outDir);
  const files = buildFiles(outDir);
  const changes: DatasetReferencesRewriteChange[] = [];
  const patchedRows: JsonObject[] = [];
  const processRows: JsonObject[] = [];
  const lifecyclemodelRows: JsonObject[] = [];

  rows.forEach((row) => {
    const patched = cloneJson(row.row);
    if (row.kind && types.includes(row.kind)) {
      const payload = unwrapDatasetPayload(patched);
      const addChange = (
        pathValue: string,
        field: '@refObjectId' | '@version' | '@flowUUID',
        before: string | null,
        after: string | null,
      ): void => {
        changes.push({
          row_index: row.index,
          dataset_type: row.kind,
          dataset_id: row.id,
          dataset_version: row.version,
          path: pathValue,
          field,
          before,
          after,
        });
      };
      const beforeCount = changes.length;
      if (row.kind === 'process') {
        rewriteProcessReferences(payload, from, to, '', addChange);
      } else if (row.kind === 'lifecyclemodel') {
        rewriteLifecyclemodelReferences(payload, from, to, '', addChange);
      }
      if (changes.length > beforeCount) {
        if (row.kind === 'process') {
          processRows.push(patched);
        } else if (row.kind === 'lifecyclemodel') {
          lifecyclemodelRows.push(patched);
        }
      }
    }
    patchedRows.push(patched);
  });

  writeJsonLinesArtifact(files.patched_rows, patchedRows);
  writeJsonArtifact(files.rewrite_plan, {
    from,
    to,
    filters: {
      types,
      scope: firstNonEmpty(options.scope),
    },
    changes,
  });

  const commit = options.commit === true;
  const processSaveDraftImpl = options.runProcessSaveDraftImpl ?? runProcessSaveDraft;
  const lifecyclemodelSaveDraftImpl =
    options.runLifecyclemodelSaveDraftImpl ?? runLifecyclemodelSaveDraft;
  const processReport =
    commit && processRows.length > 0
      ? await processSaveDraftImpl({
          inputPath: files.patched_rows,
          rawInput: processRows,
          outDir: path.join(outDir, 'process-save-draft'),
          commit: true,
          env: options.env,
          fetchImpl: options.fetchImpl,
        })
      : null;
  const lifecyclemodelReport =
    commit && lifecyclemodelRows.length > 0
      ? await lifecyclemodelSaveDraftImpl({
          inputPath: files.patched_rows,
          rawInput: lifecyclemodelRows,
          outDir: path.join(outDir, 'lifecyclemodel-save-draft'),
          commit: true,
          env: options.env,
          fetchImpl: options.fetchImpl,
        })
      : null;

  const commitFailed =
    processReport?.status === 'completed_with_failures' ||
    lifecyclemodelReport?.status === 'completed_with_failures';
  const report: DatasetReferencesRewriteReport = {
    generated_at_utc: (options.now ?? new Date()).toISOString(),
    input_path: path.resolve(options.inputPath),
    out_dir: outDir,
    mode: commit ? 'commit' : 'dry_run',
    status: commitFailed ? 'completed_with_failures' : 'completed',
    from,
    to,
    filters: {
      types,
      scope: firstNonEmpty(options.scope),
    },
    counts: {
      input_rows: rows.length,
      patched_rows: processRows.length + lifecyclemodelRows.length,
      changes: changes.length,
      process_rows: processRows.length,
      lifecyclemodel_rows: lifecyclemodelRows.length,
    },
    files,
    changes,
    commit_reports: {
      processes: processReport,
      lifecyclemodels: lifecyclemodelReport,
    },
  };
  writeJsonArtifact(files.summary, report);
  return report;
}

export const __testInternals = {
  normalizeTypes,
  parseReference,
  rewriteLifecyclemodelReferences,
  rewriteProcessReferences,
};
