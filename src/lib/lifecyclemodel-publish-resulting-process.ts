import path from 'node:path';
import { existsSync } from 'node:fs';
import { readJsonArtifact, writeJsonArtifact } from './artifacts.js';
import { CliError } from './errors.js';

type JsonObject = Record<string, unknown>;

function isRecord(value: unknown): value is JsonObject {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function ensureList<T = unknown>(value: unknown): T[] {
  if (value === undefined || value === null) {
    return [];
  }
  return Array.isArray(value) ? (value as T[]) : ([value] as T[]);
}

function copyJson<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T;
}

function nowIso(now: Date = new Date()): string {
  return now.toISOString();
}

function requireRunDir(runDir: string): string {
  if (!runDir.trim()) {
    throw new CliError('Missing required --run-dir for lifecyclemodel publish-resulting-process.', {
      code: 'LIFECYCLEMODEL_RUN_DIR_REQUIRED',
      exitCode: 2,
    });
  }

  return path.resolve(runDir);
}

function readRequiredJsonObject(
  filePath: string,
  missingCode: string,
  invalidCode: string,
): JsonObject {
  if (!existsSync(filePath)) {
    throw new CliError(`Required lifecyclemodel artifact not found: ${filePath}`, {
      code: missingCode,
      exitCode: 2,
    });
  }

  const value = readJsonArtifact(filePath);
  if (!isRecord(value)) {
    throw new CliError(`Expected lifecyclemodel artifact JSON object: ${filePath}`, {
      code: invalidCode,
      exitCode: 2,
    });
  }

  return value;
}

export type LifecyclemodelPublishResultingProcessReport = {
  generated_at_utc: string;
  run_dir: string;
  status: 'prepared_local_publish_bundle';
  publish_processes: boolean;
  publish_relations: boolean;
  counts: {
    projected_processes: number;
    relations: number;
  };
  source_model: JsonObject;
  files: {
    projection_bundle: string;
    projection_report: string;
    publish_bundle: string;
    publish_intent: string;
  };
};

export type RunLifecyclemodelPublishResultingProcessOptions = {
  runDir: string;
  publishProcesses: boolean;
  publishRelations: boolean;
  now?: Date;
};

export async function runLifecyclemodelPublishResultingProcess(
  options: RunLifecyclemodelPublishResultingProcessOptions,
): Promise<LifecyclemodelPublishResultingProcessReport> {
  const runDir = requireRunDir(options.runDir);
  const generatedAtUtc = nowIso(options.now);
  const projectionBundlePath = path.join(runDir, 'process-projection-bundle.json');
  const projectionReportPath = path.join(runDir, 'projection-report.json');
  const projectionBundle = readRequiredJsonObject(
    projectionBundlePath,
    'LIFECYCLEMODEL_PROJECTION_BUNDLE_MISSING',
    'LIFECYCLEMODEL_PROJECTION_BUNDLE_NOT_OBJECT',
  );
  const projectionReport = readRequiredJsonObject(
    projectionReportPath,
    'LIFECYCLEMODEL_PROJECTION_REPORT_MISSING',
    'LIFECYCLEMODEL_PROJECTION_REPORT_NOT_OBJECT',
  );

  const sourceModel = isRecord(projectionBundle.source_model)
    ? copyJson(projectionBundle.source_model)
    : {};
  const projectedProcesses = options.publishProcesses
    ? copyJson(ensureList(projectionBundle.projected_processes))
    : [];
  const relations = options.publishRelations
    ? copyJson(ensureList(projectionBundle.relations))
    : [];

  const publishBundle = {
    generated_at: generatedAtUtc,
    run_dir: runDir,
    source_model: sourceModel,
    publish_processes: options.publishProcesses,
    publish_relations: options.publishRelations,
    status: 'prepared_local_publish_bundle' as const,
    projected_processes: projectedProcesses,
    relations,
    report: copyJson(projectionReport),
  };
  const publishIntent = {
    ok: true,
    command: 'publish',
    run_dir: runDir,
    publish_processes: options.publishProcesses,
    publish_relations: options.publishRelations,
    status: 'prepared_local_publish_bundle' as const,
  };

  const publishBundlePath = writeJsonArtifact(
    path.join(runDir, 'publish-bundle.json'),
    publishBundle,
  );
  const publishIntentPath = writeJsonArtifact(
    path.join(runDir, 'publish-intent.json'),
    publishIntent,
  );

  return {
    generated_at_utc: generatedAtUtc,
    run_dir: runDir,
    status: 'prepared_local_publish_bundle',
    publish_processes: options.publishProcesses,
    publish_relations: options.publishRelations,
    counts: {
      projected_processes: projectedProcesses.length,
      relations: relations.length,
    },
    source_model: sourceModel,
    files: {
      projection_bundle: projectionBundlePath,
      projection_report: projectionReportPath,
      publish_bundle: publishBundlePath,
      publish_intent: publishIntentPath,
    },
  };
}
