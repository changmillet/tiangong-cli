import path from 'node:path';
import * as tidasSdk from '@tiangong-lca/tidas-sdk';
import { writeJsonArtifact, writeJsonLinesArtifact } from './artifacts.js';
import { CliError } from './errors.js';
import { materializeDatasetRows, type DatasetKind, type DatasetRowInput } from './dataset-local.js';
import {
  normalizeIssuePath,
  type SafeParseSchema,
  type SdkValidationFactory,
  validateSchemaWithDeepFallback,
} from './tidas-sdk-validation.js';
import {
  collectProcessPlaceholderIssues,
  collectProcessRequiredFieldIssues,
} from './process-required-fields.js';

type DatasetValidateType = 'auto' | DatasetKind;

export type DatasetValidateIssue = {
  path: string;
  message: string;
  code: string;
};

export type DatasetValidateRowReport = {
  index: number;
  id: string | null;
  version: string | null;
  type: DatasetKind | null;
  status: 'valid' | 'invalid';
  validator: string | null;
  issue_count: number;
  issues: DatasetValidateIssue[];
};

export type DatasetValidateReport = {
  generated_at_utc: string;
  input_path: string;
  requested_type: DatasetValidateType;
  status: 'completed' | 'completed_with_failures';
  counts: {
    total: number;
    valid: number;
    invalid: number;
    by_type: Record<DatasetKind, number>;
  };
  files: {
    report: string | null;
    valid_rows: string | null;
    invalid_rows: string | null;
  };
  rows: DatasetValidateRowReport[];
};

export type RunDatasetValidateOptions = {
  inputPath: string;
  type?: string | null;
  outDir?: string | null;
  rawInput?: unknown;
  now?: Date;
  schemas?: Partial<Record<DatasetKind, SafeParseSchema>>;
};

const DEFAULT_TYPE: DatasetValidateType = 'auto';

const SCHEMA_EXPORTS: Record<DatasetKind, keyof typeof tidasSdk> = {
  flow: 'FlowSchema' as keyof typeof tidasSdk,
  process: 'ProcessSchema' as keyof typeof tidasSdk,
  lifecyclemodel: 'LifeCycleModelSchema' as keyof typeof tidasSdk,
};

const ENTITY_FACTORY_EXPORTS: Record<DatasetKind, keyof typeof tidasSdk> = {
  flow: 'createFlow' as keyof typeof tidasSdk,
  process: 'createProcess' as keyof typeof tidasSdk,
  lifecyclemodel: 'createLifeCycleModel' as keyof typeof tidasSdk,
};

function normalizeType(value: string | null | undefined): DatasetValidateType {
  const normalized = value?.trim().toLowerCase();
  if (!normalized) {
    return DEFAULT_TYPE;
  }
  if (normalized === 'auto') {
    return 'auto';
  }
  if (normalized === 'flow' || normalized === 'flows') {
    return 'flow';
  }
  if (normalized === 'process' || normalized === 'processes') {
    return 'process';
  }
  if (
    normalized === 'lifecyclemodel' ||
    normalized === 'lifecyclemodels' ||
    normalized === 'model' ||
    normalized === 'models'
  ) {
    return 'lifecyclemodel';
  }
  throw new CliError('Expected --type to be auto, flow, process, or lifecyclemodel.', {
    code: 'DATASET_VALIDATE_TYPE_INVALID',
    exitCode: 2,
    details: value,
  });
}

function schemaForKind(
  kind: DatasetKind,
  schemas: Partial<Record<DatasetKind, SafeParseSchema>> | undefined,
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
      code: 'DATASET_VALIDATE_SCHEMA_UNAVAILABLE',
      exitCode: 2,
      details: { type: kind },
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

function buildUnsupportedTypeReport(row: DatasetRowInput): DatasetValidateRowReport {
  return {
    index: row.index,
    id: row.id,
    version: row.version,
    type: row.kind,
    status: 'invalid',
    validator: null,
    issue_count: 1,
    issues: [
      {
        path: '<root>',
        message: 'Could not detect dataset type. Use --type or provide a recognized TIDAS wrapper.',
        code: 'dataset_type_unknown',
      },
    ],
  };
}

function validateRow(
  row: DatasetRowInput,
  requestedType: DatasetValidateType,
  schemas: Partial<Record<DatasetKind, SafeParseSchema>> | undefined,
): DatasetValidateRowReport {
  const kind = requestedType === 'auto' ? row.kind : requestedType;
  if (!kind) {
    return buildUnsupportedTypeReport(row);
  }

  const { validator, schema, createEntity } = schemaForKind(kind, schemas);
  const outcome = validateSchemaWithDeepFallback(schema, row.payload, createEntity);
  const requiredFieldIssues =
    kind === 'process' ? collectProcessRequiredFieldIssues(row.payload) : [];
  const placeholderIssues = kind === 'process' ? collectProcessPlaceholderIssues(row.payload) : [];
  if (outcome.success && requiredFieldIssues.length === 0 && placeholderIssues.length === 0) {
    return {
      index: row.index,
      id: row.id,
      version: row.version,
      type: kind,
      status: 'valid',
      validator,
      issue_count: 0,
      issues: [],
    };
  }

  const issues = [
    ...outcome.issues.map((issue) => ({
      path: normalizeIssuePath(issue.path),
      message: issue.message ?? 'Validation failed',
      code: issue.code ?? 'custom',
    })),
    ...requiredFieldIssues,
    ...placeholderIssues,
  ];

  return {
    index: row.index,
    id: row.id,
    version: row.version,
    type: kind,
    status: 'invalid',
    validator,
    issue_count: issues.length,
    issues,
  };
}

function buildFiles(outDir: string | null | undefined): DatasetValidateReport['files'] {
  if (!outDir) {
    return {
      report: null,
      valid_rows: null,
      invalid_rows: null,
    };
  }

  const resolved = path.resolve(outDir);
  return {
    report: path.join(resolved, 'outputs', 'validation-report.json'),
    valid_rows: path.join(resolved, 'outputs', 'valid-rows.jsonl'),
    invalid_rows: path.join(resolved, 'outputs', 'invalid-rows.jsonl'),
  };
}

function summarizeByType(rows: DatasetValidateRowReport[]): Record<DatasetKind, number> {
  return {
    flow: rows.filter((row) => row.type === 'flow').length,
    process: rows.filter((row) => row.type === 'process').length,
    lifecyclemodel: rows.filter((row) => row.type === 'lifecyclemodel').length,
  };
}

export async function runDatasetValidate(
  options: RunDatasetValidateOptions,
): Promise<DatasetValidateReport> {
  const requestedType = normalizeType(options.type);
  const rows = materializeDatasetRows(options.inputPath, options.rawInput);
  const reports = rows.map((row) => validateRow(row, requestedType, options.schemas));
  const invalidRows = reports.filter((row) => row.status === 'invalid');
  const files = buildFiles(options.outDir);

  const report: DatasetValidateReport = {
    generated_at_utc: (options.now ?? new Date()).toISOString(),
    input_path: path.resolve(options.inputPath),
    requested_type: requestedType,
    status: invalidRows.length > 0 ? 'completed_with_failures' : 'completed',
    counts: {
      total: reports.length,
      valid: reports.length - invalidRows.length,
      invalid: invalidRows.length,
      by_type: summarizeByType(reports),
    },
    files,
    rows: reports,
  };

  if (files.report) {
    writeJsonArtifact(files.report, report);
  }
  if (files.valid_rows) {
    writeJsonLinesArtifact(
      files.valid_rows,
      rows.filter((_, index) => reports[index]?.status === 'valid').map((row) => row.row),
    );
  }
  if (files.invalid_rows) {
    writeJsonLinesArtifact(
      files.invalid_rows,
      rows.flatMap((row, index) =>
        reports[index]?.status === 'invalid'
          ? [
              {
                row: row.row,
                validation: reports[index],
              },
            ]
          : [],
      ),
    );
  }

  return report;
}

export const __testInternals = {
  ENTITY_FACTORY_EXPORTS,
  SCHEMA_EXPORTS,
  normalizeType,
  schemaForKind,
  validateRow,
};
