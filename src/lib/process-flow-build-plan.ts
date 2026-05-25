import path from 'node:path';
import * as tidasSdk from '@tiangong-lca/tidas-sdk';
import { writeJsonArtifact } from './artifacts.js';
import { CliError } from './errors.js';
import {
  cloneJson,
  detectDatasetKind,
  isRecord,
  unwrapDatasetPayload,
  type JsonObject,
} from './dataset-local.js';
import { readJsonInput } from './io.js';
import {
  normalizeIssuePath,
  validateSchemaWithDeepFallback,
  type SafeParseSchema,
  type SdkValidationFactory,
} from './tidas-sdk-validation.js';

type BuildPlanKind = 'process' | 'flow';
type BuildPlanAction = 'validate' | 'materialize';
type BuildPlanStatus = 'passed' | 'blocked';
type BuildPlanDecision =
  | 'reuse'
  | 'update_same_row'
  | 'version_bump'
  | 'create_new'
  | 'block_duplicate'
  | 'manual_review';

type GateFinding = {
  code: string;
  severity: 'info' | 'warning' | 'blocker';
  message: string;
  path?: string;
};

type SchemaValidationSummary = {
  status: 'passed' | 'failed' | 'not_applicable';
  validator: string | null;
  issue_count: number;
  issues: Array<{
    path: string;
    message: string;
    code: string;
  }>;
};

type BuildPlanRequiredFields = {
  required: string[];
  satisfied: string[];
  missing: string[];
};

type BuildPlanFiles = {
  gate_report: string | null;
  materialized_artifact: string | null;
};

export type BuildPlanGateReport = {
  schema_version: 1;
  generated_at_utc: string;
  kind: BuildPlanKind;
  action: BuildPlanAction;
  status: BuildPlanStatus;
  ruleset_id: string;
  ruleset_version: string;
  input_path: string;
  out_dir: string | null;
  report_only: boolean;
  inputs: {
    plan_schema_version: number | string | null;
    identity_decision: BuildPlanDecision | null;
  };
  required_fields: BuildPlanRequiredFields;
  schema_validation: SchemaValidationSummary;
  findings: GateFinding[];
  blockers: GateFinding[];
  next_action: 'materialize_payload' | 'use_materialized_artifact' | 'fix_build_plan';
  files: BuildPlanFiles;
};

export type RunBuildPlanOptions = {
  inputPath: string;
  outDir?: string | null;
  reportOnly?: boolean;
  rawInput?: unknown;
  now?: Date;
  schemas?: Partial<Record<BuildPlanKind, SafeParseSchema>>;
};

export type RunProcessBuildPlanValidateOptions = RunBuildPlanOptions;
export type RunProcessBuildPlanMaterializeOptions = RunBuildPlanOptions;
export type RunFlowBuildPlanValidateOptions = RunBuildPlanOptions;
export type RunFlowBuildPlanMaterializeOptions = RunBuildPlanOptions;

export type ProcessBuildPlanGateReport = BuildPlanGateReport & { kind: 'process' };
export type FlowBuildPlanGateReport = BuildPlanGateReport & { kind: 'flow' };

type Evaluation = {
  plan: JsonObject;
  findings: GateFinding[];
  blockers: GateFinding[];
  requiredFields: BuildPlanRequiredFields;
  decision: BuildPlanDecision | null;
};

type SchemaSpec = {
  validator: string;
  schema: SafeParseSchema;
  createEntity: SdkValidationFactory | null;
};

const AUTO_DECISIONS = new Set<BuildPlanDecision>([
  'reuse',
  'update_same_row',
  'version_bump',
  'create_new',
]);

const ALL_DECISIONS = new Set<BuildPlanDecision>([
  'reuse',
  'update_same_row',
  'version_bump',
  'create_new',
  'block_duplicate',
  'manual_review',
]);

const SCHEMA_EXPORTS: Record<BuildPlanKind, keyof typeof tidasSdk> = {
  flow: 'FlowSchema' as keyof typeof tidasSdk,
  process: 'ProcessSchema' as keyof typeof tidasSdk,
};

const ENTITY_FACTORY_EXPORTS: Record<BuildPlanKind, keyof typeof tidasSdk> = {
  flow: 'createFlow' as keyof typeof tidasSdk,
  process: 'createProcess' as keyof typeof tidasSdk,
};

function nowIso(now: Date = new Date()): string {
  return now.toISOString();
}

function requiredInputPath(inputPath: string): string {
  const normalized = inputPath.trim();
  if (!normalized) {
    throw new CliError('Missing required --input value.', {
      code: 'BUILD_PLAN_INPUT_REQUIRED',
      exitCode: 2,
    });
  }
  return normalized;
}

function asObject(value: unknown, label: string): JsonObject {
  if (!isRecord(value)) {
    throw new CliError(`${label} must be a JSON object.`, {
      code: 'BUILD_PLAN_INVALID_INPUT',
      exitCode: 2,
    });
  }
  return value;
}

function loadBuildPlan(inputPath: string, rawInput: unknown): JsonObject {
  const input = asObject(rawInput, 'build-plan input');
  const nested =
    input.build_plan ??
    input.buildPlan ??
    input.process_build_plan ??
    input.processBuildPlan ??
    input.flow_build_plan ??
    input.flowBuildPlan;
  return nested === undefined ? input : asObject(nested, 'nested build plan');
}

function readBuildPlanInput(inputPath: string, rawInput: unknown): JsonObject {
  return loadBuildPlan(inputPath, rawInput === undefined ? readJsonInput(inputPath) : rawInput);
}

function textToken(value: unknown): string | null {
  if (typeof value === 'string') {
    const trimmed = value.trim();
    return trimmed || null;
  }
  if (typeof value === 'number' && Number.isFinite(value)) {
    return String(value);
  }
  return null;
}

function valueAtPath(root: JsonObject, pathExpression: string): unknown {
  let current: unknown = root;
  for (const segment of pathExpression.split('.')) {
    if (!isRecord(current)) {
      return undefined;
    }
    current = current[segment];
  }
  return current;
}

function firstValue(root: JsonObject, paths: string[]): unknown {
  for (const candidate of paths) {
    const value = valueAtPath(root, candidate);
    if (value !== undefined && value !== null) {
      return value;
    }
  }
  return undefined;
}

function firstToken(root: JsonObject, paths: string[]): string | null {
  for (const candidate of paths) {
    const token = textToken(valueAtPath(root, candidate));
    if (token) {
      return token;
    }
  }
  return null;
}

function isNonEmptyArray(value: unknown): value is unknown[] {
  return Array.isArray(value) && value.length > 0;
}

function pathIsSatisfied(root: JsonObject, paths: string[]): boolean {
  const value = firstValue(root, paths);
  if (isNonEmptyArray(value)) {
    return true;
  }
  if (isRecord(value)) {
    return Object.keys(value).length > 0;
  }
  return Boolean(textToken(value));
}

function evidenceBindingPaths(plan: JsonObject): Set<string> {
  const evidence = firstValue(plan, ['evidence_manifest', 'evidenceManifest']);
  const bindings = isRecord(evidence)
    ? firstValue(evidence, ['field_bindings', 'fieldBindings'])
    : undefined;
  const rows = Array.isArray(bindings) ? bindings : [];
  return new Set(
    rows
      .map((row) =>
        isRecord(row) ? textToken(row.field_path ?? row.path ?? row.field ?? row.fieldPath) : null,
      )
      .filter((rowPath): rowPath is string => Boolean(rowPath)),
  );
}

function evidenceSourcesPresent(plan: JsonObject): boolean {
  const evidence = firstValue(plan, ['evidence_manifest', 'evidenceManifest']);
  const sources = isRecord(evidence) ? firstValue(evidence, ['sources']) : undefined;
  return isNonEmptyArray(sources);
}

function decisionFromPlan(plan: JsonObject): BuildPlanDecision | null {
  const raw =
    firstToken(plan, ['identity_decision.decision', 'identityDecision.decision', 'decision']) ??
    null;
  return raw && ALL_DECISIONS.has(raw as BuildPlanDecision) ? (raw as BuildPlanDecision) : null;
}

function buildPlanRuleset(plan: JsonObject, kind: BuildPlanKind): { id: string; version: string } {
  const ruleset = firstValue(plan, ['ruleset']);
  const id =
    firstToken(plan, ['ruleset_id', 'rulesetId']) ??
    (isRecord(ruleset) ? textToken(ruleset.id) : null) ??
    `${kind}-authoring/strict`;
  const version =
    firstToken(plan, ['ruleset_version', 'rulesetVersion']) ??
    (isRecord(ruleset) ? textToken(ruleset.version) : null) ??
    '1';
  return { id, version };
}

function requiredFieldSpecs(kind: BuildPlanKind): Array<{ path: string; aliases: string[] }> {
  const common = [
    { path: 'target', aliases: ['target'] },
    {
      path: 'identity_decision.decision',
      aliases: ['identity_decision.decision', 'identityDecision.decision', 'decision'],
    },
    { path: 'name_plan.base_name', aliases: ['name_plan.base_name', 'namePlan.baseName'] },
  ];
  const process = [
    { path: 'target.geography', aliases: ['target.geography', 'target.location'] },
    {
      path: 'target.technology_route',
      aliases: ['target.technology_route', 'target.technologyRoute'],
    },
    {
      path: 'quantitative_reference_plan.reference_flow_id',
      aliases: [
        'quantitative_reference_plan.reference_flow_id',
        'quantitativeReferencePlan.referenceFlowId',
        'target.intended_reference_flow',
      ],
    },
  ];
  const flow = [
    { path: 'target.flow_type', aliases: ['target.flow_type', 'target.flowType'] },
    {
      path: 'flow_property_plan.reference_property',
      aliases: ['flow_property_plan.reference_property', 'flowPropertyPlan.referenceProperty'],
    },
    {
      path: 'flow_property_plan.reference_unit',
      aliases: ['flow_property_plan.reference_unit', 'flowPropertyPlan.referenceUnit'],
    },
  ];
  return kind === 'process' ? [...common, ...process] : [...common, ...flow];
}

function makeFinding(
  code: string,
  severity: GateFinding['severity'],
  message: string,
  pathExpression?: string,
): GateFinding {
  return pathExpression
    ? { code, severity, message, path: pathExpression }
    : { code, severity, message };
}

function evaluateBuildPlan(plan: JsonObject, kind: BuildPlanKind): Evaluation {
  const findings: GateFinding[] = [];
  const blockers: GateFinding[] = [];
  const expectedKind = firstToken(plan, ['kind', 'dataset_kind', 'datasetKind']);
  if (expectedKind && expectedKind !== kind) {
    blockers.push(
      makeFinding(
        'build_plan_kind_mismatch',
        'blocker',
        `Expected ${kind} build plan but received ${expectedKind}.`,
        'kind',
      ),
    );
  }

  const decision = decisionFromPlan(plan);
  if (!decision) {
    blockers.push(
      makeFinding(
        'identity_decision_missing',
        'blocker',
        'Build plan must include a supported identity decision.',
        'identity_decision.decision',
      ),
    );
  } else if (!AUTO_DECISIONS.has(decision)) {
    blockers.push(
      makeFinding(
        'identity_decision_not_automatic',
        'blocker',
        `Build plan identity decision ${decision} cannot proceed without review.`,
        'identity_decision.decision',
      ),
    );
  }

  if (!evidenceSourcesPresent(plan)) {
    blockers.push(
      makeFinding(
        'evidence_sources_missing',
        'blocker',
        'EvidenceManifest must include at least one source.',
        'evidence_manifest.sources',
      ),
    );
  }

  const bindingPaths = evidenceBindingPaths(plan);
  const required = requiredFieldSpecs(kind);
  const satisfied: string[] = [];
  const missing: string[] = [];
  for (const spec of required) {
    if (pathIsSatisfied(plan, spec.aliases)) {
      satisfied.push(spec.path);
      if (!bindingPaths.has(spec.path)) {
        blockers.push(
          makeFinding(
            'evidence_binding_missing',
            'blocker',
            `EvidenceManifest must bind source evidence to ${spec.path}.`,
            spec.path,
          ),
        );
      }
    } else {
      missing.push(spec.path);
      blockers.push(
        makeFinding(
          'build_plan_required_field_missing',
          'blocker',
          `Build plan is missing ${spec.path}.`,
          spec.path,
        ),
      );
    }
  }

  if (blockers.length === 0) {
    findings.push(
      makeFinding(
        'build_plan_contract_satisfied',
        'info',
        `${kind} build plan satisfies the minimum authoring gate contract.`,
      ),
    );
  }

  return {
    plan,
    findings,
    blockers,
    requiredFields: {
      required: required.map((spec) => spec.path),
      satisfied,
      missing,
    },
    decision,
  };
}

function schemaForKind(
  kind: BuildPlanKind,
  schemas: Partial<Record<BuildPlanKind, SafeParseSchema>> | undefined,
): SchemaSpec {
  const injected = schemas?.[kind] ?? null;
  if (injected) {
    return {
      validator: 'injected',
      schema: injected,
      createEntity: null,
    };
  }

  const schema = (tidasSdk as unknown as Record<string, SafeParseSchema>)[
    String(SCHEMA_EXPORTS[kind])
  ];
  const createEntity = (tidasSdk as unknown as Record<string, SdkValidationFactory>)[
    String(ENTITY_FACTORY_EXPORTS[kind])
  ];
  return {
    validator: `@tiangong-lca/tidas-sdk/${String(SCHEMA_EXPORTS[kind])}`,
    schema,
    createEntity,
  };
}

function normalizeSchemaIssue(issue: {
  path?: Array<string | number>;
  message?: string;
  code?: string;
}): { path: string; message: string; code: string } {
  return {
    path: normalizeIssuePath(issue.path),
    message: issue.message ?? 'Validation failed',
    code: issue.code ?? 'custom',
  };
}

function validateMaterializedSchema(
  artifact: JsonObject,
  kind: BuildPlanKind,
  schemas: Partial<Record<BuildPlanKind, SafeParseSchema>> | undefined,
): SchemaValidationSummary {
  const detectedKind = detectDatasetKind(artifact);
  if (!detectedKind) {
    return {
      status: 'not_applicable',
      validator: null,
      issue_count: 0,
      issues: [],
    };
  }
  if (detectedKind !== kind) {
    return {
      status: 'failed',
      validator: null,
      issue_count: 1,
      issues: [
        {
          path: '<root>',
          message: `Expected ${kind} payload but detected ${detectedKind}.`,
          code: 'dataset_kind_mismatch',
        },
      ],
    };
  }

  const { validator, schema, createEntity } = schemaForKind(kind, schemas);
  const payload = unwrapDatasetPayload(artifact);
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
    issues: outcome.issues.map(normalizeSchemaIssue),
  };
}

function materializePlan(plan: JsonObject, kind: BuildPlanKind, inputPath: string): JsonObject {
  const payload = firstValue(plan, ['payload', 'materialized_payload', 'materializedPayload']);
  if (isRecord(payload)) {
    return cloneJson(payload);
  }
  return {
    schema_version: 1,
    kind,
    source_build_plan: inputPath,
    target: cloneJson(firstValue(plan, ['target']) ?? {}),
    identity_decision: cloneJson(firstValue(plan, ['identity_decision', 'identityDecision']) ?? {}),
    evidence_manifest: cloneJson(firstValue(plan, ['evidence_manifest', 'evidenceManifest']) ?? {}),
    name_plan: cloneJson(firstValue(plan, ['name_plan', 'namePlan']) ?? {}),
    quantitative_reference_plan: cloneJson(
      firstValue(plan, ['quantitative_reference_plan', 'quantitativeReferencePlan']) ?? {},
    ),
    flow_property_plan: cloneJson(
      firstValue(plan, ['flow_property_plan', 'flowPropertyPlan']) ?? {},
    ),
    exchange_plan: cloneJson(firstValue(plan, ['exchange_plan', 'exchangePlan']) ?? {}),
  };
}

function emptySchemaValidation(): SchemaValidationSummary {
  return {
    status: 'not_applicable',
    validator: null,
    issue_count: 0,
    issues: [],
  };
}

function reportPaths(outDir: string | null, kind: BuildPlanKind): BuildPlanFiles {
  if (!outDir) {
    return {
      gate_report: null,
      materialized_artifact: null,
    };
  }
  return {
    gate_report: path.join(outDir, 'outputs', 'build-plan-gate-report.json'),
    materialized_artifact: path.join(outDir, 'outputs', `materialized-${kind}.json`),
  };
}

function makeReport(options: {
  kind: BuildPlanKind;
  action: BuildPlanAction;
  inputPath: string;
  outDir: string | null;
  reportOnly: boolean;
  evaluation: Evaluation;
  schemaValidation: SchemaValidationSummary;
  generatedAt: string;
  files: BuildPlanFiles;
}): BuildPlanGateReport {
  const ruleset = buildPlanRuleset(options.evaluation.plan, options.kind);
  const schemaBlockers =
    options.schemaValidation.status === 'failed'
      ? [
          makeFinding(
            'materialized_schema_failed',
            'blocker',
            'Materialized payload failed schema validation.',
            'materialized_artifact',
          ),
        ]
      : [];
  const blockers = [...options.evaluation.blockers, ...schemaBlockers];
  const status: BuildPlanStatus = blockers.length > 0 ? 'blocked' : 'passed';
  return {
    schema_version: 1,
    generated_at_utc: options.generatedAt,
    kind: options.kind,
    action: options.action,
    status,
    ruleset_id: ruleset.id,
    ruleset_version: ruleset.version,
    input_path: options.inputPath,
    out_dir: options.outDir,
    report_only: options.reportOnly,
    inputs: {
      plan_schema_version:
        textToken(options.evaluation.plan.schema_version) ??
        textToken(options.evaluation.plan.schemaVersion),
      identity_decision: options.evaluation.decision,
    },
    required_fields: options.evaluation.requiredFields,
    schema_validation: options.schemaValidation,
    findings: options.evaluation.findings,
    blockers,
    next_action:
      status === 'blocked'
        ? 'fix_build_plan'
        : options.action === 'validate'
          ? 'materialize_payload'
          : 'use_materialized_artifact',
    files: options.files,
  };
}

async function runBuildPlan(
  kind: BuildPlanKind,
  action: BuildPlanAction,
  options: RunBuildPlanOptions,
): Promise<BuildPlanGateReport> {
  const inputPath = requiredInputPath(options.inputPath);
  const outDir = options.outDir?.trim() ? options.outDir.trim() : null;
  const files = reportPaths(outDir, kind);
  const evaluation = evaluateBuildPlan(readBuildPlanInput(inputPath, options.rawInput), kind);
  const materialized =
    action === 'materialize' && evaluation.blockers.length === 0
      ? materializePlan(evaluation.plan, kind, inputPath)
      : null;
  const schemaValidation = materialized
    ? validateMaterializedSchema(materialized, kind, options.schemas)
    : emptySchemaValidation();
  const report = makeReport({
    kind,
    action,
    inputPath,
    outDir,
    reportOnly: Boolean(options.reportOnly),
    evaluation,
    schemaValidation,
    generatedAt: nowIso(options.now),
    files,
  });

  if (files.gate_report) {
    writeJsonArtifact(files.gate_report, report);
  }
  if (files.materialized_artifact && materialized) {
    writeJsonArtifact(files.materialized_artifact, materialized);
  }
  return report;
}

export async function runProcessBuildPlanValidate(
  options: RunProcessBuildPlanValidateOptions,
): Promise<ProcessBuildPlanGateReport> {
  return (await runBuildPlan('process', 'validate', options)) as ProcessBuildPlanGateReport;
}

export async function runProcessBuildPlanMaterialize(
  options: RunProcessBuildPlanMaterializeOptions,
): Promise<ProcessBuildPlanGateReport> {
  return (await runBuildPlan('process', 'materialize', options)) as ProcessBuildPlanGateReport;
}

export async function runFlowBuildPlanValidate(
  options: RunFlowBuildPlanValidateOptions,
): Promise<FlowBuildPlanGateReport> {
  return (await runBuildPlan('flow', 'validate', options)) as FlowBuildPlanGateReport;
}

export async function runFlowBuildPlanMaterialize(
  options: RunFlowBuildPlanMaterializeOptions,
): Promise<FlowBuildPlanGateReport> {
  return (await runBuildPlan('flow', 'materialize', options)) as FlowBuildPlanGateReport;
}

export const __testInternals = {
  decisionFromPlan,
  evidenceBindingPaths,
  evaluateBuildPlan,
  loadBuildPlan,
  materializePlan,
  validateMaterializedSchema,
};
