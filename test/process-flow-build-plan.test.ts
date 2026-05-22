import assert from 'node:assert/strict';
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import {
  runFlowBuildPlanMaterialize,
  runFlowBuildPlanValidate,
  runProcessBuildPlanMaterialize,
  runProcessBuildPlanValidate,
  __testInternals,
} from '../src/lib/process-flow-build-plan.js';
import type { SafeParseSchema } from '../src/lib/tidas-sdk-validation.js';

const now = new Date('2026-05-22T00:00:00.000Z');

function passingSchema(): SafeParseSchema {
  return {
    safeParse: () => ({
      success: true as const,
      data: {},
    }),
  };
}

function failingSchema(): SafeParseSchema {
  return {
    safeParse: () => ({
      success: false as const,
      error: {
        issues: [
          {
            path: ['processDataSet', 'processInformation'],
            message: 'Missing process information',
            code: 'custom',
          },
        ],
      },
    }),
  };
}

function processPlan(overrides: Record<string, unknown> = {}) {
  return {
    schema_version: 1,
    kind: 'process',
    ruleset: {
      id: 'process-authoring/strict',
      version: '1',
    },
    target: {
      geography: 'CN',
      technology_route: 'PV installation',
    },
    identity_decision: {
      decision: 'create_new',
    },
    evidence_manifest: {
      sources: [{ id: 'source-1', type: 'local-fixture' }],
      field_bindings: [
        { field_path: 'target' },
        { field_path: 'identity_decision.decision' },
        { field_path: 'name_plan.base_name' },
        { field_path: 'target.geography' },
        { field_path: 'target.technology_route' },
        { field_path: 'quantitative_reference_plan.reference_flow_id' },
      ],
    },
    name_plan: {
      base_name: '3kWp facade installation, multi-Si, laminated, integrated, at building {CN}',
    },
    quantitative_reference_plan: {
      reference_flow_id: '190f39ca-0ec8-5aab-b2d9-c91fc55ee58d',
      reference_unit: 'unit',
    },
    ...overrides,
  };
}

function flowPlan(overrides: Record<string, unknown> = {}) {
  return {
    schemaVersion: 1,
    kind: 'flow',
    ruleset_id: 'flow-authoring/strict',
    ruleset_version: '1',
    target: {
      flow_type: 'Product flow',
    },
    identityDecision: {
      decision: 'create_new',
    },
    evidenceManifest: {
      sources: [{ id: 'source-1', type: 'local-fixture' }],
      fieldBindings: [
        { fieldPath: 'target' },
        { fieldPath: 'identity_decision.decision' },
        { fieldPath: 'name_plan.base_name' },
        { fieldPath: 'target.flow_type' },
        { fieldPath: 'flow_property_plan.reference_property' },
        { fieldPath: 'flow_property_plan.reference_unit' },
      ],
    },
    namePlan: {
      baseName: 'Fluoroethylene carbonate',
    },
    flowPropertyPlan: {
      referenceProperty: 'mass',
      referenceUnit: 'kg',
    },
    ...overrides,
  };
}

test('process build-plan validate passes and writes a gate report', async () => {
  const outDir = mkdtempSync(path.join(os.tmpdir(), 'process-build-plan-'));
  try {
    const report = await runProcessBuildPlanValidate({
      inputPath: '/tmp/process-build-plan.json',
      outDir,
      rawInput: processPlan(),
      now,
    });

    assert.equal(report.generated_at_utc, '2026-05-22T00:00:00.000Z');
    assert.equal(report.kind, 'process');
    assert.equal(report.action, 'validate');
    assert.equal(report.status, 'passed');
    assert.equal(report.next_action, 'materialize_payload');
    assert.equal(report.ruleset_id, 'process-authoring/strict');
    assert.equal(report.inputs.plan_schema_version, '1');
    assert.equal(report.inputs.identity_decision, 'create_new');
    assert.equal(report.required_fields.missing.length, 0);
    assert.equal(
      report.files.materialized_artifact,
      path.join(outDir, 'outputs', 'materialized-process.json'),
    );
    assert.equal(existsSync(path.join(outDir, 'outputs', 'build-plan-gate-report.json')), true);
  } finally {
    rmSync(outDir, { recursive: true, force: true });
  }
});

test('flow build-plan materialize writes a deterministic seed artifact without a canonical payload', async () => {
  const outDir = mkdtempSync(path.join(os.tmpdir(), 'flow-build-plan-'));
  try {
    const report = await runFlowBuildPlanMaterialize({
      inputPath: '/tmp/flow-build-plan.json',
      outDir,
      rawInput: {
        build_plan: flowPlan(),
      },
      now,
    });

    assert.equal(report.status, 'passed');
    assert.equal(report.kind, 'flow');
    assert.equal(report.action, 'materialize');
    assert.equal(report.next_action, 'use_materialized_artifact');
    assert.equal(report.schema_validation.status, 'not_applicable');
    assert.equal(
      report.files.materialized_artifact,
      path.join(outDir, 'outputs', 'materialized-flow.json'),
    );

    const materialized = JSON.parse(
      readFileSync(path.join(outDir, 'outputs', 'materialized-flow.json'), 'utf8'),
    ) as { kind: string; source_build_plan: string; name_plan: { baseName: string } };
    assert.equal(materialized.kind, 'flow');
    assert.equal(materialized.source_build_plan, '/tmp/flow-build-plan.json');
    assert.equal(materialized.name_plan.baseName, 'Fluoroethylene carbonate');
  } finally {
    rmSync(outDir, { recursive: true, force: true });
  }
});

test('process build-plan materialize validates supplied canonical payloads', async () => {
  const report = await runProcessBuildPlanMaterialize({
    inputPath: '/tmp/process-build-plan.json',
    rawInput: processPlan({
      payload: {
        processDataSet: {
          processInformation: {},
        },
      },
    }),
    now,
    schemas: {
      process: passingSchema(),
    },
  });

  assert.equal(report.status, 'passed');
  assert.equal(report.schema_validation.status, 'passed');
  assert.equal(report.schema_validation.validator, 'injected');
});

test('build-plan gates block schema failures and mismatched canonical payload kinds', async () => {
  const schemaFailure = await runProcessBuildPlanMaterialize({
    inputPath: '/tmp/process-build-plan.json',
    rawInput: processPlan({
      payload: {
        processDataSet: {},
      },
    }),
    now,
    schemas: {
      process: failingSchema(),
    },
  });
  assert.equal(schemaFailure.status, 'blocked');
  assert.equal(schemaFailure.schema_validation.status, 'failed');
  assert.equal(
    schemaFailure.schema_validation.issues[0]?.path,
    'processDataSet.processInformation',
  );
  assert.equal(schemaFailure.blockers.at(-1)?.code, 'materialized_schema_failed');

  const kindMismatch = await runFlowBuildPlanMaterialize({
    inputPath: '/tmp/flow-build-plan.json',
    rawInput: flowPlan({
      payload: {
        processDataSet: {},
      },
    }),
    now,
  });
  assert.equal(kindMismatch.status, 'blocked');
  assert.equal(kindMismatch.schema_validation.issues[0]?.code, 'dataset_kind_mismatch');
});

test('build-plan validation blocks missing evidence, review decisions, missing fields, and kind mismatch', async () => {
  const report = await runProcessBuildPlanValidate({
    inputPath: '/tmp/process-build-plan.json',
    reportOnly: true,
    rawInput: {
      processBuildPlan: processPlan({
        kind: 'flow',
        target: {},
        identity_decision: {
          decision: 'manual_review',
        },
        evidence_manifest: {
          sources: [],
          field_bindings: [{ field_path: 'target' }],
        },
        name_plan: {},
        quantitative_reference_plan: {},
      }),
    },
    now,
  });

  assert.equal(report.status, 'blocked');
  assert.equal(report.report_only, true);
  assert.equal(report.next_action, 'fix_build_plan');
  assert.ok(report.blockers.some((finding) => finding.code === 'build_plan_kind_mismatch'));
  assert.ok(report.blockers.some((finding) => finding.code === 'identity_decision_not_automatic'));
  assert.ok(report.blockers.some((finding) => finding.code === 'evidence_sources_missing'));
  assert.ok(
    report.blockers.some((finding) => finding.code === 'build_plan_required_field_missing'),
  );
});

test('build-plan validation blocks unsupported or absent identity decisions', async () => {
  const invalidDecision = await runFlowBuildPlanValidate({
    inputPath: '/tmp/flow-build-plan.json',
    rawInput: flowPlan({
      decision: 'unsupported',
      identityDecision: {},
    }),
    now,
  });
  assert.equal(invalidDecision.status, 'blocked');
  assert.ok(
    invalidDecision.blockers.some((finding) => finding.code === 'identity_decision_missing'),
  );

  const absentDecision = await runFlowBuildPlanValidate({
    inputPath: '/tmp/flow-build-plan.json',
    rawInput: flowPlan({
      identityDecision: {},
    }),
    now,
  });
  assert.equal(absentDecision.inputs.identity_decision, null);
  assert.ok(
    absentDecision.blockers.some((finding) => finding.code === 'identity_decision_missing'),
  );
});

test('build-plan reports default ruleset values when no explicit ruleset is provided', async () => {
  const plan = flowPlan();
  (plan as Record<string, unknown>).ruleset_id = '   ';
  (plan as Record<string, unknown>).ruleset_version = '';

  const report = await runFlowBuildPlanValidate({
    inputPath: '/tmp/flow-build-plan.json',
    rawInput: plan,
    now,
  });

  assert.equal(report.status, 'passed');
  assert.equal(report.ruleset_id, 'flow-authoring/strict');
  assert.equal(report.ruleset_version, '1');
});

test('build-plan required-field checks accept non-empty array values', async () => {
  const report = await runFlowBuildPlanValidate({
    inputPath: '/tmp/flow-build-plan.json',
    rawInput: flowPlan({
      target: ['array-target'],
    }),
    now,
  });

  assert.equal(report.status, 'blocked');
  assert.ok(report.required_fields.satisfied.includes('target'));
  assert.ok(report.blockers.some((finding) => finding.path === 'target.flow_type'));
});

test('build-plan commands read JSON files and report input shape errors', async () => {
  const workDir = mkdtempSync(path.join(os.tmpdir(), 'build-plan-input-'));
  try {
    const inputPath = path.join(workDir, 'plan.json');
    writeFileSync(inputPath, JSON.stringify(flowPlan()), 'utf8');
    const report = await runFlowBuildPlanValidate({ inputPath, now });
    assert.equal(report.status, 'passed');
    assert.equal(report.ruleset_id, 'flow-authoring/strict');

    await assert.rejects(
      () => runFlowBuildPlanValidate({ inputPath: '   ', rawInput: flowPlan() }),
      /Missing required --input value/u,
    );
    await assert.rejects(
      () => runFlowBuildPlanValidate({ inputPath: '/tmp/plan.json', rawInput: 1 }),
      /build-plan input must be a JSON object/u,
    );
    await assert.rejects(
      () =>
        runFlowBuildPlanValidate({
          inputPath: '/tmp/plan.json',
          rawInput: { buildPlan: 'invalid' },
        }),
      /nested build plan must be a JSON object/u,
    );
  } finally {
    rmSync(workDir, { recursive: true, force: true });
  }
});

test('build-plan internals cover evidence path normalization and SDK schema fallback', async () => {
  const bindingPaths = __testInternals.evidenceBindingPaths({
    evidence_manifest: {
      field_bindings: [null, { path: 'target' }, { field: 'name_plan.base_name' }],
    },
  });
  assert.deepEqual([...bindingPaths].sort(), ['name_plan.base_name', 'target']);
  assert.deepEqual([...__testInternals.evidenceBindingPaths({ evidence_manifest: 'none' })], []);

  const materialized = __testInternals.materializePlan(
    flowPlan({
      materializedPayload: {
        flowDataSet: {},
      },
    }),
    'flow',
    '/tmp/flow-plan.json',
  );
  assert.deepEqual(materialized, { flowDataSet: {} });

  const schema = __testInternals.validateMaterializedSchema(
    { processDataSet: {} },
    'process',
    undefined,
  );
  assert.equal(schema.status, 'failed');
  assert.match(schema.validator ?? '', /ProcessSchema/u);

  const flowSchema = __testInternals.validateMaterializedSchema(
    { flowDataSet: {} },
    'flow',
    undefined,
  );
  assert.equal(flowSchema.status, 'failed');
  assert.match(flowSchema.validator ?? '', /FlowSchema/u);

  const defaultedSchemaIssue = __testInternals.validateMaterializedSchema(
    { flowDataSet: {} },
    'flow',
    {
      flow: {
        safeParse: () => ({
          success: false as const,
          error: {
            issues: [{ path: ['flowDataSet'] }],
          },
        }),
      },
    },
  );
  assert.equal(defaultedSchemaIssue.issues[0]?.message, 'Validation failed');
  assert.equal(defaultedSchemaIssue.issues[0]?.code, 'custom');

  const blockedDecision = __testInternals.evaluateBuildPlan(
    processPlan({
      identity_decision: {
        decision: 'block_duplicate',
      },
    }),
    'process',
  );
  assert.ok(
    blockedDecision.blockers.some((finding) => finding.code === 'identity_decision_not_automatic'),
  );

  const defaultRuleset = __testInternals.evaluateBuildPlan(
    flowPlan({
      ruleset_id: '   ',
      evidenceManifest: 'none',
    }),
    'flow',
  );
  assert.ok(defaultRuleset.blockers.some((finding) => finding.code === 'evidence_sources_missing'));

  const emptySeed = __testInternals.materializePlan({}, 'flow', '/tmp/empty-flow-plan.json');
  assert.deepEqual(emptySeed, {
    schema_version: 1,
    kind: 'flow',
    source_build_plan: '/tmp/empty-flow-plan.json',
    target: {},
    identity_decision: {},
    evidence_manifest: {},
    name_plan: {},
    quantitative_reference_plan: {},
    flow_property_plan: {},
    exchange_plan: {},
  });
});
