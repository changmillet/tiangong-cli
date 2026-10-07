import assert from 'node:assert/strict';
import test from 'node:test';
import { mkdtempSync, rmSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import * as sdk from '@tiangong-lca/tidas-sdk';
import {
  analyzeProcessPayloadSemantics,
  semanticContextFromInput,
} from '../src/lib/process-semantic-validation.js';
import { validateProcessPayload } from '../src/lib/process-payload-validation.js';
import { runDatasetValidate } from '../src/lib/dataset-validate.js';
import { runProcessSaveDraft } from '../src/lib/process-save-draft-run.js';
import { runPublish } from '../src/lib/publish.js';
import { sha256Json } from '../src/lib/canonical-json-hash.js';

type Json = Record<string, unknown>;
const nested = (value: unknown) => value as Json;
import { allocationFixture } from './helpers/process-allocation-fixture.js';

test('real SDK preserves four allocation target direction/type combinations and exact evidence binding', async () => {
  for (const direction of ['Input', 'Output'])
    for (const type of ['Product flow', 'Waste flow']) {
      const { payload, context } = allocationFixture(direction, type);
      const original = structuredClone(payload);
      const result = validateProcessPayload(payload, undefined, undefined, context);
      assert.equal(result.ok, true, JSON.stringify(result.issues));
      assert.deepEqual(payload, original);
      assert.equal(result.allocation_semantics?.status, 'passed');
      assert.equal(result.allocation_semantics?.tolerance, sdk.ALLOCATION_SUM_TOLERANCE);
      assert.equal(result.allocation_semantics?.candidate_sha256, sha256Json(payload));
      assert.equal(
        result.allocation_semantics?.dependencies[0].content_sha256,
        sha256Json(context.flow_documents[0]),
      );
      const row = { json_ordered: payload, semantic_context: context };
      const report = await runDatasetValidate({
        inputPath: '/private/tmp/allocation-fixture.json',
        rawInput: row,
        type: 'process',
      });
      assert.equal(report.counts.valid, 1);
    }
});

test('unresolved, ineligible, stale and malformed declarations block public draft/publish transport', async () => {
  const dir = mkdtempSync(path.join(os.tmpdir(), 'cli-allocation-gates-'));
  try {
    const cases: Array<{ payload: Json; context: Json }> = [];
    const eligible = allocationFixture();
    cases.push({ payload: eligible.payload, context: {} });
    cases.push(allocationFixture('Input', 'Elementary flow'));
    const missing = allocationFixture();
    nested(
      (nested(nested(missing.payload.processDataSet).exchanges).exchange as Json[])[0].allocations,
    ).allocation = { '@internalReferenceToCoProduct': '999', '@allocatedFraction': '100' };
    cases.push(missing);
    const stale = allocationFixture();
    cases.push({
      payload: stale.payload,
      context: { ...stale.context, candidate_sha256: 'stale' },
    });
    const mismatch = allocationFixture();
    nested(
      nested(nested(mismatch.context.flow_documents[0].flowDataSet).administrativeInformation)
        .publicationAndOwnership,
    )['common:dataSetVersion'] = '99.00.000';
    cases.push(mismatch);
    const duplicate = allocationFixture();
    const items = nested(nested(duplicate.payload.processDataSet).exchanges).exchange as Json[];
    items.push(structuredClone(items[1]));
    cases.push(duplicate);
    for (const { payload, context } of cases) {
      let writes = 0;
      const row = { json_ordered: payload, semantic_context: context };
      const draft = await runProcessSaveDraft({
        inputPath: path.join(dir, 'input.json'),
        outDir: path.join(dir, `draft-${cases.findIndex((item) => item.payload === payload)}`),
        rawInput: row,
        commit: true,
        env: {},
        fetchImpl: async () => {
          writes++;
          throw Error('unexpected write');
        },
      });
      assert.equal(draft.processes[0].status, 'failed');
      assert.equal(writes, 0);
      const publish = await runPublish({
        inputPath: path.join(dir, 'request.json'),
        outDir: path.join(dir, 'publish'),
        rawRequest: { inputs: { processes: [row] }, publish: { commit: true } },
        executors: {
          processes: () => {
            writes++;
            return {};
          },
        },
      });
      assert.equal(publish.processes[0].status, 'failed');
      assert.equal(writes, 0);
    }
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('semantic context recomputes changed same-ID/version bytes and rejects foreign generator proof', () => {
  const { payload, context } = allocationFixture();
  const original = analyzeProcessPayloadSemantics(payload, context);
  const changed = structuredClone(context);
  nested(nested(nested(changed.flow_documents[0].flowDataSet).flowInformation).dataSetInformation)[
    'common:generalComment'
  ] = { '@xml:lang': 'en', '#text': 'Changed exact Flow bytes' };
  const result = analyzeProcessPayloadSemantics(payload, changed);
  assert.notEqual(result.dependencies[0].content_sha256, original.dependencies[0].content_sha256);
  assert.equal(
    analyzeProcessPayloadSemantics(payload, {
      ...context,
      allocation_transformation: {
        profile: 'tiangong.resulting-process-allocation.v1',
        candidate_sha256: 'stale',
      },
    }).status,
    'failed',
  );
  const bound = semanticContextFromInput({
    semantic_context: { ...context, candidate_sha256: sha256Json(payload) },
    allocation_transformation: {
      profile: 'tiangong.resulting-process-allocation.v1',
      candidate_sha256: sha256Json(payload),
    },
  });
  assert.equal(analyzeProcessPayloadSemantics(payload, bound).status, 'passed');
  const transported = semanticContextFromInput({ semantic_context: bound });
  assert.deepEqual(transported.allocation_transformation, bound.allocation_transformation);
  assert.equal(analyzeProcessPayloadSemantics(payload, transported).status, 'passed');
  assert.equal(
    analyzeProcessPayloadSemantics(
      payload,
      semanticContextFromInput({
        semantic_context: bound,
        allocation_transformation: bound.allocation_transformation,
      }),
    ).status,
    'passed',
  );
  for (const marker of [
    'malformed',
    [],
    null,
    { profile: 'wrong', candidate_sha256: sha256Json(payload) },
  ]) {
    assert.equal(
      analyzeProcessPayloadSemantics(
        payload,
        semanticContextFromInput({ semantic_context: bound, allocation_transformation: marker }),
      ).status,
      'failed',
    );
    assert.equal(
      analyzeProcessPayloadSemantics(
        payload,
        semanticContextFromInput({ semantic_context: context, allocation_transformation: marker }),
      ).status,
      'failed',
    );
  }
  assert.equal(
    analyzeProcessPayloadSemantics(
      payload,
      semanticContextFromInput({
        semantic_context: {
          ...bound,
          allocation_transformation: { profile: 'wrong', candidate_sha256: sha256Json(payload) },
        },
      }),
    ).status,
    'failed',
  );
  assert.equal(analyzeProcessPayloadSemantics(payload, {}, {}).status, 'unresolved');
  assert.equal(analyzeProcessPayloadSemantics(payload, { flow_documents: [{}] }).status, 'failed');
  assert.deepEqual(semanticContextFromInput(null), {});
});

test('Flow schema defaults cannot supply missing exact caller identity', () => {
  const { payload } = allocationFixture();
  const sdk = { FlowSchema: { safeParse: () => ({ success: true as const }) } };
  assert.equal(
    analyzeProcessPayloadSemantics(payload, { flow_documents: [{}] }, sdk).status,
    'failed',
  );
});

test('malformed provided semantic bindings fail rather than becoming absent evidence', () => {
  const { payload, context } = allocationFixture();
  for (const supplied of [
    null,
    'malformed',
    { ...context, candidate_sha256: 123 },
    { ...context, candidate_sha256: null },
    { ...context, candidate_sha256: 'invalid' },
    { ...context, flow_documents: {} },
    { ...context, flow_documents: [...context.flow_documents, 'malformed'] },
    { ...context, invalid_bindings: null },
    { ...context, invalid_bindings: [123] },
  ]) {
    const parsed = semanticContextFromInput({ semantic_context: supplied });
    assert.equal(analyzeProcessPayloadSemantics(payload, parsed).status, 'failed');
    assert.equal(
      analyzeProcessPayloadSemantics(
        payload,
        semanticContextFromInput({ semantic_context: parsed }),
      ).status,
      'failed',
    );
  }
  assert.deepEqual(semanticContextFromInput({ semantic_context: { invalid_bindings: [] } }), {});
});
