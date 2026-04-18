import assert from 'node:assert/strict';
import test from 'node:test';
import * as tidasSdk from '@tiangong-lca/tidas-sdk';
import {
  summarizeProcessPayloadValidation,
  validateProcessPayload,
} from '../src/lib/process-payload-validation.js';

test('process payload validation summarizes ok and failure results with normalized issue paths', () => {
  const originalSafeParse = tidasSdk.ProcessSchema.safeParse;

  try {
    tidasSdk.ProcessSchema.safeParse = (() =>
      ({
        success: true,
        data: {},
      }) as unknown as ReturnType<typeof originalSafeParse>) as typeof originalSafeParse;

    const okResult = validateProcessPayload({});
    assert.deepEqual(okResult, {
      ok: true,
      validator: '@tiangong-lca/tidas-sdk/ProcessSchema',
      issue_count: 0,
      issues: [],
    });
    assert.equal(
      summarizeProcessPayloadValidation(okResult),
      'local ProcessSchema validation passed',
    );

    tidasSdk.ProcessSchema.safeParse = (() =>
      ({
        success: false,
        error: {
          issues: [
            {
              path: [],
              message: 'Top-level failure',
              code: 'custom',
            },
            {
              path: ['processDataSet', 'exchanges', 0],
            },
          ],
        },
      }) as unknown as ReturnType<typeof originalSafeParse>) as typeof originalSafeParse;

    const invalidResult = validateProcessPayload({});
    assert.equal(invalidResult.ok, false);
    assert.equal(invalidResult.issue_count, 2);
    assert.deepEqual(invalidResult.issues, [
      {
        path: '<root>',
        message: 'Top-level failure',
        code: 'custom',
      },
      {
        path: 'processDataSet.exchanges.0',
        message: 'Validation failed',
        code: 'custom',
      },
    ]);
    assert.match(
      summarizeProcessPayloadValidation(invalidResult),
      /local ProcessSchema validation failed with 2 issue\(s\) \(<root>: Top-level failure; processDataSet\.exchanges\.0: Validation failed\)/u,
    );

    tidasSdk.ProcessSchema.safeParse = (() =>
      ({
        success: false,
        error: undefined,
      }) as unknown as ReturnType<typeof originalSafeParse>) as typeof originalSafeParse;
    const emptyIssueResult = validateProcessPayload({});
    assert.equal(emptyIssueResult.ok, false);
    assert.equal(emptyIssueResult.issue_count, 0);
    assert.equal(
      summarizeProcessPayloadValidation(emptyIssueResult),
      'local ProcessSchema validation failed with 0 issue(s)',
    );

    tidasSdk.ProcessSchema.safeParse = undefined as unknown as typeof originalSafeParse;
    assert.throws(
      () => validateProcessPayload({}),
      /@tiangong-lca\/tidas-sdk\/ProcessSchema is unavailable/u,
    );
  } finally {
    tidasSdk.ProcessSchema.safeParse = originalSafeParse;
  }
});
