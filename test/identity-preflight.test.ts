import assert from 'node:assert/strict';
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import {
  runFlowIdentityPreflight,
  runProcessIdentityPreflight,
  __testInternals,
} from '../src/lib/identity-preflight.js';
import type { SafeParseSchema } from '../src/lib/tidas-sdk-validation.js';
import {
  buildSupabaseTestEnv,
  isSupabaseAuthTokenUrl,
  makeSupabaseAuthResponse,
} from './helpers/supabase-auth.js';

const now = new Date('2026-05-22T00:00:00.000Z');

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

function passingSchema(): SafeParseSchema {
  return {
    safeParse: () => ({
      success: true as const,
      data: {},
    }),
  };
}

function failingSchemaWithoutIssueDetails(): SafeParseSchema {
  return {
    safeParse: () => ({
      success: false as const,
      error: {
        issues: [
          {
            path: ['flowDataSet'],
          },
        ],
      },
    }),
  } as SafeParseSchema;
}

test('process identity preflight allows a new loose target when no candidates match', async () => {
  const report = await runProcessIdentityPreflight({
    inputPath: '/tmp/process-preflight.json',
    rawInput: {
      target: {
        name_en: 'market for hydrogen, gaseous',
        reference_flow_id: 'flow-h2',
        operation: 'produce',
        geography: 'CN',
      },
      candidates: [],
    },
    now,
  });

  assert.equal(report.generated_at_utc, '2026-05-22T00:00:00.000Z');
  assert.equal(report.kind, 'process');
  assert.equal(report.status, 'passed');
  assert.equal(report.decision, 'create_new');
  assert.equal(report.next_action, 'materialize_new_payload');
  assert.equal(report.target.schema_validation.status, 'not_applicable');
  assert.equal(report.blockers.length, 0);
});

test('identity preflight validates required object input shapes', async () => {
  await assert.rejects(
    () =>
      runProcessIdentityPreflight({
        inputPath: '   ',
        rawInput: { target: {} },
      }),
    /Missing required --input value/u,
  );

  await assert.rejects(
    () =>
      runProcessIdentityPreflight({
        inputPath: '/tmp/process-preflight.json',
        rawInput: 1,
      }),
    /identity preflight input must be a JSON object/u,
  );

  await assert.rejects(
    () =>
      runProcessIdentityPreflight({
        inputPath: '/tmp/process-preflight.json',
        rawInput: {
          target: 'not-object',
        },
      }),
    /identity preflight target must be a JSON object/u,
  );

  await assert.rejects(
    () =>
      runProcessIdentityPreflight({
        inputPath: '/tmp/process-preflight.json',
        rawInput: {
          target: {},
          candidates: ['not-object'],
        },
      }),
    /candidates\[0\] must be a JSON object/u,
  );
});

test('process identity preflight blocks duplicate semantic and exchange fingerprints', async () => {
  const rawInput = {
    target: {
      process_id: 'new-process',
      name_en: 'market for electricity, medium voltage',
      reference_flow_id: 'flow-electricity',
      operation: 'produce',
      geography: 'CN',
      exchanges: [
        {
          flow_id: 'flow-coal-electricity',
          direction: 'Input',
          mean_amount: '1.0',
        },
      ],
    },
    candidates: [
      {
        process_id: 'existing-process',
        name_en: 'market for electricity, medium voltage',
        reference_flow_id: 'flow-electricity',
        operation: 'produce',
        geography: 'CN',
        exchanges: [
          {
            flow_id: 'flow-coal-electricity',
            direction: 'Input',
            mean_amount: '1.0',
          },
        ],
      },
    ],
  };

  const report = await runProcessIdentityPreflight({
    inputPath: '/tmp/process-preflight.json',
    rawInput,
    now,
  });

  assert.equal(report.status, 'blocked');
  assert.equal(report.decision, 'block_duplicate');
  assert.equal(report.confidence, 'high');
  assert.equal(report.next_action, 'stop_duplicate');
  assert.equal(report.candidates[0]?.decision_hint, 'block_duplicate');
  assert.deepEqual(report.candidates[0]?.match_reasons, [
    'same_identity_key',
    'same_exchange_signature',
    'overlapping_name',
    'overlapping_identity_field',
  ]);
  assert.equal(report.blockers[0]?.code, 'process_duplicate_candidate');
});

test('process identity preflight maps same-id candidates to reuse and update decisions', async () => {
  const draft = await runProcessIdentityPreflight({
    inputPath: '/tmp/process-preflight.json',
    rawInput: {
      target: {
        id: 'process-a',
        version: '01.00.000',
        name_en: ['Process A', { '#text': 'Process A alias', nested: 12 }],
        state_code: '100',
        exchanges: [{ exchangeDirection: 'Input' }],
      },
      candidates: {
        rows: [
          {
            id: 'process-a',
            version: '01.00.000',
            state_code: 0,
            name_en: 'Process A',
            exchanges: [{ flow_id: 'flow-a' }],
          },
        ],
      },
    },
    now,
  });

  assert.equal(draft.status, 'passed');
  assert.equal(draft.decision, 'update_same_row');
  assert.equal(draft.next_action, 'repair_existing_draft');
  assert.equal(draft.target.exchange_signature.length, 0);

  const versionBump = await runProcessIdentityPreflight({
    inputPath: '/tmp/process-preflight.json',
    rawInput: {
      target: {
        id: 'process-a',
        version: '02.00.000',
      },
      candidates: [
        {
          id: 'process-a',
          version: '01.00.000',
          stateCode: '100',
        },
      ],
    },
    now,
  });

  assert.equal(versionBump.decision, 'version_bump');
  assert.equal(versionBump.next_action, 'prepare_version_update');

  const reuse = await runProcessIdentityPreflight({
    inputPath: '/tmp/process-preflight.json',
    rawInput: {
      target: {
        id: 'process-a',
        version: '01.00.000',
      },
      candidates: [
        {
          id: 'process-a',
          version: '01.00.000',
          state_code: 100,
        },
      ],
    },
    now,
  });

  assert.equal(reuse.decision, 'reuse');
  assert.equal(reuse.next_action, 'reuse_existing');
});

test('process identity preflight queues manual review for weaker process similarities', async () => {
  const exchangeOnly = await runProcessIdentityPreflight({
    inputPath: '/tmp/process-preflight.json',
    rawInput: {
      target: {
        name_en: 'target process',
        exchanges: [{ flow_id: 'flow-x' }],
      },
      candidates: [
        {
          name_en: 'candidate process',
          exchanges: [{ flow_id: 'flow-x' }],
        },
      ],
    },
    now,
  });

  assert.equal(exchangeOnly.status, 'needs_review');
  assert.equal(exchangeOnly.decision, 'manual_review');
  assert.equal(exchangeOnly.confidence, 'low');
  assert.equal(exchangeOnly.next_action, 'queue_manual_review');
  assert.equal(exchangeOnly.findings[0]?.code, 'process_manual_review_candidate');

  const nameOnly = await runProcessIdentityPreflight({
    inputPath: '/tmp/process-preflight.json',
    rawInput: {
      target: {
        name_en: 'shared process name',
        geography: 'CN',
      },
      candidates: [
        {
          name_en: 'shared process name',
          geography: 'US',
        },
      ],
    },
    now,
  });

  assert.equal(nameOnly.status, 'needs_review');
  assert.equal(nameOnly.decision, 'manual_review');
  assert.deepEqual(nameOnly.candidates[0]?.match_reasons, ['overlapping_name']);
});

test('process identity preflight blocks exact exchange fingerprints with local candidate scans', async () => {
  const workDir = mkdtempSync(path.join(os.tmpdir(), 'identity-preflight-local-scan-'));
  const inputPath = path.join(workDir, 'process-preflight.json');
  const candidateDir = path.join(workDir, 'candidates');
  const nestedDir = path.join(candidateDir, 'nested');
  const candidateJsonl = path.join(candidateDir, 'existing.jsonl');
  const hiddenJson = path.join(candidateDir, '.hidden.json');
  const candidateJson = path.join(nestedDir, 'more.json');
  try {
    writeFileSync(
      inputPath,
      JSON.stringify({
        target: {
          name_en: 'market for electricity, medium voltage, renewable adjustment',
          reference_flow_id: 'flow-electricity',
          geography: 'CN',
          exchanges: [{ flow_id: 'flow-wind', direction: 'Input', mean_amount: '1.0' }],
        },
      }),
      'utf8',
    );
    mkdirSync(nestedDir, { recursive: true });
    writeFileSync(
      candidateJsonl,
      `${JSON.stringify({
        process_id: 'existing-process',
        name_en: 'electricity supply candidate with different wording',
        reference_flow_id: 'flow-electricity',
        geography: 'CN',
        exchanges: [{ flow_id: 'flow-wind', direction: 'Input', mean_amount: '1.0' }],
      })}\n`,
      'utf8',
    );
    writeFileSync(
      candidateJson,
      JSON.stringify({
        rows: [{ process_id: 'unrelated', name_en: 'unrelated process' }],
      }),
      'utf8',
    );
    writeFileSync(
      hiddenJson,
      JSON.stringify({
        rows: [{ process_id: 'hidden', name_en: 'hidden process' }],
      }),
      'utf8',
    );

    const report = await runProcessIdentityPreflight({
      inputPath,
      candidateInputPaths: [candidateDir],
      now,
    });

    assert.equal(report.status, 'blocked');
    assert.equal(report.decision, 'block_duplicate');
    assert.equal(report.candidates.length, 2);
    assert.equal(report.candidate_sources[0]?.kind, 'directory');
    assert.equal(report.candidate_sources[0]?.row_count, 2);
    assert.ok(report.candidate_sources[0]?.scanned_files.includes(candidateJsonl));
    assert.ok(report.candidate_sources[0]?.scanned_files.includes(candidateJson));
    assert.equal(report.candidate_sources[0]?.scanned_files.includes(hiddenJson), false);
    assert.ok(report.candidates[0]?.match_reasons.includes('same_exchange_fingerprint'));
  } finally {
    rmSync(workDir, { recursive: true, force: true });
  }
});

test('process identity preflight can block duplicates from remote hybrid search', async () => {
  const observed: {
    url: string | null;
    headers: Record<string, string> | null;
    body: Record<string, unknown> | null;
  } = {
    url: null,
    headers: null,
    body: null,
  };

  const report = await runProcessIdentityPreflight({
    inputPath: '/tmp/process-preflight.json',
    rawInput: {
      target: {
        name_en: 'market for electricity, medium voltage',
        reference_flow_id: 'flow-electricity',
        geography: 'CN',
        exchanges: [{ flow_id: 'flow-grid', direction: 'Input', mean_amount: '1.0' }],
      },
      remote_candidate_search: {
        enabled: true,
        query: 'electricity medium voltage',
        limit: 1,
      },
    },
    env: buildSupabaseTestEnv({
      TIANGONG_LCA_API_BASE_URL: 'https://example.com/functions/v1',
      TIANGONG_LCA_REGION: 'cn-east-1',
    }),
    fetchImpl: async (input, init) => {
      const url = String(input);
      if (isSupabaseAuthTokenUrl(url)) {
        return makeSupabaseAuthResponse();
      }

      observed.url = url;
      observed.headers = init?.headers as Record<string, string>;
      observed.body = JSON.parse(String(init?.body)) as Record<string, unknown>;
      return {
        ok: true,
        status: 200,
        headers: {
          get(name: string): string | null {
            return name.toLowerCase() === 'content-type' ? 'application/json' : null;
          },
        },
        async text(): Promise<string> {
          return JSON.stringify({
            data: [
              {
                id: 'existing-process',
                version: '01.00.000',
                state_code: 100,
                name_en: 'market for electricity, medium voltage',
                reference_flow_id: 'flow-electricity',
                geography: 'CN',
                exchanges: [{ flow_id: 'flow-grid', direction: 'Input', mean_amount: '1.0' }],
              },
            ],
          });
        },
      };
    },
    now,
  });

  assert.equal(report.status, 'blocked');
  assert.equal(report.decision, 'block_duplicate');
  assert.equal(report.candidate_sources[0]?.kind, 'remote_search');
  assert.equal(report.candidate_sources[0]?.endpoint, 'process_hybrid_search');
  assert.equal(report.candidate_sources[0]?.query, 'electricity medium voltage');
  assert.equal(report.candidate_sources[0]?.row_count, 1);
  assert.equal(observed.url, 'https://example.com/functions/v1/process_hybrid_search');
  assert.equal(observed.headers?.Authorization, 'Bearer access-token');
  assert.equal(observed.headers?.['x-region'], 'cn-east-1');
  assert.equal(observed.body?.limit, 1);
});

test('flow identity preflight sends type filters to remote hybrid search', async () => {
  const observed: {
    url: string | null;
    body: Record<string, unknown> | null;
  } = {
    url: null,
    body: null,
  };

  const report = await runFlowIdentityPreflight({
    inputPath: '/tmp/flow-preflight.json',
    rawInput: {
      target: {
        name_en: 'electricity, medium voltage',
        type_of_dataset: ['Product flow'],
      },
    },
    remoteCandidateSearch: true,
    remoteQuery: 'electricity flow',
    remoteLimit: 2,
    env: buildSupabaseTestEnv({
      TIANGONG_LCA_API_BASE_URL: 'https://example.com/functions/v1',
    }),
    fetchImpl: async (input, init) => {
      const url = String(input);
      if (isSupabaseAuthTokenUrl(url)) {
        return makeSupabaseAuthResponse();
      }

      observed.url = url;
      observed.body = JSON.parse(String(init?.body)) as Record<string, unknown>;
      return {
        ok: true,
        status: 200,
        headers: {
          get(name: string): string | null {
            return name.toLowerCase() === 'content-type' ? 'application/json' : null;
          },
        },
        async text(): Promise<string> {
          return JSON.stringify({ rows: [] });
        },
      };
    },
    now,
  });

  assert.equal(report.status, 'passed');
  assert.equal(report.candidate_sources[0]?.kind, 'remote_search');
  assert.equal(report.candidate_sources[0]?.endpoint, 'flow_hybrid_search');
  assert.equal(observed.url, 'https://example.com/functions/v1/flow_hybrid_search');
  assert.deepEqual(observed.body?.filter, { flowType: 'Product flow' });
  assert.equal(observed.body?.limit, 2);
});

test('identity preflight requires a remote query when the target has no identity text', async () => {
  await assert.rejects(
    () =>
      runProcessIdentityPreflight({
        inputPath: '/tmp/process-preflight.json',
        rawInput: {
          target: {},
          remote_candidate_search: true,
        },
        now,
      }),
    /Remote identity candidate search requires a query/u,
  );
});

test('identity preflight can use process env and global fetch for remote candidates', async () => {
  const envKeys = [
    'TIANGONG_LCA_API_BASE_URL',
    'TIANGONG_LCA_API_KEY',
    'TIANGONG_LCA_SUPABASE_PUBLISHABLE_KEY',
    'TIANGONG_LCA_DISABLE_SESSION_CACHE',
    'TIANGONG_LCA_REGION',
  ] as const;
  const originalEnv = Object.fromEntries(envKeys.map((key) => [key, process.env[key]]));
  const originalFetch = globalThis.fetch;
  const observed: {
    body: Record<string, unknown> | null;
  } = {
    body: null,
  };

  try {
    const testEnv = buildSupabaseTestEnv({
      TIANGONG_LCA_API_BASE_URL: 'https://env.example.com/functions/v1',
      TIANGONG_LCA_DISABLE_SESSION_CACHE: '1',
      TIANGONG_LCA_REGION: '',
    });
    for (const key of envKeys) {
      const value = testEnv[key];
      if (value === undefined) {
        delete process.env[key];
      } else {
        process.env[key] = value;
      }
    }
    globalThis.fetch = (async (input, init) => {
      const url = String(input);
      if (isSupabaseAuthTokenUrl(url)) {
        return makeSupabaseAuthResponse();
      }

      observed.body = JSON.parse(String(init?.body)) as Record<string, unknown>;
      return {
        ok: true,
        status: 200,
        headers: {
          get(name: string): string | null {
            return name.toLowerCase() === 'content-type' ? 'application/json' : null;
          },
        },
        async text(): Promise<string> {
          return JSON.stringify({
            data: [
              { id: 'candidate-a', name_en: 'candidate a' },
              { id: 'candidate-b', name_en: 'candidate b' },
            ],
          });
        },
      };
    }) as typeof fetch;

    const report = await runProcessIdentityPreflight({
      inputPath: '/tmp/process-preflight.json',
      rawInput: {
        target: {
          name_en: 'market for heat',
        },
        remote_candidate_search: true,
      },
      now,
    });

    assert.equal(report.candidate_sources[0]?.row_count, 2);
    assert.equal(observed.body?.query, 'market for heat');
    assert.equal('limit' in (observed.body ?? {}), false);
  } finally {
    globalThis.fetch = originalFetch;
    for (const key of envKeys) {
      const value = originalEnv[key];
      if (value === undefined) {
        delete process.env[key];
      } else {
        process.env[key] = value;
      }
    }
  }
});

test('identity preflight reads file candidate sources and rejects invalid source paths', async () => {
  const workDir = mkdtempSync(path.join(os.tmpdir(), 'identity-preflight-candidate-file-'));
  const candidatePath = path.join(workDir, 'candidates.json');
  try {
    writeFileSync(
      candidatePath,
      JSON.stringify({
        rows: [{ name_en: 'same flow', type_of_dataset: 'Product flow' }],
      }),
      'utf8',
    );

    const report = await runFlowIdentityPreflight({
      inputPath: '/tmp/flow-preflight.json',
      rawInput: {
        flow: { name_en: 'target flow' },
      },
      candidateInputPaths: [candidatePath],
      now,
    });

    assert.equal(report.candidate_sources[0]?.kind, 'file');
    assert.equal(report.candidate_sources[0]?.row_count, 1);
    assert.deepEqual(report.candidate_sources[0]?.scanned_files, [candidatePath]);

    const requestPathReport = await runFlowIdentityPreflight({
      inputPath: '/tmp/flow-preflight.json',
      rawInput: {
        flow: { name_en: 'target flow' },
        candidate_inputs: `${candidatePath}, , ${candidatePath}`,
      },
      now,
    });

    assert.equal(requestPathReport.candidate_sources.length, 2);
    assert.equal(requestPathReport.candidate_sources[0]?.path, candidatePath);

    await assert.rejects(
      () =>
        runFlowIdentityPreflight({
          inputPath: '/tmp/flow-preflight.json',
          rawInput: { flow: {} },
          candidateInputPaths: [path.join(workDir, 'missing.jsonl')],
          now,
        }),
      /Candidate input not found/u,
    );

    assert.throws(
      () => __testInternals.readCandidateSource(os.devNull),
      /Candidate input must be a JSON\/JSONL file or directory/u,
    );

    await assert.rejects(
      () =>
        runFlowIdentityPreflight({
          inputPath: '/tmp/flow-preflight.json',
          rawInput: {
            flow: {},
            candidate_inputs: [1],
          },
          now,
        }),
      /candidate_inputs\[0\] must be a string path/u,
    );
  } finally {
    rmSync(workDir, { recursive: true, force: true });
  }
});

test('flow identity preflight blocks equivalent flow identities and writes artifacts', async () => {
  const outDir = mkdtempSync(path.join(os.tmpdir(), 'identity-preflight-'));
  try {
    const report = await runFlowIdentityPreflight({
      inputPath: '/tmp/flow-preflight.json',
      outDir,
      rawInput: {
        target: {
          flow_id: 'new-flow',
          type_of_dataset: 'Product flow',
          name_en: 'electricity, medium voltage',
          reference_unit: 'kWh',
          flow_property: 'Energy',
        },
        candidates: [
          {
            flow_id: 'existing-flow',
            type_of_dataset: 'Product flow',
            name_en: 'electricity, medium voltage',
            reference_unit: 'kWh',
            flow_property: 'Energy',
          },
        ],
      },
      now,
    });

    assert.equal(report.kind, 'flow');
    assert.equal(report.status, 'blocked');
    assert.equal(report.decision, 'block_duplicate');
    assert.equal(
      report.files.identity_decision,
      path.join(outDir, 'outputs', 'identity-decision.json'),
    );
    assert.equal(
      report.files.candidates,
      path.join(outDir, 'outputs', 'identity-candidates.jsonl'),
    );
    assert.equal(
      report.files.candidate_sources,
      path.join(outDir, 'outputs', 'identity-candidate-sources.json'),
    );
    assert.equal(existsSync(report.files.identity_decision as string), true);
    assert.equal(existsSync(report.files.candidates as string), true);
    assert.equal(existsSync(report.files.candidate_sources as string), true);

    const artifact = JSON.parse(readFileSync(report.files.identity_decision as string, 'utf8')) as {
      files: { identity_decision: string };
      decision: string;
    };
    assert.equal(artifact.files.identity_decision, report.files.identity_decision);
    assert.equal(artifact.decision, 'block_duplicate');
  } finally {
    rmSync(outDir, { recursive: true, force: true });
  }
});

test('flow identity preflight blocks alias-equivalent flow core fields', async () => {
  const report = await runFlowIdentityPreflight({
    inputPath: '/tmp/flow-preflight.json',
    rawInput: {
      target: {
        type_of_dataset: 'Product flow',
        name_en: ['electricity, high voltage', 'power, high voltage'],
        reference_unit: 'kWh',
        flow_property: 'Energy',
        category: 'energy carrier',
      },
      candidates: [
        {
          type_of_dataset: 'Product flow',
          name_en: 'power, high voltage',
          reference_unit: 'kWh',
          flow_property: 'Energy',
          category: 'energy carrier',
        },
      ],
    },
    now,
  });

  assert.equal(report.status, 'blocked');
  assert.equal(report.decision, 'block_duplicate');
  assert.ok(report.candidates[0]?.match_reasons.includes('equivalent_flow_core_fields'));
});

test('flow identity preflight accepts sparse file input and numeric text fields', async () => {
  const workDir = mkdtempSync(path.join(os.tmpdir(), 'identity-preflight-input-'));
  const inputPath = path.join(workDir, 'flow-preflight.json');
  try {
    writeFileSync(
      inputPath,
      JSON.stringify({
        flow: {
          name_en: 42,
        },
        candidates: [
          {
            name_en: '   ',
            state_code: 'not-a-number',
          },
        ],
      }),
      'utf8',
    );

    const report = await runFlowIdentityPreflight({
      inputPath,
    });

    assert.equal(report.status, 'passed');
    assert.equal(report.decision, 'create_new');
    assert.equal(report.input_path, inputPath);
    assert.equal(report.out_dir, null);
    assert.match(report.generated_at_utc, /^\d{4}-\d{2}-\d{2}T/u);
    assert.equal(report.target.identity_key, '42');
  } finally {
    rmSync(workDir, { recursive: true, force: true });
  }
});

test('identity preflight routes schema failures to blocker findings', async () => {
  const report = await runProcessIdentityPreflight({
    inputPath: '/tmp/process-preflight.json',
    rawInput: {
      target: {
        processDataSet: {},
      },
    },
    schemas: {
      process: failingSchema(),
    },
    now,
  });

  assert.equal(report.status, 'blocked');
  assert.equal(report.decision, 'manual_review');
  assert.equal(report.next_action, 'queue_manual_review');
  assert.equal(report.target.schema_validation.status, 'failed');
  assert.equal(
    report.target.schema_validation.issues[0]?.path,
    'processDataSet.processInformation',
  );
  assert.equal(report.blockers[0]?.code, 'process_schema_invalid');
});

test('identity preflight handles schema success, mismatches, and default issue fallbacks', async () => {
  const passed = await runProcessIdentityPreflight({
    inputPath: '/tmp/process-preflight.json',
    rawInput: {
      target: {
        processDataSet: {},
      },
    },
    schemas: {
      process: passingSchema(),
    },
    now,
  });

  assert.equal(passed.status, 'passed');
  assert.equal(passed.target.schema_validation.status, 'passed');
  assert.equal(passed.target.schema_validation.validator, 'injected');

  const mismatch = await runProcessIdentityPreflight({
    inputPath: '/tmp/process-preflight.json',
    rawInput: {
      target: {
        flowDataSet: {},
      },
    },
    now,
  });

  assert.equal(mismatch.status, 'blocked');
  assert.equal(mismatch.target.schema_validation.issues[0]?.code, 'dataset_kind_mismatch');

  const defaultIssueFallback = await runFlowIdentityPreflight({
    inputPath: '/tmp/flow-preflight.json',
    rawInput: {
      target: {
        flowDataSet: {},
      },
    },
    schemas: {
      flow: failingSchemaWithoutIssueDetails(),
    },
    now,
  });

  assert.equal(
    defaultIssueFallback.target.schema_validation.issues[0]?.message,
    'Validation failed',
  );
  assert.equal(defaultIssueFallback.target.schema_validation.issues[0]?.code, 'custom');
});

test('identity preflight internals cover schema lookup and decision confidence edges', () => {
  const schema = __testInternals.schemaForKind('process', undefined);
  assert.match(schema.validator, /ProcessSchema/u);
  assert.equal(typeof schema.schema.safeParse, 'function');

  const originalProcessFactory = __testInternals.entityFactoryExports.process;
  try {
    __testInternals.entityFactoryExports.process = 'missingProcessFactory' as never;
    const schemaWithoutFactory = __testInternals.schemaForKind('process', undefined);
    assert.equal(schemaWithoutFactory.createEntity, null);
  } finally {
    __testInternals.entityFactoryExports.process = originalProcessFactory;
  }

  assert.throws(
    () => __testInternals.schemaForKind('missing' as never, undefined),
    /undefined is unavailable/u,
  );

  const duplicate = __testInternals.chooseDecision(
    [
      {
        report: {
          index: 0,
          id: 'candidate-a',
          version: null,
          state_code: null,
          identity_key: 'weak',
          match_score: 50,
          match_reasons: ['same_identity_key'],
          decision_hint: 'block_duplicate',
        },
        findings: [],
      },
    ],
    { status: 'passed', validator: null, issue_count: 0, issues: [] },
    'flow',
  );
  assert.equal(duplicate.decision, 'block_duplicate');
  assert.equal(duplicate.confidence, 'medium');

  const manual = __testInternals.chooseDecision(
    [
      {
        report: {
          index: 0,
          id: null,
          version: null,
          state_code: null,
          identity_key: 'manual',
          match_score: 60,
          match_reasons: ['same_exchange_signature', 'overlapping_name'],
          decision_hint: null,
        },
        findings: [],
      },
    ],
    { status: 'passed', validator: null, issue_count: 0, issues: [] },
    'process',
  );
  assert.equal(manual.decision, 'manual_review');
  assert.equal(manual.confidence, 'medium');

  const weakReuse = __testInternals.chooseDecision(
    [
      {
        report: {
          index: 0,
          id: 'process-a',
          version: null,
          state_code: null,
          identity_key: 'weak',
          match_score: 50,
          match_reasons: ['same_dataset_id'],
          decision_hint: 'reuse',
        },
        findings: [],
      },
    ],
    { status: 'passed', validator: null, issue_count: 0, issues: [] },
    'process',
  );
  assert.equal(weakReuse.decision, 'reuse');
  assert.equal(weakReuse.confidence, 'medium');
});

test('identity preflight internals normalize remote search inputs', () => {
  assert.deepEqual(__testInternals.normalizeRemoteCandidateSearch(undefined), {
    enabled: false,
    query: null,
    filter: null,
    limit: null,
  });
  assert.deepEqual(__testInternals.normalizeRemoteCandidateSearch(true), {
    enabled: true,
    query: null,
    filter: null,
    limit: null,
  });
  assert.deepEqual(
    __testInternals.normalizeRemoteCandidateSearch({
      enabled: false,
      search_query: 'grid electricity',
      filter: { flowType: 'Product flow' },
      limit: '2',
    }),
    {
      enabled: false,
      query: 'grid electricity',
      filter: { flowType: 'Product flow' },
      limit: 2,
    },
  );
  assert.equal(__testInternals.normalizeRemoteCandidateSearch({ limit: '' }).limit, null);
  assert.deepEqual(__testInternals.rowsFromRemoteSearchResponse({ data: [{ id: 'a' }] }), [
    { id: 'a' },
  ]);
  assert.deepEqual(__testInternals.rowsFromRemoteSearchResponse({ results: [{ id: 'b' }] }), [
    { id: 'b' },
  ]);
  assert.deepEqual(__testInternals.rowsFromRemoteSearchResponse({ candidates: [{ id: 'c' }] }), [
    { id: 'c' },
  ]);
  assert.deepEqual(__testInternals.rowsFromRemoteSearchResponse([{ id: 'raw' }]), [{ id: 'raw' }]);
  assert.deepEqual(__testInternals.rowsFromRemoteSearchResponse({}), []);
  assert.deepEqual(__testInternals.rowsFromRemoteSearchResponse(null), []);
  assert.equal(
    __testInternals.defaultRemoteQuery({
      id: null,
      version: null,
      state_code: null,
      names: [],
      normalized_names: [],
      fields: { category: 'energy carrier' },
      exchange_signature: [],
      identity_key: 'fallback-key',
    }),
    'energy carrier',
  );
  assert.equal(
    __testInternals.defaultRemoteQuery({
      id: null,
      version: null,
      state_code: null,
      names: [],
      normalized_names: [],
      fields: {},
      exchange_signature: [],
      identity_key: 'fallback-key',
    }),
    'fallback-key',
  );
  assert.deepEqual(
    __testInternals.remoteSearchFilter(
      'flow',
      {
        id: null,
        version: null,
        state_code: null,
        names: [],
        normalized_names: [],
        fields: { type_of_dataset: ['Elementary flow'] },
        exchange_signature: [],
        identity_key: '',
      },
      null,
    ),
    { flowType: 'Elementary flow' },
  );
  assert.deepEqual(
    __testInternals.remoteSearchFilter(
      'flow',
      {
        id: null,
        version: null,
        state_code: null,
        names: [],
        normalized_names: [],
        fields: { type_of_dataset: 'Product flow' },
        exchange_signature: [],
        identity_key: '',
      },
      { flowType: 'explicit' },
    ),
    { flowType: 'explicit' },
  );
  assert.equal(
    __testInternals.remoteSearchFilter(
      'flow',
      {
        id: null,
        version: null,
        state_code: null,
        names: [],
        normalized_names: [],
        fields: {},
        exchange_signature: [],
        identity_key: '',
      },
      null,
    ),
    null,
  );

  assert.throws(
    () => __testInternals.normalizeRemoteCandidateSearch('yes'),
    /remote_candidate_search must be a boolean or object/u,
  );
  assert.throws(
    () => __testInternals.normalizeRemoteCandidateSearch({ limit: 0 }),
    /positive integer/u,
  );
});

test('identity preflight internals normalize candidate input aliases', () => {
  const normalized = __testInternals.normalizePreflightInput(
    {
      candidate: { name_en: 'target' },
      existing: { name_en: 'candidate-a' },
      rows: [{ name_en: 'candidate-b' }],
    },
    'process',
  );

  assert.deepEqual(normalized.target, { name_en: 'target' });
  assert.equal(normalized.candidates.length, 2);
  assert.equal(normalized.candidates[0]?.name_en, 'candidate-a');
  assert.equal(normalized.candidates[1]?.name_en, 'candidate-b');

  const flowNormalized = __testInternals.normalizePreflightInput(
    {
      flow: { name_en: 'flow-target' },
      candidates: {
        rows: [{ name_en: 'candidate-c' }],
      },
    },
    'flow',
  );

  assert.deepEqual(flowNormalized.target, { name_en: 'flow-target' });
  assert.equal(flowNormalized.candidates[0]?.name_en, 'candidate-c');

  const defaultTarget = __testInternals.normalizePreflightInput({ name_en: 'target' }, 'process');
  assert.deepEqual(defaultTarget.target, { name_en: 'target' });

  const processNormalized = __testInternals.normalizePreflightInput(
    {
      process: { name_en: 'process-target' },
    },
    'process',
  );
  assert.deepEqual(processNormalized.target, { name_en: 'process-target' });
});
