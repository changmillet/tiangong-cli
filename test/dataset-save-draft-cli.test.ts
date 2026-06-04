import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { executeCli } from '../src/cli.js';
import { __testInternals, runDatasetSaveDraft } from '../src/lib/dataset-save-draft-run.js';
import type { DatasetSaveDraftReport } from '../src/lib/dataset-save-draft-run.js';
import type { DotEnvLoadResult } from '../src/lib/dotenv.js';
import type { FetchLike } from '../src/lib/http.js';
import {
  buildSupabaseTestEnv,
  isSupabaseAuthTokenUrl,
  makeSupabaseAuthResponse,
} from './helpers/supabase-auth.js';

const dotEnvStatus: DotEnvLoadResult = {
  loaded: false,
  path: '/tmp/.env',
  count: 0,
};

function makeDeps(overrides = {}) {
  return {
    env: {} as NodeJS.ProcessEnv,
    dotEnvStatus,
    fetchImpl: async () => ({
      ok: true,
      status: 200,
      headers: {
        get: () => 'application/json',
      },
      text: async () => JSON.stringify({ ok: true }),
    }),
    ...overrides,
  };
}

function jsonResponse(body: unknown): Awaited<ReturnType<FetchLike>> {
  return {
    ok: true,
    status: 200,
    headers: {
      get: (name: string) => (name.toLowerCase() === 'content-type' ? 'application/json' : null),
    },
    text: async () => JSON.stringify(body),
  };
}

function localized(text: string): { '@xml:lang': 'en'; '#text': string } {
  return { '@xml:lang': 'en', '#text': text };
}

function datasetRef(
  type: string,
  id: string,
  version: string,
  description: string,
): {
  '@type': string;
  '@refObjectId': string;
  '@version': string;
  '@uri': string;
  'common:shortDescription': { '@xml:lang': 'en'; '#text': string };
} {
  return {
    '@type': type,
    '@refObjectId': id,
    '@version': version,
    '@uri': `../datasets/${id}_${version}.xml`,
    'common:shortDescription': localized(description),
  };
}

function makeFlow(typeOfDataSet = 'Product flow'): Record<string, unknown> {
  return {
    flowDataSet: {
      '@xmlns': 'http://lca.jrc.it/ILCD/Flow',
      '@xmlns:common': 'http://lca.jrc.it/ILCD/Common',
      '@xmlns:ecn': 'http://eplca.jrc.ec.europa.eu/ILCD/Extensions/2018/ECNumber',
      '@xmlns:xsi': 'http://www.w3.org/2001/XMLSchema-instance',
      '@version': '1.1',
      '@locations': '../ILCDLocations.xml',
      '@xsi:schemaLocation': 'http://lca.jrc.it/ILCD/Flow ../../schemas/ILCD_FlowDataSet.xsd',
      flowInformation: {
        dataSetInformation: {
          'common:UUID': '11111111-1111-1111-1111-111111111111',
          name: {
            baseName: localized('Test flow'),
            treatmentStandardsRoutes: localized('not applicable'),
            mixAndLocationTypes: localized('market'),
          },
          classificationInformation: {
            'common:classification': {
              'common:class': {
                '@level': '0',
                '@classId': '001',
                '#text': 'General',
              },
            },
          },
        },
        quantitativeReference: {
          referenceToReferenceFlowProperty: '0',
        },
      },
      modellingAndValidation: {
        LCIMethod: {
          typeOfDataSet,
        },
        complianceDeclarations: {
          compliance: {
            'common:referenceToComplianceSystem': datasetRef(
              'source data set',
              '22222222-2222-2222-2222-222222222222',
              '00.00.001',
              'Compliance',
            ),
            'common:approvalOfOverallCompliance': 'Not defined',
          },
        },
      },
      administrativeInformation: {
        dataEntryBy: {
          'common:timeStamp': '2026-06-04T00:00:00.000Z',
          'common:referenceToDataSetFormat': datasetRef(
            'source data set',
            '33333333-3333-3333-3333-333333333333',
            '00.00.001',
            'Format',
          ),
        },
        publicationAndOwnership: {
          'common:dataSetVersion': '00.00.001',
          'common:referenceToOwnershipOfDataSet': datasetRef(
            'contact data set',
            '44444444-4444-4444-4444-444444444444',
            '00.00.001',
            'Owner',
          ),
        },
      },
      flowProperties: {
        flowProperty: {
          '@dataSetInternalID': '0',
          referenceToFlowPropertyDataSet: datasetRef(
            'flow property data set',
            '55555555-5555-5555-5555-555555555555',
            '00.00.001',
            'Mass',
          ),
          meanValue: '1.0',
        },
      },
    },
  };
}

function setFirstFlowPropertyVersion(flow: Record<string, unknown>, version: string): void {
  (
    flow.flowDataSet as {
      flowProperties: {
        flowProperty: {
          referenceToFlowPropertyDataSet: { '@version'?: string };
        };
      };
    }
  ).flowProperties.flowProperty.referenceToFlowPropertyDataSet['@version'] = version;
}

function flowPropertyId(flow: Record<string, unknown>): string {
  return (
    flow.flowDataSet as {
      flowProperties: {
        flowProperty: {
          referenceToFlowPropertyDataSet: { '@refObjectId': string };
        };
      };
    }
  ).flowProperties.flowProperty.referenceToFlowPropertyDataSet['@refObjectId'];
}

function makeSaveDraftFetch(options: {
  rootRows?: unknown[];
  latestSupportRows?: unknown[];
  exactSupportRows?: unknown[];
  createBody?: { ok: true; data?: unknown };
  observedUrls: string[];
  observedBodies?: unknown[];
}): FetchLike {
  return async (input, init) => {
    const url = String(input);
    options.observedUrls.push(url);
    if (isSupabaseAuthTokenUrl(url)) {
      return makeSupabaseAuthResponse({ accessToken: 'dataset-save-draft-token' });
    }

    const parsed = new URL(url);
    const table = parsed.pathname.split('/').at(-1);
    if (table === 'flows') {
      return jsonResponse(options.rootRows ?? []);
    }
    if (table === 'flowproperties') {
      return jsonResponse(
        parsed.searchParams.has('version')
          ? (options.exactSupportRows ?? [])
          : (options.latestSupportRows ?? []),
      );
    }
    if (parsed.pathname.endsWith('/functions/v1/app_dataset_create')) {
      if (typeof init?.body === 'string') {
        options.observedBodies?.push(JSON.parse(init.body));
      }
      return jsonResponse(options.createBody ?? { ok: true, data: { id: 'created' } });
    }
    return jsonResponse({ ok: true });
  };
}

function report(status: DatasetSaveDraftReport['status'] = 'completed'): DatasetSaveDraftReport {
  return {
    schema_version: 1,
    generated_at_utc: '2026-06-02T00:00:00.000Z',
    input_path: 'contacts.jsonl',
    requested_type: 'contact',
    out_dir: 'out',
    commit: true,
    mode: 'commit',
    status,
    counts: {
      selected: 1,
      prepared: status === 'completed' ? 0 : 1,
      executed: status === 'completed' ? 1 : 0,
      failed: status === 'completed' ? 0 : 1,
      by_table: {
        contacts: 1,
      },
      operations: {
        [status === 'completed' ? 'insert' : 'skipped_invalid']: 1,
      },
    },
    files: {
      selected_rows: 'out/outputs/dataset-save-draft/selected-rows.jsonl',
      progress_jsonl: 'out/outputs/dataset-save-draft/progress.jsonl',
      failures_jsonl: 'out/outputs/dataset-save-draft/failures.jsonl',
      summary_json: 'out/outputs/dataset-save-draft/summary.json',
    },
    rows: [],
  };
}

test('executeCli exposes generic dataset save-draft for support rows', async () => {
  const observed: unknown[] = [];
  const help = await executeCli(['dataset', 'save-draft', '--help'], makeDeps());
  assert.equal(help.exitCode, 0);
  assert.match(help.stdout, /tiangong-lca dataset save-draft --input <file>/u);
  assert.match(help.stdout, /auto, contact, source, flow, process/u);
  assert.match(help.stdout, /Unit group and flow property rows are reference-only/u);

  const result = await executeCli(
    [
      'dataset',
      'save-draft',
      '--json',
      '--input',
      'contacts.jsonl',
      '--type',
      'contact',
      '--out-dir',
      'contact-save',
      '--commit',
    ],
    makeDeps({
      runDatasetSaveDraftImpl: async (options: unknown) => {
        observed.push(options);
        return report();
      },
    }),
  );

  assert.equal(result.exitCode, 0);
  assert.deepEqual(JSON.parse(result.stdout), report());
  assert.equal(observed.length, 1);
  assert.deepEqual(observed[0], {
    inputPath: 'contacts.jsonl',
    type: 'contact',
    outDir: 'contact-save',
    commit: true,
    env: {},
    fetchImpl: (observed[0] as { fetchImpl: unknown }).fetchImpl,
  });
});

test('executeCli maps generic dataset save-draft failures and rejects mode conflicts', async () => {
  const failed = await executeCli(
    ['dataset', 'save-draft', '--input', 'contacts.jsonl', '--type', 'contact'],
    makeDeps({
      runDatasetSaveDraftImpl: async () => report('completed_with_failures'),
    }),
  );
  assert.equal(failed.exitCode, 1);

  const conflict = await executeCli(
    [
      'dataset',
      'save-draft',
      '--input',
      'contacts.jsonl',
      '--type',
      'contact',
      '--commit',
      '--dry-run',
    ],
    makeDeps(),
  );
  assert.equal(conflict.exitCode, 2);
  assert.match(conflict.stderr, /Cannot pass both --commit and --dry-run/u);
});

test('dataset save-draft rejects explicit reference-only support types', () => {
  assert.throws(
    () => __testInternals.normalizeType('flowproperty'),
    /Flow properties are reference-only support data/u,
  );
  assert.throws(
    () => __testInternals.normalizeType('unitgroup'),
    /Unit groups are reference-only support data/u,
  );
});

test('dataset save-draft support reference helpers find reference-only support rows', () => {
  const references = __testInternals.uniqueReferenceOnlySupportReferences({
    processDataSet: {
      exchanges: {
        exchange: [
          {
            referenceToFlowDataSet: {
              '@type': 'flow data set',
              '@refObjectId': 'flow-1',
              '@version': '01.00.000',
            },
          },
          {
            referenceToFlowPropertyDataSet: {
              '@type': 'flow property data set',
              '@refObjectId': 'fp-1',
              '@version': '01.00.000',
            },
          },
          {
            referenceToReferenceUnitGroup: {
              '@type': 'unit group data set',
              '@refObjectId': 'ug-1',
              '@version': '01.00.000',
            },
          },
          {
            referenceToFlowPropertyDataSet: {
              '@type': 'flow property data set',
              '@refObjectId': 'fp-1',
              '@version': '01.00.000',
            },
          },
        ],
      },
    },
  });

  assert.deepEqual(
    references.map((reference) => `${reference.table}:${reference.id}`),
    ['flowproperties:fp-1', 'unitgroups:ug-1'],
  );
  assert.equal(__testInternals.flowType({}), null);
  assert.equal(__testInternals.isElementaryFlowPayload(makeFlow('Elementary flow')), true);
  assert.equal(__testInternals.isElementaryFlowPayload(makeFlow('Product flow')), false);
});

test('runDatasetSaveDraft blocks elementary flow inserts after support resolution', async () => {
  const dir = mkdtempSync(path.join(os.tmpdir(), 'tg-cli-dataset-save-draft-elementary-'));
  const flow = makeFlow('Elementary flow');
  const urls: string[] = [];
  const supportId = flowPropertyId(flow);

  try {
    const report = await runDatasetSaveDraft({
      inputPath: path.join(dir, 'flows.json'),
      rawInput: { rows: [flow] },
      type: 'flow',
      outDir: dir,
      commit: true,
      now: new Date('2026-06-04T00:00:00.000Z'),
      env: buildSupabaseTestEnv(),
      fetchImpl: makeSaveDraftFetch({
        observedUrls: urls,
        exactSupportRows: [{ id: supportId, version: '00.00.001' }],
        latestSupportRows: [{ id: supportId, version: '00.00.001' }],
      }),
    });

    assert.equal(report.status, 'completed_with_failures');
    assert.equal(report.counts.executed, 0);
    assert.equal(report.counts.failed, 1);
    assert.equal(report.rows[0]?.operation, 'elementary_flow_insert_blocked');
    assert.equal(
      (report.rows[0]?.error?.details as { code?: string }).code,
      'DATASET_SAVE_DRAFT_ELEMENTARY_FLOW_INSERT_BLOCKED',
    );
    assert.equal(
      urls.some((url) => url.endsWith('/functions/v1/app_dataset_create')),
      false,
    );
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('runDatasetSaveDraft blocks flow writes with missing reference-only support versions', async () => {
  const dir = mkdtempSync(path.join(os.tmpdir(), 'tg-cli-dataset-save-draft-support-'));
  const flow = makeFlow('Product flow');
  const urls: string[] = [];
  const supportId = flowPropertyId(flow);
  setFirstFlowPropertyVersion(flow, '99.00.000');

  try {
    const report = await runDatasetSaveDraft({
      inputPath: path.join(dir, 'flows.json'),
      rawInput: { rows: [flow] },
      type: 'flow',
      outDir: dir,
      commit: true,
      now: new Date('2026-06-04T00:00:00.000Z'),
      env: buildSupabaseTestEnv(),
      fetchImpl: makeSaveDraftFetch({
        observedUrls: urls,
        exactSupportRows: [],
        latestSupportRows: [{ id: supportId, version: '00.00.001' }],
      }),
    });

    const details = report.rows[0]?.error?.details as {
      code?: string;
      references?: Array<{ status: string; id: string; version: string | null }>;
    };
    assert.equal(report.status, 'completed_with_failures');
    assert.equal(report.rows[0]?.operation, 'reference_only_support_missing');
    assert.equal(details.code, 'DATASET_SAVE_DRAFT_REFERENCE_ONLY_SUPPORT_MISSING');
    assert.deepEqual(details.references, [
      {
        table: 'flowproperties',
        id: supportId,
        version: '99.00.000',
        path: '/flowDataSet/flowProperties/flowProperty/referenceToFlowPropertyDataSet',
        short_description: 'Mass',
        status: 'missing_version',
      },
    ]);
    assert.equal(
      urls.some((url) => url.endsWith('/functions/v1/app_dataset_create')),
      false,
    );
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('runDatasetSaveDraft allows product flow inserts when reference-only support exists', async () => {
  const dir = mkdtempSync(path.join(os.tmpdir(), 'tg-cli-dataset-save-draft-product-'));
  const flow = makeFlow('Product flow');
  const urls: string[] = [];
  const bodies: unknown[] = [];
  const supportId = flowPropertyId(flow);

  try {
    const report = await runDatasetSaveDraft({
      inputPath: path.join(dir, 'flows.json'),
      rawInput: { rows: [flow] },
      type: 'flow',
      outDir: dir,
      commit: true,
      now: new Date('2026-06-04T00:00:00.000Z'),
      env: buildSupabaseTestEnv(),
      fetchImpl: makeSaveDraftFetch({
        observedUrls: urls,
        observedBodies: bodies,
        exactSupportRows: [{ id: supportId, version: '00.00.001' }],
        latestSupportRows: [{ id: supportId, version: '00.00.001' }],
      }),
    });

    assert.equal(report.status, 'completed');
    assert.equal(report.rows[0]?.operation, 'insert');
    assert.equal(report.counts.executed, 1);
    assert.equal(
      urls.some((url) => url.endsWith('/functions/v1/app_dataset_create')),
      true,
    );
    assert.deepEqual((bodies[0] as { table?: string; ruleVerification?: boolean })?.table, 'flows');
    assert.equal((bodies[0] as { ruleVerification?: boolean })?.ruleVerification, true);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
