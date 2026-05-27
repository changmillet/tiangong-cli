import assert from 'node:assert/strict';
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { CliError } from '../src/lib/errors.js';
import { __testInternals, runDatasetEvidenceSearch } from '../src/lib/dataset-evidence-search.js';

function readJson(filePath: string): unknown {
  return JSON.parse(readFileSync(filePath, 'utf8'));
}

function readJsonl(filePath: string): unknown[] {
  return readFileSync(filePath, 'utf8')
    .split(/\r?\n/u)
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => JSON.parse(line));
}

const chinaPowerRequest = {
  question: '中国2026年电力结构数据',
  field: {
    dataset_type: 'process',
    field_path: '/processInformation/time/referenceYear',
  },
  preferred_domains: ['chinapower.org.cn'],
  required_terms: ['发电量', '火电', '风电', '太阳能'],
  required_evidence: {
    temporal_scope: '2026 full-year electricity generation mix',
    require_complete_year: true,
  },
  budget: {
    max_queries: 5,
    max_results_per_query: 3,
  },
};

test('dataset evidence-search plan writes a deterministic query matrix', async () => {
  const dir = mkdtempSync(path.join(os.tmpdir(), 'tg-cli-evidence-plan-'));
  try {
    const report = await runDatasetEvidenceSearch({
      mode: 'plan',
      rawInput: chinaPowerRequest,
      outDir: dir,
      now: new Date('2026-05-27T00:00:00.000Z'),
    });

    assert.equal(report.status, 'planned');
    assert.equal(report.question, '中国2026年电力结构数据');
    assert.equal(report.plan.query_count, 5);
    assert.equal(report.evidence_quality.temporal_coverage_status, 'incomplete_current_year');
    assert.equal(existsSync(report.files.plan ?? ''), true);
    assert.equal(existsSync(report.files.results ?? ''), true);
    assert.deepEqual(
      (readJson(report.files.plan ?? '') as { queries: unknown[] }).queries.length,
      5,
    );
    assert.deepEqual(readJsonl(report.files.results ?? ''), []);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('dataset evidence-search run records partial current-year evidence and declaration', async () => {
  const dir = mkdtempSync(path.join(os.tmpdir(), 'tg-cli-evidence-run-'));
  const resultsPath = path.join(dir, 'search-results.json');
  writeFileSync(
    resultsPath,
    JSON.stringify({
      queries: [
        {
          provider: 'web.run',
          query_id: 'q01',
          query: '中国2026年电力结构数据',
          items: [
            {
              title: '国家能源局发布2026年1-4月份全国电力统计数据',
              url: 'https://www.nea.gov.cn/20260525/c509435a0f09497cb3d2ca361fa262de/c.html',
              snippet:
                '截至4月底，全国累计发电装机容量39.9亿千瓦，其中太阳能发电装机容量12.5亿千瓦，风电装机容量6.6亿千瓦。',
              published_at: '2026-05-25',
            },
            {
              title: '中电联发布2026年一季度全国电力供需形势分析预测报告',
              url: 'https://chinapower.org.cn/detail/457464.html',
              snippet: '一季度全口径非化石能源发电量占总发电量比重为40.5%，煤电发电量占比为53.5%。',
              published_at: '2026-04-28',
            },
          ],
        },
      ],
    }),
    'utf8',
  );

  try {
    const report = await runDatasetEvidenceSearch({
      mode: 'run',
      rawInput: chinaPowerRequest,
      resultsPath,
      outDir: dir,
      now: new Date('2026-05-27T00:00:00.000Z'),
    });

    assert.equal(report.status, 'completed_with_partial_evidence');
    assert.equal(report.run.provider_count, 1);
    assert.equal(report.run.normalized_result_count, 2);
    assert.equal(report.run.authoritative_result_count, 2);
    assert.equal(report.run.stop_reason, 'complete_annual_scope_not_available');
    assert.equal(existsSync(report.files.declaration ?? ''), true);
    const declaration = readJson(report.files.declaration ?? '') as { declaration_type: string };
    assert.equal(declaration.declaration_type, 'partial_temporal_evidence');
    const normalized = readJsonl(report.files.results ?? '') as Array<{ source_tier: string }>;
    assert.deepEqual(
      normalized.map((result) => result.source_tier),
      ['official_statistics', 'preferred_domain'],
    );
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('dataset evidence-search run can call a generic JSON provider endpoint', async () => {
  const captured: Array<{ url: string; init?: RequestInit }> = [];
  const report = await runDatasetEvidenceSearch({
    mode: 'run',
    query: 'China 2025 electricity generation mix',
    providerUrl: 'https://search.example/query',
    providerKey: 'provider-token',
    maxQueries: 2,
    maxResultsPerQuery: 2,
    timeoutMs: 20,
    now: new Date('2026-05-27T00:00:00.000Z'),
    fetchImpl: async (url, init) => {
      captured.push({ url, init });
      return {
        ok: true,
        status: 200,
        headers: {
          get: () => 'application/json',
        },
        text: async () =>
          JSON.stringify({
            items: [
              {
                title: 'China electricity generation mix 2025',
                link: 'https://stats.gov.cn/electricity-2025',
                description: '2025 electricity generation coal hydro nuclear wind solar data.',
              },
            ],
          }),
      };
    },
  });

  assert.equal(report.status, 'completed_with_evidence');
  assert.equal(report.run.provider_call_count, 2);
  assert.equal(captured.length, 2);
  assert.equal(captured[0]?.url, 'https://search.example/query');
  assert.deepEqual(captured[0]?.init?.headers, {
    'Content-Type': 'application/json',
    Authorization: 'Bearer provider-token',
  });
  assert.equal(JSON.parse(String(captured[0]?.init?.body)).limit, 2);
});

test('dataset evidence-search validates required inputs and unsupported modes', async () => {
  await assert.rejects(
    () =>
      runDatasetEvidenceSearch({
        mode: 'plan',
        rawInput: {},
      }),
    /requires --query or input.question/u,
  );

  const primitiveInputReport = await runDatasetEvidenceSearch({
    mode: 'plan',
    query: 'fallback question',
    rawInput: 'not-an-object',
  });
  assert.equal(primitiveInputReport.question, 'fallback question');

  await assert.rejects(
    () =>
      runDatasetEvidenceSearch({
        mode: 'plan',
        query: 'x',
        profile: 'wide',
      }),
    /--profile/u,
  );

  await assert.rejects(
    () =>
      runDatasetEvidenceSearch({
        mode: 'run',
        query: 'x',
      }),
    /requires --results or --provider-url/u,
  );

  await assert.rejects(
    () =>
      runDatasetEvidenceSearch({
        mode: 'run',
        query: 'x',
        providerUrl: 'https://search.example/query',
      }),
    (error: unknown) => {
      assert.equal(error instanceof CliError, true);
      assert.equal((error as CliError).code, 'EVIDENCE_SEARCH_FETCH_IMPL_REQUIRED');
      return true;
    },
  );

  await assert.rejects(
    () =>
      runDatasetEvidenceSearch({
        mode: 'plan',
        query: 'x',
        maxQueries: 0,
      }),
    /positive integer/u,
  );
});

test('dataset evidence-search normalizes JSONL result files and insufficient evidence', async () => {
  const dir = mkdtempSync(path.join(os.tmpdir(), 'tg-cli-evidence-jsonl-'));
  const resultsPath = path.join(dir, 'results.jsonl');
  writeFileSync(
    resultsPath,
    `${JSON.stringify({
      title: 'Unrelated page',
      url: 'https://example.com/page',
      snippet: 'No relevant statistics here.',
    })}\n`,
    'utf8',
  );

  try {
    const report = await runDatasetEvidenceSearch({
      mode: 'run',
      query: 'China 2024 electricity generation mix',
      resultsPath,
      outDir: dir,
      now: new Date('2026-05-27T00:00:00.000Z'),
    });

    assert.equal(report.status, 'completed_no_sufficient_evidence');
    assert.equal(report.run.normalized_result_count, 1);
    assert.equal(report.files.declaration?.endsWith('evidence-search-declaration.json'), true);
    const declaration = readJson(report.files.declaration ?? '') as { declaration_type: string };
    assert.equal(declaration.declaration_type, 'no_sufficient_evidence');
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('dataset evidence-search normalizes mixed result shapes and keeps strongest duplicate', async () => {
  const report = await runDatasetEvidenceSearch({
    mode: 'run',
    query: 'China electricity mix',
    profile: 'deep',
    resultsPath: 'ignored-results.json',
    rawInput: {
      field: 'not-object',
      preferred_domains: 'not-array',
      required_terms: [null, 'solar'],
      budget: {
        max_queries: '3',
        max_results_per_query: '4',
      },
    },
    rawResults: [
      {
        provider: 'mixed-provider',
        text: 'China electricity mix',
        data: [
          'not-a-result',
          { title: 'Missing URL' },
          {
            title: 'Bad URL result',
            url: 'not a url',
            text: 'solar electricity mix',
          },
          {
            name: 'China electricity generation mix from NBS',
            href: 'https://www.stats.gov.cn/china-electricity',
            summary: 'Coal hydro nuclear wind solar generation mix data.',
            date: '2026-01-01',
          },
          {
            title: 'Duplicate lower score',
            url: 'https://example.com/duplicate',
            snippet: 'misc',
          },
          {
            title: 'Duplicate stronger score',
            url: 'https://example.com/duplicate',
            snippet: 'solar electricity generation mix data',
          },
        ],
      },
    ],
    now: new Date('2026-05-27T00:00:00.000Z'),
  });

  assert.equal(report.status, 'completed_with_evidence');
  assert.equal(report.profile, 'deep');
  assert.equal(report.budget.max_queries, 3);
  assert.equal(report.budget.max_results_per_query, 4);
  assert.equal(report.run.provider_count, 1);
  assert.equal(report.run.normalized_result_count, 2);
  assert.equal(report.files.report, null);

  const normalized = __testInternals.normalizeResults({
    rawResults: [
      {
        provider: 'mixed-provider',
        text: 'China electricity mix',
        data: [
          { title: 'Bad URL result', url: 'not a url', text: 'solar electricity mix' },
          {
            name: 'China electricity generation mix from IEA',
            href: 'https://reports.iea.org/china-electricity',
            summary: 'Coal hydro nuclear wind solar generation mix data.',
            date: '2026-01-01',
          },
          { title: 'Duplicate lower score', url: 'https://example.com/duplicate', snippet: 'misc' },
          {
            title: 'Duplicate stronger score',
            url: 'https://example.com/duplicate',
            snippet: 'solar electricity generation mix data',
          },
        ],
      },
      {
        provider: 'tie-provider',
        results: [
          { title: 'Later equal score', url: 'https://example.org/later' },
          { title: 'Earlier equal score', url: 'https://example.org/earlier' },
        ],
      },
      {
        query: 'fallback provider row',
        web_results: [{ title: 'Fallback provider row', url: 'https://example.net/row' }],
      },
    ],
    terms: ['solar', 'electricity', 'generation', 'mix'],
    preferredDomains: [],
    maxResultsPerQuery: 4,
  });
  assert.deepEqual(
    normalized.map((result) => [result.url, result.source_tier]),
    [
      ['https://reports.iea.org/china-electricity', 'international_statistics'],
      ['https://example.com/duplicate', 'open_web'],
      ['not a url', 'open_web'],
      ['https://example.org/later', 'open_web'],
      ['https://example.net/row', 'open_web'],
      ['https://example.org/earlier', 'open_web'],
    ],
  );

  const queryGroupFallbacks = __testInternals.normalizeResults({
    rawResults: {
      queries: [
        {
          provider: '   ',
          query_id: '   ',
          text: 'query group fallback text',
          results: [{ title: 'Fallback query group', url: 'https://example.net/query-group' }],
        },
        'ignored',
      ],
    },
    terms: [],
    preferredDomains: [],
    maxResultsPerQuery: 2,
  });
  assert.deepEqual(queryGroupFallbacks[0], {
    query_id: null,
    query: 'query group fallback text',
    provider: 'external',
    rank: 1,
    title: 'Fallback query group',
    url: 'https://example.net/query-group',
    snippet: null,
    published_at: null,
    source_domain: 'example.net',
    source_tier: 'open_web',
    matched_terms: [],
    score: 1,
  });
});

test('dataset evidence-search rejects invalid JSON and JSONL result files', async () => {
  const dir = mkdtempSync(path.join(os.tmpdir(), 'tg-cli-evidence-invalid-'));
  try {
    const requestPath = path.join(dir, 'request.json');
    const invalidJson = path.join(dir, 'invalid.json');
    const invalidJsonl = path.join(dir, 'invalid.jsonl');
    const emptyResults = path.join(dir, 'empty.jsonl');
    writeFileSync(requestPath, JSON.stringify({ question: 'question from file' }), 'utf8');
    writeFileSync(invalidJson, '{not-json', 'utf8');
    writeFileSync(invalidJsonl, 'not-json\n', 'utf8');
    writeFileSync(emptyResults, '\n  \n', 'utf8');

    const fileInputReport = await runDatasetEvidenceSearch({
      mode: 'plan',
      inputPath: requestPath,
    });
    assert.equal(fileInputReport.question, 'question from file');

    const emptyReport = await runDatasetEvidenceSearch({
      mode: 'run',
      query: 'China electricity mix',
      resultsPath: emptyResults,
    });
    assert.equal(emptyReport.status, 'completed_no_sufficient_evidence');
    assert.equal(emptyReport.run.normalized_result_count, 0);

    await assert.rejects(
      () =>
        runDatasetEvidenceSearch({
          mode: 'run',
          query: 'China electricity mix',
          resultsPath: invalidJson,
        }),
      /not valid JSON/u,
    );
    await assert.rejects(
      () =>
        runDatasetEvidenceSearch({
          mode: 'run',
          query: 'China electricity mix',
          resultsPath: invalidJsonl,
        }),
      /invalid JSONL/u,
    );
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('dataset evidence-search provider supports array payloads without authorization', async () => {
  const captured: Array<{ url: string; init?: RequestInit }> = [];
  const report = await runDatasetEvidenceSearch({
    mode: 'run',
    query: 'China 2027 electricity mix',
    rawInput: {
      required_evidence: {
        temporal_scope: '2027 full-year electricity generation mix',
        temporal_coverage: 'annual_complete',
      },
    },
    providerUrl: 'https://search.example/query',
    maxQueries: 2,
    fetchImpl: async (url, init) => {
      captured.push({ url, init });
      const responseText =
        captured.length === 1
          ? JSON.stringify([
              {
                title: 'China 2027 electricity generation mix forecast',
                url: 'https://stats.gov.cn/forecast',
                snippet: 'Forecast coal hydro nuclear wind solar electricity generation mix.',
              },
            ])
          : JSON.stringify('unsupported provider payload');
      return {
        ok: true,
        status: 200,
        headers: {
          get: () => 'application/json',
        },
        text: async () => responseText,
      };
    },
    now: new Date('2026-05-27T00:00:00.000Z'),
  });

  assert.equal(report.status, 'completed_with_partial_evidence');
  assert.equal(report.evidence_quality.temporal_coverage_status, 'future_year');
  assert.equal(report.run.provider_call_count, 2);
  assert.deepEqual(captured[0]?.init?.headers, {
    'Content-Type': 'application/json',
  });
});

test('dataset evidence-search internals classify domains and complete-year timing', () => {
  assert.equal(__testInternals.classifySourceTier(null, []), 'open_web');
  assert.equal(__testInternals.classifySourceTier('sub.stats.gov.cn', []), 'official_statistics');
  assert.equal(__testInternals.classifySourceTier('www.cec.org.cn', []), 'industry_association');
  assert.equal(
    __testInternals.classifySourceTier('reports.iea.org', []),
    'international_statistics',
  );
  assert.equal(
    __testInternals.classifySourceTier('example.com', ['example.com']),
    'preferred_domain',
  );
  assert.equal(__testInternals.classifySourceTier('example.com', []), 'open_web');
  assert.equal(
    __testInternals.temporalCoverageStatus(
      {
        question: 'x',
        field: { dataset_type: null, field_path: null },
        profile: 'balanced',
        budget: { max_queries: 1, max_results_per_query: 1, max_provider_calls: 1 },
        preferred_domains: [],
        required_terms: [],
        required_complete_year: false,
        requested_year: null,
      },
      new Date('2026-05-27T00:00:00.000Z'),
    ),
    'not_required',
  );
  assert.equal(
    __testInternals.temporalCoverageStatus(
      {
        question: 'x',
        field: { dataset_type: null, field_path: null },
        profile: 'balanced',
        budget: { max_queries: 1, max_results_per_query: 1, max_provider_calls: 1 },
        preferred_domains: [],
        required_terms: [],
        required_complete_year: true,
        requested_year: 2025,
      },
      new Date('2026-05-27T00:00:00.000Z'),
    ),
    'complete_year_possible',
  );
});
