import path from 'node:path';
import { readFileSync } from 'node:fs';
import { writeJsonArtifact, writeJsonLinesArtifact } from './artifacts.js';
import { CliError } from './errors.js';
import type { FetchLike } from './http.js';
import { postJson } from './http.js';
import { readJsonInput } from './io.js';

type JsonObject = Record<string, unknown>;

export type EvidenceSearchMode = 'plan' | 'run';
export type EvidenceSearchProfile = 'shallow' | 'balanced' | 'deep';
export type EvidenceSearchStatus =
  | 'planned'
  | 'completed_with_evidence'
  | 'completed_with_partial_evidence'
  | 'completed_no_sufficient_evidence';

export type EvidenceSearchBudget = {
  max_queries: number;
  max_results_per_query: number;
  max_provider_calls: number;
};

export type EvidenceSearchQuery = {
  query_id: string;
  text: string;
  purpose: string;
  source_tier: string;
  priority: number;
  expected_terms: string[];
};

export type EvidenceSearchResult = {
  query_id: string | null;
  query: string | null;
  provider: string;
  rank: number;
  title: string;
  url: string;
  snippet: string | null;
  published_at: string | null;
  source_domain: string | null;
  source_tier: string;
  matched_terms: string[];
  score: number;
};

export type EvidenceSearchDeclaration = {
  schema_version: 1;
  generated_at_utc: string;
  question: string;
  declaration_type: 'no_sufficient_evidence' | 'partial_temporal_evidence';
  statement: string;
  search_scope: {
    query_count: number;
    provider_count: number;
    normalized_result_count: number;
    authoritative_result_count: number;
    required_complete_year: boolean;
    requested_year: number | null;
    temporal_coverage_status: string;
  };
  limits: string[];
};

export type EvidenceSearchReport = {
  schema_version: 1;
  generated_at_utc: string;
  mode: EvidenceSearchMode;
  status: EvidenceSearchStatus;
  question: string;
  field: {
    dataset_type: string | null;
    field_path: string | null;
  };
  profile: EvidenceSearchProfile;
  budget: EvidenceSearchBudget;
  plan: {
    query_count: number;
    queries: EvidenceSearchQuery[];
  };
  run: {
    provider_count: number;
    provider_call_count: number;
    normalized_result_count: number;
    authoritative_result_count: number;
    high_confidence_result_count: number;
    stop_reason: string;
  };
  evidence_quality: {
    sufficient: boolean;
    temporal_coverage_status: string;
    requested_year: number | null;
    required_complete_year: boolean;
  };
  files: {
    plan: string | null;
    results: string | null;
    report: string | null;
    declaration: string | null;
  };
};

export type RunDatasetEvidenceSearchOptions = {
  mode: EvidenceSearchMode;
  query?: string | null;
  inputPath?: string | null;
  resultsPath?: string | null;
  providerUrl?: string | null;
  providerKey?: string | null;
  profile?: string | null;
  outDir?: string | null;
  maxQueries?: number | null;
  maxResultsPerQuery?: number | null;
  timeoutMs?: number | null;
  rawInput?: unknown;
  rawResults?: unknown;
  now?: Date;
  fetchImpl?: FetchLike;
};

type EvidenceSearchRequest = {
  question: string;
  field: {
    dataset_type: string | null;
    field_path: string | null;
  };
  profile: EvidenceSearchProfile;
  budget: EvidenceSearchBudget;
  preferred_domains: string[];
  required_terms: string[];
  required_complete_year: boolean;
  requested_year: number | null;
};

type ProviderQueryResult = {
  provider: string;
  query_id: string | null;
  query: string | null;
  items: unknown[];
};

const DEFAULT_BUDGETS: Record<EvidenceSearchProfile, EvidenceSearchBudget> = {
  shallow: {
    max_queries: 4,
    max_results_per_query: 5,
    max_provider_calls: 4,
  },
  balanced: {
    max_queries: 8,
    max_results_per_query: 8,
    max_provider_calls: 8,
  },
  deep: {
    max_queries: 14,
    max_results_per_query: 10,
    max_provider_calls: 14,
  },
};

const PUBLIC_OFFICIAL_DOMAINS = [
  'gov.cn',
  'stats.gov.cn',
  'nea.gov.cn',
  'ndrc.gov.cn',
  'samr.gov.cn',
];

const ELECTRICITY_TERMS = [
  '中国',
  '全国',
  '2026',
  '电力',
  '电源',
  '结构',
  '数据',
  '发电',
  '发电量',
  '装机',
  '火电',
  '水电',
  '核电',
  '风电',
  '太阳能',
  '非化石',
  'coal',
  'hydro',
  'nuclear',
  'wind',
  'solar',
  'generation',
  'electricity',
  'mix',
];

function nowIso(now: Date = new Date()): string {
  return now.toISOString();
}

function isRecord(value: unknown): value is JsonObject {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function trimToken(value: unknown): string | null {
  if (typeof value !== 'string') {
    return null;
  }
  const trimmed = value.trim();
  return trimmed ? trimmed : null;
}

function normalizeProfile(value: string | null | undefined): EvidenceSearchProfile {
  const normalized = value?.trim().toLowerCase();
  if (!normalized) {
    return 'balanced';
  }
  if (normalized === 'shallow' || normalized === 'balanced' || normalized === 'deep') {
    return normalized;
  }
  throw new CliError("--profile must be 'shallow', 'balanced', or 'deep'.", {
    code: 'EVIDENCE_SEARCH_PROFILE_INVALID',
    exitCode: 2,
    details: value,
  });
}

function readPositiveInteger(value: unknown, fallback: number, label: string): number {
  if (value === undefined || value === null || value === '') {
    return fallback;
  }
  const numberValue = typeof value === 'number' ? value : Number(value);
  if (!Number.isInteger(numberValue) || numberValue < 1) {
    throw new CliError(`${label} must be a positive integer.`, {
      code: 'EVIDENCE_SEARCH_BUDGET_INVALID',
      exitCode: 2,
      details: value,
    });
  }
  return numberValue;
}

function readStringArray(value: unknown): string[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value.map((item) => trimToken(item)).filter((item): item is string => item !== null);
}

function readBoolean(value: unknown): boolean {
  return value === true;
}

function readRequestedYear(question: string, request: JsonObject): number | null {
  const rawTemporal = isRecord(request.required_evidence)
    ? trimToken(request.required_evidence.temporal_scope)
    : null;
  const source = rawTemporal ? `${question} ${rawTemporal}` : question;
  const match = /(?:19|20)\d{2}/u.exec(source);
  return match ? Number(match[0]) : null;
}

function requestRequiresCompleteYear(request: JsonObject): boolean {
  if (!isRecord(request.required_evidence)) {
    return false;
  }
  return (
    readBoolean(request.required_evidence.require_complete_year) ||
    trimToken(request.required_evidence.temporal_coverage)?.toLowerCase() === 'annual_complete'
  );
}

function normalizeDomain(url: string): string | null {
  try {
    return new URL(url).hostname.toLowerCase().replace(/^www\./u, '');
  } catch {
    return null;
  }
}

function domainMatches(domain: string | null, candidate: string): boolean {
  if (!domain) {
    return false;
  }
  const normalized = candidate.toLowerCase().replace(/^www\./u, '');
  return domain === normalized || domain.endsWith(`.${normalized}`);
}

function classifySourceTier(domain: string | null, preferredDomains: string[]): string {
  if (preferredDomains.some((preferred) => domainMatches(domain, preferred))) {
    return 'preferred_domain';
  }
  if (PUBLIC_OFFICIAL_DOMAINS.some((official) => domainMatches(domain, official))) {
    return 'official_statistics';
  }
  if (domainMatches(domain, 'cec.org.cn') || domainMatches(domain, 'chinapower.org.cn')) {
    return 'industry_association';
  }
  if (domainMatches(domain, 'iea.org') || domainMatches(domain, 'ember-energy.org')) {
    return 'international_statistics';
  }
  return 'open_web';
}

function extractTerms(question: string, configuredTerms: string[]): string[] {
  const terms = new Set<string>();
  for (const term of configuredTerms) {
    terms.add(term.toLowerCase());
  }
  for (const term of ELECTRICITY_TERMS) {
    if (question.toLowerCase().includes(term.toLowerCase())) {
      terms.add(term.toLowerCase());
    }
  }
  const latinTerms = question.match(/[a-z0-9][a-z0-9-]{2,}/giu) ?? [];
  for (const term of latinTerms) {
    terms.add(term.toLowerCase());
  }
  return [...terms];
}

function dedupeStrings(values: string[]): string[] {
  const seen = new Set<string>();
  const result: string[] = [];
  for (const value of values) {
    const normalized = value.replace(/\s+/gu, ' ').trim();
    const key = normalized.toLowerCase();
    if (normalized && !seen.has(key)) {
      seen.add(key);
      result.push(normalized);
    }
  }
  return result;
}

function buildQueryTexts(request: EvidenceSearchRequest): string[] {
  const yearText = request.requested_year ? String(request.requested_year) : '';
  const preferredQueries = request.preferred_domains.map(
    (domain) => `site:${domain} ${request.question}`,
  );
  const generated = [
    request.question,
    `site:stats.gov.cn ${request.question}`,
    `site:nea.gov.cn ${request.question}`,
    yearText
      ? `国家统计局 ${yearText} 发电量 火电 水电 核电 风电 太阳能`
      : '国家统计局 发电量 火电 水电 核电 风电 太阳能',
    yearText
      ? `国家能源局 ${yearText} 全国电力统计数据 发电装机容量 火电 水电 风电 太阳能`
      : '国家能源局 全国电力统计数据 发电装机容量 火电 水电 风电 太阳能',
    yearText
      ? `中电联 ${yearText} 电力供需形势 非化石能源 发电量 占比`
      : '中电联 电力供需形势 非化石能源 发电量 占比',
    yearText
      ? `China ${yearText} electricity generation mix coal hydro nuclear wind solar`
      : 'China electricity generation mix coal hydro nuclear wind solar',
    yearText
      ? `China ${yearText} installed power capacity mix thermal hydro nuclear wind solar`
      : 'China installed power capacity mix thermal hydro nuclear wind solar',
    ...preferredQueries,
  ];
  return dedupeStrings(generated).slice(0, request.budget.max_queries);
}

function buildSearchPlan(request: EvidenceSearchRequest): EvidenceSearchQuery[] {
  const expectedTerms = extractTerms(request.question, request.required_terms);
  return buildQueryTexts(request).map((text, index) => ({
    query_id: `q${String(index + 1).padStart(2, '0')}`,
    text,
    purpose: index === 0 ? 'broad_discovery' : 'source_targeted_discovery',
    source_tier: text.startsWith('site:') ? 'targeted' : 'general',
    priority: index + 1,
    expected_terms: expectedTerms,
  }));
}

function normalizeRequest(options: RunDatasetEvidenceSearchOptions): EvidenceSearchRequest {
  const rawInput = options.rawInput ?? (options.inputPath ? readJsonInput(options.inputPath) : {});
  const input = isRecord(rawInput) ? rawInput : {};
  const question = trimToken(options.query) ?? trimToken(input.question);
  if (!question) {
    throw new CliError('Evidence search requires --query or input.question.', {
      code: 'EVIDENCE_SEARCH_QUESTION_REQUIRED',
      exitCode: 2,
    });
  }

  const profile = normalizeProfile(options.profile ?? trimToken(input.profile));
  const defaultBudget = DEFAULT_BUDGETS[profile];
  const budgetInput = isRecord(input.budget) ? input.budget : {};
  const maxQueries = readPositiveInteger(
    options.maxQueries ?? budgetInput.max_queries,
    defaultBudget.max_queries,
    '--max-queries',
  );
  const maxResultsPerQuery = readPositiveInteger(
    options.maxResultsPerQuery ?? budgetInput.max_results_per_query,
    defaultBudget.max_results_per_query,
    '--max-results-per-query',
  );
  const budget = {
    max_queries: maxQueries,
    max_results_per_query: maxResultsPerQuery,
    max_provider_calls: maxQueries,
  };
  const fieldInput = isRecord(input.field) ? input.field : {};

  return {
    question,
    field: {
      dataset_type: trimToken(fieldInput.dataset_type),
      field_path: trimToken(fieldInput.field_path),
    },
    profile,
    budget,
    preferred_domains: readStringArray(input.preferred_domains),
    required_terms: readStringArray(input.required_terms),
    required_complete_year: requestRequiresCompleteYear(input),
    requested_year: readRequestedYear(question, input),
  };
}

function readResultsInput(resultsPath: string, rawResults?: unknown): unknown {
  if (rawResults !== undefined) {
    return rawResults;
  }
  const text = readFileSync(resultsPath, 'utf8');
  const trimmed = text.trim();
  if (!trimmed) {
    return [];
  }
  if (trimmed.startsWith('{') || trimmed.startsWith('[')) {
    try {
      return JSON.parse(trimmed);
    } catch (error) {
      throw new CliError(`Search results file is not valid JSON: ${resultsPath}`, {
        code: 'EVIDENCE_SEARCH_RESULTS_INVALID_JSON',
        exitCode: 2,
        details: String(error),
      });
    }
  }
  return trimmed
    .split(/\r?\n/u)
    .map((line, index) => {
      try {
        return JSON.parse(line);
      } catch (error) {
        throw new CliError(`Search results file has invalid JSONL at line ${index + 1}.`, {
          code: 'EVIDENCE_SEARCH_RESULTS_INVALID_JSONL',
          exitCode: 2,
          details: String(error),
        });
      }
    })
    .filter((item) => item !== null);
}

function extractItems(value: JsonObject): unknown[] {
  for (const key of ['items', 'results', 'data', 'web_results']) {
    const candidate = value[key];
    if (Array.isArray(candidate)) {
      return candidate;
    }
  }
  return [];
}

function normalizeResultGroups(rawResults: unknown): ProviderQueryResult[] {
  const root = rawResults;
  if (isRecord(root) && Array.isArray(root.queries)) {
    return root.queries.filter(isRecord).map((queryGroup) => ({
      provider: trimToken(queryGroup.provider) ?? 'external',
      query_id: trimToken(queryGroup.query_id),
      query: trimToken(queryGroup.query ?? queryGroup.text),
      items: extractItems(queryGroup),
    }));
  }
  const rows = Array.isArray(root) ? root : [root];
  if (rows.every(isRecord) && rows.some((row) => extractItems(row).length > 0)) {
    return rows.filter(isRecord).map((queryGroup) => ({
      provider: trimToken(queryGroup.provider) ?? 'external',
      query_id: trimToken(queryGroup.query_id),
      query: trimToken(queryGroup.query ?? queryGroup.text),
      items: extractItems(queryGroup),
    }));
  }
  return [
    {
      provider: 'external',
      query_id: null,
      query: null,
      items: rows,
    },
  ];
}

function normalizeOneResult(options: {
  item: unknown;
  group: ProviderQueryResult;
  rank: number;
  terms: string[];
  preferredDomains: string[];
}): EvidenceSearchResult | null {
  if (!isRecord(options.item)) {
    return null;
  }
  const title = trimToken(options.item.title ?? options.item.name);
  const url = trimToken(options.item.url ?? options.item.link ?? options.item.href);
  if (!title || !url) {
    return null;
  }
  const snippet = trimToken(
    options.item.snippet ?? options.item.description ?? options.item.summary ?? options.item.text,
  );
  const publishedAt = trimToken(
    options.item.published_at ?? options.item.publishedAt ?? options.item.date,
  );
  const sourceText = `${title} ${snippet ?? ''}`.toLowerCase();
  const matchedTerms = options.terms.filter((term) => sourceText.includes(term));
  const domain = normalizeDomain(url);
  const sourceTier = classifySourceTier(domain, options.preferredDomains);
  const authorityScore =
    sourceTier === 'official_statistics'
      ? 4
      : sourceTier === 'preferred_domain' || sourceTier === 'industry_association'
        ? 3
        : sourceTier === 'international_statistics'
          ? 2
          : 1;
  const termScore = Math.min(matchedTerms.length, 6);
  return {
    query_id: options.group.query_id,
    query: options.group.query,
    provider: options.group.provider,
    rank: options.rank,
    title,
    url,
    snippet,
    published_at: publishedAt,
    source_domain: domain,
    source_tier: sourceTier,
    matched_terms: matchedTerms,
    score: authorityScore + termScore,
  };
}

function normalizeResults(options: {
  rawResults: unknown;
  terms: string[];
  preferredDomains: string[];
  maxResultsPerQuery: number;
}): EvidenceSearchResult[] {
  const byUrl = new Map<string, EvidenceSearchResult>();
  for (const group of normalizeResultGroups(options.rawResults)) {
    group.items.slice(0, options.maxResultsPerQuery).forEach((item, index) => {
      const result = normalizeOneResult({
        item,
        group,
        rank: index + 1,
        terms: options.terms,
        preferredDomains: options.preferredDomains,
      });
      if (!result) {
        return;
      }
      const previous = byUrl.get(result.url);
      if (!previous || result.score > previous.score) {
        byUrl.set(result.url, result);
      }
    });
  }
  return [...byUrl.values()].sort(
    (left, right) => right.score - left.score || left.rank - right.rank,
  );
}

async function fetchProviderResults(options: {
  providerUrl: string;
  providerKey: string | null;
  plan: EvidenceSearchQuery[];
  request: EvidenceSearchRequest;
  timeoutMs: number;
  fetchImpl: FetchLike;
}): Promise<{ rawResults: ProviderQueryResult[]; callCount: number }> {
  const rawResults: ProviderQueryResult[] = [];
  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
  };
  if (options.providerKey) {
    headers.Authorization = `Bearer ${options.providerKey}`;
  }
  for (const query of options.plan.slice(0, options.request.budget.max_provider_calls)) {
    const payload = await postJson({
      url: options.providerUrl,
      headers,
      body: {
        query: query.text,
        query_id: query.query_id,
        limit: options.request.budget.max_results_per_query,
        context: {
          question: options.request.question,
          field: options.request.field,
          expected_terms: query.expected_terms,
        },
      },
      timeoutMs: options.timeoutMs,
      fetchImpl: options.fetchImpl,
    });
    rawResults.push({
      provider: options.providerUrl,
      query_id: query.query_id,
      query: query.text,
      items: isRecord(payload) ? extractItems(payload) : Array.isArray(payload) ? payload : [],
    });
  }
  return { rawResults, callCount: rawResults.length };
}

function temporalCoverageStatus(request: EvidenceSearchRequest, now: Date): string {
  if (!request.required_complete_year || !request.requested_year) {
    return 'not_required';
  }
  const currentYear = now.getUTCFullYear();
  if (request.requested_year > currentYear) {
    return 'future_year';
  }
  if (request.requested_year === currentYear && now.getUTCMonth() < 11) {
    return 'incomplete_current_year';
  }
  return 'complete_year_possible';
}

function buildDeclaration(options: {
  generatedAt: string;
  request: EvidenceSearchRequest;
  queryCount: number;
  providerCount: number;
  resultCount: number;
  authoritativeResultCount: number;
  temporalStatus: string;
  partial: boolean;
}): EvidenceSearchDeclaration {
  const declarationType = options.partial ? 'partial_temporal_evidence' : 'no_sufficient_evidence';
  const statement =
    declarationType === 'partial_temporal_evidence'
      ? 'The configured search found current-year or forecast evidence, but not complete annual evidence for the requested year.'
      : 'Within the configured search scope, query matrix, source policy, and budget, no sufficient evidence was found for the requested field.';
  return {
    schema_version: 1,
    generated_at_utc: options.generatedAt,
    question: options.request.question,
    declaration_type: declarationType,
    statement,
    search_scope: {
      query_count: options.queryCount,
      provider_count: options.providerCount,
      normalized_result_count: options.resultCount,
      authoritative_result_count: options.authoritativeResultCount,
      required_complete_year: options.request.required_complete_year,
      requested_year: options.request.requested_year,
      temporal_coverage_status: options.temporalStatus,
    },
    limits: [
      'The command records deterministic search scope and result normalization; it does not prove that the open web contains no undiscoverable source.',
      'Browser-only, paywalled, login-protected, or unindexed sources require separate readback or manual evidence capture.',
      'A no-evidence or partial-evidence declaration is valid only for the configured query matrix, provider set, and budget.',
    ],
  };
}

function buildReport(options: {
  generatedAt: string;
  mode: EvidenceSearchMode;
  request: EvidenceSearchRequest;
  plan: EvidenceSearchQuery[];
  results: EvidenceSearchResult[];
  providerCount: number;
  providerCallCount: number;
  files: EvidenceSearchReport['files'];
  now: Date;
}): EvidenceSearchReport {
  const authoritativeResultCount = options.results.filter((result) =>
    ['official_statistics', 'preferred_domain', 'industry_association'].includes(
      result.source_tier,
    ),
  ).length;
  const highConfidenceResultCount = options.results.filter((result) => result.score >= 7).length;
  const temporalStatus = temporalCoverageStatus(options.request, options.now);
  const hasEvidence = authoritativeResultCount > 0 || highConfidenceResultCount > 0;
  const partial =
    hasEvidence && ['future_year', 'incomplete_current_year'].includes(temporalStatus);
  const sufficient = hasEvidence && !partial;
  const status: EvidenceSearchStatus =
    options.mode === 'plan'
      ? 'planned'
      : sufficient
        ? 'completed_with_evidence'
        : partial
          ? 'completed_with_partial_evidence'
          : 'completed_no_sufficient_evidence';
  const stopReason =
    options.mode === 'plan'
      ? 'plan_only'
      : sufficient
        ? 'sufficient_authoritative_evidence_found'
        : partial
          ? 'complete_annual_scope_not_available'
          : 'budget_exhausted_without_sufficient_evidence';

  return {
    schema_version: 1,
    generated_at_utc: options.generatedAt,
    mode: options.mode,
    status,
    question: options.request.question,
    field: options.request.field,
    profile: options.request.profile,
    budget: options.request.budget,
    plan: {
      query_count: options.plan.length,
      queries: options.plan,
    },
    run: {
      provider_count: options.providerCount,
      provider_call_count: options.providerCallCount,
      normalized_result_count: options.results.length,
      authoritative_result_count: authoritativeResultCount,
      high_confidence_result_count: highConfidenceResultCount,
      stop_reason: stopReason,
    },
    evidence_quality: {
      sufficient,
      temporal_coverage_status: temporalStatus,
      requested_year: options.request.requested_year,
      required_complete_year: options.request.required_complete_year,
    },
    files: options.files,
  };
}

export async function runDatasetEvidenceSearch(
  options: RunDatasetEvidenceSearchOptions,
): Promise<EvidenceSearchReport> {
  const request = normalizeRequest(options);
  const plan = buildSearchPlan(request);
  const generatedAt = nowIso(options.now);
  const now = options.now ?? new Date();
  const outputDir = options.outDir ? path.resolve(options.outDir) : null;
  const planFile = outputDir ? path.join(outputDir, 'outputs', 'evidence-search-plan.json') : null;
  const resultsFile = outputDir
    ? path.join(outputDir, 'outputs', 'evidence-search-results.jsonl')
    : null;
  const reportFile = outputDir
    ? path.join(outputDir, 'outputs', 'evidence-search-report.json')
    : null;
  const declarationFile = outputDir
    ? path.join(outputDir, 'outputs', 'evidence-search-declaration.json')
    : null;

  if (planFile) {
    writeJsonArtifact(planFile, {
      schema_version: 1,
      generated_at_utc: generatedAt,
      question: request.question,
      field: request.field,
      profile: request.profile,
      budget: request.budget,
      queries: plan,
    });
  }

  let rawResults: unknown = [];
  let providerCount = 0;
  let providerCallCount = 0;

  if (options.mode === 'run') {
    if (options.resultsPath) {
      rawResults = readResultsInput(options.resultsPath, options.rawResults);
      providerCount += 1;
    }
    if (options.providerUrl) {
      if (!options.fetchImpl) {
        throw new CliError('Evidence search provider mode requires fetchImpl.', {
          code: 'EVIDENCE_SEARCH_FETCH_IMPL_REQUIRED',
          exitCode: 2,
        });
      }
      const providerResults = await fetchProviderResults({
        providerUrl: options.providerUrl,
        providerKey: options.providerKey ?? null,
        plan,
        request,
        timeoutMs: options.timeoutMs ?? 30_000,
        fetchImpl: options.fetchImpl,
      });
      rawResults = [...normalizeResultGroups(rawResults), ...providerResults.rawResults];
      providerCount += 1;
      providerCallCount += providerResults.callCount;
    }
    if (!options.resultsPath && !options.providerUrl) {
      throw new CliError('dataset evidence-search run requires --results or --provider-url.', {
        code: 'EVIDENCE_SEARCH_PROVIDER_REQUIRED',
        exitCode: 2,
      });
    }
  }

  const terms = extractTerms(request.question, request.required_terms);
  const results =
    options.mode === 'run'
      ? normalizeResults({
          rawResults,
          terms,
          preferredDomains: request.preferred_domains,
          maxResultsPerQuery: request.budget.max_results_per_query,
        })
      : [];
  if (resultsFile) {
    writeJsonLinesArtifact(resultsFile, results);
  }

  const preliminaryReport = buildReport({
    generatedAt,
    mode: options.mode,
    request,
    plan,
    results,
    providerCount,
    providerCallCount,
    files: {
      plan: planFile,
      results: resultsFile,
      report: reportFile,
      declaration: null,
    },
    now,
  });

  let finalDeclarationFile: string | null = null;
  if (
    outputDir &&
    options.mode === 'run' &&
    preliminaryReport.status !== 'completed_with_evidence'
  ) {
    const declaration = buildDeclaration({
      generatedAt,
      request,
      queryCount: plan.length,
      providerCount,
      resultCount: results.length,
      authoritativeResultCount: preliminaryReport.run.authoritative_result_count,
      temporalStatus: preliminaryReport.evidence_quality.temporal_coverage_status,
      partial: preliminaryReport.status === 'completed_with_partial_evidence',
    });
    finalDeclarationFile = writeJsonArtifact(declarationFile!, declaration);
  }

  const report = {
    ...preliminaryReport,
    files: {
      ...preliminaryReport.files,
      declaration: finalDeclarationFile,
    },
  };
  if (reportFile) {
    writeJsonArtifact(reportFile, report);
  }
  return report;
}

export const __testInternals = {
  buildSearchPlan,
  classifySourceTier,
  normalizeResults,
  normalizeRequest,
  temporalCoverageStatus,
};
