import { existsSync, readdirSync, statSync } from 'node:fs';
import path from 'node:path';
import { writeJsonArtifact, writeTextArtifact } from './artifacts.js';
import { CliError } from './errors.js';
import type { FetchLike } from './http.js';
import { readJsonInput } from './io.js';
import { invokeLlm, readLlmRuntimeEnv, type LlmRuntimeEnv } from './llm.js';

type JsonRecord = Record<string, unknown>;

const KIND_RE = /\[tg_io_kind_tag=([^\]]+)\]/gu;
const UOM_RE = /\[tg_io_uom_tag=([^\]]+)\]/gu;

const ENERGY_WORDS = [
  'electric',
  'electricity',
  'kwh',
  'mj',
  'heat',
  'steam',
  'fuel',
  'diesel',
  'gas',
  'power',
  '电',
  '能',
] as const;

const RAW_WORDS = [
  'raw material',
  'feedstock',
  'fertilizer',
  'water',
  'seed',
  'pesticide',
  '原材料',
  '投入',
  '肥料',
  '种子',
  '农药',
  '用水',
] as const;

const BYP_WORDS = ['by-product', 'co-product', '副产品', '联产'] as const;
const WASTE_WORDS = ['waste', '废', 'residue', 'sludge'] as const;

type BaseInfoCheck = {
  name_zh_en_ok: boolean;
  functional_unit_ok: boolean;
  system_boundary_ok: boolean;
  time_ok: boolean;
  geo_ok: boolean;
  tech_ok: boolean;
  admin_ok: boolean;
  completeness_score: number;
  base_names: string[];
};

type ClassifiedExchange = {
  classification:
    | 'raw_material_input'
    | 'energy_input'
    | 'other_input'
    | 'product_output'
    | 'byproduct_output'
    | 'waste_output'
    | 'other_output'
    | 'other';
  kinds: string[];
  uoms: string[];
  blob: string;
};

type UnitIssue = {
  flow_uuid: string;
  current_unit: string;
  suggested_unit: string;
  basis: string;
  confidence: string;
};

type ProcessReviewRow = {
  process_file: string;
  raw_input: number;
  product: number;
  byproduct: number;
  waste: number;
  energy_excluded: number;
  delta: number;
  relative_deviation: number | null;
};

type ProcessReviewTotals = {
  raw_input: number;
  product_plus_byproduct_plus_waste: number;
  delta: number;
  relative_deviation: number | null;
  energy_excluded: number;
};

type ProcessSummaryForLlm = {
  process_file: string;
  base_names: string[];
  base_checks: {
    name_zh_en_ok: boolean;
    functional_unit_ok: boolean;
    system_boundary_ok: boolean;
    time_ok: boolean;
    geo_ok: boolean;
    tech_ok: boolean;
    admin_ok: boolean;
  };
  balance: {
    raw_in: number;
    product: number;
    byproduct: number;
    waste: number;
    energy_excluded: number;
    relative_deviation: number | null;
  };
};

export type ProcessReviewLlmResult =
  | {
      enabled: false;
      reason: string;
    }
  | {
      enabled: true;
      ok: true;
      result: JsonRecord;
    }
  | {
      enabled: true;
      ok: false;
      reason: string;
      raw?: string;
    };

export type ProcessReviewSummary = {
  run_id: string;
  logic_version: string;
  process_count: number;
  totals: ProcessReviewTotals;
  llm: ProcessReviewLlmResult;
};

export type ProcessReviewReport = {
  schema_version: 1;
  generated_at_utc: string;
  status: 'completed_local_process_review';
  run_id: string;
  run_root: string;
  out_dir: string;
  logic_version: string;
  process_count: number;
  totals: ProcessReviewTotals;
  files: {
    review_zh: string;
    review_en: string;
    timing: string;
    unit_issue_log: string;
    summary: string;
    report: string;
  };
  llm: ProcessReviewLlmResult;
};

export type RunProcessReviewOptions = {
  runRoot: string;
  runId: string;
  outDir: string;
  startTs?: string;
  endTs?: string;
  logicVersion?: string;
  enableLlm?: boolean;
  llmModel?: string;
  llmMaxProcesses?: number;
  env?: NodeJS.ProcessEnv;
  fetchImpl?: FetchLike;
  now?: () => Date;
};

function isRecord(value: unknown): value is JsonRecord {
  return Boolean(value && typeof value === 'object' && !Array.isArray(value));
}

function requiredNonEmpty(value: string | undefined, label: string, code: string): string {
  if (typeof value === 'string' && value.trim()) {
    return value.trim();
  }

  throw new CliError(`Missing required ${label}.`, {
    code,
    exitCode: 2,
  });
}

function textFromValue(value: unknown): string {
  if (Array.isArray(value)) {
    return value
      .filter(isRecord)
      .map((item) => String(item['#text'] ?? ''))
      .join(' ');
  }

  if (isRecord(value)) {
    return String(value['#text'] ?? '');
  }

  return String(value ?? '');
}

function toNumber(value: unknown): number {
  if (typeof value === 'number') {
    return Number.isFinite(value) ? value : 0;
  }

  if (typeof value === 'string' && value.trim()) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : 0;
  }

  return 0;
}

function deepGet(value: unknown, pathParts: string[]): unknown {
  let current: unknown = value;

  for (const key of pathParts) {
    if (!isRecord(current)) {
      return undefined;
    }
    current = current[key];
    if (current === undefined || current === null) {
      return undefined;
    }
  }

  return current;
}

function hasNonEmpty(value: unknown): boolean {
  if (value === null || value === undefined) {
    return false;
  }

  if (typeof value === 'string') {
    return value.trim().length > 0;
  }

  if (Array.isArray(value)) {
    return value.some((item) => hasNonEmpty(item));
  }

  if (isRecord(value)) {
    if ('#text' in value) {
      return hasNonEmpty(value['#text']);
    }
    return Object.values(value).some((item) => hasNonEmpty(item));
  }

  return true;
}

function extractBaseNames(processPayload: JsonRecord): [boolean, boolean, string[]] {
  const base = deepGet(processPayload, [
    'processDataSet',
    'processInformation',
    'dataSetInformation',
    'name',
    'baseName',
  ]);
  const items = Array.isArray(base) ? base : base ? [base] : [];
  let zh = false;
  let en = false;
  const values: string[] = [];

  items.forEach((item) => {
    if (!isRecord(item)) {
      return;
    }
    const lang = String(item['@xml:lang'] ?? '').toLowerCase();
    const text = String(item['#text'] ?? '').trim();
    if (!text) {
      return;
    }
    values.push(text);
    if (lang.startsWith('zh')) {
      zh = true;
    }
    if (lang.startsWith('en')) {
      en = true;
    }
  });

  if (!(zh && en) && values.length >= 2) {
    zh = zh || true;
    en = en || true;
  }

  return [zh, en, values];
}

function classifyExchange(exchange: JsonRecord): ClassifiedExchange {
  const comments =
    `${textFromValue(exchange.commonComment)} ${textFromValue(exchange.generalComment)}`.toLowerCase();
  const flowDescription = textFromValue(
    isRecord(exchange.referenceToFlowDataSet)
      ? exchange.referenceToFlowDataSet['common:shortDescription']
      : undefined,
  ).toLowerCase();
  const blob = `${comments} ${flowDescription}`.trim();
  const kinds = Array.from(blob.matchAll(KIND_RE), (match) => match[1].toLowerCase());
  const kindSet = new Set(kinds);
  const uoms = Array.from(blob.matchAll(UOM_RE), (match) => match[1].toLowerCase());
  const direction = String(exchange.exchangeDirection ?? '').toLowerCase();
  const isEnergy =
    ENERGY_WORDS.some((word) => blob.includes(word)) ||
    uoms.some((uom) => uom === 'kwh' || uom === 'mj' || uom === 'gj');

  if (direction === 'input') {
    if (kindSet.has('energy') || isEnergy) {
      return { classification: 'energy_input', kinds, uoms, blob };
    }
    if (kindSet.has('waste')) {
      return { classification: 'other_input', kinds, uoms, blob };
    }
    if (kindSet.has('raw_material') || kindSet.has('resource')) {
      return { classification: 'raw_material_input', kinds, uoms, blob };
    }
    if (kindSet.has('product') || RAW_WORDS.some((word) => blob.includes(word))) {
      return { classification: 'raw_material_input', kinds, uoms, blob };
    }
    return { classification: 'other_input', kinds, uoms, blob };
  }

  if (direction === 'output') {
    if (kindSet.has('waste') || WASTE_WORDS.some((word) => blob.includes(word))) {
      return { classification: 'waste_output', kinds, uoms, blob };
    }
    if (BYP_WORDS.some((word) => blob.includes(word))) {
      return { classification: 'byproduct_output', kinds, uoms, blob };
    }
    if (kindSet.has('product')) {
      return { classification: 'product_output', kinds, uoms, blob };
    }
    return { classification: 'other_output', kinds, uoms, blob };
  }

  return { classification: 'other', kinds, uoms, blob };
}

function unitIssueCheck(exchange: JsonRecord, uoms: string[], blob: string): UnitIssue[] {
  const flowUuid = isRecord(exchange.referenceToFlowDataSet)
    ? String(exchange.referenceToFlowDataSet['@refObjectId'] ?? '')
    : '';

  if (!flowUuid) {
    return [];
  }

  const currentUnit = uoms[0] ?? '';
  if (
    ['electric', 'electricity', '交流电', '电力'].some((word) => blob.includes(word)) &&
    currentUnit &&
    !['kwh', 'mj', 'gj'].includes(currentUnit)
  ) {
    return [
      {
        flow_uuid: flowUuid,
        current_unit: currentUnit,
        suggested_unit: 'kWh',
        basis: 'flow 描述为电力/电能，但 uom 标签非能量单位',
        confidence: '高',
      },
    ];
  }

  if (
    ['water', '用水', '工艺用水'].some((word) => blob.includes(word)) &&
    currentUnit &&
    ['kwh', 'mj', 'gj'].includes(currentUnit)
  ) {
    return [
      {
        flow_uuid: flowUuid,
        current_unit: currentUnit,
        suggested_unit: 'm3 或 kg',
        basis: 'flow 描述为水，但 uom 标签为能量单位',
        confidence: '高',
      },
    ];
  }

  if (
    ['co2', 'carbon dioxide', '二氧化碳'].some((word) => blob.includes(word)) &&
    currentUnit &&
    ['kwh', 'mj', 'gj'].includes(currentUnit)
  ) {
    return [
      {
        flow_uuid: flowUuid,
        current_unit: currentUnit,
        suggested_unit: 'kg',
        basis: '排放流通常质量单位计，当前为能量单位',
        confidence: '中',
      },
    ];
  }

  return [];
}

function baseInfoCheck(processPayload: JsonRecord): BaseInfoCheck {
  const [zhOk, enOk, values] = extractBaseNames(processPayload);
  const functionalUnit = deepGet(processPayload, [
    'processDataSet',
    'processInformation',
    'quantitativeReference',
    'functionalUnitOrOther',
  ]);
  const mixAndLocation = deepGet(processPayload, [
    'processDataSet',
    'processInformation',
    'geography',
    'mixAndLocationTypes',
  ]);
  const route = deepGet(processPayload, [
    'processDataSet',
    'modellingAndValidation',
    'LCIMethodAndAllocation',
    'typeOfDataSet',
  ]);
  const time = deepGet(processPayload, ['processDataSet', 'processInformation', 'time']);
  const geography = deepGet(processPayload, ['processDataSet', 'processInformation', 'geography']);
  const technology = deepGet(processPayload, ['processDataSet', 'modellingAndValidation']);
  const administrativeInformation = deepGet(processPayload, [
    'processDataSet',
    'administrativeInformation',
  ]);

  const nameOk = zhOk && enOk;
  const functionalUnitOk = hasNonEmpty(functionalUnit);
  const systemBoundaryOk = hasNonEmpty(mixAndLocation) || hasNonEmpty(route);
  const timeOk = hasNonEmpty(time);
  const geographyOk = hasNonEmpty(geography);
  const technologyOk = hasNonEmpty(technology);
  const administrativeOk = hasNonEmpty(administrativeInformation);
  const completenessScore = [
    nameOk,
    functionalUnitOk,
    systemBoundaryOk,
    timeOk,
    geographyOk,
    technologyOk,
    administrativeOk,
  ].filter(Boolean).length;

  return {
    name_zh_en_ok: nameOk,
    functional_unit_ok: functionalUnitOk,
    system_boundary_ok: systemBoundaryOk,
    time_ok: timeOk,
    geo_ok: geographyOk,
    tech_ok: technologyOk,
    admin_ok: administrativeOk,
    completeness_score: completenessScore,
    base_names: values,
  };
}

function unwrapProcessPayload(value: unknown, filePath: string): JsonRecord {
  if (!isRecord(value)) {
    throw new CliError(`Expected process review file to contain a JSON object: ${filePath}`, {
      code: 'PROCESS_REVIEW_INPUT_INVALID',
      exitCode: 2,
    });
  }

  const candidate =
    (isRecord(value.json_ordered) && value.json_ordered) ||
    (isRecord(value.jsonOrdered) && value.jsonOrdered) ||
    (isRecord(value.json) && value.json) ||
    value;

  if (!isRecord(candidate.processDataSet)) {
    throw new CliError(`Process review file is missing processDataSet: ${filePath}`, {
      code: 'PROCESS_REVIEW_INPUT_INVALID',
      exitCode: 2,
    });
  }

  return candidate;
}

function readProcessFiles(runRoot: string): string[] {
  const processDir = path.join(runRoot, 'exports', 'processes');
  if (!existsSync(processDir) || !statSync(processDir).isDirectory()) {
    throw new CliError(`Process review exports directory not found: ${processDir}`, {
      code: 'PROCESS_REVIEW_EXPORTS_NOT_FOUND',
      exitCode: 2,
    });
  }

  return readdirSync(processDir)
    .filter((entry) => entry.endsWith('.json'))
    .sort()
    .map((entry) => path.join(processDir, entry));
}

function buildPrompt(processSummaries: ProcessSummaryForLlm[]): string {
  return [
    '请基于以下 process 摘要做语义一致性审核（中英名称一致性、边界表达、修订建议）。',
    '要求：',
    '1) 只根据给定摘要；',
    '2) 证据不足必须明确标注；',
    '3) 输出 JSON，格式：{"findings":[{"process_file","severity","fixability","evidence","action"}]}。',
    '',
    `输入摘要:\n${JSON.stringify(processSummaries, null, 2)}`,
  ].join('\n');
}

function parseLlmJsonOutput(output: string): JsonRecord | null {
  const start = output.indexOf('{');
  const end = output.lastIndexOf('}');
  const candidate = start >= 0 && end > start ? output.slice(start, end + 1) : output;

  try {
    const parsed = JSON.parse(candidate) as unknown;
    return isRecord(parsed) ? parsed : null;
  } catch {
    return null;
  }
}

async function runOptionalLlmReview(
  processSummaries: ProcessSummaryForLlm[],
  options: {
    enableLlm: boolean;
    llmModel: string | undefined;
    env: NodeJS.ProcessEnv;
    fetchImpl: FetchLike;
    outDir: string;
  },
): Promise<ProcessReviewLlmResult> {
  if (!options.enableLlm) {
    return {
      enabled: false,
      reason: 'disabled',
    };
  }

  let env = readLlmRuntimeEnv(options.env);
  if (options.llmModel) {
    env = {
      ...env,
      model: options.llmModel,
    } satisfies LlmRuntimeEnv;
  }

  try {
    const response = await invokeLlm({
      env,
      input: {
        prompt: '你是严谨的LCA审核助手。只给基于输入证据的判断，不得臆造。输出必须是JSON对象。',
        context: buildPrompt(processSummaries),
      },
      fetchImpl: options.fetchImpl,
      timeoutMs: 45_000,
      cacheDir: path.join(options.outDir, '.llm-cache'),
      tracePath: path.join(options.outDir, 'llm-trace.jsonl'),
      module: 'review-process',
      stage: 'semantic-review',
      runId: 'review-process',
    });

    const parsed = parseLlmJsonOutput(response.output);
    if (!parsed) {
      return {
        enabled: true,
        ok: false,
        reason: 'llm_non_json_output',
        raw: response.output.slice(0, 8_000),
      };
    }

    return {
      enabled: true,
      ok: true,
      result: parsed,
    };
  } catch (error) {
    return {
      enabled: true,
      ok: false,
      reason: error instanceof Error ? error.message : String(error),
    };
  }
}

function formatPercent(value: number | null): string {
  return value === null ? '' : `${(value * 100).toFixed(2)}%`;
}

function renderZhReview(options: {
  runId: string;
  logicVersion: string;
  baseRows: Array<[string, BaseInfoCheck]>;
  rows: ProcessReviewRow[];
  totals: ProcessReviewTotals;
  llmResult: ProcessReviewLlmResult;
  evidenceStrong: string[];
  evidenceWeak: string[];
}): string {
  const lines = [
    '# one_flow_rerun_review_v2_1_zh\n',
    `- run_id: \`${options.runId}\`\n`,
    `- logic_version: \`${options.logicVersion}\`\n`,
    '\n## 2.1 基础信息核查\n',
    '|process file|中英名称|功能单位|系统边界|时间|地理|技术|管理元数据|完整性得分(0-7)|\n',
    '|---|---|---|---|---|---|---|---|---:|\n',
  ];

  options.baseRows.forEach(([fileName, base]) => {
    lines.push(
      `|${fileName}|${base.name_zh_en_ok ? '✅' : '❌'}|${base.functional_unit_ok ? '✅' : '❌'}|${base.system_boundary_ok ? '✅' : '❌'}|${base.time_ok ? '✅' : '❌'}|${base.geo_ok ? '✅' : '❌'}|${base.tech_ok ? '✅' : '❌'}|${base.admin_ok ? '✅' : '❌'}|${base.completeness_score}|\n`,
    );
  });

  lines.push(
    '\n## 物料平衡口径\n- 物料平衡：仅核查 `原材料投入 = 产品+副产品+废物`\n- 能量投入：单列记录，不计入平衡\n',
  );
  lines.push(
    '\n## 分过程结果\n|process file|原材料投入|产品|副产品|废物|能量投入(不计平衡)|差值(输出-投入)|相对偏差|\n|---|---:|---:|---:|---:|---:|---:|---:|\n',
  );

  options.rows.forEach((row) => {
    lines.push(
      `|${row.process_file}|${row.raw_input.toPrecision(6)}|${row.product.toPrecision(6)}|${row.byproduct.toPrecision(6)}|${row.waste.toPrecision(6)}|${row.energy_excluded.toPrecision(6)}|${row.delta.toPrecision(6)}|${formatPercent(row.relative_deviation)}|\n`,
    );
  });

  lines.push(
    `\n## 汇总\n- 原材料投入合计: **${options.totals.raw_input.toPrecision(6)}**\n- 产品+副产品+废物合计: **${options.totals.product_plus_byproduct_plus_waste.toPrecision(6)}**\n- 差值(输出-投入): **${options.totals.delta.toPrecision(6)}**\n- 相对偏差: **${formatPercent(options.totals.relative_deviation)}**\n- 能量投入(不计平衡)合计: **${options.totals.energy_excluded.toPrecision(6)}**\n`,
  );
  lines.push('\n## LLM 语义审核层（可选）\n');

  if (options.llmResult.enabled && options.llmResult.ok) {
    const findings = Array.isArray(options.llmResult.result.findings)
      ? options.llmResult.result.findings
      : [];
    if (findings.length > 0) {
      lines.push('\n|process file|severity|fixability|evidence|action|\n|---|---|---|---|---|\n');
      findings.slice(0, 50).forEach((finding) => {
        const evidenceValue = isRecord(finding) ? finding.evidence : '';
        const evidence =
          typeof evidenceValue === 'string'
            ? evidenceValue
            : JSON.stringify(evidenceValue ?? '', null, 0);
        const action = isRecord(finding) ? String(finding.action ?? finding.suggestion ?? '') : '';
        lines.push(
          `|${String(isRecord(finding) ? (finding.process_file ?? '') : '').replaceAll('|', '/')}|${String(isRecord(finding) ? (finding.severity ?? '') : '').replaceAll('|', '/')}|${String(isRecord(finding) ? (finding.fixability ?? 'review-needed') : 'review-needed').replaceAll('|', '/')}|${evidence.replaceAll('|', '/')}|${action.replaceAll('|', '/')}|\n`,
        );
      });
    }
  } else {
    lines.push(`- 未启用或调用失败：\`${options.llmResult.reason}\`\n`);
  }

  lines.push(
    `\n## 证据充足的结论\n${options.evidenceStrong.map((item) => `- ${item}`).join('\n')}\n`,
  );
  lines.push(
    `\n## 证据不足的结论/限制\n${options.evidenceWeak.map((item) => `- ${item}`).join('\n')}\n`,
  );

  return lines.join('');
}

function renderEnReview(options: {
  runId: string;
  logicVersion: string;
  baseRows: Array<[string, BaseInfoCheck]>;
  rows: ProcessReviewRow[];
  totals: ProcessReviewTotals;
  evidenceStrong: string[];
  evidenceWeak: string[];
}): string {
  const lines = [
    '# one_flow_rerun_review_v2_1_en\n',
    `- run_id: \`${options.runId}\`\n`,
    `- logic_version: \`${options.logicVersion}\`\n`,
    '\n## 2.1 Basic info checks\n',
    '|process file|zh+en names|functional unit|system boundary|time|geo|tech|admin metadata|completeness(0-7)|\n',
    '|---|---|---|---|---|---|---|---|---:|\n',
  ];

  options.baseRows.forEach(([fileName, base]) => {
    lines.push(
      `|${fileName}|${base.name_zh_en_ok ? '✅' : '❌'}|${base.functional_unit_ok ? '✅' : '❌'}|${base.system_boundary_ok ? '✅' : '❌'}|${base.time_ok ? '✅' : '❌'}|${base.geo_ok ? '✅' : '❌'}|${base.tech_ok ? '✅' : '❌'}|${base.admin_ok ? '✅' : '❌'}|${base.completeness_score}|\n`,
    );
  });

  lines.push(
    '\n## Material balance scope\n- Check only `raw material input = product + by-product + waste`\n- Energy inputs are listed but excluded from balance\n',
  );
  lines.push(
    '\n## Per-process results\n|process file|raw material in|product|by-product|waste|energy in (excluded)|delta(out-in)|relative deviation|\n|---|---:|---:|---:|---:|---:|---:|---:|\n',
  );

  options.rows.forEach((row) => {
    lines.push(
      `|${row.process_file}|${row.raw_input.toPrecision(6)}|${row.product.toPrecision(6)}|${row.byproduct.toPrecision(6)}|${row.waste.toPrecision(6)}|${row.energy_excluded.toPrecision(6)}|${row.delta.toPrecision(6)}|${formatPercent(row.relative_deviation)}|\n`,
    );
  });

  lines.push(
    `\n## Summary\n- Raw material input total: **${options.totals.raw_input.toPrecision(6)}**\n- Product+by-product+waste total: **${options.totals.product_plus_byproduct_plus_waste.toPrecision(6)}**\n- Delta (out-in): **${options.totals.delta.toPrecision(6)}**\n- Relative deviation: **${formatPercent(options.totals.relative_deviation)}**\n- Energy input total (excluded from balance): **${options.totals.energy_excluded.toPrecision(6)}**\n`,
  );
  lines.push(
    `\n## Evidence-sufficient conclusions\n${options.evidenceStrong.map((item) => `- ${item}`).join('\n')}\n`,
  );
  lines.push(
    `\n## Evidence-insufficient conclusions / limitations\n${options.evidenceWeak.map((item) => `- ${item}`).join('\n')}\n`,
  );

  return lines.join('');
}

function renderTiming(options: {
  runId: string;
  startTs: string | undefined;
  endTs: string | undefined;
  processCount: number;
}): string {
  const lines = ['# one_flow_rerun_timing\n', `- run_id: \`${options.runId}\`\n`];

  if (options.startTs && options.endTs) {
    const start = new Date(options.startTs);
    const end = new Date(options.endTs);
    if (Number.isNaN(start.getTime()) || Number.isNaN(end.getTime())) {
      throw new CliError('Expected --start-ts and --end-ts to be valid ISO timestamps.', {
        code: 'PROCESS_REVIEW_INVALID_TIMESTAMP',
        exitCode: 2,
      });
    }
    lines.push(`- start: \`${options.startTs}\`\n`);
    lines.push(`- end: \`${options.endTs}\`\n`);
    lines.push(
      `- total elapsed: **${((end.getTime() - start.getTime()) / 60_000).toFixed(2)} min**\n`,
    );
  }

  lines.push(`- process files reviewed: \`${options.processCount}\`\n`);
  lines.push(
    '- major time consumers: references retrieval, flow matching/search, flow metadata lookups.\n',
  );
  return lines.join('');
}

function renderUnitIssues(runId: string, unitIssues: UnitIssue[]): string {
  const lines = [
    '# flow_unit_issue_log\n',
    `- run_id: \`${runId}\`\n`,
    '\n|flow UUID|current unit|suggested unit|basis|confidence|\n|---|---|---|---|---|\n',
  ];

  if (unitIssues.length === 0) {
    lines.push('|无|无|无|未发现基于直接证据的单位矛盾|—|\n');
    return lines.join('');
  }

  const seen = new Set<string>();
  unitIssues.forEach((issue) => {
    const key = JSON.stringify(issue);
    if (seen.has(key)) {
      return;
    }
    seen.add(key);
    lines.push(
      `|${issue.flow_uuid}|${issue.current_unit}|${issue.suggested_unit}|${issue.basis}|${issue.confidence}|\n`,
    );
  });

  return lines.join('');
}

export async function runProcessReview(
  options: RunProcessReviewOptions,
): Promise<ProcessReviewReport> {
  const runRoot = path.resolve(
    requiredNonEmpty(options.runRoot, '--run-root', 'PROCESS_REVIEW_RUN_ROOT_REQUIRED'),
  );
  const runId = requiredNonEmpty(options.runId, '--run-id', 'PROCESS_REVIEW_RUN_ID_REQUIRED');
  const outDir = path.resolve(
    requiredNonEmpty(options.outDir, '--out-dir', 'PROCESS_REVIEW_OUT_DIR_REQUIRED'),
  );
  const logicVersion = options.logicVersion?.trim() || 'v2.1';
  const fetchImpl = options.fetchImpl ?? (fetch as FetchLike);
  const env = options.env ?? process.env;
  const llmMaxProcesses =
    typeof options.llmMaxProcesses === 'number' &&
    Number.isInteger(options.llmMaxProcesses) &&
    options.llmMaxProcesses > 0
      ? options.llmMaxProcesses
      : 8;
  const processFiles = readProcessFiles(runRoot);
  const baseRows: Array<[string, BaseInfoCheck]> = [];
  const rows: ProcessReviewRow[] = [];
  const unitIssues: UnitIssue[] = [];
  const processSummariesForLlm: ProcessSummaryForLlm[] = [];

  let totalRaw = 0;
  let totalProduct = 0;
  let totalByproduct = 0;
  let totalWaste = 0;
  let totalEnergy = 0;

  processFiles.forEach((filePath) => {
    const processPayload = unwrapProcessPayload(readJsonInput(filePath), filePath);
    const exchangesValue = deepGet(processPayload, ['processDataSet', 'exchanges', 'exchange']);
    const exchanges = Array.isArray(exchangesValue)
      ? exchangesValue.filter(isRecord)
      : isRecord(exchangesValue)
        ? [exchangesValue]
        : [];
    const base = baseInfoCheck(processPayload);
    const fileName = path.basename(filePath);
    baseRows.push([fileName, base]);

    let rawInput = 0;
    let product = 0;
    let byproduct = 0;
    let waste = 0;
    let energyExcluded = 0;

    exchanges.forEach((exchange) => {
      const classified = classifyExchange(exchange);
      const amount = toNumber(exchange.meanAmount ?? exchange.resultingAmount);
      if (classified.classification === 'raw_material_input') {
        rawInput += amount;
      } else if (classified.classification === 'product_output') {
        product += amount;
      } else if (classified.classification === 'byproduct_output') {
        byproduct += amount;
      } else if (classified.classification === 'waste_output') {
        waste += amount;
      } else if (classified.classification === 'energy_input') {
        energyExcluded += amount;
      }
      unitIssues.push(...unitIssueCheck(exchange, classified.uoms, classified.blob));
    });

    const balanceOut = product + byproduct + waste;
    const delta = balanceOut - rawInput;
    const relativeDeviation = rawInput > 0 ? Math.abs(delta) / rawInput : null;

    rows.push({
      process_file: fileName,
      raw_input: rawInput,
      product,
      byproduct,
      waste,
      energy_excluded: energyExcluded,
      delta,
      relative_deviation: relativeDeviation,
    });

    totalRaw += rawInput;
    totalProduct += product;
    totalByproduct += byproduct;
    totalWaste += waste;
    totalEnergy += energyExcluded;

    if (processSummariesForLlm.length < Math.max(1, llmMaxProcesses)) {
      processSummariesForLlm.push({
        process_file: fileName,
        base_names: base.base_names.slice(0, 4),
        base_checks: {
          name_zh_en_ok: base.name_zh_en_ok,
          functional_unit_ok: base.functional_unit_ok,
          system_boundary_ok: base.system_boundary_ok,
          time_ok: base.time_ok,
          geo_ok: base.geo_ok,
          tech_ok: base.tech_ok,
          admin_ok: base.admin_ok,
        },
        balance: {
          raw_in: rawInput,
          product,
          byproduct,
          waste,
          energy_excluded: energyExcluded,
          relative_deviation: relativeDeviation,
        },
      });
    }
  });

  const totals: ProcessReviewTotals = {
    raw_input: totalRaw,
    product_plus_byproduct_plus_waste: totalProduct + totalByproduct + totalWaste,
    delta: totalProduct + totalByproduct + totalWaste - totalRaw,
    relative_deviation:
      totalRaw > 0
        ? Math.abs(totalProduct + totalByproduct + totalWaste - totalRaw) / totalRaw
        : null,
    energy_excluded: totalEnergy,
  };

  const evidenceStrong = [
    '已基于 exchange 的 comment 标签/描述做口径过滤，仅核算 原材料投入 vs 产品+副产品+废物，能量单列不计入平衡。',
    ...(unitIssues.length > 0
      ? ['发现单位疑似错误时均附带 flow 描述与单位标签的直接矛盾证据。']
      : []),
  ];

  const evidenceWeak = [
    '部分 exchange 缺少结构化 type 标签，仅能依赖文本关键词分类，存在误判风险。',
    '未逐条拉取 flow 数据集参考单位做机器核对，单位结论以评论标签与流名称语义一致性为主。',
  ];

  const llmResult = await runOptionalLlmReview(processSummariesForLlm, {
    enableLlm: Boolean(options.enableLlm),
    llmModel: options.llmModel,
    env,
    fetchImpl,
    outDir,
  });

  const summary: ProcessReviewSummary = {
    run_id: runId,
    logic_version: logicVersion,
    process_count: processFiles.length,
    totals,
    llm: llmResult,
  };

  const reviewZhPath = writeTextArtifact(
    path.join(outDir, 'one_flow_rerun_review_v2_1_zh.md'),
    renderZhReview({
      runId,
      logicVersion,
      baseRows,
      rows,
      totals,
      llmResult,
      evidenceStrong,
      evidenceWeak,
    }),
  );
  const reviewEnPath = writeTextArtifact(
    path.join(outDir, 'one_flow_rerun_review_v2_1_en.md'),
    renderEnReview({
      runId,
      logicVersion,
      baseRows,
      rows,
      totals,
      evidenceStrong,
      evidenceWeak,
    }),
  );
  const timingPath = writeTextArtifact(
    path.join(outDir, 'one_flow_rerun_timing.md'),
    renderTiming({
      runId,
      startTs: options.startTs,
      endTs: options.endTs,
      processCount: processFiles.length,
    }),
  );
  const unitIssuePath = writeTextArtifact(
    path.join(outDir, 'flow_unit_issue_log.md'),
    renderUnitIssues(runId, unitIssues),
  );
  const summaryPath = writeJsonArtifact(path.join(outDir, 'review_summary_v2_1.json'), summary);

  const report: ProcessReviewReport = {
    schema_version: 1,
    generated_at_utc: (options.now ?? (() => new Date()))().toISOString(),
    status: 'completed_local_process_review',
    run_id: runId,
    run_root: runRoot,
    out_dir: outDir,
    logic_version: logicVersion,
    process_count: processFiles.length,
    totals,
    files: {
      review_zh: reviewZhPath,
      review_en: reviewEnPath,
      timing: timingPath,
      unit_issue_log: unitIssuePath,
      summary: summaryPath,
      report: '',
    },
    llm: llmResult,
  };

  const reportPath = writeJsonArtifact(path.join(outDir, 'process-review-report.json'), report);
  report.files.report = reportPath;
  writeJsonArtifact(reportPath, report);
  return report;
}

export const __testInternals = {
  requiredNonEmpty,
  textFromValue,
  toNumber,
  deepGet,
  hasNonEmpty,
  extractBaseNames,
  classifyExchange,
  unitIssueCheck,
  baseInfoCheck,
  unwrapProcessPayload,
  buildPrompt,
  parseLlmJsonOutput,
  runOptionalLlmReview,
  renderZhReview,
  renderEnReview,
  renderTiming,
  renderUnitIssues,
};
