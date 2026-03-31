import { readdirSync, readFileSync } from 'node:fs';
import path from 'node:path';

type CoverageMetric = {
  total: number;
  covered: number;
  skipped: number;
  pct: number;
};

type CoverageMetricKey = 'lines' | 'statements' | 'functions' | 'branches';

type CoverageFileSummary = Record<CoverageMetricKey, CoverageMetric>;

type CoverageSummary = {
  total: CoverageFileSummary;
} & Record<string, unknown>;

const COVERAGE_METRIC_KEYS: CoverageMetricKey[] = ['lines', 'statements', 'functions', 'branches'];

function listTrackedSourceFiles(rootDir: string, relativeDir: string): string[] {
  const absoluteDir = path.join(rootDir, relativeDir);
  const entries = readdirSync(absoluteDir, { withFileTypes: true });
  const files: string[] = [];

  for (const entry of entries) {
    const relativePath = path.join(relativeDir, entry.name);
    if (entry.isDirectory()) {
      files.push(...listTrackedSourceFiles(rootDir, relativePath));
      continue;
    }
    if (relativePath.endsWith('.d.ts')) {
      continue;
    }
    if (relativePath.endsWith('.ts') || relativePath.endsWith('.js')) {
      files.push(path.join(rootDir, relativePath));
    }
  }

  return files;
}

function isCoverageMetric(value: unknown): value is CoverageMetric {
  return Boolean(
    value &&
    typeof value === 'object' &&
    typeof (value as CoverageMetric).total === 'number' &&
    typeof (value as CoverageMetric).covered === 'number' &&
    typeof (value as CoverageMetric).skipped === 'number' &&
    typeof (value as CoverageMetric).pct === 'number',
  );
}

function isCoverageFileSummary(value: unknown): value is CoverageFileSummary {
  if (!value || typeof value !== 'object') {
    return false;
  }

  return COVERAGE_METRIC_KEYS.every((key) => isCoverageMetric((value as CoverageFileSummary)[key]));
}

function aggregateCoverage(entries: CoverageFileSummary[]): CoverageFileSummary {
  return Object.fromEntries(
    COVERAGE_METRIC_KEYS.map((key) => {
      const total = entries.reduce((sum, entry) => sum + entry[key].total, 0);
      const covered = entries.reduce((sum, entry) => sum + entry[key].covered, 0);
      const skipped = entries.reduce((sum, entry) => sum + entry[key].skipped, 0);
      return [
        key,
        {
          total,
          covered,
          skipped,
          pct: total === 0 ? 100 : (covered / total) * 100,
        },
      ];
    }),
  ) as CoverageFileSummary;
}

const summaryPath = path.join(process.cwd(), 'coverage', 'coverage-summary.json');
const summary = JSON.parse(readFileSync(summaryPath, 'utf8')) as CoverageSummary;
const coveredFiles = new Set(Object.keys(summary).filter((key) => key !== 'total'));
const expectedFiles = listTrackedSourceFiles(process.cwd(), 'src');
const expectedCoverageEntries = expectedFiles.map((filePath) => {
  const coverageEntry = summary[filePath];

  if (!isCoverageFileSummary(coverageEntry)) {
    throw new Error(`Expected coverage summary to include ${filePath}, but it was missing.`);
  }

  return coverageEntry;
});
const aggregate = aggregateCoverage(expectedCoverageEntries);

for (const key of COVERAGE_METRIC_KEYS) {
  const value = aggregate[key];
  if (value.covered !== value.total || value.pct !== 100) {
    throw new Error(
      `Expected ${key} coverage across src files to equal 100 but received ${value.pct} (${value.covered}/${value.total}).`,
    );
  }
}

for (const filePath of expectedFiles) {
  if (!coveredFiles.has(filePath)) {
    throw new Error(`Expected coverage summary to include ${filePath}, but it was missing.`);
  }
}

process.stdout.write(
  'Coverage assertion passed: 100% on lines, statements, functions, and branches across src files.\n',
);
