import { readdirSync, readFileSync } from 'node:fs';
import path from 'node:path';

type CoverageMetric = {
  pct: number;
};

type CoverageSummary = {
  total: Record<'lines' | 'statements' | 'functions' | 'branches', CoverageMetric>;
} & Record<string, unknown>;

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
};

const summaryPath = path.join(process.cwd(), 'coverage', 'coverage-summary.json');
const summary = JSON.parse(readFileSync(summaryPath, 'utf8')) as CoverageSummary;
const coveredFiles = new Set(Object.keys(summary).filter((key) => key !== 'total'));
const expectedFiles = listTrackedSourceFiles(process.cwd(), 'src');

for (const key of ['lines', 'statements', 'functions', 'branches'] as const) {
  const value = summary.total[key].pct;
  if (value !== 100) {
    throw new Error(`Expected ${key} coverage to equal 100 but received ${value}.`);
  }
}

for (const filePath of expectedFiles) {
  if (!coveredFiles.has(filePath)) {
    throw new Error(`Expected coverage summary to include ${filePath}, but it was missing.`);
  }
}

process.stdout.write('Coverage assertion passed: 100% on lines, statements, functions, and branches.\n');
