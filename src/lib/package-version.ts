import { readFileSync } from 'node:fs';

const PACKAGE_JSON_CANDIDATES = ['../package.json', '../../package.json'] as const;

type PackageVersionReader = (url: URL) => string;

function parsePackageVersion(candidate: URL, readText: PackageVersionReader): string | null {
  try {
    const payload = JSON.parse(readText(candidate)) as { version?: unknown };
    return typeof payload.version === 'string' && payload.version.length > 0
      ? payload.version
      : null;
  } catch {
    return null;
  }
}

export function loadCliPackageVersion(
  // Bind the launcher-side module location explicitly; this helper is one directory deeper.
  importMetaUrl: string,
  readText: PackageVersionReader = (url) => readFileSync(url, 'utf8'),
): string {
  for (const relativePath of PACKAGE_JSON_CANDIDATES) {
    const version = parsePackageVersion(new URL(relativePath, importMetaUrl), readText);
    if (version) {
      return version;
    }
  }

  throw new Error('Could not resolve CLI package version from package.json.');
}
