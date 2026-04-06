#!/usr/bin/env node

const { spawnSync } = require('node:child_process');
const fs = require('node:fs');
const path = require('node:path');

const repoRoot = path.resolve(__dirname, '..', '..');
const packageJsonPath = 'package.json';
const zeroSha = '0'.repeat(40);

function usage() {
  process.stderr.write(
    [
      'Usage: node ./scripts/ci/detect-release-changes.cjs --base-ref <ref> [--head-ref <ref>] [--github-output <path>]',
      '',
    ].join('\n'),
  );
}

function fail(message) {
  process.stderr.write(`${message}\n`);
  process.exit(1);
}

function parseArgs(argv) {
  const args = {
    baseRef: '',
    headRef: 'HEAD',
    githubOutput: process.env.GITHUB_OUTPUT || '',
  };

  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    const value = argv[index + 1];

    switch (token) {
      case '--base-ref':
        args.baseRef = value || '';
        index += 1;
        break;
      case '--head-ref':
        args.headRef = value || '';
        index += 1;
        break;
      case '--github-output':
        args.githubOutput = value || '';
        index += 1;
        break;
      default:
        fail(`error: unknown argument '${token}'`);
    }
  }

  if (!args.baseRef) {
    usage();
    fail('error: --base-ref is required');
  }

  return args;
}

function loadRefFile(ref, relativePath) {
  if (!ref || ref === zeroSha) {
    return null;
  }

  const result = spawnSync('git', ['-C', repoRoot, 'show', `${ref}:${relativePath}`], {
    encoding: 'utf8',
  });
  if (result.status !== 0) {
    return null;
  }

  return result.stdout;
}

function parsePackageVersion(raw) {
  return String(JSON.parse(raw).version || '').trim();
}

function buildOutputs(baseRef, headRef) {
  const baseRaw = loadRefFile(baseRef, packageJsonPath);
  const headRaw = loadRefFile(headRef, packageJsonPath);

  const previousVersion = baseRaw ? parsePackageVersion(baseRaw) : '';
  const version = headRaw ? parsePackageVersion(headRaw) : '';
  const changed = Boolean(previousVersion && version && previousVersion !== version);

  return {
    cli_previous_version: previousVersion,
    cli_version: version,
    cli_changed: changed ? 'true' : 'false',
    cli_tag: changed ? `cli-v${version}` : '',
    any_changed: changed ? 'true' : 'false',
  };
}

function writeOutputs(outputs, githubOutput) {
  const lines = Object.entries(outputs).map(([key, value]) => `${key}=${value}`);

  if (githubOutput) {
    fs.appendFileSync(githubOutput, `${lines.join('\n')}\n`, 'utf8');
    return;
  }

  process.stdout.write(`${lines.join('\n')}\n`);
}

const args = parseArgs(process.argv.slice(2));
writeOutputs(buildOutputs(args.baseRef, args.headRef), args.githubOutput);
