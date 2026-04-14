import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';

const maybeTest = process.env.TIANGONG_LCA_COVERAGE === '1' ? test.skip : test;

function runNpm(args: string[], cwd: string) {
  return spawnSync('npm', args, {
    cwd,
    encoding: 'utf8',
    shell: process.platform === 'win32',
  });
}

function assertSuccessfulExit(
  result: ReturnType<typeof spawnSync>,
  failureOutput: string | undefined,
) {
  const errorMessage =
    result.error instanceof Error ? (result.error.stack ?? result.error.message) : undefined;
  assert.equal(result.error, undefined, failureOutput ?? String(result.error));
  assert.equal(result.status, 0, failureOutput ?? errorMessage ?? '');
}

maybeTest('runFromBin executes when imported without direct auto-run', async () => {
  const { runFromBin } = await import('../bin/tiangong.js');
  const originalStdoutWrite = process.stdout.write.bind(process.stdout);
  const originalStderrWrite = process.stderr.write.bind(process.stderr);
  let stdout = '';
  let stderr = '';

  process.stdout.write = ((chunk: string | Uint8Array) => {
    stdout += String(chunk);
    return true;
  }) as typeof process.stdout.write;
  process.stderr.write = ((chunk: string | Uint8Array) => {
    stderr += String(chunk);
    return true;
  }) as typeof process.stderr.write;

  try {
    const exitCode = await runFromBin(['doctor', '--json'], {
      TIANGONG_LCA_API_BASE_URL: 'https://example.com/functions/v1',
      TIANGONG_LCA_API_KEY: 'secret-token',
      TIANGONG_LCA_SUPABASE_PUBLISHABLE_KEY: 'sb_publishable_test_key',
    });

    assert.equal(exitCode, 0);
    assert.match(stdout, /"ok":true/u);
    assert.equal(stderr, '');
  } finally {
    process.stdout.write = originalStdoutWrite;
    process.stderr.write = originalStderrWrite;
  }
});

maybeTest('packed tarball exposes non-empty help through installed bin and npm exec', () => {
  const repoRoot = process.cwd();
  const tempInstallDir = mkdtempSync(path.join(os.tmpdir(), 'tg-cli-pack-install-'));
  const tempExecDir = mkdtempSync(path.join(os.tmpdir(), 'tg-cli-pack-exec-'));
  const packResult = runNpm(['pack', '--silent'], repoRoot);

  assertSuccessfulExit(packResult, packResult.stderr);

  const tarballName = packResult.stdout.trim().split('\n').at(-1);
  assert.ok(tarballName);
  const tarballPath = path.join(repoRoot, tarballName);
  const installedBinPath = path.join(
    tempInstallDir,
    'node_modules',
    '.bin',
    process.platform === 'win32' ? 'tiangong.cmd' : 'tiangong',
  );

  try {
    const initResult = runNpm(['init', '-y'], tempInstallDir);
    assertSuccessfulExit(initResult, initResult.stderr);

    const installResult = runNpm(['install', '--silent', tarballPath], tempInstallDir);
    assertSuccessfulExit(installResult, installResult.stderr);

    const binHelpResult = spawnSync(installedBinPath, ['--help'], {
      cwd: tempInstallDir,
      encoding: 'utf8',
      shell: process.platform === 'win32',
    });
    assertSuccessfulExit(binHelpResult, binHelpResult.stderr);
    assert.match(binHelpResult.stdout, /Unified TianGong command entrypoint/u);

    const execHelpResult = runNpm(
      ['exec', '--yes', `--package=${tarballPath}`, '--', 'tiangong', '--help'],
      tempExecDir,
    );
    assertSuccessfulExit(execHelpResult, execHelpResult.stderr);
    assert.match(execHelpResult.stdout, /Unified TianGong command entrypoint/u);
  } finally {
    rmSync(tarballPath, { force: true });
    rmSync(tempInstallDir, { recursive: true, force: true });
    rmSync(tempExecDir, { recursive: true, force: true });
  }
});
