import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { executeRemoteCommand, getRemoteCommandHelp } from '../src/lib/remote.js';

function makeInputFile(content: string): { dir: string; filePath: string } {
  const dir = mkdtempSync(path.join(os.tmpdir(), 'tg-cli-remote-'));
  const filePath = path.join(dir, 'input.json');
  writeFileSync(filePath, content, 'utf8');
  return { dir, filePath };
}

test('getRemoteCommandHelp returns command-specific help text', () => {
  assert.match(getRemoteCommandHelp('search:flow'), /search flow/u);
});

test('executeRemoteCommand supports dry run and masks authorization', async () => {
  const { dir, filePath } = makeInputFile('{"query":"demo"}');
  try {
    const output = await executeRemoteCommand({
      commandKey: 'search:flow',
      inputPath: filePath,
      apiBaseUrl: 'https://example.com/functions/v1/',
      apiKey: 'secret-token',
      region: 'us-east-1',
      timeoutMs: 50,
      dryRun: true,
      compactJson: false,
      fetchImpl: async () => {
        throw new Error('should not be called');
      },
    });

    assert.match(output, /flow_hybrid_search/u);
    assert.match(output, /Bearer \*\*\*\*/u);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('executeRemoteCommand posts JSON and omits region for embedding command', async () => {
  const { dir, filePath } = makeInputFile('[{"jobId":1}]');
  try {
    let observedHeaders: Record<string, string | undefined> = {};
    let observedUrl = '';
    const output = await executeRemoteCommand({
      commandKey: 'admin:embedding-run',
      inputPath: filePath,
      apiBaseUrl: 'https://example.com/functions/v1',
      apiKey: 'secret-token',
      region: 'us-east-1',
      timeoutMs: 50,
      dryRun: false,
      compactJson: true,
      fetchImpl: async (input, init) => {
        observedUrl = input;
        observedHeaders = { ...(init?.headers as Record<string, string> | undefined) };
        return {
          ok: true,
          status: 200,
          headers: {
            get: () => 'application/json',
          },
          text: async () => '{"completedJobs":1}',
        };
      },
    });

    assert.equal(output, '{"completedJobs":1}\n');
    assert.equal(observedUrl, 'https://example.com/functions/v1/embedding_ft');
    assert.equal(observedHeaders.Authorization, 'Bearer secret-token');
    assert.equal(observedHeaders['x-region'], undefined);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('executeRemoteCommand validates required API config and unsupported commands', async () => {
  const { dir, filePath } = makeInputFile('{"query":"demo"}');
  try {
    await assert.rejects(
      () =>
        executeRemoteCommand({
          commandKey: 'search:flow',
          inputPath: filePath,
          apiBaseUrl: null,
          apiKey: 'secret-token',
          region: 'us-east-1',
          timeoutMs: 50,
          dryRun: false,
          compactJson: false,
          fetchImpl: async () => {
            throw new Error('unreachable');
          },
        }),
      /Missing API base URL/u,
    );

    await assert.rejects(
      () =>
        executeRemoteCommand({
          commandKey: 'search:flow',
          inputPath: filePath,
          apiBaseUrl: 'https://example.com',
          apiKey: null,
          region: 'us-east-1',
          timeoutMs: 50,
          dryRun: false,
          compactJson: false,
          fetchImpl: async () => {
            throw new Error('unreachable');
          },
        }),
      /Missing API key/u,
    );

    await assert.rejects(
      () =>
        executeRemoteCommand({
          commandKey: 'search:unknown' as never,
          inputPath: filePath,
          apiBaseUrl: 'https://example.com',
          apiKey: 'secret-token',
          region: 'us-east-1',
          timeoutMs: 50,
          dryRun: false,
          compactJson: false,
          fetchImpl: async () => {
            throw new Error('unreachable');
          },
        }),
      /Unsupported remote command/u,
    );
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
