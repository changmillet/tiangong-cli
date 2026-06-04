import test from 'node:test';
import assert from 'node:assert/strict';
import { existsSync, readFileSync, rmSync, mkdtempSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import type { FetchLike, ResponseLike } from '../src/lib/http.js';
import { runDatasetMaintenanceClearAccount } from '../src/lib/dataset-maintenance-clear-account.js';
import {
  buildSupabaseTestEnv,
  isSupabaseAuthTokenUrl,
  makeSupabaseAuthResponse,
} from './helpers/supabase-auth.js';

function jsonResponse(body: unknown, status = 200): ResponseLike {
  return {
    ok: status >= 200 && status < 300,
    status,
    headers: {
      get(name: string): string | null {
        return name.toLowerCase() === 'content-type' ? 'application/json' : null;
      },
    },
    async text(): Promise<string> {
      return JSON.stringify(body);
    },
  };
}

function rowsForTable(table: string): unknown[] {
  if (table === 'processes') {
    return [
      {
        id: 'proc-1',
        version: '01.00.000',
        user_id: 'user-1',
        state_code: 0,
        modified_at: '2026-06-01T00:00:00.000Z',
      },
    ];
  }

  if (table === 'flows') {
    return [
      {
        id: 'flow-1',
        version: '01.00.000',
        user_id: 'user-1',
        state_code: 0,
        modified_at: '2026-06-01T00:00:00.000Z',
      },
    ];
  }

  return [];
}

test('runDatasetMaintenanceClearAccount writes a dry-run account snapshot', async () => {
  const outDir = mkdtempSync(path.join(os.tmpdir(), 'tg-clear-account-dry-run-'));
  const observedUrls: string[] = [];
  const fetchImpl: FetchLike = async (input) => {
    const url = String(input);
    if (isSupabaseAuthTokenUrl(url)) {
      return makeSupabaseAuthResponse({ email: 'user@example.com', userId: 'user-1' });
    }
    observedUrls.push(url);
    if (url.endsWith('/auth/v1/user')) {
      return jsonResponse({ id: 'user-1', email: 'user@example.com' });
    }
    const table = new URL(url).pathname.split('/').pop() ?? '';
    return jsonResponse(rowsForTable(table));
  };

  const report = await runDatasetMaintenanceClearAccount({
    outDir,
    stateCodes: [0],
    now: new Date('2026-06-04T00:00:00.000Z'),
    env: buildSupabaseTestEnv({
      TIANGONG_LCA_API_BASE_URL: 'https://example.supabase.co/functions/v1',
      TIANGONG_LCA_DISABLE_SESSION_CACHE: '1',
    }),
    fetchImpl,
  });

  assert.equal(report.status, 'planned_account_clear');
  assert.equal(report.mode, 'dry-run');
  assert.equal(report.account.email, 'user@example.com');
  assert.equal(report.summary.total_candidates, 2);
  assert.equal(report.summary.total_deleted, 0);
  assert.equal(report.summary.by_table.processes.candidates, 1);
  assert.equal(report.summary.by_table.flows.candidates, 1);
  assert.equal(existsSync(report.artifacts.rls_visible_snapshot), true);
  assert.equal(existsSync(report.artifacts.dry_run_report), true);
  assert.match(readFileSync(report.artifacts.rls_visible_snapshot, 'utf8'), /proc-1/u);
  assert.equal(observedUrls.filter((url) => url.includes('/rest/v1/processes')).length, 1);
  assert.match(observedUrls.join('\n'), /state_code=in.%280%29/u);
  rmSync(outDir, { recursive: true, force: true });
});

test('runDatasetMaintenanceClearAccount deletes account rows and verifies readback', async () => {
  const outDir = mkdtempSync(path.join(os.tmpdir(), 'tg-clear-account-commit-'));
  const deletedTables: string[] = [];
  const readbackCounts = new Map<string, number>();
  const fetchImpl: FetchLike = async (input, init) => {
    const url = String(input);
    if (isSupabaseAuthTokenUrl(url)) {
      return makeSupabaseAuthResponse({ email: 'user@example.com', userId: 'user-1' });
    }
    if (url.endsWith('/auth/v1/user')) {
      return jsonResponse({ id: 'user-1', email: 'user@example.com' });
    }

    const table = new URL(url).pathname.split('/').pop() ?? '';
    if (init?.method === 'POST' && url.endsWith('/functions/v1/app_dataset_delete')) {
      const body = JSON.parse(String(init.body)) as { table: string };
      deletedTables.push(body.table);
      return jsonResponse({
        ok: true,
        command: 'dataset_delete',
        data: rowsForTable(body.table)[0] ?? null,
      });
    }

    if (deletedTables.includes(table)) {
      readbackCounts.set(table, (readbackCounts.get(table) ?? 0) + 1);
      return jsonResponse([]);
    }

    return jsonResponse(rowsForTable(table));
  };

  const report = await runDatasetMaintenanceClearAccount({
    outDir,
    commit: true,
    confirm: 'user@example.com',
    now: new Date('2026-06-04T00:00:00.000Z'),
    env: buildSupabaseTestEnv({
      TIANGONG_LCA_API_BASE_URL: 'https://example.supabase.co/functions/v1',
      TIANGONG_LCA_DISABLE_SESSION_CACHE: '1',
    }),
    fetchImpl,
  });

  assert.equal(report.status, 'cleared_account');
  assert.equal(report.mode, 'commit');
  assert.deepEqual(deletedTables, ['processes', 'flows']);
  assert.equal(readbackCounts.get('processes'), 1);
  assert.equal(readbackCounts.get('flows'), 1);
  assert.equal(report.summary.total_candidates, 2);
  assert.equal(report.summary.total_deleted, 2);
  assert.equal(report.summary.total_remaining, 0);
  assert.equal(existsSync(report.artifacts.approval_record!), true);
  assert.equal(existsSync(report.artifacts.commit_report!), true);
  assert.equal(existsSync(report.artifacts.readback_verify_report!), true);
  rmSync(outDir, { recursive: true, force: true });
});

test('runDatasetMaintenanceClearAccount requires matching commit confirmation', async () => {
  await assert.rejects(
    () =>
      runDatasetMaintenanceClearAccount({
        commit: true,
        confirm: 'other@example.com',
        now: new Date('2026-06-04T00:00:00.000Z'),
        env: buildSupabaseTestEnv({
          TIANGONG_LCA_API_BASE_URL: 'https://example.supabase.co/functions/v1',
          TIANGONG_LCA_DISABLE_SESSION_CACHE: '1',
        }),
        fetchImpl: (async (input) => {
          const url = String(input);
          if (isSupabaseAuthTokenUrl(url)) {
            return makeSupabaseAuthResponse({ email: 'user@example.com', userId: 'user-1' });
          }
          return jsonResponse({ id: 'user-1', email: 'user@example.com' });
        }) as FetchLike,
      }),
    /--commit requires --confirm/u,
  );
});
