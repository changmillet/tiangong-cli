import test from 'node:test';
import assert from 'node:assert/strict';
import { buildDoctorReport, maskSecret, resolveEnv } from '../src/lib/env';

test('resolveEnv prefers canonical env keys', () => {
  const resolved = resolveEnv(
    {
      canonical: 'A',
      aliases: ['B'],
      required: true,
      description: 'demo',
    },
    { A: 'canonical', B: 'alias' },
  );

  assert.equal(resolved.source, 'canonical');
  assert.equal(resolved.value, 'canonical');
});

test('resolveEnv falls back to alias and defaults', () => {
  const aliasResolved = resolveEnv(
    {
      canonical: 'A',
      aliases: ['B'],
      required: true,
      description: 'demo',
    },
    { B: 'alias' },
  );
  assert.equal(aliasResolved.source, 'alias:B');
  assert.equal(aliasResolved.value, 'alias');

  const defaultResolved = resolveEnv(
    {
      canonical: 'A',
      aliases: [],
      required: false,
      description: 'demo',
      defaultValue: 'fallback',
    },
    {},
  );
  assert.equal(defaultResolved.source, 'default');
  assert.equal(defaultResolved.value, 'fallback');
});

test('resolveEnv reports missing when nothing is available', () => {
  const resolved = resolveEnv(
    {
      canonical: 'A',
      aliases: [],
      required: true,
      description: 'demo',
    },
    {},
  );

  assert.equal(resolved.source, 'missing');
  assert.equal(resolved.present, false);
  assert.equal(resolved.value, null);
});

test('maskSecret leaves short values unchanged and masks longer values', () => {
  assert.equal(maskSecret(null), null);
  assert.equal(maskSecret('short'), 'short');
  assert.equal(maskSecret('1234567890'), '1234...7890');
});

test('buildDoctorReport records missing required env keys', () => {
  const report = buildDoctorReport(
    {
      SUPABASE_FUNCTIONS_URL: 'https://example.com',
      TIANGONG_LCA_APIKEY: 'secret-token',
    },
    { loaded: true, path: '/tmp/.env', count: 2 },
  );

  assert.equal(report.ok, true);
  const apiKeyCheck = report.checks.find((check) => check.key === 'TIANGONG_API_KEY');
  assert.equal(apiKeyCheck?.source, 'alias:TIANGONG_LCA_APIKEY');
});
