import assert from 'node:assert/strict';
import test from 'node:test';
import { loadDistModule } from './helpers/load-dist-module.js';
import {
  __testInternals,
  decodeUserApiKey,
  fingerprintSecret,
  fingerprintUserApiKey,
  redactEmail,
  requireUserApiKeyCredentials,
} from '../src/lib/user-api-key.js';

function encodeUserApiKey(payload: unknown): string {
  return Buffer.from(JSON.stringify(payload), 'utf8').toString('base64');
}

test('decodeUserApiKey accepts the TianGong user API key payload shape', () => {
  const apiKey = encodeUserApiKey({
    email: ' user@example.com ',
    password: ' secret-password ',
  });

  assert.deepEqual(decodeUserApiKey(apiKey), {
    email: 'user@example.com',
    password: 'secret-password',
  });
});

test('decodeUserApiKey rejects empty, invalid, and incomplete payloads', () => {
  assert.equal(decodeUserApiKey(''), null);
  assert.equal(decodeUserApiKey('%%%'), null);
  assert.equal(decodeUserApiKey(Buffer.from('[]', 'utf8').toString('base64')), null);
  assert.equal(
    decodeUserApiKey(
      encodeUserApiKey({
        email: 'user@example.com',
      }),
    ),
    null,
  );
  assert.equal(
    decodeUserApiKey(
      encodeUserApiKey({
        password: 'secret-password',
      }),
    ),
    null,
  );
});

test('requireUserApiKeyCredentials throws when the API key is not usable', () => {
  assert.throws(
    () => requireUserApiKeyCredentials('not-a-valid-api-key'),
    /TIANGONG_LCA_API_KEY is invalid/u,
  );
});

test('fingerprint helpers are stable and reject empty values', () => {
  const apiKey = encodeUserApiKey({
    email: 'user@example.com',
    password: 'secret-password',
  });

  assert.equal(fingerprintUserApiKey(apiKey), fingerprintUserApiKey(` ${apiKey} `));
  assert.match(fingerprintSecret('secret-value'), /^sha256:/u);
  assert.throws(() => fingerprintSecret('   '), /Cannot fingerprint an empty secret value/u);
});

test('redactEmail and normalizers cover edge cases', () => {
  assert.equal(redactEmail('ab@example.com'), '****@example.com');
  assert.equal(redactEmail('abcdef@example.com'), 'ab****@example.com');
  assert.equal(redactEmail('invalid-address'), '****');
  assert.equal(__testInternals.normalizeToken(' token '), 'token');
  assert.equal(__testInternals.normalizeToken(null), '');
  assert.equal(__testInternals.normalizeCredentialValue(' value '), 'value');
});

test('user-api-key helpers behave the same from the built dist module', async () => {
  const module =
    await loadDistModule<typeof import('../src/lib/user-api-key.js')>('src/lib/user-api-key.js');
  const apiKey = encodeUserApiKey({
    email: 'user@example.com',
    password: 'secret-password',
  });

  assert.deepEqual(module.decodeUserApiKey(apiKey), {
    email: 'user@example.com',
    password: 'secret-password',
  });
  assert.equal(module.redactEmail('abcdef@example.com'), 'ab****@example.com');
  assert.throws(() => module.requireUserApiKeyCredentials('bad-api-key'), /invalid/u);
});
