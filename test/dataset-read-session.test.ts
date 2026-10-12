import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import fs from 'node:fs';
import { syncBuiltinESMExports } from 'node:module';
import path from 'node:path';
import test from 'node:test';
import {
  createDatasetReadSession,
  type CreateDatasetReadSessionOptions,
} from '../src/lib/dataset-read-session.js';
import { createSupabaseDataClient, runSupabaseQuery } from '../src/lib/supabase-client.js';
import { CliError } from '../src/lib/errors.js';
import type { FetchLike } from '../src/lib/http.js';
import { buildSupabaseTestEnv } from './helpers/supabase-auth.js';

const ACTOR = '11111111-1111-4111-8111-111111111111';
const FOREIGN = '22222222-2222-4222-8222-222222222222';
const CANARY = 'PRIVATE_AUTH_FAILURE_CANARY';
const EMAIL = 'user@example.com';
function token(claims: unknown = { sub: ACTOR, email: EMAIL }) {
  return `e30.${Buffer.from(JSON.stringify(claims)).toString('base64url')}.${randomUUID()}`;
}
function response(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'content-type': 'application/json' },
  });
}
function safe(error: unknown) {
  assert.ok(error instanceof CliError);
  assert.equal(error.code, 'DATASET_READ_SESSION_INVALID');
  assert.equal(error.details, undefined);
  assert.equal(
    error.message,
    'Dataset read session does not match its verified project and actor.',
  );
  assert.equal(JSON.stringify(error).includes(CANARY), false);
  return true;
}
function fixture(headless = false) {
  const initial = token();
  const replacement = token();
  const env = buildSupabaseTestEnv(
    headless
      ? { TIANGONG_LCA_AUTH_MODE: 'access-token', TIANGONG_LCA_ACCESS_TOKEN: initial }
      : { TIANGONG_LCA_ACCESS_TOKEN: initial },
  );
  const requests: Array<{ url: URL; authorization: string | null; key: string | null }> = [];
  const bodies: Array<{ url: URL; authorization: string | null }> = [];
  let refreshed = replacement;
  let auth: () => unknown = () => ({ id: ACTOR, email: EMAIL, role: 'authenticated' });
  let body: (url: URL, authorization: string | null) => Response | Promise<Response> = () =>
    response([{ id: ACTOR }]);
  const fetchImpl: FetchLike = async (input, init) => {
    const url = new URL(input);
    const headers = new Headers(init?.headers);
    const authorization = headers.get('authorization');
    requests.push({ url, authorization, key: headers.get('apikey') });
    if (url.pathname === '/auth/v1/user') return response(auth());
    if (url.pathname === '/auth/v1/oauth/token')
      return response({
        access_token: refreshed,
        refresh_token: 'synthetic-rotated',
        token_type: 'bearer',
        expires_in: 3600,
        scope: 'openid email profile',
      });
    if (url.pathname === '/auth/v1/oauth/userinfo') {
      const claims = JSON.parse(
        Buffer.from(refreshed.split('.')[1]!, 'base64url').toString('utf8'),
      ) as { sub: string; email: string };
      return response(claims);
    }
    assert.ok(url.pathname.startsWith('/rest/v1/'));
    bodies.push({ url, authorization });
    return body(url, authorization);
  };
  const options: CreateDatasetReadSessionOptions = {
    env,
    fetchImpl,
    cliVersion: '0.1.28',
    expectedProjectRef: 'example',
    expectedUserId: ACTOR,
    now: new Date('2026-10-13T00:00:00.000Z'),
  };
  const replaceSession = (accessToken: string, extra: Record<string, unknown> = {}) => {
    const file = env.TIANGONG_LCA_SESSION_FILE!;
    const original = JSON.parse(fs.readFileSync(file, 'utf8')) as Record<string, unknown>;
    fs.writeFileSync(file, JSON.stringify({ ...original, access_token: accessToken, ...extra }));
  };
  return {
    initial,
    replacement,
    env,
    options,
    requests,
    bodies,
    replaceSession,
    setBody: (next: typeof body) => {
      body = next;
    },
    setAuth: (next: typeof auth) => {
      auth = next;
    },
    setRefreshed: (next: string) => {
      refreshed = next;
    },
    close: () => {
      if (env.TIANGONG_LCA_SESSION_FILE)
        fs.rmSync(path.dirname(env.TIANGONG_LCA_SESSION_FILE), { recursive: true, force: true });
    },
  };
}

test('each actual request takes the current same-actor token rather than an identity-time static copy', async () => {
  const f = fixture();
  try {
    const session = await createDatasetReadSession(f.options);
    assert.equal(session.identity.identity.user_id, ACTOR);
    assert.ok(Object.isFrozen(session.runtime));
    const { client, restBaseUrl } = createSupabaseDataClient(
      session.runtime,
      f.options.fetchImpl,
      10_000,
    );
    await runSupabaseQuery(
      client.from('processes').select('id').limit(1),
      restBaseUrl + '/processes',
    );
    f.replaceSession(f.replacement);
    await runSupabaseQuery(
      client.from('lifecyclemodels').select('id').limit(1),
      restBaseUrl + '/lifecyclemodels',
    );
    assert.deepEqual(
      f.bodies.map((request) => request.authorization),
      [`Bearer ${f.initial}`, `Bearer ${f.replacement}`],
    );
    await session.assertCurrentActor();
  } finally {
    f.close();
  }
});

test('switching the cached actor before the first body request blocks that actual adapter request', async () => {
  const f = fixture();
  try {
    const session = await createDatasetReadSession(f.options);
    f.replaceSession(token({ sub: FOREIGN, email: EMAIL }));
    const { client, restBaseUrl } = createSupabaseDataClient(
      session.runtime,
      f.options.fetchImpl,
      10_000,
    );
    await assert.rejects(
      runSupabaseQuery(client.from('processes').select('id'), restBaseUrl + '/processes'),
    );
    assert.equal(f.bodies.length, 0);
    await assert.rejects(session.assertCurrentActor(), safe);
  } finally {
    f.close();
  }
});

test('a session switch after the Process response blocks the following Model request before any Authorization is sent', async () => {
  const f = fixture();
  f.setBody((url) => {
    if (url.pathname.endsWith('/processes'))
      f.replaceSession(token({ sub: FOREIGN, email: EMAIL }));
    return response([{ id: ACTOR }]);
  });
  try {
    const session = await createDatasetReadSession(f.options);
    const { client, restBaseUrl } = createSupabaseDataClient(
      session.runtime,
      f.options.fetchImpl,
      10_000,
    );
    await runSupabaseQuery(client.from('processes').select('id'), restBaseUrl + '/processes');
    await assert.rejects(
      runSupabaseQuery(
        client.from('lifecyclemodels').select('id'),
        restBaseUrl + '/lifecyclemodels',
      ),
    );
    assert.deepEqual(
      f.bodies.map((request) => request.url.pathname),
      ['/rest/v1/processes'],
    );
    assert.equal(f.bodies[0]?.authorization, `Bearer ${f.initial}`);
  } finally {
    f.close();
  }
});

test('an actor mismatch permanently invalidates one capture even after the cache is restored before SDK retry or final assertion', async (t) => {
  const f = fixture();
  const read = fs.readFileSync;
  try {
    const session = await createDatasetReadSession(f.options);
    const identityBefore = structuredClone(session.identity);
    const file = f.env.TIANGONG_LCA_SESSION_FILE!;
    const originalRecord = read(file, 'utf8');
    f.replaceSession(token({ sub: FOREIGN, email: EMAIL }));
    let reads = 0;
    t.mock.method(fs, 'readFileSync', (...args: Parameters<typeof fs.readFileSync>) => {
      const bytes = read(...args);
      if (String(args[0]) === file) {
        reads += 1;
        // Return the already observed foreign snapshot while restoring the legit account.
        fs.writeFileSync(file, originalRecord);
      }
      return bytes;
    });
    syncBuiltinESMExports();
    const { client, restBaseUrl } = createSupabaseDataClient(
      session.runtime,
      f.options.fetchImpl,
      10_000,
    );
    await assert.rejects(
      runSupabaseQuery(client.from('processes').select('id'), restBaseUrl + '/processes'),
    );
    assert.equal(reads, 1, 'SDK retries must not obtain another auth snapshot after invalidation');
    assert.equal(f.bodies.length, 0, 'restoring the actor must not admit a retry');
    const requestsBefore = f.requests.length;
    await assert.rejects(session.assertCurrentActor(), safe);
    await assert.rejects(session.runtime.getAccessToken(), safe);
    await assert.rejects(session.runtime.refreshAccessToken!(), safe);
    assert.equal(reads, 1, 'later current/refresh checks must fail without reading auth again');
    assert.equal(f.requests.length, requestsBefore);
    assert.deepEqual(
      session.identity,
      identityBefore,
      'initial valid identity evidence remains intact',
    );
    t.mock.restoreAll();
    syncBuiltinESMExports();
    const freshCapture = await createDatasetReadSession(f.options);
    await freshCapture.assertCurrentActor();
    assert.equal(
      await freshCapture.runtime.getAccessToken(),
      f.initial,
      'the failure does not invalidate the global session',
    );
  } finally {
    t.mock.restoreAll();
    syncBuiltinESMExports();
    f.close();
  }
});

test('a concurrent getter cannot return its valid token after another in-flight getter invalidates the capture', async (t) => {
  const f = fixture();
  const read = fs.readFileSync;
  try {
    const session = await createDatasetReadSession(f.options);
    const file = f.env.TIANGONG_LCA_SESSION_FILE!;
    const originalRecord = read(file, 'utf8');
    let injected = false;
    let concurrentFailure: Promise<unknown> | undefined;
    t.mock.method(fs, 'readFileSync', (...args: Parameters<typeof fs.readFileSync>) => {
      const bytes = read(...args);
      if (String(args[0]) === file && !injected) {
        injected = true;
        f.replaceSession(token({ sub: FOREIGN, email: EMAIL }));
        // Both calls still use the real session getter. The second resolves its foreign
        // snapshot before the first resumes from its valid snapshot.
        concurrentFailure = session.runtime.getAccessToken().catch((error: unknown) => error);
      }
      return bytes;
    });
    syncBuiltinESMExports();
    await assert.rejects(session.runtime.getAccessToken(), safe);
    assert.ok(concurrentFailure);
    safe(await concurrentFailure);
    assert.equal(f.bodies.length, 0);
    t.mock.restoreAll();
    syncBuiltinESMExports();
    fs.writeFileSync(file, originalRecord);
    await assert.rejects(session.assertCurrentActor(), safe);
  } finally {
    t.mock.restoreAll();
    syncBuiltinESMExports();
    f.close();
  }
});

test('a 401 refresh for the same actor preserves existing refresh behavior and uses the exact refreshed token', async () => {
  const f = fixture();
  f.options.expectedEmail = EMAIL;
  f.setBody((_url, authorization) =>
    authorization === `Bearer ${f.initial}`
      ? response({ message: 'synthetic expired token' }, 401)
      : response([{ id: ACTOR }]),
  );
  try {
    const session = await createDatasetReadSession(f.options);
    assert.equal(typeof session.runtime.refreshAccessToken, 'function');
    const { client, restBaseUrl } = createSupabaseDataClient(
      session.runtime,
      f.options.fetchImpl,
      10_000,
    );
    assert.deepEqual(
      await runSupabaseQuery(client.from('processes').select('id'), restBaseUrl + '/processes'),
      [{ id: ACTOR }],
    );
    assert.deepEqual(
      f.bodies.map((request) => request.authorization),
      [`Bearer ${f.initial}`, `Bearer ${f.replacement}`],
    );
    assert.equal(
      f.requests.filter((request) => request.url.pathname === '/auth/v1/oauth/token').length,
      1,
    );
    await session.assertCurrentActor();
  } finally {
    f.close();
  }
});

test('a foreign 401 refresh is validated and blocked before the retry body request', async () => {
  const f = fixture();
  const foreign = token({ sub: FOREIGN, email: EMAIL });
  f.setRefreshed(foreign);
  f.setBody(() => response({ message: 'synthetic expired token' }, 401));
  try {
    const session = await createDatasetReadSession(f.options);
    const { client, restBaseUrl } = createSupabaseDataClient(
      session.runtime,
      f.options.fetchImpl,
      10_000,
    );
    await assert.rejects(
      runSupabaseQuery(client.from('processes').select('id'), restBaseUrl + '/processes'),
    );
    assert.equal(
      f.requests.filter((request) => request.url.pathname === '/auth/v1/oauth/token').length,
      1,
    );
    assert.equal(f.bodies.length, 1);
    assert.equal(f.bodies[0]?.authorization, `Bearer ${f.initial}`);
    assert.equal(
      f.bodies.some((request) => request.authorization === `Bearer ${foreign}`),
      false,
    );
    await assert.rejects(session.runtime.refreshAccessToken!(), safe);
  } finally {
    f.close();
  }
});

test('the token validated by the getter is the very token sent even if the session file switches in that window', async () => {
  const f = fixture();
  try {
    const session = await createDatasetReadSession(f.options);
    const foreign = token({ sub: FOREIGN, email: EMAIL });
    // This shim changes only timing after the real guarded getter; it never substitutes its token.
    const timedRuntime = {
      ...session.runtime,
      getAccessToken: async () => {
        const validated = await session.runtime.getAccessToken();
        f.replaceSession(foreign);
        return validated;
      },
    };
    const { client, restBaseUrl } = createSupabaseDataClient(
      timedRuntime,
      f.options.fetchImpl,
      10_000,
    );
    await runSupabaseQuery(client.from('processes').select('id'), restBaseUrl + '/processes');
    assert.equal(f.bodies[0]?.authorization, `Bearer ${f.initial}`);
    assert.equal(
      f.bodies.some((request) => request.authorization === `Bearer ${foreign}`),
      false,
    );
    await assert.rejects(session.assertCurrentActor(), safe);
  } finally {
    f.close();
  }
});

test('project, key, expectations and caller environment are captured before identity verification awaits', async () => {
  const f = fixture();
  const originalFetch = f.options.fetchImpl;
  const capturedKey = f.env.TIANGONG_LCA_SUPABASE_PUBLISHABLE_KEY;
  f.options.expectedEmail = EMAIL;
  f.options.fetchImpl = async (input, init) => {
    if (new URL(input).pathname === '/auth/v1/user') {
      f.env.TIANGONG_LCA_API_BASE_URL = 'https://foreign.supabase.co';
      f.env.TIANGONG_LCA_SUPABASE_PUBLISHABLE_KEY = 'foreign-project-key';
      f.options.expectedUserId = FOREIGN;
      f.options.expectedProjectRef = 'foreign';
      f.options.expectedEmail = 'foreign@example.com';
    }
    return originalFetch(input, init);
  };
  try {
    const session = await createDatasetReadSession(f.options);
    const { client, restBaseUrl } = createSupabaseDataClient(
      session.runtime,
      f.options.fetchImpl,
      10_000,
    );
    await runSupabaseQuery(client.from('processes').select('id'), restBaseUrl + '/processes');
    assert.equal(session.identity.project.project_ref, 'example');
    assert.ok(f.requests.every((request) => request.url.origin === 'https://example.supabase.co'));
    assert.equal(f.requests.at(-1)?.key, capturedKey);
    assert.equal(session.runtime.publishableKey, capturedKey);
    session.identity.identity.user_id = FOREIGN;
    session.identity.project.project_base_url = 'https://foreign.supabase.co';
    await session.assertCurrentActor();
    f.replaceSession(token({ sub: FOREIGN, email: EMAIL }));
    await assert.rejects(session.assertCurrentActor(), safe);
  } finally {
    f.close();
  }
});

test('undecodable or mismatched current tokens and optional email fail without retaining credentials', async () => {
  for (const [claims, expectedEmail] of [
    [null, undefined],
    [[], undefined],
    [{}, undefined],
    [{ sub: FOREIGN }, undefined],
    [{ sub: ACTOR, email: 'other@example.com' }, EMAIL],
    [{ sub: ACTOR }, EMAIL],
    [{ sub: ACTOR, email: 7 }, EMAIL],
    [CANARY, undefined],
  ] as const) {
    const f = fixture();
    try {
      if (expectedEmail) f.options.expectedEmail = expectedEmail;
      const session = await createDatasetReadSession(f.options);
      f.replaceSession(token(claims));
      await assert.rejects(session.runtime.getAccessToken(), safe);
      assert.equal(f.bodies.length, 0);
    } finally {
      f.close();
    }
  }
  const f = fixture();
  try {
    const session = await createDatasetReadSession(f.options);
    f.replaceSession(CANARY);
    await assert.rejects(session.runtime.getAccessToken(), safe);
    f.replaceSession(`e30.${Buffer.from('{').toString('base64url')}.signature`);
    await assert.rejects(session.runtime.getAccessToken(), safe);
  } finally {
    f.close();
  }
});

test('headless sessions remain refresh-free and need only the verified actor when no email was asserted', async () => {
  const f = fixture(true);
  f.env.TIANGONG_LCA_ACCESS_TOKEN = token({ sub: ACTOR });
  delete f.options.now;
  f.options.timeoutMs = 20_000;
  try {
    const session = await createDatasetReadSession(f.options);
    assert.equal(session.runtime.refreshAccessToken, undefined);
    const actual = await session.runtime.getAccessToken();
    const { client, restBaseUrl } = createSupabaseDataClient(
      session.runtime,
      f.options.fetchImpl,
      10_000,
    );
    await runSupabaseQuery(client.from('processes').select('id'), restBaseUrl + '/processes');
    assert.equal(f.bodies[0]?.authorization, `Bearer ${actual}`);
  } finally {
    f.close();
  }
});

test('raw identity and session failures have a static safe error without provider bodies or tokens', async () => {
  for (const mode of ['auth-throws', 'invalid-config', 'wrong-project', 'deleted-session']) {
    const f = fixture();
    try {
      if (mode === 'auth-throws')
        f.setAuth(() => {
          throw new Error(CANARY);
        });
      if (mode === 'invalid-config')
        f.env.TIANGONG_LCA_API_BASE_URL = `https://example.supabase.co/${CANARY}`;
      if (mode === 'wrong-project') f.options.expectedProjectRef = 'foreign';
      if (mode === 'deleted-session') fs.unlinkSync(f.env.TIANGONG_LCA_SESSION_FILE!);
      await assert.rejects(createDatasetReadSession(f.options), safe);
      assert.equal(f.bodies.length, 0);
    } finally {
      f.close();
    }
  }
});
