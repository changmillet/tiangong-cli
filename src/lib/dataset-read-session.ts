import { runAuthIdentityReceipt, type AuthIdentityReceipt } from './auth-identity-receipt.js';
import { CliError } from './errors.js';
import type { FetchLike } from './http.js';
import {
  deriveSupabaseProjectBaseUrl,
  requireSupabaseRestRuntime,
  type SupabaseDataRuntime,
} from './supabase-client.js';
import { createSupabaseDataRuntime } from './supabase-session.js';

export type CreateDatasetReadSessionOptions = {
  env: NodeJS.ProcessEnv;
  fetchImpl: FetchLike;
  cliVersion: string;
  expectedProjectRef: string;
  expectedUserId: string;
  expectedEmail?: string;
  timeoutMs?: number;
  now?: Date;
};
export type DatasetReadSession = {
  identity: AuthIdentityReceipt;
  runtime: SupabaseDataRuntime;
  assertCurrentActor: () => Promise<void>;
};

function invalidSession(): never {
  throw new CliError('Dataset read session does not match its verified project and actor.', {
    code: 'DATASET_READ_SESSION_INVALID',
    exitCode: 1,
  });
}

/** Validate the exact token consumed by each Data API request and authenticated retry. */
export async function createDatasetReadSession(
  options: CreateDatasetReadSessionOptions,
): Promise<DatasetReadSession> {
  try {
    // Configuration and expectations are primitive snapshots before any identity/session await.
    const env = { ...options.env };
    const config = Object.freeze({ ...requireSupabaseRestRuntime(env) });
    const fetchImpl = options.fetchImpl;
    const cliVersion = options.cliVersion;
    const expectedProjectRef = options.expectedProjectRef;
    const expectedUserId = options.expectedUserId;
    const expectedEmail = options.expectedEmail;
    const timeoutMs = options.timeoutMs;
    const now = options.now === undefined ? undefined : new Date(options.now.getTime());
    const projectBaseUrl = deriveSupabaseProjectBaseUrl(config.apiBaseUrl);
    const apiBaseUrl = config.apiBaseUrl;
    const publishableKey = config.publishableKey;
    const original = createSupabaseDataRuntime({ runtime: config, fetchImpl, timeoutMs, now });
    const getAccessToken = original.getAccessToken.bind(original);
    const refreshAccessToken = original.refreshAccessToken?.bind(original);
    const identity = await runAuthIdentityReceipt({
      env,
      fetchImpl,
      cliVersion,
      expectedProjectRef,
      expectedUserId,
      timeoutMs,
      now,
    });
    const verifiedUserId = identity.identity.user_id;
    const verifiedProjectBaseUrl = identity.project.project_base_url;
    const guardedToken = async (obtain: () => Promise<string>): Promise<string> => {
      try {
        const token = await obtain();
        const claims: unknown = JSON.parse(
          Buffer.from(token.split('.')[1] ?? '', 'base64url').toString('utf8'),
        );
        if (
          claims === null ||
          typeof claims !== 'object' ||
          Array.isArray(claims) ||
          !('sub' in claims) ||
          claims.sub !== verifiedUserId ||
          verifiedUserId !== expectedUserId ||
          (expectedEmail !== undefined &&
            (!('email' in claims) ||
              typeof claims.email !== 'string' ||
              claims.email.toLowerCase() !== expectedEmail)) ||
          projectBaseUrl !== verifiedProjectBaseUrl
        )
          invalidSession();
        return token;
      } catch {
        // Tokens, credential-derived hashes and underlying auth/provider failures are never exposed.
        return invalidSession();
      }
    };
    const runtime: SupabaseDataRuntime = Object.freeze({
      apiBaseUrl,
      publishableKey,
      getAccessToken: () => guardedToken(getAccessToken),
      ...(refreshAccessToken ? { refreshAccessToken: () => guardedToken(refreshAccessToken) } : {}),
    });
    return {
      identity,
      runtime,
      assertCurrentActor: async () => {
        await runtime.getAccessToken();
      },
    };
  } catch {
    return invalidSession();
  }
}
