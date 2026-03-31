import { CliError } from './errors.js';
import type { FetchLike } from './http.js';
import { getJson } from './http.js';

type JsonObject = Record<string, unknown>;

export type SupabaseRestRuntime = {
  apiBaseUrl: string;
  apiKey: string;
};

export type SupabaseProcessRow = {
  id: string;
  version: string;
  json: unknown;
  modified_at: string | null;
  state_code: number | null;
};

export type SupabaseProcessLookup = {
  row: SupabaseProcessRow;
  sourceUrl: string;
  resolution:
    | 'remote_supabase_exact'
    | 'remote_supabase_latest'
    | 'remote_supabase_latest_fallback';
};

function isRecord(value: unknown): value is JsonObject {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function requireSupabaseRestRuntime(env: NodeJS.ProcessEnv): SupabaseRestRuntime {
  const apiBaseUrl =
    typeof env.TIANGONG_LCA_API_BASE_URL === 'string' && env.TIANGONG_LCA_API_BASE_URL.trim()
      ? env.TIANGONG_LCA_API_BASE_URL.trim()
      : null;
  const apiKey =
    typeof env.TIANGONG_LCA_API_KEY === 'string' && env.TIANGONG_LCA_API_KEY.trim()
      ? env.TIANGONG_LCA_API_KEY.trim()
      : null;
  const missing: string[] = [];

  if (!apiBaseUrl) {
    missing.push('TIANGONG_LCA_API_BASE_URL');
  }

  if (!apiKey) {
    missing.push('TIANGONG_LCA_API_KEY');
  }

  if (missing.length > 0) {
    throw new CliError(`Missing Supabase REST runtime env: ${missing.join(', ')}`, {
      code: 'SUPABASE_REST_ENV_REQUIRED',
      exitCode: 2,
      details: { missing },
    });
  }

  return {
    apiBaseUrl: apiBaseUrl as string,
    apiKey: apiKey as string,
  };
}

function deriveSupabaseRestBaseUrl(apiBaseUrl: string): string {
  const trimmed = apiBaseUrl.trim().replace(/\/+$/u, '');

  if (!trimmed) {
    throw new CliError('Cannot derive a Supabase REST base URL from an empty API base URL.', {
      code: 'SUPABASE_REST_BASE_URL_INVALID',
      exitCode: 2,
    });
  }

  if (trimmed.endsWith('/functions/v1')) {
    return trimmed.replace(/\/functions\/v1$/u, '/rest/v1');
  }

  if (trimmed.endsWith('/rest/v1')) {
    return trimmed;
  }

  if (/^https?:\/\/[^/]+$/u.test(trimmed)) {
    return `${trimmed}/rest/v1`;
  }

  throw new CliError(
    'Cannot derive a Supabase REST base URL from TIANGONG_LCA_API_BASE_URL. Use a Supabase project base URL, a /functions/v1 base URL, or a /rest/v1 base URL.',
    {
      code: 'SUPABASE_REST_BASE_URL_INVALID',
      exitCode: 2,
      details: trimmed,
    },
  );
}

function buildHeaders(apiKey: string): Record<string, string> {
  return {
    Accept: 'application/json',
    Authorization: `Bearer ${apiKey}`,
    apikey: apiKey,
  };
}

function buildProcessUrl(
  restBaseUrl: string,
  options: {
    id: string;
    version?: string | null;
    latestOnly?: boolean;
  },
): string {
  const url = new URL(`${restBaseUrl.replace(/\/+$/u, '')}/processes`);
  url.searchParams.set('select', 'id,version,json,modified_at,state_code');
  url.searchParams.set('id', `eq.${options.id}`);

  if (options.version) {
    url.searchParams.set('version', `eq.${options.version}`);
  } else if (options.latestOnly) {
    url.searchParams.set('order', 'version.desc');
    url.searchParams.set('limit', '1');
  }

  return url.toString();
}

function parseRows(payload: unknown, url: string): SupabaseProcessRow[] {
  if (!Array.isArray(payload)) {
    throw new CliError(`Supabase REST response was not a JSON array for ${url}`, {
      code: 'SUPABASE_REST_RESPONSE_INVALID',
      exitCode: 1,
      details: payload,
    });
  }

  return payload.map((item, index) => {
    if (!isRecord(item)) {
      throw new CliError(`Supabase REST row ${index} was not a JSON object for ${url}`, {
        code: 'SUPABASE_REST_RESPONSE_INVALID',
        exitCode: 1,
        details: item,
      });
    }

    return {
      id: typeof item.id === 'string' ? item.id : '',
      version: typeof item.version === 'string' ? item.version : '',
      json: item.json,
      modified_at: typeof item.modified_at === 'string' ? item.modified_at : null,
      state_code: typeof item.state_code === 'number' ? item.state_code : null,
    };
  });
}

function normalizeSupabaseProcessPayload(payload: unknown, lookupKey: string): JsonObject {
  if (typeof payload === 'string') {
    try {
      const parsed = JSON.parse(payload);
      if (!isRecord(parsed)) {
        throw new CliError(`Supabase REST payload was not a JSON object for ${lookupKey}.`, {
          code: 'SUPABASE_REST_PAYLOAD_INVALID',
          exitCode: 1,
          details: parsed,
        });
      }
      return parsed;
    } catch (error) {
      if (error instanceof CliError) {
        throw error;
      }

      throw new CliError(`Supabase REST payload was not valid JSON for ${lookupKey}.`, {
        code: 'SUPABASE_REST_PAYLOAD_INVALID_JSON',
        exitCode: 1,
        details: String(error),
      });
    }
  }

  if (!isRecord(payload)) {
    throw new CliError(`Supabase REST payload was missing json for ${lookupKey}.`, {
      code: 'SUPABASE_REST_PAYLOAD_MISSING',
      exitCode: 1,
      details: payload,
    });
  }

  return payload;
}

async function fetchExactOrLatestProcessRow(options: {
  runtime: SupabaseRestRuntime;
  id: string;
  version?: string | null;
  timeoutMs: number;
  fetchImpl: FetchLike;
  fallbackToLatest?: boolean;
}): Promise<SupabaseProcessLookup | null> {
  const restBaseUrl = deriveSupabaseRestBaseUrl(options.runtime.apiBaseUrl);
  const headers = buildHeaders(options.runtime.apiKey);
  if (!options.version) {
    const latestUrl = buildProcessUrl(restBaseUrl, {
      id: options.id,
      latestOnly: true,
    });
    const latestRows = parseRows(
      await getJson({
        url: latestUrl,
        headers,
        timeoutMs: options.timeoutMs,
        fetchImpl: options.fetchImpl,
      }),
      latestUrl,
    );

    if (latestRows.length === 0) {
      return null;
    }

    return {
      row: latestRows[0] as SupabaseProcessRow,
      sourceUrl: latestUrl,
      resolution: 'remote_supabase_latest',
    };
  }

  const exactUrl = buildProcessUrl(restBaseUrl, {
    id: options.id,
    version: options.version,
  });
  const exactRows = parseRows(
    await getJson({
      url: exactUrl,
      headers,
      timeoutMs: options.timeoutMs,
      fetchImpl: options.fetchImpl,
    }),
    exactUrl,
  );

  if (exactRows.length > 0) {
    return {
      row: exactRows[0] as SupabaseProcessRow,
      sourceUrl: exactUrl,
      resolution: 'remote_supabase_exact',
    };
  }

  if (!options.fallbackToLatest) {
    return null;
  }

  const latestUrl = buildProcessUrl(restBaseUrl, {
    id: options.id,
    latestOnly: true,
  });
  const latestRows = parseRows(
    await getJson({
      url: latestUrl,
      headers,
      timeoutMs: options.timeoutMs,
      fetchImpl: options.fetchImpl,
    }),
    latestUrl,
  );

  if (latestRows.length === 0) {
    return null;
  }

  return {
    row: latestRows[0] as SupabaseProcessRow,
    sourceUrl: latestUrl,
    resolution: 'remote_supabase_latest_fallback',
  };
}

export {
  requireSupabaseRestRuntime,
  deriveSupabaseRestBaseUrl,
  normalizeSupabaseProcessPayload,
  fetchExactOrLatestProcessRow,
};
