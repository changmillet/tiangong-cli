import { CliError } from './errors.js';

export type ResponseLike = {
  ok: boolean;
  status: number;
  headers: {
    get(name: string): string | null;
  };
  text(): Promise<string>;
};

export type FetchLike = (input: string, init?: RequestInit) => Promise<ResponseLike>;

async function parseResponse(response: ResponseLike, url: string): Promise<unknown> {
  const rawText = await response.text();
  const contentType = response.headers.get('content-type') ?? '';

  if (!response.ok) {
    throw new CliError(`HTTP ${response.status} returned from ${url}`, {
      code: 'REMOTE_REQUEST_FAILED',
      exitCode: 1,
      details: rawText,
    });
  }

  if (contentType.includes('application/json')) {
    try {
      return JSON.parse(rawText);
    } catch (error) {
      throw new CliError(`Remote response was not valid JSON for ${url}`, {
        code: 'REMOTE_INVALID_JSON',
        exitCode: 1,
        details: String(error),
      });
    }
  }

  return rawText;
}

export async function postJson(options: {
  url: string;
  headers: Record<string, string>;
  body: unknown;
  timeoutMs: number;
  fetchImpl: FetchLike;
}): Promise<unknown> {
  const signal = AbortSignal.timeout(options.timeoutMs);
  const response = await options.fetchImpl(options.url, {
    method: 'POST',
    headers: options.headers,
    body: JSON.stringify(options.body),
    signal,
  });

  return parseResponse(response, options.url);
}

export async function getJson(options: {
  url: string;
  headers: Record<string, string>;
  timeoutMs: number;
  fetchImpl: FetchLike;
}): Promise<unknown> {
  const signal = AbortSignal.timeout(options.timeoutMs);
  const response = await options.fetchImpl(options.url, {
    method: 'GET',
    headers: options.headers,
    signal,
  });

  return parseResponse(response, options.url);
}
