import { CliError } from './errors';

export type ResponseLike = {
  ok: boolean;
  status: number;
  headers: {
    get(name: string): string | null;
  };
  text(): Promise<string>;
};

export type FetchLike = (input: string, init?: RequestInit) => Promise<ResponseLike>;

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

  const rawText = await response.text();
  const contentType = response.headers.get('content-type') ?? '';

  if (!response.ok) {
    throw new CliError(`HTTP ${response.status} returned from ${options.url}`, {
      code: 'REMOTE_REQUEST_FAILED',
      exitCode: 1,
      details: rawText,
    });
  }

  if (contentType.includes('application/json')) {
    try {
      return JSON.parse(rawText);
    } catch (error) {
      throw new CliError(`Remote response was not valid JSON for ${options.url}`, {
        code: 'REMOTE_INVALID_JSON',
        exitCode: 1,
        details: String(error),
      });
    }
  }

  return rawText;
}
