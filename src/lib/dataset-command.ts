import { CliError } from './errors.js';
import type { ResponseLike } from './http.js';
import {
  createSupabaseFetch,
  deriveSupabaseFunctionsBaseUrl,
  type SupabaseDataRuntime,
} from './supabase-client.js';

type JsonObject = Record<string, unknown>;

export type DatasetCommandTable =
  | 'contacts'
  | 'sources'
  | 'unitgroups'
  | 'flowproperties'
  | 'flows'
  | 'processes'
  | 'lifecyclemodels';

export type DatasetCommandName = 'create' | 'save_draft';

type DatasetCommandFailurePayload = {
  ok: false;
  code: string;
  message: string;
  details?: unknown;
};

type DatasetCommandSuccessEnvelope = {
  ok: true;
  data?: unknown;
};

export type DatasetCommandCreateInput = {
  table: DatasetCommandTable;
  id: string;
  jsonOrdered: unknown;
  modelId?: string | null;
  ruleVerification?: boolean | null;
};

export type DatasetCommandSaveDraftInput = DatasetCommandCreateInput & {
  version: string;
};

export type DatasetCommandClient = {
  create: (input: DatasetCommandCreateInput) => Promise<unknown>;
  saveDraft: (input: DatasetCommandSaveDraftInput) => Promise<unknown>;
};

function isRecord(value: unknown): value is JsonObject {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function trimToken(value: unknown): string {
  return typeof value === 'string' ? value.trim() : '';
}

function command_endpoint(command: DatasetCommandName): string {
  return command === 'create' ? 'app_dataset_create' : 'app_dataset_save_draft';
}

export function buildDatasetCommandUrl(apiBaseUrl: string, command: DatasetCommandName): string {
  return `${deriveSupabaseFunctionsBaseUrl(apiBaseUrl)}/${command_endpoint(command)}`;
}

export function buildDatasetCommandHeaders(
  region: string | null | undefined,
): Record<string, string> {
  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
  };
  const normalizedRegion = trimToken(region);
  if (normalizedRegion) {
    headers['x-region'] = normalizedRegion;
  }
  return headers;
}

export function buildDatasetCommandBody(
  command: DatasetCommandName,
  input: DatasetCommandCreateInput | DatasetCommandSaveDraftInput,
): JsonObject {
  const body: JsonObject = {
    table: input.table,
    id: input.id,
    jsonOrdered: input.jsonOrdered,
  };

  if (command === 'save_draft') {
    body.version = (input as DatasetCommandSaveDraftInput).version;
  }

  if ('modelId' in input && input.modelId !== undefined) {
    body.modelId = input.modelId;
  }

  if ('ruleVerification' in input && input.ruleVerification !== undefined) {
    body.ruleVerification = input.ruleVerification;
  }

  return body;
}

function parseJsonText(rawText: string, url: string): unknown {
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

function isDatasetCommandFailurePayload(value: unknown): value is DatasetCommandFailurePayload {
  return (
    isRecord(value) &&
    value.ok === false &&
    typeof value.code === 'string' &&
    typeof value.message === 'string'
  );
}

function unwrapDatasetCommandPayload(payload: unknown): unknown {
  if (isDatasetCommandFailurePayload(payload)) {
    throw new CliError(payload.message, {
      code: 'REMOTE_REQUEST_FAILED',
      exitCode: 1,
      details: `${payload.code}: ${payload.message}`,
    });
  }

  if (isRecord(payload) && payload.ok === true && 'data' in payload) {
    return (payload as DatasetCommandSuccessEnvelope).data ?? null;
  }

  return payload;
}

function parseDatasetCommandResponse(
  response: ResponseLike,
  url: string,
  rawText: string,
): unknown {
  const contentType = response.headers.get('content-type') ?? '';
  const parsed =
    rawText.length === 0
      ? null
      : contentType.includes('application/json')
        ? parseJsonText(rawText, url)
        : rawText;

  if (!response.ok) {
    if (isDatasetCommandFailurePayload(parsed)) {
      throw new CliError(`HTTP ${response.status} returned from ${url}`, {
        code: 'REMOTE_REQUEST_FAILED',
        exitCode: 1,
        details: `${parsed.code}: ${parsed.message}`,
      });
    }

    throw new CliError(`HTTP ${response.status} returned from ${url}`, {
      code: 'REMOTE_REQUEST_FAILED',
      exitCode: 1,
      details: typeof parsed === 'string' ? parsed : rawText || undefined,
    });
  }

  return unwrapDatasetCommandPayload(parsed);
}

async function executeDatasetCommand(
  fetchWithAuth: typeof fetch,
  url: string,
  headers: Record<string, string>,
  body: JsonObject,
): Promise<unknown> {
  const response = await fetchWithAuth(url, {
    method: 'POST',
    headers,
    body: JSON.stringify(body),
  });
  return parseDatasetCommandResponse(response, url, await response.text());
}

export function createDatasetCommandClient(options: {
  runtime: SupabaseDataRuntime;
  fetchImpl: (input: string, init?: RequestInit) => Promise<ResponseLike>;
  timeoutMs: number;
  region?: string | null;
}): DatasetCommandClient {
  const fetchWithAuth = createSupabaseFetch(options.fetchImpl, options.timeoutMs, options.runtime);
  const headers = buildDatasetCommandHeaders(options.region);
  const createUrl = buildDatasetCommandUrl(options.runtime.apiBaseUrl, 'create');
  const saveDraftUrl = buildDatasetCommandUrl(options.runtime.apiBaseUrl, 'save_draft');

  return {
    create: (input) =>
      executeDatasetCommand(
        fetchWithAuth,
        createUrl,
        headers,
        buildDatasetCommandBody('create', input),
      ),
    saveDraft: (input) =>
      executeDatasetCommand(
        fetchWithAuth,
        saveDraftUrl,
        headers,
        buildDatasetCommandBody('save_draft', input),
      ),
  };
}

export const __testInternals = {
  buildDatasetCommandBody,
  buildDatasetCommandHeaders,
  buildDatasetCommandUrl,
  command_endpoint,
  parseDatasetCommandResponse,
  unwrapDatasetCommandPayload,
};
