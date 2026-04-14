import { CliError } from './errors.js';
import { readRuntimeEnv } from './env.js';
import type { FetchLike } from './http.js';
import { postJson } from './http.js';
import { readJsonInput, stringifyJson } from './io.js';
import { deriveSupabaseFunctionsBaseUrl, requireSupabaseRestRuntime } from './supabase-client.js';
import { resolveSupabaseUserSession } from './supabase-session.js';

type RemoteCommandSpec = {
  endpoint: string;
  includeRegion: boolean;
  help: string;
};

const REMOTE_COMMANDS: Record<string, RemoteCommandSpec> = {
  'search:flow': {
    endpoint: 'flow_hybrid_search',
    includeRegion: true,
    help: 'tiangong search flow --input ./request.json [--dry-run] [--json]',
  },
  'search:process': {
    endpoint: 'process_hybrid_search',
    includeRegion: true,
    help: 'tiangong search process --input ./request.json [--dry-run] [--json]',
  },
  'search:lifecyclemodel': {
    endpoint: 'lifecyclemodel_hybrid_search',
    includeRegion: true,
    help: 'tiangong search lifecyclemodel --input ./request.json [--dry-run] [--json]',
  },
  'admin:embedding-run': {
    endpoint: 'embedding_ft',
    includeRegion: false,
    help: 'tiangong admin embedding-run --input ./jobs.json [--dry-run] [--json]',
  },
};

export type RemoteCommandOptions = {
  commandKey: keyof typeof REMOTE_COMMANDS;
  inputPath: string;
  env: NodeJS.ProcessEnv;
  timeoutMs: number;
  dryRun: boolean;
  compactJson: boolean;
  fetchImpl: FetchLike;
};

function buildUrl(baseUrl: string, endpoint: string): string {
  return `${deriveSupabaseFunctionsBaseUrl(baseUrl)}/${endpoint}`;
}

function buildHeaders(
  accessToken: string,
  includeRegion: boolean,
  region: string | null,
): Record<string, string> {
  const headers: Record<string, string> = {
    Authorization: `Bearer ${accessToken}`,
    'Content-Type': 'application/json',
  };
  if (includeRegion && region) {
    headers['x-region'] = region;
  }
  return headers;
}

export async function executeRemoteCommand(options: RemoteCommandOptions): Promise<string> {
  const spec = REMOTE_COMMANDS[options.commandKey];
  if (!spec) {
    throw new CliError(`Unsupported remote command: ${options.commandKey}`, {
      code: 'UNSUPPORTED_REMOTE_COMMAND',
      exitCode: 2,
    });
  }

  const runtimeEnv = readRuntimeEnv(options.env);
  const runtime = requireSupabaseRestRuntime(options.env);
  const body = readJsonInput(options.inputPath);
  const url = buildUrl(runtime.apiBaseUrl, spec.endpoint);

  if (options.dryRun) {
    const dryRunHeaders = buildHeaders('****', spec.includeRegion, runtimeEnv.region);
    return stringifyJson(
      {
        dryRun: true,
        request: {
          method: 'POST',
          url,
          headers: {
            ...dryRunHeaders,
          },
          inputPath: options.inputPath,
          body,
          timeoutMs: options.timeoutMs,
        },
      },
      options.compactJson,
    );
  }

  const session = await resolveSupabaseUserSession({
    runtime,
    fetchImpl: options.fetchImpl,
    timeoutMs: options.timeoutMs,
  });
  const headers = buildHeaders(session.accessToken, spec.includeRegion, runtimeEnv.region);
  const response = await postJson({
    url,
    headers,
    body,
    timeoutMs: options.timeoutMs,
    fetchImpl: options.fetchImpl,
  });
  return stringifyJson(response, options.compactJson);
}

export function getRemoteCommandHelp(commandKey: keyof typeof REMOTE_COMMANDS): string {
  return REMOTE_COMMANDS[commandKey].help;
}
