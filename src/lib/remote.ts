import { CliError } from './errors';
import type { FetchLike } from './http';
import { postJson } from './http';
import { readJsonInput, stringifyJson } from './io';

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
  apiBaseUrl: string | null;
  apiKey: string | null;
  region: string | null;
  timeoutMs: number;
  dryRun: boolean;
  compactJson: boolean;
  fetchImpl: FetchLike;
};

function buildUrl(baseUrl: string, endpoint: string): string {
  return `${baseUrl.replace(/\/+$/u, '')}/${endpoint}`;
}

function buildHeaders(apiKey: string, includeRegion: boolean, region: string | null): Record<string, string> {
  const headers: Record<string, string> = {
    Authorization: `Bearer ${apiKey}`,
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

  if (!options.apiBaseUrl) {
    throw new CliError('Missing API base URL. Set TIANGONG_API_BASE_URL or SUPABASE_FUNCTIONS_URL.', {
      code: 'API_BASE_URL_REQUIRED',
      exitCode: 2,
    });
  }

  if (!options.apiKey) {
    throw new CliError('Missing API key. Set TIANGONG_API_KEY or TIANGONG_LCA_APIKEY.', {
      code: 'API_KEY_REQUIRED',
      exitCode: 2,
    });
  }

  const body = readJsonInput(options.inputPath);
  const url = buildUrl(options.apiBaseUrl, spec.endpoint);
  const headers = buildHeaders(options.apiKey, spec.includeRegion, options.region);

  if (options.dryRun) {
    return stringifyJson(
      {
        dryRun: true,
        request: {
          method: 'POST',
          url,
          headers: {
            ...headers,
            Authorization: 'Bearer ****',
          },
          inputPath: options.inputPath,
          body,
          timeoutMs: options.timeoutMs,
        },
      },
      options.compactJson,
    );
  }

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
