import { parseArgs } from 'node:util';
import { buildDoctorReport } from './lib/env';
import type { DotEnvLoadResult } from './lib/dotenv';
import { CliError, toErrorPayload } from './lib/errors';
import type { FetchLike } from './lib/http';
import { executeRemoteCommand, getRemoteCommandHelp } from './lib/remote';

export type CliDeps = {
  env: NodeJS.ProcessEnv;
  dotEnvStatus: DotEnvLoadResult;
  fetchImpl: FetchLike;
};

export type CliResult = {
  exitCode: number;
  stdout: string;
  stderr: string;
};

type RootFlags = {
  help: boolean;
  version: boolean;
};

function renderMainHelp(dotEnvStatus: DotEnvLoadResult): string {
  return `TianGong LCA CLI

Unified TianGong command entrypoint.

Design principles:
  - direct REST / Edge Function access
  - no MCP inside the CLI
  - TypeScript source on Node 24
  - file-first input and JSON-first output

Usage:
  tiangong <command> [subcommand] [options]

Commands:
  auth       whoami | doctor-auth
  search     flow | process | lifecyclemodel
  review     flow | process
  flow       get | list | remediate | publish-version | regen-product
  process    get | auto-build | resume-build | publish-build | batch-build
  job        get | wait | logs
  admin      embedding-run
  doctor     show environment and migration alias resolution

Examples:
  tiangong doctor
  tiangong search flow --input ./request.json
  tiangong search process --input ./request.json --dry-run
  tiangong admin embedding-run --input ./jobs.json

Environment:
  .env loaded: ${dotEnvStatus.loaded ? `yes (${dotEnvStatus.path}, ${dotEnvStatus.count} keys)` : 'no'}
`.trim();
}

function renderDoctorHelp(): string {
  return `Usage:
  tiangong doctor [--json]

Options:
  --json    Print structured environment diagnostics
  -h, --help
`.trim();
}

function renderSearchHelp(): string {
  return `Usage:
  tiangong search <flow|process|lifecyclemodel> --input <file> [options]

Options:
  --input <file>   JSON request file
  --json           Print compact JSON
  --dry-run        Print the planned HTTP request without sending it
  --api-key <key>  Override TIANGONG_API_KEY
  --base-url <url> Override TIANGONG_API_BASE_URL
  --region <name>  Override TIANGONG_REGION
  --timeout-ms <n> Request timeout in milliseconds
  -h, --help
`.trim();
}

function renderAdminHelp(): string {
  return `Usage:
  tiangong admin embedding-run --input <file> [options]

Options:
  --input <file>   JSON request file
  --json           Print compact JSON
  --dry-run        Print the planned HTTP request without sending it
  --api-key <key>  Override TIANGONG_API_KEY
  --base-url <url> Override TIANGONG_API_BASE_URL
  --timeout-ms <n> Request timeout in milliseconds
  -h, --help
`.trim();
}

function renderDoctorText(report: ReturnType<typeof buildDoctorReport>): string {
  const lines = ['TianGong CLI doctor', `  .env loaded: ${report.loadedDotEnv ? `yes (${report.dotEnvKeysLoaded} keys)` : 'no'}`, `  .env path:   ${report.dotEnvPath}`, ''];
  for (const check of report.checks) {
    const status = check.present ? 'OK ' : 'MISS';
    lines.push(`  [${status}] ${check.key} (${check.source})${check.required ? ' [required]' : ''}`);
  }
  if (!report.ok) {
    lines.push('', 'Missing required environment keys:');
    for (const check of report.checks) {
      if (check.required && !check.present) {
        lines.push(`  - ${check.key}`);
      }
    }
  }
  return `${lines.join('\n')}\n`;
}

type CommandDispatch = {
  flags: RootFlags;
  command: string | null;
  subcommand: string | null;
  commandArgs: string[];
};

function parseCommandLine(args: string[]): CommandDispatch {
  const flags: RootFlags = {
    help: false,
    version: false,
  };

  let index = 0;
  while (index < args.length) {
    const arg = args[index];
    if (arg === '--') {
      index += 1;
      break;
    }
    if (arg === '--help' || arg === '-h') {
      flags.help = true;
      index += 1;
      continue;
    }
    if (arg === '--version' || arg === '-v') {
      flags.version = true;
      index += 1;
      continue;
    }
    if (arg.startsWith('-')) {
      throw new CliError(`Unknown root option: ${arg}`, {
        code: 'UNKNOWN_ROOT_OPTION',
        exitCode: 2,
      });
    }
    break;
  }

  const command = args[index] ?? null;
  if (!command) {
    return {
      flags,
      command: null,
      subcommand: null,
      commandArgs: [],
    };
  }

  const maybeSubcommand = args[index + 1];
  const subcommand = maybeSubcommand && !maybeSubcommand.startsWith('-')
    ? maybeSubcommand
    : null;
  const commandArgs = args.slice(index + 1 + (subcommand ? 1 : 0));

  return {
    flags,
    command,
    subcommand,
    commandArgs,
  };
}

function parseDoctorFlags(args: string[]): {
  help: boolean;
  json: boolean;
} {
  let values: ReturnType<typeof parseArgs>['values'];
  try {
    ({ values } = parseArgs({
      args,
      allowPositionals: false,
      strict: true,
      options: {
        help: { type: 'boolean', short: 'h' },
        json: { type: 'boolean' },
      },
    }));
  } catch (error) {
    throw new CliError(String(error), {
      code: 'INVALID_ARGS',
      exitCode: 2,
    });
  }

  return {
    help: Boolean(values.help),
    json: Boolean(values.json),
  };
}

function parseRemoteFlags(args: string[]): {
  help: boolean;
  json: boolean;
  dryRun: boolean;
  inputPath: string;
  apiKey: string | null;
  apiBaseUrl: string | null;
  region: string | null;
  timeoutMs: number;
} {
  let values: ReturnType<typeof parseArgs>['values'];
  try {
    ({ values } = parseArgs({
      args,
      allowPositionals: false,
      strict: true,
      options: {
        help: { type: 'boolean', short: 'h' },
        json: { type: 'boolean' },
        'dry-run': { type: 'boolean' },
        input: { type: 'string' },
        'api-key': { type: 'string' },
        'base-url': { type: 'string' },
        region: { type: 'string' },
        'timeout-ms': { type: 'string' },
      },
    }));
  } catch (error) {
    throw new CliError(String(error), {
      code: 'INVALID_ARGS',
      exitCode: 2,
    });
  }

  const timeoutText = typeof values['timeout-ms'] === 'string' ? values['timeout-ms'] : undefined;
  const timeoutMs = timeoutText ? Number.parseInt(timeoutText, 10) : 60_000;
  if (!Number.isInteger(timeoutMs) || timeoutMs <= 0) {
    throw new CliError('Expected --timeout-ms to be a positive integer.', {
      code: 'INVALID_TIMEOUT',
      exitCode: 2,
    });
  }

  return {
    help: Boolean(values.help),
    json: Boolean(values.json),
    dryRun: Boolean(values['dry-run']),
    inputPath: typeof values.input === 'string' ? values.input : '',
    apiKey: typeof values['api-key'] === 'string' ? values['api-key'] : null,
    apiBaseUrl: typeof values['base-url'] === 'string' ? values['base-url'] : null,
    region: typeof values.region === 'string' ? values.region : null,
    timeoutMs,
  };
}

function plannedCommand(command: string, subcommand?: string): CliResult {
  const suffix = subcommand ? ` ${subcommand}` : '';
  return {
    exitCode: 2,
    stdout: '',
    stderr: `Command '${command}${suffix}' is part of the planned unified surface but is not implemented yet.\n`,
  };
}

export async function executeCli(argv: string[], deps: CliDeps): Promise<CliResult> {
  try {
    const { flags, command, subcommand, commandArgs } = parseCommandLine(argv);

    if (flags.version) {
      return { exitCode: 0, stdout: '0.0.1\n', stderr: '' };
    }

    if (!command || command === 'help' || flags.help) {
      return { exitCode: 0, stdout: `${renderMainHelp(deps.dotEnvStatus)}\n`, stderr: '' };
    }

    if (command === 'doctor') {
      const doctorFlags = parseDoctorFlags(commandArgs);
      if (doctorFlags.help) {
        return { exitCode: 0, stdout: `${renderDoctorHelp()}\n`, stderr: '' };
      }
      const report = buildDoctorReport(deps.env, deps.dotEnvStatus);
      return {
        exitCode: report.ok ? 0 : 1,
        stdout: doctorFlags.json ? `${JSON.stringify(report)}\n` : renderDoctorText(report),
        stderr: '',
      };
    }

    if (command === 'search' && !subcommand && commandArgs.includes('--help')) {
      return { exitCode: 0, stdout: `${renderSearchHelp()}\n`, stderr: '' };
    }

    if (command === 'search' && subcommand) {
      const remoteFlags = parseRemoteFlags(commandArgs);
      const commandKey = `search:${subcommand}` as const;
      if (remoteFlags.help) {
        return { exitCode: 0, stdout: `${getRemoteCommandHelp(commandKey)}\n`, stderr: '' };
      }

      return {
        exitCode: 0,
        stdout: await executeRemoteCommand({
          commandKey,
          inputPath: remoteFlags.inputPath,
          apiBaseUrl:
            remoteFlags.apiBaseUrl ??
            deps.env.TIANGONG_API_BASE_URL ??
            deps.env.SUPABASE_FUNCTIONS_URL ??
            null,
          apiKey:
            remoteFlags.apiKey ??
            deps.env.TIANGONG_API_KEY ??
            deps.env.TIANGONG_LCA_APIKEY ??
            null,
          region:
            remoteFlags.region ??
            deps.env.TIANGONG_REGION ??
            deps.env.SUPABASE_FUNCTION_REGION ??
            'us-east-1',
          timeoutMs: remoteFlags.timeoutMs,
          dryRun: remoteFlags.dryRun,
          compactJson: remoteFlags.json,
          fetchImpl: deps.fetchImpl,
        }),
        stderr: '',
      };
    }

    if (command === 'admin' && !subcommand && commandArgs.includes('--help')) {
      return { exitCode: 0, stdout: `${renderAdminHelp()}\n`, stderr: '' };
    }

    if (command === 'admin' && subcommand === 'embedding-run') {
      const remoteFlags = parseRemoteFlags(commandArgs);
      if (remoteFlags.help) {
        return { exitCode: 0, stdout: `${getRemoteCommandHelp('admin:embedding-run')}\n`, stderr: '' };
      }

      return {
        exitCode: 0,
        stdout: await executeRemoteCommand({
          commandKey: 'admin:embedding-run',
          inputPath: remoteFlags.inputPath,
          apiBaseUrl:
            remoteFlags.apiBaseUrl ??
            deps.env.TIANGONG_API_BASE_URL ??
            deps.env.SUPABASE_FUNCTIONS_URL ??
            null,
          apiKey:
            remoteFlags.apiKey ??
            deps.env.TIANGONG_API_KEY ??
            deps.env.TIANGONG_LCA_APIKEY ??
            null,
          region:
            remoteFlags.region ??
            deps.env.TIANGONG_REGION ??
            deps.env.SUPABASE_FUNCTION_REGION ??
            'us-east-1',
          timeoutMs: remoteFlags.timeoutMs,
          dryRun: remoteFlags.dryRun,
          compactJson: remoteFlags.json,
          fetchImpl: deps.fetchImpl,
        }),
        stderr: '',
      };
    }

    return plannedCommand(command, subcommand ?? undefined);
  } catch (error) {
    const payload = toErrorPayload(error);
    const exitCode = error instanceof CliError ? error.exitCode : 1;
    return {
      exitCode,
      stdout: '',
      stderr: `${JSON.stringify(payload)}\n`,
    };
  }
}
