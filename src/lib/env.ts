export type EnvSpec = {
  canonical: string;
  aliases: string[];
  required: boolean;
  description: string;
  defaultValue?: string;
};

export type ResolvedEnv = {
  key: string;
  source: 'canonical' | 'default' | 'missing' | `alias:${string}`;
  value: string | null;
  present: boolean;
};

export const ENV_SPECS: EnvSpec[] = [
  {
    canonical: 'TIANGONG_API_BASE_URL',
    aliases: ['SUPABASE_FUNCTIONS_URL'],
    required: true,
    description: 'Main TianGong API / Edge Functions base URL',
  },
  {
    canonical: 'TIANGONG_API_KEY',
    aliases: ['TIANGONG_LCA_APIKEY'],
    required: true,
    description: 'Main TianGong API key',
  },
  {
    canonical: 'TIANGONG_REGION',
    aliases: ['SUPABASE_FUNCTION_REGION'],
    required: false,
    description: 'Target API region',
    defaultValue: 'us-east-1',
  },
  {
    canonical: 'OPENAI_API_KEY',
    aliases: [],
    required: false,
    description: 'LLM API key for review / remediation / build workflows',
  },
  {
    canonical: 'OPENAI_MODEL',
    aliases: [],
    required: false,
    description: 'Default LLM model',
    defaultValue: 'gpt-5',
  },
  {
    canonical: 'TIANGONG_KB_BASE_URL',
    aliases: [],
    required: false,
    description: 'Knowledge base API base URL',
  },
  {
    canonical: 'TIANGONG_KB_API_KEY',
    aliases: [],
    required: false,
    description: 'Knowledge base API key',
  },
  {
    canonical: 'TIANGONG_MINERU_BASE_URL',
    aliases: ['TIANGONG_MINERU_WITH_IMAGE_URL'],
    required: false,
    description: 'MinerU API base URL',
  },
  {
    canonical: 'TIANGONG_MINERU_API_KEY',
    aliases: ['TIANGONG_MINERU_WITH_IMAGE_API_KEY'],
    required: false,
    description: 'MinerU API key',
  },
];

export type DoctorCheck = {
  key: string;
  description: string;
  required: boolean;
  aliases: string[];
  source: ResolvedEnv['source'];
  present: boolean;
  valuePreview: string | null;
};

export type DoctorReport = {
  ok: boolean;
  loadedDotEnv: boolean;
  dotEnvPath: string;
  dotEnvKeysLoaded: number;
  checks: DoctorCheck[];
};

export function resolveEnv(spec: EnvSpec, env: NodeJS.ProcessEnv): ResolvedEnv {
  const canonicalValue = env[spec.canonical];
  if (canonicalValue) {
    return {
      key: spec.canonical,
      source: 'canonical',
      value: canonicalValue,
      present: true,
    };
  }

  for (const alias of spec.aliases) {
    const aliasValue = env[alias];
    if (aliasValue) {
      return {
        key: spec.canonical,
        source: `alias:${alias}`,
        value: aliasValue,
        present: true,
      };
    }
  }

  if (spec.defaultValue !== undefined) {
    return {
      key: spec.canonical,
      source: 'default',
      value: spec.defaultValue,
      present: true,
    };
  }

  return {
    key: spec.canonical,
    source: 'missing',
    value: null,
    present: false,
  };
}

export function maskSecret(value: string | null): string | null {
  if (!value) {
    return value;
  }
  if (value.length <= 8) {
    return value;
  }
  return `${value.slice(0, 4)}...${value.slice(-4)}`;
}

export function buildDoctorReport(
  env: NodeJS.ProcessEnv,
  dotEnvStatus: { loaded: boolean; path: string; count: number },
): DoctorReport {
  const checks = ENV_SPECS.map((spec) => {
    const resolved = resolveEnv(spec, env);
    return {
      key: spec.canonical,
      description: spec.description,
      required: spec.required,
      aliases: spec.aliases,
      source: resolved.source,
      present: resolved.present,
      valuePreview: maskSecret(resolved.value),
    };
  });

  return {
    ok: checks.every((check) => !check.required || check.present),
    loadedDotEnv: dotEnvStatus.loaded,
    dotEnvPath: dotEnvStatus.path,
    dotEnvKeysLoaded: dotEnvStatus.count,
    checks,
  };
}
