import { createHash } from 'node:crypto';
import { CliError } from './errors.js';

export type UserApiKeyCredentials = {
  email: string;
  password: string;
};

function normalizeToken(value: unknown): string {
  return typeof value === 'string' ? value.trim() : '';
}

function normalizeCredentialValue(value: unknown): string {
  return typeof value === 'string' ? value.trim() : '';
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

export function decodeUserApiKey(apiKey: string): UserApiKeyCredentials | null {
  const normalizedApiKey = normalizeToken(apiKey);
  if (!normalizedApiKey) {
    return null;
  }

  try {
    const jsonText = Buffer.from(normalizedApiKey, 'base64').toString('utf8');
    const parsed = JSON.parse(jsonText);
    if (!isRecord(parsed)) {
      return null;
    }

    const email = normalizeCredentialValue(parsed.email);
    const password = normalizeCredentialValue(parsed.password);
    if (!email || !password) {
      return null;
    }

    return {
      email,
      password,
    };
  } catch {
    return null;
  }
}

export function requireUserApiKeyCredentials(apiKey: string): UserApiKeyCredentials {
  const credentials = decodeUserApiKey(apiKey);
  if (!credentials) {
    throw new CliError(
      'TIANGONG_LCA_API_KEY is invalid. Generate a new user API key from the TianGong user API key page.',
      {
        code: 'USER_API_KEY_INVALID',
        exitCode: 2,
      },
    );
  }

  return credentials;
}

export function fingerprintSecret(value: string): string {
  const normalized = normalizeToken(value);
  if (!normalized) {
    throw new CliError('Cannot fingerprint an empty secret value.', {
      code: 'SECRET_FINGERPRINT_VALUE_REQUIRED',
      exitCode: 2,
    });
  }

  return `sha256:${createHash('sha256').update(normalized).digest('hex')}`;
}

export function fingerprintUserApiKey(apiKey: string): string {
  return fingerprintSecret(apiKey);
}

export function redactEmail(email: string): string {
  const normalized = normalizeCredentialValue(email);
  const [localPart, domainPart] = normalized.split('@');
  if (!localPart || !domainPart) {
    return '****';
  }

  if (localPart.length <= 2) {
    return `****@${domainPart}`;
  }

  return `${localPart.slice(0, 2)}****@${domainPart}`;
}

export const __testInternals = {
  normalizeCredentialValue,
  normalizeToken,
};
