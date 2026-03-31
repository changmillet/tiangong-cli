import { readFileSync } from 'node:fs';
import path from 'node:path';
import { CliError } from './errors.js';

export type JsonRecord = Record<string, unknown>;

export function isRecord(value: unknown): value is JsonRecord {
  return Boolean(value && typeof value === 'object' && !Array.isArray(value));
}

export type FlowRecord = {
  id: string;
  version: string;
  name: string;
  flowType: string;
  shortDescription: JsonRecord | null;
  row: JsonRecord;
};

export function listify<T>(value: T | T[] | null | undefined): T[] {
  if (Array.isArray(value)) {
    return value;
  }

  return value === undefined || value === null ? [] : [value];
}

export function deepGet(
  value: unknown,
  pathParts: string[],
  defaultValue: unknown = undefined,
): unknown {
  let current: unknown = value;

  for (const part of pathParts) {
    if (!isRecord(current)) {
      return defaultValue;
    }

    current = current[part];
    if (current === undefined || current === null) {
      return defaultValue;
    }
  }

  return current;
}

export function coerceText(value: unknown): string {
  if (typeof value === 'string') {
    return value.trim();
  }

  if (isRecord(value)) {
    if (typeof value['#text'] === 'string') {
      return String(value['#text']).trim();
    }

    for (const nested of Object.values(value)) {
      const candidate = coerceText(nested);
      if (candidate) {
        return candidate;
      }
    }

    return '';
  }

  if (Array.isArray(value)) {
    for (const item of value) {
      const candidate = coerceText(item);
      if (candidate) {
        return candidate;
      }
    }

    return '';
  }

  return value === undefined || value === null ? '' : String(value).trim();
}

export function firstLangEntry(value: unknown, fallback = ''): JsonRecord | null {
  if (isRecord(value) && ('#text' in value || '@xml:lang' in value)) {
    const text = coerceText(value);
    if (text) {
      return {
        '@xml:lang': coerceText(value['@xml:lang']) || 'en',
        '#text': text,
      };
    }
  }

  if (Array.isArray(value)) {
    for (const item of value) {
      const result = firstLangEntry(item);
      if (result) {
        return result;
      }
    }
  }

  if (isRecord(value)) {
    for (const nested of Object.values(value)) {
      const result = firstLangEntry(nested);
      if (result) {
        return result;
      }
    }
  }

  return fallback
    ? {
        '@xml:lang': 'en',
        '#text': fallback,
      }
    : null;
}

export function normalizeText(value: string): string {
  return value
    .replace(/[^0-9a-zA-Z\u4e00-\u9fff]+/gu, ' ')
    .replace(/\s+/gu, ' ')
    .trim()
    .toLowerCase();
}

export function datasetPayloadFromRow(row: JsonRecord): JsonRecord {
  if (isRecord(row.json_ordered)) {
    return row.json_ordered;
  }

  if (isRecord(row.json)) {
    return row.json;
  }

  return row;
}

export function flowDatasetFromRow(row: JsonRecord): JsonRecord {
  const payload = datasetPayloadFromRow(row);
  return isRecord(payload.flowDataSet) ? payload.flowDataSet : payload;
}

export function extractFlowRecord(row: JsonRecord): FlowRecord {
  const dataset = flowDatasetFromRow(row);
  const info = deepGet(dataset, ['flowInformation', 'dataSetInformation'], {}) as JsonRecord;
  const id = coerceText(row.id) || coerceText(info['common:UUID']);
  const version =
    coerceText(row.version) ||
    coerceText(
      deepGet(dataset, [
        'administrativeInformation',
        'publicationAndOwnership',
        'common:dataSetVersion',
      ]),
    ) ||
    '01.00.000';
  const nameBlock =
    deepGet(info, ['name', 'baseName']) ??
    deepGet(info, ['name']) ??
    info['common:shortDescription'];
  const name = coerceText(nameBlock) || id;
  const flowType =
    coerceText(
      deepGet(dataset, ['modellingAndValidation', 'LCIMethodAndAllocation', 'typeOfDataSet']),
    ) ||
    coerceText(deepGet(dataset, ['modellingAndValidation', 'LCIMethod', 'typeOfDataSet'])) ||
    coerceText(row.typeOfDataSet);

  return {
    id,
    version,
    name,
    flowType,
    shortDescription: firstLangEntry(info['common:shortDescription'], name),
    row,
  };
}

export function loadRowsFromFile(filePath: string): JsonRecord[] {
  const resolved = path.resolve(filePath);
  const text = readFileSync(resolved, 'utf8');

  if (resolved.endsWith('.jsonl')) {
    return text
      .split(/\r?\n/gu)
      .map((line) => line.trim())
      .filter(Boolean)
      .map((line) => {
        const parsed = JSON.parse(line) as unknown;
        if (!isRecord(parsed)) {
          throw new CliError(`Expected JSON object rows in JSONL file: ${resolved}`, {
            code: 'FLOW_ROWS_INVALID_JSONL_ROW',
            exitCode: 2,
          });
        }
        return parsed;
      });
  }

  const parsed = JSON.parse(text) as unknown;
  if (!Array.isArray(parsed) || !parsed.every(isRecord)) {
    throw new CliError(`Expected JSON array of objects: ${resolved}`, {
      code: 'FLOW_ROWS_INVALID_JSON',
      exitCode: 2,
    });
  }

  return parsed;
}

export const __testInternals = {
  isRecord,
};
