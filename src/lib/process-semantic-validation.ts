import * as tidasSdk from '@tiangong-lca/tidas-sdk';
import { sha256Json } from './canonical-json-hash.js';
import { type SafeParseSchema, normalizeIssuePath } from './tidas-sdk-validation.js';

type JsonObject = Record<string, unknown>;
export type ProcessSemanticContext = {
  /** Complete exact Flow documents; identity, type and hash are recomputed on every admission. */
  flow_documents?: readonly JsonObject[];
  candidate_sha256?: string;
  allocation_transformation?: JsonObject;
};
type SemanticAnalysis = {
  profile: string;
  tolerance: number;
  complete: boolean;
  valid: boolean;
  coverage: Array<{ check: string; status: string; path: Array<string | number> }>;
  interpretations?: Array<{
    exchangeId: string;
    mode: string;
    allocations?: Array<{ targetId: string; fraction: number }>;
  }>;
  reference?: { ids: string[] };
  validationIssues: Array<{ code: string; path: Array<string | number>; message: string }>;
};
type FlowEvidence = { uuid: string; version: string; type: string; contentHash: string };
type Analyzer = (payload: unknown, options: { flows: FlowEvidence[] }) => SemanticAnalysis;
function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}
function record(value: unknown): JsonObject {
  return isRecord(value) ? value : {};
}
export function semanticContextFromInput(input: unknown): ProcessSemanticContext {
  const context = record(record(input).semantic_context);
  const transformation =
    record(input).allocation_transformation ?? context.allocation_transformation;
  return {
    ...(isRecord(transformation) ? { allocation_transformation: transformation } : {}),
    ...(Array.isArray(context.flow_documents)
      ? { flow_documents: context.flow_documents.filter(isRecord) }
      : {}),
    ...(typeof context.candidate_sha256 === 'string'
      ? { candidate_sha256: context.candidate_sha256 }
      : {}),
  };
}
export function analyzeProcessPayloadSemantics(
  payload: JsonObject,
  context: ProcessSemanticContext = {},
  sdk: {
    FlowSchema?: SafeParseSchema;
    analyzeProcessSemantics?: Analyzer;
  } = tidasSdk as unknown as { FlowSchema?: SafeParseSchema; analyzeProcessSemantics?: Analyzer },
) {
  const candidate_sha256 = sha256Json(payload);
  const flows: FlowEvidence[] = [];
  const contextIssues: Array<{ path: string; code: string; message: string }> = [];
  if (context.candidate_sha256 !== undefined && context.candidate_sha256 !== candidate_sha256)
    contextIssues.push({
      path: '<root>',
      code: 'allocation_candidate_evidence_stale',
      message: 'Semantic context belongs to different candidate bytes.',
    });
  if (
    context.allocation_transformation &&
    (context.allocation_transformation.profile !== 'tiangong.resulting-process-allocation.v1' ||
      context.allocation_transformation.candidate_sha256 !== candidate_sha256)
  )
    contextIssues.push({
      path: 'allocation_transformation',
      code: 'allocation_transformation_evidence_stale',
      message:
        'Generated candidate changed after provenance qualification; rebuild against the exact source instances.',
    });
  for (const [index, document] of (context.flow_documents ?? []).entries()) {
    const schema = sdk.FlowSchema;
    if (!schema?.safeParse(structuredClone(document)).success) {
      contextIssues.push({
        path: `semantic_context.flow_documents.${index}`,
        code: 'allocation_flow_document_schema_invalid',
        message: 'Exact Flow evidence must satisfy the pinned Flow schema.',
      });
      continue;
    }
    const root = record(document.flowDataSet);
    const info = record(record(root.flowInformation).dataSetInformation);
    const publication = record(record(root.administrativeInformation).publicationAndOwnership);
    const method = record(record(root.modellingAndValidation).LCIMethod);
    const uuid = info['common:UUID'];
    const version = publication['common:dataSetVersion'];
    const type = method.typeOfDataSet;
    if (typeof uuid !== 'string' || typeof version !== 'string' || typeof type !== 'string') {
      contextIssues.push({
        path: `semantic_context.flow_documents.${index}`,
        code: 'allocation_flow_document_invalid',
        message: 'Complete exact Flow identity, version and type are required.',
      });
    } else flows.push({ uuid, version, type, contentHash: sha256Json(document) });
  }
  const analysis = sdk.analyzeProcessSemantics
    ? sdk.analyzeProcessSemantics(structuredClone(payload), { flows })
    : {
        profile: 'tidas.process-allocation-reference.v1',
        tolerance: 0.0010000001,
        complete: false,
        valid: false,
        coverage: [{ check: 'semantic-runtime', status: 'unresolved', path: ['processDataSet'] }],
        validationIssues: [
          {
            code: 'allocation_semantic_runtime_unavailable',
            path: ['processDataSet'],
            message:
              'The pinned SDK does not provide the required versioned semantic analyzer; qualify the released SDK before admission.',
          },
        ],
      };
  const issues = [
    ...contextIssues,
    ...analysis.validationIssues.map((issue) => ({
      code: issue.code,
      message: issue.message,
      path: normalizeIssuePath(issue.path),
    })),
  ];
  return {
    status: contextIssues.length
      ? ('failed' as const)
      : !analysis.complete
        ? ('unresolved' as const)
        : analysis.valid
          ? ('passed' as const)
          : ('failed' as const),
    issue_count: issues.length,
    issues,
    profile: analysis.profile,
    tolerance: analysis.tolerance,
    candidate_sha256,
    dependencies: flows.map(({ uuid, version, contentHash }) => ({
      uuid,
      version,
      content_sha256: contentHash,
    })),
    options_sha256: sha256Json({ profile: analysis.profile, tolerance: analysis.tolerance }),
    coverage: analysis.coverage,
    ...(analysis.reference ? { reference: analysis.reference } : {}),
    ...(analysis.interpretations ? { interpretations: analysis.interpretations } : {}),
  };
}
