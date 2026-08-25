/**
 * The receipts page's client: signed work records through the serve bridge.
 * A record is exported from a stored run's evidence, then verified (do the
 * signatures and digests hold?) and optionally evaluated against a workflow
 * (does the record show that workflow was actually followed?).
 */

export interface WorkRecordCheck {
  id: string
  /** 'PASSED', 'FAILED', or 'SKIPPED' (skipped: could not run by design). */
  status: string
  detail?: string
}

export interface WorkflowStepReplay {
  stepName: string
  recordedStatus?: string
  ok: boolean
  detail?: string
}

export interface ExportedRecord {
  recordBase64: string
  manifestDigest?: string
  recordId?: string
  maskedPaths?: string[]
}

export interface Verification {
  verified: boolean
  manifestDigest?: string
  checks: WorkRecordCheck[]
  /** What the record deliberately does NOT claim. */
  nonClaims: string[]
}

export interface Evaluation {
  accepted: boolean
  manifestDigest?: string
  policyId?: string
  evaluatedAt?: string
  checks: WorkRecordCheck[]
  replaySteps: WorkflowStepReplay[]
  nonClaims: string[]
}

const SERVE = '/api/serve/grpc-json/ProtoMoltService'

async function verb<T>(name: string, body: unknown, fetchFn: typeof fetch): Promise<T> {
  const response = await fetchFn(`${SERVE}/${name}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  })
  const json = (await response.json()) as Record<string, unknown>
  if (!response.ok) {
    throw new Error(String(json.message ?? json.error ?? `HTTP ${response.status}`))
  }
  return json as T
}

export async function exportRecord(
  runId: string,
  fetchFn: typeof fetch = fetch,
): Promise<ExportedRecord> {
  return verb('ExportWorkRecord', { runId }, fetchFn)
}

export async function verifyRecord(
  recordBase64: string,
  fetchFn: typeof fetch = fetch,
): Promise<Verification> {
  const result = await verb<Record<string, unknown>>('VerifyWorkRecord',
      { recordBase64 }, fetchFn)
  return {
    verified: result.verified === true,
    manifestDigest: result.manifestDigest as string | undefined,
    checks: (result.checks ?? []) as WorkRecordCheck[],
    nonClaims: (result.nonClaims ?? []) as string[],
  }
}

export async function evaluateRecord(
  recordBase64: string,
  workflow: Record<string, unknown> | null,
  fetchFn: typeof fetch = fetch,
): Promise<Evaluation> {
  const body: Record<string, unknown> = { recordBase64 }
  if (workflow) body.workflow = workflow
  const result = await verb<Record<string, unknown>>('EvaluateWorkRecord', body, fetchFn)
  return {
    accepted: result.accepted === true,
    manifestDigest: result.manifestDigest as string | undefined,
    policyId: result.policyId as string | undefined,
    evaluatedAt: result.evaluatedAt as string | undefined,
    checks: (result.checks ?? []) as WorkRecordCheck[],
    replaySteps: (result.replaySteps ?? []) as WorkflowStepReplay[],
    nonClaims: (result.nonClaims ?? []) as string[],
  }
}

export function passed(check: WorkRecordCheck): boolean {
  return check.status === 'PASSED'
}

export function skipped(check: WorkRecordCheck): boolean {
  return check.status === 'SKIPPED'
}
