/**
 * The workflows page's client: stored workflows through the registry bridge
 * (/api/protomolt/protomolt/workflows), verification and execution through the serve bridge
 * (check-workflow / run-workflow). Pure request/response shaping — the view stays thin.
 */

export interface WorkflowFinding {
  step: string
  kind: string
  error: string
}

export interface WorkflowCheck {
  ok: boolean
  findings: WorkflowFinding[]
}

export interface WorkflowStepOutcome {
  name: string
  skipped: boolean
}

export interface WorkflowRun {
  ok: boolean
  outputType?: string
  output?: unknown
  steps?: WorkflowStepOutcome[]
  failedStep?: string
  error?: string
}

const REGISTRY_WORKFLOWS = '/api/protomolt/protomolt/workflows'
const SERVE = '/api/serve/grpc-json/ProtoMoltService'

async function json<T>(res: Response): Promise<T> {
  const body = await res.json()
  if (!res.ok) {
    const message = body?.message ?? body?.error ?? `HTTP ${res.status}`
    const error = new Error(String(message)) as Error & { findings?: WorkflowFinding[] }
    if (Array.isArray(body?.findings)) error.findings = body.findings
    throw error
  }
  return body as T
}

export async function listWorkflows(fetchFn: typeof fetch = fetch): Promise<string[]> {
  return json<string[]>(await fetchFn(REGISTRY_WORKFLOWS))
}

export async function getWorkflow(
  name: string,
  fetchFn: typeof fetch = fetch,
): Promise<Record<string, unknown>> {
  return json(await fetchFn(`${REGISTRY_WORKFLOWS}/${encodeURIComponent(name)}`))
}

/** PUT is gated server-side by check-workflow; gate findings ride on the thrown error. */
export async function putWorkflow(
  name: string,
  definition: unknown,
  fetchFn: typeof fetch = fetch,
): Promise<void> {
  await json(await fetchFn(`${REGISTRY_WORKFLOWS}/${encodeURIComponent(name)}`, {
    method: 'PUT',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(definition),
  }))
}

export async function checkWorkflow(
  definition: unknown,
  fetchFn: typeof fetch = fetch,
): Promise<WorkflowCheck> {
  return json(await fetchFn(`${SERVE}/CheckWorkflow`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ workflow: definition }),
  }))
}

export async function runWorkflow(
  name: string,
  input: unknown,
  fetchFn: typeof fetch = fetch,
): Promise<WorkflowRun> {
  return json(await fetchFn(`${SERVE}/RunWorkflow`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ workflowName: name, input }),
  }))
}

/** A one-line human summary of a definition: input type and the step pipeline. */
export function workflowSummary(definition: Record<string, unknown>): string {
  const steps = Array.isArray(definition.steps)
    ? (definition.steps as Array<{ name?: string }>).map((s) => s.name ?? '?')
    : []
  const input = typeof definition.inputType === 'string' ? definition.inputType : '?'
  return `${input} → ${steps.join(' → ')}`
}

/** Parses editor text into a definition, with a single friendly error. */
export function parseDefinition(text: string): Record<string, unknown> {
  let parsed: unknown
  try {
    parsed = JSON.parse(text)
  } catch (e) {
    throw new Error(`Not valid JSON: ${(e as Error).message}`)
  }
  if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) {
    throw new Error('A workflow definition is a JSON object')
  }
  return parsed as Record<string, unknown>
}
