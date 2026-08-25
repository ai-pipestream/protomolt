/**
 * The workflows page's client: stored workflows through the registry bridge
 * (/api/protomolt/protomolt/workflows), verification and execution through the serve bridge
 * (check-workflow / run-workflow). Pure request/response shaping — the view stays thin.
 */
import { unwrap, verb } from './services'

export interface WorkflowFinding {
  /** The step the finding is attributed to, or '' for the workflow itself. */
  step: string
  /** 'method', 'when', 'rule', 'celRule', 'output', 'workflow', or 'contract'. */
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

export async function listWorkflows(fetchFn: typeof fetch = fetch): Promise<string[]> {
  return unwrap<string[]>(await fetchFn(REGISTRY_WORKFLOWS))
}

export async function getWorkflow(
  name: string,
  fetchFn: typeof fetch = fetch,
): Promise<Record<string, unknown>> {
  return unwrap(await fetchFn(`${REGISTRY_WORKFLOWS}/${encodeURIComponent(name)}`))
}

/** PUT is gated server-side by check-workflow; gate findings ride on the thrown error. */
export async function putWorkflow(
  name: string,
  definition: unknown,
  fetchFn: typeof fetch = fetch,
): Promise<void> {
  await unwrap(await fetchFn(`${REGISTRY_WORKFLOWS}/${encodeURIComponent(name)}`, {
    method: 'PUT',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(definition),
  }))
}

export async function checkWorkflow(
  definition: unknown,
  fetchFn: typeof fetch = fetch,
): Promise<WorkflowCheck> {
  return verb('CheckWorkflow', { workflow: definition }, fetchFn)
}

/** Compiles an authoring definition into the durable workflow message, as JSON. */
export async function compileWorkflow(
  definition: unknown,
  fetchFn: typeof fetch = fetch,
): Promise<Record<string, unknown>> {
  const compiled = await verb<{ workflow?: Record<string, unknown> }>(
      'CompileWorkflow', { workflow: definition }, fetchFn)
  if (!compiled.workflow) throw new Error('the compiler answered without a workflow')
  return compiled.workflow
}

export async function runWorkflow(
  name: string,
  input: unknown,
  fetchFn: typeof fetch = fetch,
): Promise<WorkflowRun> {
  return verb('RunWorkflow', { workflowName: name, input }, fetchFn)
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
