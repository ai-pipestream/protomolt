/**
 * The chains page's client: stored chains through the registry bridge
 * (/api/protomolt/protomolt/chains), verification and execution through the serve bridge
 * (check-chain / run-chain). Pure request/response shaping — the view stays thin.
 */

export interface ChainFinding {
  step: string
  kind: string
  error: string
}

export interface ChainCheck {
  ok: boolean
  findings: ChainFinding[]
}

export interface ChainStepOutcome {
  name: string
  skipped: boolean
}

export interface ChainRun {
  ok: boolean
  outputType?: string
  output?: unknown
  steps?: ChainStepOutcome[]
  failedStep?: string
  error?: string
}

const REGISTRY_CHAINS = '/api/protomolt/protomolt/chains'
const SERVE = '/api/serve/grpc-json/ProtoMoltService'

async function json<T>(res: Response): Promise<T> {
  const body = await res.json()
  if (!res.ok) {
    const message = body?.message ?? body?.error ?? `HTTP ${res.status}`
    const error = new Error(String(message)) as Error & { findings?: ChainFinding[] }
    if (Array.isArray(body?.findings)) error.findings = body.findings
    throw error
  }
  return body as T
}

export async function listChains(fetchFn: typeof fetch = fetch): Promise<string[]> {
  return json<string[]>(await fetchFn(REGISTRY_CHAINS))
}

export async function getChain(
  name: string,
  fetchFn: typeof fetch = fetch,
): Promise<Record<string, unknown>> {
  return json(await fetchFn(`${REGISTRY_CHAINS}/${encodeURIComponent(name)}`))
}

/** PUT is gated server-side by check-chain; gate findings ride on the thrown error. */
export async function putChain(
  name: string,
  definition: unknown,
  fetchFn: typeof fetch = fetch,
): Promise<void> {
  await json(await fetchFn(`${REGISTRY_CHAINS}/${encodeURIComponent(name)}`, {
    method: 'PUT',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(definition),
  }))
}

export async function checkChain(
  definition: unknown,
  fetchFn: typeof fetch = fetch,
): Promise<ChainCheck> {
  return json(await fetchFn(`${SERVE}/CheckChain`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ chain: definition }),
  }))
}

export async function runChain(
  name: string,
  input: unknown,
  fetchFn: typeof fetch = fetch,
): Promise<ChainRun> {
  return json(await fetchFn(`${SERVE}/RunChain`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ chainName: name, input }),
  }))
}

/** A one-line human summary of a definition: input type and the step pipeline. */
export function chainSummary(definition: Record<string, unknown>): string {
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
    throw new Error('A chain definition is a JSON object')
  }
  return parsed as Record<string, unknown>
}
