import { newLaunchId } from './workflowLaunch'

export interface AuthoringCheck { name: string; description: string }
export interface AuthoringTemplate {
  spec: {
    objective: string
    requiredChecks: AuthoringCheck[]
    context: unknown[]
    contract: { typeName: string }
  }
  leaseSeconds: number
}
export interface AuthoringTemplateResponse {
  template: AuthoringTemplate
  templateSha256: string
}
export interface AuthoringStartRequest {
  taskId: string
  workerId: string
  templateSha256: string
  objective: string
}
export interface AuthoringStartResponse {
  request: AuthoringStartRequest
  offer: {
    attempt: number
    spec: { objective: string }
    startBindingSha256: string
    resumeFrom?: unknown
  }
}
export interface SavedAuthoringStart {
  request: AuthoringStartRequest
  createdAt: number
  response?: AuthoringStartResponse
}

export class WorkflowAuthoringApiError extends Error {
  constructor(public readonly status: number, code: string) {
    super(code)
    this.name = 'WorkflowAuthoringApiError'
  }
}

type FetchLike = (input: string, init?: RequestInit) => Promise<Response>
type StorageLike = Pick<Storage, 'length' | 'key' | 'getItem' | 'setItem' | 'removeItem'>
const PREFIX = 'protomolt.workflow-authoring.start.v1.'
const HASH = /^[0-9a-f]{64}$/
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/

function validRequest(value: unknown): value is AuthoringStartRequest {
  if (!value || typeof value !== 'object') return false
  const request = value as Partial<AuthoringStartRequest>
  return typeof request.taskId === 'string' && UUID.test(request.taskId)
    && typeof request.workerId === 'string' && request.workerId.length <= 128
    && /^[a-z0-9]+(?:[._-][a-z0-9]+)*$/.test(request.workerId)
    && typeof request.templateSha256 === 'string' && HASH.test(request.templateSha256)
    && typeof request.objective === 'string' && request.objective.length > 0
    && [...request.objective].length <= 4096
}

function sameRequest(left: AuthoringStartRequest, right: AuthoringStartRequest): boolean {
  return left.taskId === right.taskId && left.workerId === right.workerId
    && left.templateSha256 === right.templateSha256 && left.objective === right.objective
}

function validResponse(value: unknown, request: AuthoringStartRequest): value is AuthoringStartResponse {
  if (!value || typeof value !== 'object') return false
  const response = value as Partial<AuthoringStartResponse>
  const offer = response.offer
  return validRequest(response.request) && sameRequest(response.request, request)
    && !!offer && offer.attempt === 1 && offer.resumeFrom === undefined
    && offer.spec?.objective === request.objective
    && typeof offer.startBindingSha256 === 'string' && HASH.test(offer.startBindingSha256)
}

function validTemplate(value: unknown): value is AuthoringTemplateResponse {
  if (!value || typeof value !== 'object') return false
  const response = value as Partial<AuthoringTemplateResponse>
  const spec = response.template?.spec
  return typeof response.templateSha256 === 'string' && HASH.test(response.templateSha256)
    && !!spec && typeof spec.objective === 'string'
    && typeof spec.contract?.typeName === 'string' && !!spec.contract.typeName
    && Array.isArray(spec.requiredChecks) && spec.requiredChecks.length > 0
    && spec.requiredChecks.every((check) => typeof check.name === 'string' && !!check.name)
    && Array.isArray(spec.context) && spec.context.length > 0
    && typeof response.template?.leaseSeconds === 'number'
    && Number.isInteger(response.template.leaseSeconds) && response.template.leaseSeconds > 0
}

/** Keyed by task UUID so a pending start cannot be silently replaced. */
export class WorkflowAuthoringStartStore {
  constructor(readonly principal: string, private readonly providedStorage?: StorageLike) {
    if (!principal) throw new Error('A named console principal is required for saved authoring starts.')
  }
  private get storage(): StorageLike { return this.providedStorage ?? localStorage }
  private get prefix(): string { return `${PREFIX}${encodeURIComponent(this.principal)}.` }

  list(): SavedAuthoringStart[] {
    const values: SavedAuthoringStart[] = []
    for (let i = 0; i < this.storage.length; i++) {
      const entryKey = this.storage.key(i)
      if (!entryKey?.startsWith(this.prefix)) continue
      try {
        const saved = JSON.parse(this.storage.getItem(entryKey) ?? '') as SavedAuthoringStart
        if (!validRequest(saved.request) || entryKey !== this.prefix + saved.request.taskId) continue
        if (saved.response && !validResponse(saved.response, saved.request)) continue
        values.push(saved)
      } catch { /* Invalid local data cannot become a start request. */ }
    }
    return values.sort((left, right) => left.createdAt - right.createdAt)
  }

  persist(request: AuthoringStartRequest): void {
    if (!validRequest(request)) throw new Error('The authoring start request is incomplete.')
    const key = this.prefix + request.taskId
    const prior = this.storage.getItem(key)
    if (prior !== null) {
      let existing: SavedAuthoringStart
      try { existing = JSON.parse(prior) as SavedAuthoringStart }
      catch { throw new Error('The saved task UUID is corrupt.') }
      if (!validRequest(existing.request) || !sameRequest(existing.request, request)) {
        throw new Error('The saved task UUID belongs to another request.')
      }
      return
    }
    const recorded = JSON.stringify({ request, createdAt: Date.now() })
    this.storage.setItem(key, recorded)
    if (this.storage.getItem(key) !== recorded) {
      throw new Error('The authoring request could not be saved. Nothing was sent.')
    }
  }

  recordResponse(request: AuthoringStartRequest, response: AuthoringStartResponse): void {
    if (!validResponse(response, request)) throw new Error('The authoring response does not match the saved request.')
    const key = this.prefix + request.taskId
    const prior = this.storage.getItem(key)
    if (prior === null) throw new Error('The saved authoring request is missing.')
    const existing = JSON.parse(prior) as SavedAuthoringStart
    if (!validRequest(existing.request) || !sameRequest(existing.request, request)) {
      throw new Error('The saved authoring request has changed.')
    }
    const recorded = JSON.stringify({ ...existing, response })
    this.storage.setItem(key, recorded)
    if (this.storage.getItem(key) !== recorded) {
      throw new Error('The authoring response could not be saved. Retry the same request.')
    }
  }
}

export class WorkflowAuthoringApi {
  constructor(
    readonly starts: WorkflowAuthoringStartStore,
    private readonly fetchFn: FetchLike = (input, init) => fetch(input, init),
  ) {}

  async template(signal?: AbortSignal): Promise<AuthoringTemplateResponse> {
    const result = await this.post('template', {}, signal)
    if (!validTemplate(result)) throw new Error('The configured authoring template is invalid.')
    return result
  }

  newRequest(workerId: string, objective: string, templateSha256: string): AuthoringStartRequest {
    return { taskId: newLaunchId(), workerId, templateSha256, objective }
  }

  async start(request: AuthoringStartRequest): Promise<AuthoringStartResponse> {
    this.starts.persist(request)
    const result = await this.post('start', request)
    if (!validResponse(result, request)) throw new Error('The authoring response does not match the saved request.')
    this.starts.recordResponse(request, result)
    return result
  }

  private async post(path: string, body: unknown, signal?: AbortSignal): Promise<unknown> {
    const result = await this.fetchFn(`/api/workflow-authoring/${path}`, {
      method: 'POST', credentials: 'same-origin',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body), signal,
    })
    if (!result.ok) {
      let code = `HTTP ${result.status}`
      try {
        const value = await result.json() as { error?: string }
        if (value.error) code = value.error
      } catch { /* Keep status when an intermediary returns non-JSON. */ }
      throw new WorkflowAuthoringApiError(result.status, code)
    }
    return result.json()
  }
}
