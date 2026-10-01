/** Scoped browser launch calls. The HttpOnly task-console cookie supplies authority. */
export interface AcceptedWorkflow {
  taskId: string
  attempt: number
  revision: number
  taskSpecSha256: string
  candidateSha256: string
  acceptedEntrySha256: string
}

export interface ArtifactReference {
  sha256: string
  mediaType: string
  sizeBytes: string | number
  redacted?: boolean
}

export interface LaunchInputContract {
  acceptance: AcceptedWorkflow
  inputType: string
  descriptors: ArtifactReference
  descriptorSet: string
}

export interface PreparedLaunchInput {
  acceptance: AcceptedWorkflow
  input: ArtifactReference
}

/** The complete immutable request sent to LaunchAcceptedWorkflow. */
export interface LaunchIntent {
  launchId: string
  acceptance: AcceptedWorkflow
  input: ArtifactReference
}

export interface LaunchResult {
  jobId: string
  authorization: ArtifactReference
}

export interface SavedLaunch {
  intent: LaunchIntent
  createdAt?: number
  result?: LaunchResult
}

export type WorkflowLaunchJobState =
  | 'WORKFLOW_LAUNCH_JOB_STATE_QUEUED'
  | 'WORKFLOW_LAUNCH_JOB_STATE_RUNNING'
  | 'WORKFLOW_LAUNCH_JOB_STATE_WAITING'
  | 'WORKFLOW_LAUNCH_JOB_STATE_COMPLETED'
  | 'WORKFLOW_LAUNCH_JOB_STATE_FAILED'
  | 'WORKFLOW_LAUNCH_JOB_STATE_DEAD'

export interface WorkflowLaunchJobStatus {
  jobId: string
  state: WorkflowLaunchJobState
  attempt: number
  maxAttempts: number
  createdAt: string
  updatedAt: string
  completedAt?: string
}

export type WorkflowLaunchStatus =
  | { request: LaunchIntent; notAuthorized: true }
  | { request: LaunchIntent; authorizedNotQueued: true }
  | { request: LaunchIntent; job: WorkflowLaunchJobStatus }

export class WorkflowLaunchApiError extends Error {
  constructor(public readonly status: number, code: string) {
    super(code)
    this.name = 'WorkflowLaunchApiError'
  }
}

type FetchLike = (input: string, init?: RequestInit) => Promise<Response>
type StorageLike = Pick<Storage, 'length' | 'key' | 'getItem' | 'setItem'>

const PREFIX = 'protomolt.workflow-launch.intent.v1.'
const MAX_INPUT_BYTES = 4 * 1024 * 1024
const JOB_STATES = new Set<WorkflowLaunchJobState>([
  'WORKFLOW_LAUNCH_JOB_STATE_QUEUED', 'WORKFLOW_LAUNCH_JOB_STATE_RUNNING',
  'WORKFLOW_LAUNCH_JOB_STATE_WAITING', 'WORKFLOW_LAUNCH_JOB_STATE_COMPLETED',
  'WORKFLOW_LAUNCH_JOB_STATE_FAILED', 'WORKFLOW_LAUNCH_JOB_STATE_DEAD',
])

export function newLaunchId(): string {
  if (typeof crypto.randomUUID === 'function') return crypto.randomUUID()
  const bytes = crypto.getRandomValues(new Uint8Array(16))
  bytes[6] = (bytes[6]! & 0x0f) | 0x40
  bytes[8] = (bytes[8]! & 0x3f) | 0x80
  const hex = [...bytes].map((byte) => byte.toString(16).padStart(2, '0')).join('')
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`
}

export function sameAcceptance(left: AcceptedWorkflow, right: AcceptedWorkflow): boolean {
  return left.taskId === right.taskId && left.attempt === right.attempt
    && left.revision === right.revision && left.taskSpecSha256 === right.taskSpecSha256
    && left.candidateSha256 === right.candidateSha256
    && left.acceptedEntrySha256 === right.acceptedEntrySha256
}

export function sameArtifact(left: ArtifactReference, right: ArtifactReference): boolean {
  return left.sha256 === right.sha256 && left.mediaType === right.mediaType
    && String(left.sizeBytes) === String(right.sizeBytes)
    && Boolean(left.redacted) === Boolean(right.redacted)
}

function isReference(value: unknown): value is ArtifactReference {
  if (!value || typeof value !== 'object') return false
  const ref = value as Partial<ArtifactReference>
  return typeof ref.sha256 === 'string' && /^[0-9a-f]{64}$/.test(ref.sha256)
    && typeof ref.mediaType === 'string'
    && (typeof ref.sizeBytes === 'number' || typeof ref.sizeBytes === 'string')
}

function isAuthorizationReference(value: unknown): value is ArtifactReference {
  if (!isReference(value)) return false
  const size = typeof value.sizeBytes === 'number' ? value.sizeBytes
    : /^\d+$/.test(value.sizeBytes) ? Number(value.sizeBytes) : NaN
  return value.mediaType === 'application/x-protobuf'
    && (value.redacted === undefined || value.redacted === false)
    && Number.isSafeInteger(size) && size >= 0 && size <= MAX_INPUT_BYTES
}

function isAcceptance(value: unknown): value is AcceptedWorkflow {
  if (!value || typeof value !== 'object') return false
  const accepted = value as Partial<AcceptedWorkflow>
  return typeof accepted.taskId === 'string' && typeof accepted.attempt === 'number'
    && typeof accepted.revision === 'number'
    && [accepted.taskSpecSha256, accepted.candidateSha256, accepted.acceptedEntrySha256]
      .every((hash) => typeof hash === 'string' && /^[0-9a-f]{64}$/.test(hash))
}

function isIntent(value: unknown): value is LaunchIntent {
  if (!value || typeof value !== 'object') return false
  const intent = value as Partial<LaunchIntent>
  return typeof intent.launchId === 'string' && isAcceptance(intent.acceptance)
    && isReference(intent.input)
}

function canonicalLaunchId(value: string): string { return value.toLowerCase() }

function isJobStatus(value: unknown, launchId: string): value is WorkflowLaunchJobStatus {
  if (!value || typeof value !== 'object') return false
  const job = value as Partial<WorkflowLaunchJobStatus>
  if (job.jobId !== launchId || !JOB_STATES.has(job.state as WorkflowLaunchJobState)
      || !Number.isInteger(job.attempt) || job.attempt! < 0
      || !Number.isInteger(job.maxAttempts) || job.maxAttempts! < 1
      || typeof job.createdAt !== 'string' || !Number.isFinite(Date.parse(job.createdAt))
      || typeof job.updatedAt !== 'string' || !Number.isFinite(Date.parse(job.updatedAt))) return false
  const terminal = job.state === 'WORKFLOW_LAUNCH_JOB_STATE_COMPLETED'
    || job.state === 'WORKFLOW_LAUNCH_JOB_STATE_FAILED'
    || job.state === 'WORKFLOW_LAUNCH_JOB_STATE_DEAD'
  return terminal ? typeof job.completedAt === 'string' && Number.isFinite(Date.parse(job.completedAt))
    : job.completedAt === undefined
}

function isStatus(value: unknown, intent: LaunchIntent): value is WorkflowLaunchStatus {
  if (!value || typeof value !== 'object') return false
  const status = value as Partial<WorkflowLaunchStatus>
  const request = status.request
  if (!request || !isIntent(request)
      || request.launchId !== canonicalLaunchId(intent.launchId)
      || !sameAcceptance(request.acceptance, intent.acceptance)
      || !sameArtifact(request.input, intent.input)) return false
  const raw = value as Record<string, unknown>
  const outcomes = ['notAuthorized', 'authorizedNotQueued', 'job']
    .filter((field) => Object.prototype.hasOwnProperty.call(raw, field))
  if (outcomes.length !== 1) return false
  if (outcomes[0] === 'notAuthorized') return raw.notAuthorized === true
  if (outcomes[0] === 'authorizedNotQueued') return raw.authorizedNotQueued === true
  return isJobStatus(raw.job, request.launchId)
}

function key(intent: LaunchIntent): string {
  return `${PREFIX}${intent.acceptance.taskId}.${intent.launchId}`
}

/** One key per launch UUID avoids overwriting another pending intent in a second tab. */
export class WorkflowLaunchIntentStore {
  constructor(private readonly providedStorage?: StorageLike) {}

  private get storage(): StorageLike { return this.providedStorage ?? localStorage }

  list(taskId: string): SavedLaunch[] {
    const prefix = `${PREFIX}${taskId}.`
    const saved: SavedLaunch[] = []
    for (let i = 0; i < this.storage.length; i++) {
      const entryKey = this.storage.key(i)
      if (!entryKey?.startsWith(prefix)) continue
      try {
        const value = JSON.parse(this.storage.getItem(entryKey) ?? '') as SavedLaunch
        if (isIntent(value.intent) && key(value.intent) === entryKey) saved.push(value)
      } catch {
        // Invalid local data cannot become a launch request.
      }
    }
    return saved.sort((a, b) => (a.createdAt ?? 0) - (b.createdAt ?? 0))
  }

  persist(intent: LaunchIntent): void {
    if (!isIntent(intent)) throw new Error('Launch intent is incomplete.')
    const entryKey = key(intent)
    const prior = this.storage.getItem(entryKey)
    if (prior !== null) {
      const existing = JSON.parse(prior) as SavedLaunch
      if (!isIntent(existing.intent) || JSON.stringify(existing.intent) !== JSON.stringify(intent)) {
        throw new Error('The saved launch UUID belongs to another request.')
      }
      return
    }
    const recorded = JSON.stringify({ intent, createdAt: Date.now() })
    this.storage.setItem(entryKey, recorded)
    if (this.storage.getItem(entryKey) !== recorded) {
      throw new Error('The launch request could not be saved. Nothing was sent.')
    }
  }

  recordResult(intent: LaunchIntent, result: LaunchResult): void {
    const entryKey = key(intent)
    const prior = this.storage.getItem(entryKey)
    if (prior === null) throw new Error('The saved launch request is missing.')
    const existing = JSON.parse(prior) as SavedLaunch
    if (!isIntent(existing.intent) || JSON.stringify(existing.intent) !== JSON.stringify(intent)) {
      throw new Error('The saved launch request has changed.')
    }
    const recorded = JSON.stringify({ ...existing, result })
    this.storage.setItem(entryKey, recorded)
    if (this.storage.getItem(entryKey) !== recorded) {
      throw new Error('The launch result could not be saved. Retry the same request to recover it.')
    }
  }
}

export class WorkflowLaunchApi {
  constructor(
    private readonly fetchFn: FetchLike = (input, init) => fetch(input, init),
    readonly intents: WorkflowLaunchIntentStore = new WorkflowLaunchIntentStore(),
  ) {}

  accepted(taskId: string, signal?: AbortSignal): Promise<AcceptedWorkflow> {
    return this.post('accepted', { taskId }, signal)
  }

  contract(acceptance: AcceptedWorkflow, signal?: AbortSignal): Promise<LaunchInputContract> {
    return this.post('contract', { acceptance }, signal)
  }

  prepare(acceptance: AcceptedWorkflow, json: string, signal?: AbortSignal): Promise<PreparedLaunchInput> {
    const bytes = new TextEncoder().encode(json)
    if (bytes.length < 1 || bytes.length > MAX_INPUT_BYTES) {
      throw new Error('Input JSON must be between 1 byte and 4 MiB.')
    }
    let binary = ''
    for (const byte of bytes) binary += String.fromCharCode(byte)
    return this.post('prepare', { acceptance, inputJson: btoa(binary) }, signal)
  }

  /** Persist the exact request before the first network send and every retry. */
  async launch(intent: LaunchIntent): Promise<LaunchResult> {
    this.intents.persist(intent)
    const result = await this.post<LaunchResult>('launch', intent)
    if (result.jobId !== intent.launchId || !isAuthorizationReference(result.authorization)) {
      throw new Error('Launch response does not match the saved request.')
    }
    this.intents.recordResult(intent, result)
    return result
  }

  /** Read only the job bound to this exact saved intent; never submit on refresh. */
  async status(intent: LaunchIntent, signal?: AbortSignal): Promise<WorkflowLaunchStatus> {
    if (!isIntent(intent)) throw new Error('Launch intent is incomplete.')
    const result = await this.post<unknown>('status', { request: intent }, signal)
    if (!isStatus(result, intent)) {
      throw new Error('Launch status does not match the saved request.')
    }
    return result
  }

  private async post<T>(path: string, body: unknown, signal?: AbortSignal): Promise<T> {
    const response = await this.fetchFn(`/api/workflow-launch/${path}`, {
      method: 'POST',
      credentials: 'same-origin',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
      signal,
    })
    if (!response.ok) {
      let code = `HTTP ${response.status}`
      try {
        const value = await response.json() as { error?: string }
        if (value.error) code = value.error
      } catch { /* Preserve the status if an intermediary returns a non-JSON error. */ }
      throw new WorkflowLaunchApiError(response.status, code)
    }
    return await response.json() as T
  }
}

export const workflowLaunchApi = new WorkflowLaunchApi()
