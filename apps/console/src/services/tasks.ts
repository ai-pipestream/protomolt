export type TaskMessageKind = 'question' | 'answer' | 'guidance' | 'note'

export interface TaskSessionStatus {
  authenticated: boolean
  loginRequired: boolean
}

export interface WorkerSummary {
  workerId: string
  admitted: boolean
  connected: boolean
  provider: string
  model: string
  capabilities: string[]
}

export interface TaskSummary {
  taskId: string
  phase: string
  attempt: number
  workerId: string
  objective: string
  candidateRevision: number
  lastProgressSeq: number
  lastCheckpointSeq: number
  lastCursor: number
}

export interface TaskEvent {
  cursor: number
  workerId: string
  taskId: string
  lane: string
  entry: Record<string, unknown>
}

export interface TaskList {
  tasks: TaskSummary[]
  cursor: number
  findings: TaskFinding[]
}

export interface TaskFinding {
  taskId: string
  frameId: string
  kind: string
  error: string
}

export interface TaskDetail {
  task: TaskSummary
  events: TaskEvent[]
  cursor: number
  findings: TaskFinding[]
}

export interface TaskEvents {
  events: TaskEvent[]
  cursor: number
  truncated: boolean
}

export interface ReviewResult {
  decision: 'accept' | 'revise'
  phase: string
}

export interface OfferedCheck {
  name: string
  description: string
}

export interface OfferResult {
  taskId: string
  workerId: string
}

export interface ExportedTaskRecord {
  recordBase64: string
  transcriptBase64: string
  manifestDigest: string
  recordId: string
}

/** One acceptance check joined with the latest candidate's evidence for it. */
export interface CheckStatus {
  name: string
  description: string
  /** 'passed', 'failed', or 'unproven' (no evidence recorded for it yet). */
  status: 'passed' | 'failed' | 'unproven'
  detail: string
}

export interface CandidateView {
  revision: number
  attempt: number
  summary: string
  cursor: number
  result?: Record<string, unknown>
}

export interface DeliverableContract {
  descriptorSet: string
  typeName: string
}

export interface OfferOptions {
  contract?: DeliverableContract
  taskId?: string
  resumeFrom?: { attempt: number; checkpointSeq: number; resumeToken: string }
}

type FetchLike = (input: string, init?: RequestInit) => Promise<Response>

/** An HTTP failure returned by the bounded task API. */
export class TaskApiError extends Error {
  constructor(
    public readonly status: number,
    message: string,
  ) {
    super(message)
    this.name = 'TaskApiError'
  }
}

export class TaskApi {
  constructor(
    private readonly base = '/api/tasks',
    private readonly sessionBase = '/api/task-session',
    private readonly fetchFn: FetchLike = (input, init) => fetch(input, init),
  ) {}

  async sessionStatus(): Promise<TaskSessionStatus> {
    const response = await this.fetchFn(this.sessionBase, {
      method: 'GET',
      credentials: 'same-origin',
    })
    if (response.status !== 200 && response.status !== 401) {
      throw await taskError(response)
    }
    return (await response.json()) as TaskSessionStatus
  }

  login(token: string): Promise<TaskSessionStatus> {
    return this.json('POST', this.sessionBase, { token })
  }

  async logout(): Promise<void> {
    const response = await this.fetchFn(this.sessionBase, {
      method: 'DELETE',
      credentials: 'same-origin',
    })
    if (!response.ok) throw await taskError(response)
  }

  listTasks(): Promise<TaskList> {
    return this.json('GET', this.base)
  }

  async listWorkers(): Promise<WorkerSummary[]> {
    const result = await this.json<{ workers: WorkerSummary[] }>('GET', `${this.base}/workers`)
    return result.workers
  }

  task(taskId: string): Promise<TaskDetail> {
    return this.json('GET', `${this.base}/${encodeURIComponent(taskId)}`)
  }

  watchEvents(
    after: number,
    taskId = '',
    timeoutMs = 25_000,
    maxEvents = 128,
    signal?: AbortSignal,
  ): Promise<TaskEvents> {
    const query = new URLSearchParams({
      after: String(after),
      timeoutMs: String(timeoutMs),
      maxEvents: String(maxEvents),
    })
    if (taskId) query.set('taskId', taskId)
    return this.json('GET', `${this.base}/events?${query}`, undefined, signal)
  }

  sendMessage(
    taskId: string,
    recipient: string,
    kind: TaskMessageKind,
    text: string,
    replyTo = '',
  ): Promise<{ message: Record<string, unknown> }> {
    return this.json('POST', `${this.base}/${encodeURIComponent(taskId)}/messages`, {
      recipient,
      kind,
      text,
      ...(replyTo ? { replyTo } : {}),
    })
  }

  /** Offers a durable task to a worker; the server generates the task identity. */
  offerTask(
    workerId: string,
    objective: string,
    requiredChecks: OfferedCheck[],
    allowedScopes: string[] = [],
    leaseMinutes = 30,
    options: OfferOptions = {},
  ): Promise<OfferResult> {
    return this.json('POST', `${this.base}/offer`, {
      workerId,
      objective,
      requiredChecks,
      ...(allowedScopes.length ? { allowedScopes } : {}),
      leaseMinutes,
      ...options,
    })
  }

  cancelTask(taskId: string, reason: string): Promise<{ taskId: string }> {
    return this.json('POST', `${this.base}/${encodeURIComponent(taskId)}/cancel`, { reason })
  }

  /** The task's transcript, projected into a signed work record. */
  exportRecord(taskId: string): Promise<ExportedTaskRecord> {
    return this.json('POST', `${this.base}/${encodeURIComponent(taskId)}/record`, {})
  }

  /** Accepts the open candidate, with the reviewer's verdict on the record. */
  reviewAccept(
    taskId: string,
    attempt: number,
    revision: number,
    verdict: string,
  ): Promise<ReviewResult> {
    return this.json('POST', `${this.base}/${encodeURIComponent(taskId)}/review`, {
      decision: 'accept',
      attempt,
      revision,
      verdict,
    })
  }

  /** Returns the open candidate for revision, naming the checks that failed. */
  reviewRevise(
    taskId: string,
    attempt: number,
    revision: number,
    feedback: string,
    failedChecks: string[] = [],
  ): Promise<ReviewResult> {
    return this.json('POST', `${this.base}/${encodeURIComponent(taskId)}/review`, {
      decision: 'revise',
      attempt,
      revision,
      feedback,
      ...(failedChecks.length ? { failedChecks } : {}),
    })
  }

  private async json<T>(
    method: string,
    path: string,
    body?: unknown,
    signal?: AbortSignal,
  ): Promise<T> {
    const response = await this.fetchFn(path, {
      method,
      credentials: 'same-origin',
      signal,
      ...(body === undefined
        ? {}
        : {
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body),
          }),
    })
    if (!response.ok) throw await taskError(response)
    return (await response.json()) as T
  }
}

type Frame = Record<string, any>

function coordinatorFrame(event: TaskEvent): Frame {
  return (event.entry as Frame).coordinatorFrame ?? {}
}

function workerFrame(event: TaskEvent): Frame {
  return (event.entry as Frame).workerFrame ?? {}
}

function frame(event: TaskEvent): Frame {
  return (event.entry as Frame).coordinatorFrame ?? (event.entry as Frame).workerFrame ?? {}
}

// The union of DelegateRequest and DelegateResponse payload arms, by JSON name.
const FRAME_KINDS = [
  'hello', 'accept', 'reject', 'heartbeat', 'progress', 'checkpoint', 'blocked',
  'failed', 'cancelled', 'completion', 'admission', 'offer', 'renewal', 'expired',
  'cancellation', 'revisionRequested', 'accepted', 'taskMessage',
] as const

/** Which protocol arm a recorded frame carries; 'frame' when none is recognized. */
export function frameKind(event: TaskEvent): string {
  const value = frame(event)
  return FRAME_KINDS.find((key) => value[key] !== undefined) ?? 'frame'
}

/** The frame's human line: its message, reason, feedback, or objective. */
export function frameText(event: TaskEvent): string {
  const value = frame(event)[frameKind(event)] as Frame | undefined
  if (!value) return 'Recorded protocol frame'
  return (
    value.text ??
    value.message ??
    value.summary ??
    value.reason ??
    value.feedback ??
    value.verdict ??
    value.note ??
    value.spec?.objective ??
    value.resumeToken ??
    'Recorded protocol frame'
  )
}

/** The frame's recorded facts: check evidence, commits, artifacts, state refs. */
export function frameFacts(event: TaskEvent): string[] {
  const value = frame(event)[frameKind(event)] as Frame | undefined
  if (!value) return []
  const facts: string[] = []
  for (const check of value.evidence ?? value.spec?.requiredChecks ?? []) {
    facts.push(
      [check.checkName ?? check.name, check.verdict, check.detail ?? check.description]
        .filter(Boolean)
        .join(' · '),
    )
  }
  for (const name of value.failedChecks ?? []) {
    facts.push(`failed check · ${name}`)
  }
  for (const commit of value.commits ?? []) {
    facts.push(
      [commit.repository, commit.commit?.slice(0, 12), commit.subject].filter(Boolean).join(' · '),
    )
  }
  for (const artifact of value.artifacts ?? []) {
    facts.push(`artifact · ${artifact.uri ?? artifact.sha256 ?? artifact.digest ?? artifact.objectKey ?? 'recorded'}`)
  }
  if (value.state) {
    facts.push(`checkpoint state · ${value.state.uri ?? value.state.digest ?? 'recorded'}`)
  }
  return facts.filter(Boolean)
}

/**
 * The contract of done: the offer's acceptance checks joined with the latest
 * candidate's evidence. A check no candidate has proved yet is 'unproven' —
 * absence of evidence is a state of its own, never rendered as passing.
 */
export function checkStatuses(events: TaskEvent[]): CheckStatus[] {
  let required: Frame[] = []
  for (const event of events) {
    const offer = coordinatorFrame(event).offer
    if (offer?.spec?.requiredChecks) required = offer.spec.requiredChecks
  }
  const evidence = new Map<string, Frame>()
  const candidate = latestCandidate(events)
  if (candidate) {
    for (const event of events) {
      const completion = workerFrame(event).completion
      if (event.cursor === candidate.cursor && completion?.attempt === candidate.attempt
          && completion.revision === candidate.revision) {
        for (const proof of completion.evidence ?? []) {
          evidence.set(proof.checkName, proof)
        }
      }
    }
  }
  return required.map((check) => {
    const proof = evidence.get(check.name)
    return {
      name: check.name,
      description: check.description ?? '',
      status: proof === undefined ? 'unproven'
          : proof.verdict === 'CHECK_VERDICT_PASSED' ? 'passed' : 'failed',
      detail: proof?.detail ?? '',
    }
  })
}

/** The newest completion in the current offer, or null before its first candidate. */
export function latestCandidate(events: TaskEvent[]): CandidateView | null {
  let candidate: CandidateView | null = null
  for (const event of events) {
    if (coordinatorFrame(event).offer) candidate = null
    const completion = workerFrame(event).completion
    if (completion) {
      candidate = {
        revision: completion.revision ?? 0,
        attempt: completion.attempt ?? 0,
        summary: completion.summary ?? '',
        cursor: event.cursor,
        ...(completion.result ? { result: completion.result } : {}),
      }
    }
  }
  return candidate
}

/** State and recovery are derived from the protocol; no speculative agent status. */
export function taskRecovery(task: TaskSummary): { reason: string; actor: string; action: string; retry: boolean; cancel: boolean } {
  if (task.phase === 'candidate') return {
    reason: 'A validated candidate is waiting for review.', actor: 'Reviewer',
    action: 'Inspect the result and evidence, then accept or request a revision.', retry: false, cancel: true,
  }
  if (['rejected', 'blocked', 'failed', 'cancelled', 'expired'].includes(task.phase)) return {
    reason: `Attempt ${task.attempt} ended as ${task.phase}.`, actor: 'Coordinator',
    action: 'Inspect the recorded reason and offer a new attempt, optionally from a checkpoint.', retry: true, cancel: false,
  }
  if (task.phase === 'accepted') return {
    reason: 'The reviewer accepted this task.', actor: 'Coordinator',
    action: 'Export the signed record. Create a new task for further work.', retry: false, cancel: false,
  }
  return {
    reason: task.phase === 'offered' ? 'The worker has not accepted or rejected the offer.'
      : 'The worker has a lease but has not submitted a current candidate.',
    actor: task.workerId || 'Worker',
    action: 'Send guidance or answer a question. Cancel if work should stop; an idle lease expires.', retry: false, cancel: true,
  }
}

export function latestOffer(events: TaskEvent[]): Frame | undefined {
  return [...events].reverse().map((event) => coordinatorFrame(event).offer).find(Boolean)
}

export function latestCheckpoint(events: TaskEvent[]): OfferOptions['resumeFrom'] {
  for (const event of [...events].reverse()) {
    const checkpoint = workerFrame(event).checkpoint
    if (checkpoint) return {
      attempt: checkpoint.attempt, checkpointSeq: checkpoint.checkpointSeq,
      resumeToken: checkpoint.resumeToken,
    }
  }
  return undefined
}

/**
 * The transcript as a plain-text record: cursor-ordered protocol facts, one
 * frame per block, honest about what it is — a projection of recorded frames,
 * carrying no provider reasoning and no claim beyond what was recorded.
 */
export function transcriptText(task: TaskSummary, events: TaskEvent[]): string {
  const lines: string[] = []
  lines.push('PROTOMOLT DELEGATION TRANSCRIPT')
  lines.push(`task ${task.taskId}`)
  if (task.objective) lines.push(`objective ${task.objective}`)
  lines.push(`phase ${task.phase} · attempt ${task.attempt}`
      + ` · ${events.length} recorded frame${events.length === 1 ? '' : 's'}`)
  lines.push('This is a projection of the recorded protocol frames, in cursor order.')
  lines.push('It carries no provider reasoning and no claim beyond what was recorded.')
  for (const event of events) {
    lines.push('')
    const lane = event.lane === 'LANE_WORKER' ? `worker ${event.workerId}` : 'coordinator'
    lines.push(`[${event.cursor}] ${lane} · ${frameKind(event)}`)
    lines.push(`  ${frameText(event)}`)
    for (const fact of frameFacts(event)) {
      lines.push(`  - ${fact}`)
    }
  }
  lines.push('')
  return lines.join('\n')
}

async function taskError(response: Response): Promise<TaskApiError> {
  let message = `HTTP ${response.status}`
  try {
    const body = (await response.json()) as { error?: string }
    if (body.error) message = body.error
  } catch {
    // Keep the bounded status-only fallback for non-JSON proxy failures.
  }
  return new TaskApiError(response.status, message)
}

export const taskApi = new TaskApi()
