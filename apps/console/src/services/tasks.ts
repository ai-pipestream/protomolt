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

  /** Accepts the open candidate, with the reviewer's verdict on the record. */
  reviewAccept(taskId: string, verdict: string): Promise<ReviewResult> {
    return this.json('POST', `${this.base}/${encodeURIComponent(taskId)}/review`, {
      decision: 'accept',
      verdict,
    })
  }

  /** Returns the open candidate for revision, naming the checks that failed. */
  reviewRevise(
    taskId: string,
    feedback: string,
    failedChecks: string[] = [],
  ): Promise<ReviewResult> {
    return this.json('POST', `${this.base}/${encodeURIComponent(taskId)}/review`, {
      decision: 'revise',
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
    facts.push(`artifact · ${artifact.uri ?? artifact.digest ?? artifact.objectKey ?? 'recorded'}`)
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
      if (completion?.revision === candidate.revision) {
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

/** The newest completion candidate on the transcript, or null before any. */
export function latestCandidate(events: TaskEvent[]): CandidateView | null {
  let candidate: CandidateView | null = null
  for (const event of events) {
    const completion = workerFrame(event).completion
    if (completion) {
      candidate = {
        revision: completion.revision ?? 0,
        attempt: completion.attempt ?? 0,
        summary: completion.summary ?? '',
        cursor: event.cursor,
      }
    }
  }
  return candidate
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
