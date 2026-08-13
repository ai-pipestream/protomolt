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
