/**
 * Save-as job lifecycle held outside the Save-as dialog. Publish, status and cancel calls run on
 * the host's lifetime, not the dialog's, so a response that arrives as or after the dialog
 * unmounts is still applied, and a queued or running job keeps being polled until it settles.
 * The operator keeps the operation ID and the terminal job summary until dismissed.
 */
import type { Client } from '@connectrpc/connect'
import {
  computed,
  inject,
  onBeforeUnmount,
  provide,
  reactive,
  type InjectionKey,
  type Ref,
} from 'vue'
import type {
  PublishRepositorySinkRequest,
  RepositorySinkService,
} from '@/gen/ai/protomolt/proto/repo/sink/v1/repository_sink_pb'
import { isOwnCancellation } from './rpcErrorCore'
import {
  buildCancelRepositorySinkJobRequest,
  buildGetRepositorySinkJobRequest,
  classifySinkRpcError,
  describeSinkJob,
  type ClassifiedSinkError,
  type SinkJobSummary,
} from './saveAsJobCore'

export type SinkClient = Client<typeof RepositorySinkService>

/** Which call produced an entry's latest summary. */
export type SaveAsJobUpdateSource = 'publish' | 'refresh' | 'cancel'

export interface SaveAsJobEntry {
  readonly operationId: string
  readonly accountId: string
  readonly request: PublishRepositorySinkRequest
  summary: SinkJobSummary | null
  updatedBy: SaveAsJobUpdateSource | null
  error: ClassifiedSinkError | null
  submitting: boolean
  reconciling: boolean
  cancelling: boolean
  dialogOpen: boolean
}

export interface SaveAsJobStore {
  readonly items: readonly SaveAsJobEntry[]
  find(operationId: string): SaveAsJobEntry | undefined
  /**
   * Publishes (or re-publishes) the frozen request under its operation ID.
   *
   * @throws SaveAsJobCapacityError when the job is new and no tracked job can be forgotten
   */
  publish(client: SinkClient, request: PublishRepositorySinkRequest, dialogOpen: boolean): Promise<void>
  /** Re-sends a tracked job's frozen request under its operation ID; never commits twice. */
  retry(operationId: string): Promise<void>
  refresh(operationId: string): Promise<void>
  cancel(operationId: string, reason: string): Promise<void>
  setDialogOpen(operationId: string, dialogOpen: boolean): void
  /** Forgets an entry; a call still in flight for it is aborted. */
  dismiss(operationId: string): void
  /** Aborts every call and stops polling. */
  dispose(): void
}

const MAX_RETAINED_SAVE_AS_JOBS = 20
const OPEN_POLL_MS = 500
const CLOSED_POLL_MS = 5000
const MAX_POLL_BACKOFF_MS = 30_000
/**
 * How long a call may go unanswered. Past it the call ends as DeadlineExceeded, which is an
 * outcome still unknown: without a bound, a publish whose reply never came would hold the job
 * in "submitting" for good, with refresh, cancel and the dialog's close all waiting on it.
 */
const PUBLISH_TIMEOUT_MS = 120_000
const LOOKUP_TIMEOUT_MS = 30_000

/**
 * Raised when a new job cannot be tracked: every retained job is still in flight, shown in an
 * open dialog, or has an outcome the server has not confirmed, and forgetting any of them would
 * lose an operation the operator may still need to reconcile.
 */
export class SaveAsJobCapacityError extends Error {
  constructor() {
    super(
      `${MAX_RETAINED_SAVE_AS_JOBS} Save-as jobs are already being tracked and none has a confirmed `
        + 'outcome. Refresh or dismiss one before starting another.',
    )
    this.name = 'SaveAsJobCapacityError'
  }
}

function isActive(summary: SinkJobSummary | null): boolean {
  return summary !== null && (summary.phase === 'queued' || summary.phase === 'running')
}

/** Call ordering and poll backoff for one job; kept beside the entry, not shown. */
interface JobCalls {
  /** Sequence number handed to the most recently started call. */
  started: number
  /** Sequence number of the call whose job summary the entry currently shows. */
  shown: number
  /** Sequence number of the most recently started publish (a retry starts a new attempt). */
  publishStarted: number
  /** Status calls that have failed in a row. */
  failedPolls: number
  /**
   * The error of a publish whose outcome is unknown and has not been confirmed since. It
   * stands until the server describes the job, whatever later calls fail with.
   */
  unknownOutcome: ClassifiedSinkError | null
}

export function createSaveAsJobStore(): SaveAsJobStore {
  const items = reactive<SaveAsJobEntry[]>([])
  const clients = new Map<string, SinkClient>()
  const controllers = new Map<string, AbortController>()
  const timers = new Map<string, ReturnType<typeof setTimeout>>()
  const calls = new Map<string, JobCalls>()
  let disposed = false

  function callsFor(operationId: string): JobCalls {
    let state = calls.get(operationId)
    if (state === undefined) {
      state = { started: 0, shown: 0, publishStarted: 0, failedPolls: 0, unknownOutcome: null }
      calls.set(operationId, state)
    }
    return state
  }

  /** Numbers a call as it starts, so its outcome can be ordered against the others. */
  function beginCall(operationId: string): number {
    const state = callsFor(operationId)
    state.started += 1
    return state.started
  }

  /**
   * Whether the summary from call `sequence` replaces the one shown. Calls overlap, and the
   * order they start in is not the order the server answered them in, so two rules decide:
   * a summary from a later call replaces an earlier call's, and a terminal summary always
   * replaces a non-terminal one, because within one attempt a job that has ended does not
   * start running again. Answers from before the latest publish are dropped: see below.
   */
  function supersedes(entry: SaveAsJobEntry, summary: SinkJobSummary, sequence: number): boolean {
    const state = callsFor(entry.operationId)
    // A retry restarts the job on the server. A status or cancel call that began before it
    // describes the attempt it replaced, whatever that answer says.
    if (sequence < state.publishStarted) return false
    // Within one attempt a job that has ended does not start running again, so a sample
    // taken before it ended never replaces the ending, whenever that sample arrives.
    if (entry.summary !== null && entry.summary.terminal && !summary.terminal) return false
    if (sequence >= state.shown) return true
    return summary.terminal && entry.summary !== null && !entry.summary.terminal
  }

  function find(operationId: string): SaveAsJobEntry | undefined {
    return items.find((item) => item.operationId === operationId)
  }

  function signalFor(operationId: string): AbortSignal {
    let controller = controllers.get(operationId)
    if (controller === undefined) {
      controller = new AbortController()
      if (disposed) controller.abort()
      controllers.set(operationId, controller)
    }
    return controller.signal
  }

  function clearTimer(operationId: string) {
    const timer = timers.get(operationId)
    if (timer !== undefined) {
      clearTimeout(timer)
      timers.delete(operationId)
    }
  }

  function schedulePoll(entry: SaveAsJobEntry) {
    clearTimer(entry.operationId)
    if (disposed || !isActive(entry.summary)) return
    const base = entry.dialogOpen ? OPEN_POLL_MS : CLOSED_POLL_MS
    const delay = Math.min(base * 2 ** callsFor(entry.operationId).failedPolls, MAX_POLL_BACKOFF_MS)
    timers.set(
      entry.operationId,
      setTimeout(() => {
        timers.delete(entry.operationId)
        void refresh(entry.operationId)
      }, delay),
    )
  }

  function apply(
    entry: SaveAsJobEntry,
    summary: SinkJobSummary,
    source: SaveAsJobUpdateSource,
    sequence: number,
  ) {
    if (!supersedes(entry, summary, sequence)) return
    const state = callsFor(entry.operationId)
    state.shown = Math.max(state.shown, sequence)
    entry.summary = summary
    entry.updatedBy = source
    // The server has answered for this job, so an earlier call's failure no longer describes it.
    entry.error = null
    callsFor(entry.operationId).failedPolls = 0
    callsFor(entry.operationId).unknownOutcome = null
    if (isActive(summary)) {
      schedulePoll(entry)
    } else {
      clearTimer(entry.operationId)
    }
  }

  /** True when a call that started after `sequence` has already shown the job's state. */
  function answeredSince(operationId: string, sequence: number): boolean {
    const state = callsFor(operationId)
    return sequence < state.shown || sequence < state.publishStarted
  }

  /** Records that the publish call itself failed, unless a later call has shown the job. */
  function failPublish(entry: SaveAsJobEntry, error: ClassifiedSinkError, sequence: number) {
    if (answeredSince(entry.operationId, sequence)) return
    const state = callsFor(entry.operationId)
    if (error.canReconcile) {
      state.unknownOutcome = error
      entry.error = error
      return
    }
    if (state.unknownOutcome !== null) {
      // This attempt was refused outright, but an earlier one may have committed and nobody
      // has confirmed otherwise. The refusal is noted; the outcome stays unknown.
      entry.error = {
        ...state.unknownOutcome,
        message: `${state.unknownOutcome.message} A later retry was refused (${error.codeLabel}): ${error.message}`,
      }
      return
    }
    entry.error = error
  }

  /**
   * Records a failed status or cancel call. It says only that the lookup failed: while the
   * publish is still in flight, or its outcome is unknown, the lookup failing (even with "not
   * found", since admission may be pending) settles nothing, so the publish's own state stands
   * and the same request can still be retried.
   */
  function failLookup(entry: SaveAsJobEntry, error: ClassifiedSinkError, sequence: number) {
    if (answeredSince(entry.operationId, sequence)) return
    if (entry.submitting) return
    if (entry.summary === null && entry.error?.kind === 'unsettled') return
    entry.error = error
  }

  /** A job may be forgotten only once the server has confirmed how it ended. */
  function canEvict(item: SaveAsJobEntry): boolean {
    return (
      !item.dialogOpen
      && !item.submitting
      && !item.reconciling
      && !item.cancelling
      && item.summary !== null
      && item.summary.terminal
    )
  }

  function track(client: SinkClient, request: PublishRepositorySinkRequest, dialogOpen: boolean): SaveAsJobEntry {
    const existing = find(request.operationId)
    if (existing !== undefined) {
      clients.set(request.operationId, client)
      existing.dialogOpen = dialogOpen
      return existing
    }
    if (items.length >= MAX_RETAINED_SAVE_AS_JOBS) {
      const evictable = items.find(canEvict)
      if (evictable === undefined) throw new SaveAsJobCapacityError()
      dismiss(evictable.operationId)
    }
    clients.set(request.operationId, client)
    items.push({
      operationId: request.operationId,
      accountId: request.destination?.address?.accountId ?? '',
      request,
      summary: null,
      updatedBy: null,
      error: null,
      submitting: false,
      reconciling: false,
      cancelling: false,
      dialogOpen,
    })
    // Return the reactive proxy so later writes notify watchers.
    return items[items.length - 1]
  }

  async function publish(client: SinkClient, request: PublishRepositorySinkRequest, dialogOpen: boolean) {
    const entry = track(client, request, dialogOpen)
    if (entry.submitting) return
    clearTimer(entry.operationId)
    const signal = signalFor(entry.operationId)
    const sequence = beginCall(entry.operationId)
    callsFor(entry.operationId).publishStarted = sequence
    entry.submitting = true
    entry.error = null
    // Publishing again starts a new attempt on the server. What was known about the last one
    // (even "failed") no longer describes the job, so until this call is answered the job's
    // state is unknown: reconcilable, retryable and never evicted as settled.
    entry.summary = null
    entry.updatedBy = null
    try {
      const response = await client.publishRepositorySink(entry.request, {
        signal,
        timeoutMs: PUBLISH_TIMEOUT_MS,
      })
      if (response.job === undefined) {
        failPublish(
          entry,
          {
            kind: 'unsettled',
            codeLabel: 'EmptyJobResponse',
            message: 'Server returned an empty job envelope; refresh status to inspect recorded state.',
            canReconcile: true,
          },
          sequence,
        )
        return
      }
      apply(entry, describeSinkJob(response.job), 'publish', sequence)
    } catch (error) {
      if (!isOwnCancellation(error, signal)) {
        failPublish(entry, classifySinkRpcError(error), sequence)
      }
    } finally {
      entry.submitting = false
    }
  }

  async function retry(operationId: string) {
    const entry = find(operationId)
    const client = clients.get(operationId)
    if (entry === undefined || client === undefined) return
    await publish(client, entry.request, entry.dialogOpen)
  }

  async function refresh(operationId: string) {
    const entry = find(operationId)
    const client = clients.get(operationId)
    // While a publish is in flight a lookup could answer for the job as it was before or
    // after that publish reached the server, and nothing here could tell which. The publish
    // answers for itself, and polling resumes from its answer.
    if (entry === undefined || client === undefined || entry.reconciling || entry.submitting) return
    const signal = signalFor(operationId)
    const sequence = beginCall(operationId)
    entry.reconciling = true
    try {
      const response = await client.getRepositorySinkJob(
        buildGetRepositorySinkJobRequest(entry.accountId, operationId),
        { signal, timeoutMs: LOOKUP_TIMEOUT_MS },
      )
      if (response.job !== undefined) {
        apply(entry, describeSinkJob(response.job), 'refresh', sequence)
      }
    } catch (error) {
      if (!isOwnCancellation(error, signal)) {
        callsFor(operationId).failedPolls += 1
        failLookup(entry, classifySinkRpcError(error), sequence)
      }
    } finally {
      entry.reconciling = false
      // The timer that started this call is spent. A job last seen queued or running is still
      // watched, with a longer wait after each status call that failed.
      if (find(operationId) === entry && isActive(entry.summary) && !timers.has(operationId)) {
        schedulePoll(entry)
      }
    }
  }

  async function cancel(operationId: string, reason: string) {
    const entry = find(operationId)
    const client = clients.get(operationId)
    // Not while a publish is in flight, for the reason refresh gives.
    if (entry === undefined || client === undefined || entry.cancelling || entry.submitting) return
    clearTimer(operationId)
    const signal = signalFor(operationId)
    const sequence = beginCall(operationId)
    entry.cancelling = true
    try {
      const response = await client.cancelRepositorySinkJob(
        buildCancelRepositorySinkJobRequest(entry.accountId, operationId, reason),
        { signal, timeoutMs: LOOKUP_TIMEOUT_MS },
      )
      if (response.job !== undefined) {
        apply(entry, describeSinkJob(response.job), 'cancel', sequence)
      }
    } catch (error) {
      if (!isOwnCancellation(error, signal)) {
        failLookup(entry, classifySinkRpcError(error), sequence)
      }
    } finally {
      entry.cancelling = false
      // A cancel that failed leaves the job running, so it is still watched.
      if (find(operationId) === entry && isActive(entry.summary) && !timers.has(operationId)) {
        schedulePoll(entry)
      }
    }
  }

  function setDialogOpen(operationId: string, dialogOpen: boolean) {
    const entry = find(operationId)
    if (entry === undefined) return
    entry.dialogOpen = dialogOpen
    if (isActive(entry.summary)) schedulePoll(entry)
  }

  function dismiss(operationId: string) {
    clearTimer(operationId)
    controllers.get(operationId)?.abort()
    controllers.delete(operationId)
    clients.delete(operationId)
    calls.delete(operationId)
    const index = items.findIndex((item) => item.operationId === operationId)
    if (index >= 0) items.splice(index, 1)
  }

  function dispose() {
    disposed = true
    for (const timer of timers.values()) clearTimeout(timer)
    timers.clear()
    for (const controller of controllers.values()) controller.abort()
  }

  return { items, find, publish, retry, refresh, cancel, setDialogOpen, dismiss, dispose }
}

export const SAVE_AS_JOBS: InjectionKey<SaveAsJobStore> = Symbol('save-as-jobs')
const SAVE_AS_JOB_BANNER_HOSTED: InjectionKey<boolean> = Symbol('save-as-job-banner-hosted')

/**
 * Returns the host's job store, or a store local to the calling component (disposed when it
 * unmounts) in standalone component tests.
 */
export function useSaveAsJobs(): SaveAsJobStore {
  const hosted = inject(SAVE_AS_JOBS, null)
  if (hosted !== null) return hosted
  const local = createSaveAsJobStore()
  onBeforeUnmount(() => local.dispose())
  return local
}

/**
 * Installs a job store for this subtree (if none is provided above) and claims the banner for the
 * outermost host so nested views never show it twice.
 */
export function useSaveAsJobHost(): { jobs: SaveAsJobStore; showBanner: Readonly<Ref<boolean>> } {
  const existing = inject(SAVE_AS_JOBS, null)
  const jobs = existing ?? createSaveAsJobStore()
  if (existing === null) {
    provide(SAVE_AS_JOBS, jobs)
    onBeforeUnmount(() => jobs.dispose())
  }
  const hostedAbove = inject(SAVE_AS_JOB_BANNER_HOSTED, false)
  const ownsBanner = !hostedAbove
  if (ownsBanner) {
    provide(SAVE_AS_JOB_BANNER_HOSTED, true)
  }
  const showBanner = computed(() => ownsBanner && jobs.items.some((item) => !item.dialogOpen))
  return { jobs, showBanner }
}
