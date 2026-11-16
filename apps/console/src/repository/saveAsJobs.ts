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
  /** Publishes (or re-publishes) the frozen request under its operation ID. */
  publish(client: SinkClient, request: PublishRepositorySinkRequest, dialogOpen: boolean): Promise<void>
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

function isActive(summary: SinkJobSummary | null): boolean {
  return summary !== null && (summary.phase === 'queued' || summary.phase === 'running')
}

export function createSaveAsJobStore(): SaveAsJobStore {
  const items = reactive<SaveAsJobEntry[]>([])
  const clients = new Map<string, SinkClient>()
  const controllers = new Map<string, AbortController>()
  const timers = new Map<string, ReturnType<typeof setTimeout>>()
  let disposed = false

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
    const delay = entry.dialogOpen ? OPEN_POLL_MS : CLOSED_POLL_MS
    timers.set(
      entry.operationId,
      setTimeout(() => {
        timers.delete(entry.operationId)
        void refresh(entry.operationId)
      }, delay),
    )
  }

  function apply(entry: SaveAsJobEntry, summary: SinkJobSummary, source: SaveAsJobUpdateSource) {
    entry.summary = summary
    entry.updatedBy = source
    if (summary.phase === 'succeeded' || isActive(summary)) {
      entry.error = null
    }
    if (isActive(summary)) {
      schedulePoll(entry)
    } else {
      clearTimer(entry.operationId)
    }
  }

  function track(client: SinkClient, request: PublishRepositorySinkRequest, dialogOpen: boolean): SaveAsJobEntry {
    const existing = find(request.operationId)
    clients.set(request.operationId, client)
    if (existing !== undefined) {
      existing.dialogOpen = dialogOpen
      return existing
    }
    if (items.length >= MAX_RETAINED_SAVE_AS_JOBS) {
      const settledIndex = items.findIndex((item) => !item.dialogOpen && !isActive(item.summary))
      const evicted = items[settledIndex >= 0 ? settledIndex : 0]
      dismiss(evicted.operationId)
    }
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
    entry.submitting = true
    entry.error = null
    try {
      const response = await client.publishRepositorySink(entry.request, { signal })
      if (response.job === undefined) {
        entry.error = {
          kind: 'unsettled',
          codeLabel: 'EmptyJobResponse',
          message: 'Server returned an empty job envelope; refresh status to inspect recorded state.',
          canReconcile: true,
        }
        return
      }
      apply(entry, describeSinkJob(response.job), 'publish')
    } catch (error) {
      if (!isOwnCancellation(error, signal)) {
        entry.error = classifySinkRpcError(error)
      }
    } finally {
      entry.submitting = false
    }
  }

  async function refresh(operationId: string) {
    const entry = find(operationId)
    const client = clients.get(operationId)
    if (entry === undefined || client === undefined || entry.reconciling) return
    const signal = signalFor(operationId)
    entry.reconciling = true
    try {
      const response = await client.getRepositorySinkJob(
        buildGetRepositorySinkJobRequest(entry.accountId, operationId),
        { signal },
      )
      if (response.job !== undefined) {
        apply(entry, describeSinkJob(response.job), 'refresh')
      }
    } catch (error) {
      if (!isOwnCancellation(error, signal)) {
        entry.error = classifySinkRpcError(error)
      }
    } finally {
      entry.reconciling = false
    }
  }

  async function cancel(operationId: string, reason: string) {
    const entry = find(operationId)
    const client = clients.get(operationId)
    if (entry === undefined || client === undefined || entry.cancelling) return
    clearTimer(operationId)
    const signal = signalFor(operationId)
    entry.cancelling = true
    try {
      const response = await client.cancelRepositorySinkJob(
        buildCancelRepositorySinkJobRequest(entry.accountId, operationId, reason),
        { signal },
      )
      if (response.job !== undefined) {
        apply(entry, describeSinkJob(response.job), 'cancel')
      }
    } catch (error) {
      if (!isOwnCancellation(error, signal)) {
        entry.error = classifySinkRpcError(error)
      }
    } finally {
      entry.cancelling = false
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
    const index = items.findIndex((item) => item.operationId === operationId)
    if (index >= 0) items.splice(index, 1)
  }

  function dispose() {
    disposed = true
    for (const timer of timers.values()) clearTimeout(timer)
    timers.clear()
    for (const controller of controllers.values()) controller.abort()
  }

  return { items, find, publish, refresh, cancel, setDialogOpen, dismiss, dispose }
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
