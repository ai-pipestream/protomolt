/**
 * Pure Save-as job lifecycle helpers: Get/Cancel request builders, the job summary view-model
 * and RPC error classification for `RepositorySinkService` calls.
 */
import { create } from '@bufbuild/protobuf'
import { Code, ConnectError } from '@connectrpc/connect'
import { type EntryAddress, type RenditionDescriptor } from '@/gen/ai/protomolt/proto/repo/archive/v1/entry_pb'
import {
  CancelRepositorySinkJobRequestSchema,
  GetRepositorySinkJobRequestSchema,
  RepositorySinkJobState,
  type RenditionProvenance,
  type RepositorySinkFailure,
  type RepositorySinkJob,
} from '@/gen/ai/protomolt/proto/repo/sink/v1/repository_sink_pb'
import { instant } from './formatCore'
import { sanitizeDisplayMetadata } from './formatIcons'

export function buildGetRepositorySinkJobRequest(accountId: string, operationId: string) {
  return create(GetRepositorySinkJobRequestSchema, {
    accountId,
    operationId,
  })
}

export function buildCancelRepositorySinkJobRequest(accountId: string, operationId: string, reason = '') {
  return create(CancelRepositorySinkJobRequestSchema, {
    accountId,
    operationId,
    reason: reason.trim(),
  })
}

export type SinkJobPhase =
  | 'queued'
  | 'running'
  | 'succeeded'
  | 'failed'
  | 'cancelled'
  | 'unresolved'

export interface SinkJobSummary {
  operationId: string
  phase: SinkJobPhase
  stateLabel: string
  terminal: boolean
  canCancel: boolean
  canReconcile: boolean
  canRetrySameOperation: boolean
  isConflict: boolean
  replayed: boolean
  frozenSourceRevision: bigint
  destinationAddress: EntryAddress | null
  destinationRendition: RenditionDescriptor | null
  destinationVersion: bigint
  bytesRendered: bigint
  bytesCommitted: bigint
  failureMessage: string | null
  failureReasonCode: string | null
  provenance: RenditionProvenance | null
  updatedAt: string
}

/**
 * What the page that opened Save-as does when its job commits. It follows the commit (opens
 * the new version, or reloads the one shown) only while its dialog is still open on the entry
 * that was written. Once the dialog is closed the operator has moved on, the banner reports the
 * job, and the page they are reading is not replaced.
 */
export function commitFollowUp(input: {
  dialogOpen: boolean
  sameEntry: boolean
  viewedVersion: bigint | null
  committedVersion: bigint
}): 'open-version' | 'reload' | 'none' {
  if (!input.dialogOpen || !input.sameEntry) return 'none'
  if (
    input.viewedVersion !== null
    && input.committedVersion > 0n
    && input.viewedVersion !== input.committedVersion
  ) {
    return 'open-version'
  }
  return 'reload'
}

/**
 * Whether a v2 document is the one behind an archive entry: archive entries are documents
 * under the graph `archive:<account>`, addressed by archive and entry ID.
 */
export function documentBacksEntry(
  document: { accountId: string; graphId: string; graphAddressId: string; docId: string },
  entry: { accountId: string; archive: string; entryId: string } | null | undefined,
): boolean {
  return (
    entry !== undefined
    && entry !== null
    && document.accountId === entry.accountId
    && document.graphId === `archive:${entry.accountId}`
    && document.graphAddressId === entry.archive
    && document.docId === entry.entryId
  )
}

/** True when a job's phase has just become succeeded, whichever call reported it. */
export function enteredSucceeded(
  previous: SinkJobPhase | undefined,
  next: SinkJobPhase | undefined,
): boolean {
  return next === 'succeeded' && previous !== 'succeeded'
}

export function describeSinkJob(job: RepositorySinkJob): SinkJobSummary {
  let phase: SinkJobPhase
  let stateLabel: string
  switch (job.state) {
    case RepositorySinkJobState.QUEUED:
      phase = 'queued'
      stateLabel = 'Queued'
      break
    case RepositorySinkJobState.RUNNING:
      phase = 'running'
      stateLabel = 'Running'
      break
    case RepositorySinkJobState.SUCCEEDED:
      phase = 'succeeded'
      stateLabel = job.replayed ? 'Succeeded (reconciled)' : 'Succeeded'
      break
    case RepositorySinkJobState.CANCELLED:
      phase = 'cancelled'
      stateLabel = 'Cancelled'
      break
    case RepositorySinkJobState.UNRESOLVED:
      phase = 'unresolved'
      stateLabel = 'Outcome unresolved'
      break
    case RepositorySinkJobState.FAILED:
      phase = 'failed'
      stateLabel = 'Failed'
      break
    default:
      // UNSPECIFIED, or a state this console does not know. What the job is doing is unknown,
      // so it is not treated as ended: it stays reconcilable and is never evicted as settled.
      phase = 'unresolved'
      stateLabel = 'State not recognized'
      break
  }

  const failure: RepositorySinkFailure | undefined = job.failure
  const isConflict = Boolean(failure?.conflict) || failure?.grpcCode === Code.Aborted
  const retryableFailure = phase === 'failed' && Boolean(failure?.retryable) && !isConflict

  return {
    operationId: job.operationId,
    phase,
    stateLabel,
    terminal: phase === 'succeeded' || phase === 'failed' || phase === 'cancelled',
    canCancel: phase === 'queued' || phase === 'running',
    canReconcile: phase === 'queued' || phase === 'running' || phase === 'unresolved',
    // Re-publishing an unresolved operation makes the server reconcile its commit outcome, and
    // a retryable failure is requeued; both are worth sending the same request again for.
    canRetrySameOperation: phase === 'unresolved' || retryableFailure,
    isConflict,
    replayed: job.replayed,
    frozenSourceRevision: job.frozenSourceRevision,
    destinationAddress: job.destinationAddress ?? null,
    destinationRendition: job.destinationRendition ?? null,
    destinationVersion: job.destinationVersion,
    bytesRendered: job.bytesRendered,
    bytesCommitted: job.bytesCommitted,
    failureMessage: failure ? sanitizeDisplayMetadata(failure.message, 512) : null,
    failureReasonCode: failure?.reasonCode ? sanitizeDisplayMetadata(failure.reasonCode, 96) : null,
    provenance: job.provenance ?? null,
    updatedAt: instant(job.updatedAt) ?? '',
  }
}

export interface ClassifiedSinkError {
  kind: 'unsettled' | 'conflict' | 'permission-denied' | 'unauthenticated' | 'rejected'
  codeLabel: string
  message: string
  canReconcile: boolean
}

/**
 * Classify an RPC error thrown during `PublishRepositorySink` so lost responses
 * (`Unavailable`, `DeadlineExceeded`, `Unknown`, network failures) NEVER claim
 * success and instead guide reconciliation via `GetRepositorySinkJob` or retry
 * with the same `operationId`.
 */
export function classifySinkRpcError(error: unknown): ClassifiedSinkError {
  if (error instanceof ConnectError) {
    const cleanMsg = sanitizeDisplayMetadata(error.rawMessage || error.message, 512)
    if (
      error.code === Code.Unavailable
      || error.code === Code.DeadlineExceeded
      || error.code === Code.Unknown
      // The sink answers INTERNAL when a commit may or may not have gone through.
      || error.code === Code.Internal
      // A cancellation that reaches us as an error did not come from this console (its own
      // aborts are recognized before classification), and it can arrive after admission.
      || error.code === Code.Canceled
    ) {
      return {
        kind: 'unsettled',
        codeLabel: Code[error.code] ?? 'Unknown',
        message: cleanMsg || 'The connection dropped before the sink job outcome was confirmed.',
        canReconcile: true,
      }
    }
    // Failed preconditions may be schema, media or capability rejections;
    // ALREADY_EXISTS can mean a reused operation ID with different intent.
    // Neither establishes a version conflict that reloading could resolve.
    if (error.code === Code.Aborted) {
      return {
        kind: 'conflict',
        codeLabel: Code[error.code] ?? 'Conflict',
        message: cleanMsg || 'Source or destination version changed; reload and inspect before retrying.',
        canReconcile: false,
      }
    }
    if (error.code === Code.PermissionDenied) {
      return {
        kind: 'permission-denied',
        codeLabel: 'PermissionDenied',
        message: cleanMsg || 'Your session is not authorized to write to this destination.',
        canReconcile: false,
      }
    }
    if (error.code === Code.Unauthenticated) {
      return {
        kind: 'unauthenticated',
        codeLabel: 'Unauthenticated',
        message: cleanMsg || 'Sign in to publish derived renditions.',
        canReconcile: false,
      }
    }
    return {
      kind: 'rejected',
      codeLabel: Code[error.code] ?? 'Error',
      message: cleanMsg || 'Save-as request was rejected.',
      canReconcile: false,
    }
  }

  const raw = error instanceof Error ? error.message : String(error)
  return {
    kind: 'unsettled',
    codeLabel: 'NetworkError',
    message: sanitizeDisplayMetadata(raw, 512) || 'Network error before sink outcome was received.',
    canReconcile: true,
  }
}
