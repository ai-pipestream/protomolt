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
    accountId: accountId.trim(),
    operationId: operationId.trim(),
  })
}

export function buildCancelRepositorySinkJobRequest(accountId: string, operationId: string, reason = '') {
  return create(CancelRepositorySinkJobRequestSchema, {
    accountId: accountId.trim(),
    operationId: operationId.trim(),
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
    default:
      phase = 'failed'
      stateLabel = 'Failed'
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
    ) {
      return {
        kind: 'unsettled',
        codeLabel: Code[error.code] ?? 'Unknown',
        message: cleanMsg || 'The connection dropped before the sink job outcome was confirmed.',
        canReconcile: true,
      }
    }
    if (
      error.code === Code.Aborted
      || error.code === Code.FailedPrecondition
      || error.code === Code.AlreadyExists
    ) {
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
