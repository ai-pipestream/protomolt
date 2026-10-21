/**
 * Tombstone delete through DeleteDocument. A delete is itself an accession:
 * it names the exact revision it expects to replace and carries a fresh
 * operation ID, so a retry after a lost answer is the same operation and a
 * concurrent save is a conflict, never an overwrite. Bytes stay; purge is not
 * offered.
 */
import { create } from '@bufbuild/protobuf'
import { Code, ConnectError, type Client } from '@connectrpc/connect'
import type { NodeAddress } from '@/gen/ai/protomolt/proto/repo/v1/address_pb'
import {
  DeleteDocumentRequestSchema,
  type DeleteDocumentRequest,
  type DeleteDocumentResponse,
  type DocumentService,
} from '@/gen/ai/protomolt/proto/repo/v2/document_service_pb'
import {
  OperationReuseSchema,
  RevisionConflictSchema,
  type OperationReuse,
  type RevisionConflict,
} from '@/gen/ai/protomolt/proto/repo/v2/errors_pb'
import { ExpectedRevisionSchema } from '@/gen/ai/protomolt/proto/repo/v2/manifest_pb'
import { rpcFailure, type RpcFailure } from './rpcErrorCore'

/** The longest reason the contract accepts. */
export const MAX_REASON_LENGTH = 1024

/** Refused before a request is sent; `field` names the input. */
export class DeleteInputError extends Error {
  constructor(
    readonly field: string,
    message: string,
  ) {
    super(`${field}: ${message}`)
    this.name = 'DeleteInputError'
  }
}

/** The delete request for the revision the screen shows. */
export function planDelete(address: NodeAddress, currentRevision: bigint, operationId: string, reason: string): DeleteDocumentRequest {
  if (currentRevision <= 0n) throw new DeleteInputError('Revision', 'a delete needs the exact current revision')
  if (operationId.trim() === '') throw new DeleteInputError('Operation ID', 'is blank')
  if (reason.length > MAX_REASON_LENGTH) {
    throw new DeleteInputError('Reason', `is ${reason.length} characters; the limit is ${MAX_REASON_LENGTH}`)
  }
  return create(DeleteDocumentRequestSchema, {
    operationId,
    address,
    expected: create(ExpectedRevisionSchema, { expected: { case: 'revision', value: currentRevision } }),
    reason,
  })
}

/** How one send of a delete ended. */
export type DeleteAnswer =
  | { kind: 'deleted'; response: DeleteDocumentResponse }
  /** ABORTED: the expected revision is no longer current. */
  | { kind: 'stale'; conflict: RevisionConflict | null; failure: RpcFailure }
  /** PERMISSION_DENIED: the caller may not write this document. */
  | { kind: 'denied'; failure: RpcFailure }
  /** ALREADY_EXISTS: this operation ID already recorded a different intent. */
  | { kind: 'reused'; reuse: OperationReuse | null; failure: RpcFailure }
  /** Any other definite answer (NOT_FOUND, INVALID_ARGUMENT, DATA_LOSS...): nothing was deleted. */
  | { kind: 'refused'; failure: RpcFailure }
  /**
   * No settled answer (a lost connection or a transient status): the delete
   * may or may not have been recorded. Sending the same request again is safe.
   */
  | { kind: 'unsettled'; failure: RpcFailure }

// Statuses after which the delete may still have been recorded.
const UNSETTLED = new Set<Code>([Code.Unknown, Code.DeadlineExceeded, Code.Internal, Code.Unavailable, Code.Canceled])

/**
 * Sends the delete once and sorts the answer. Nothing here retries; the
 * caller resends the same request only when the answer is unsettled.
 */
export async function deleteDocument(
  client: Client<typeof DocumentService>,
  request: DeleteDocumentRequest,
  signal: AbortSignal,
): Promise<DeleteAnswer> {
  try {
    return { kind: 'deleted', response: await client.deleteDocument(request, { signal }) }
  } catch (error) {
    const connect = ConnectError.from(error)
    const failure = rpcFailure(error)
    // A client-side failure says nothing about what the service recorded.
    if (!(error instanceof ConnectError) || UNSETTLED.has(connect.code)) return { kind: 'unsettled', failure }
    if (connect.code === Code.Aborted) {
      const [conflict] = connect.findDetails(RevisionConflictSchema)
      return { kind: 'stale', conflict: conflict ?? null, failure }
    }
    if (connect.code === Code.PermissionDenied) return { kind: 'denied', failure }
    if (connect.code === Code.AlreadyExists) {
      const [reuse] = connect.findDetails(OperationReuseSchema)
      return { kind: 'reused', reuse: reuse ?? null, failure }
    }
    return { kind: 'refused', failure }
  }
}

/** The stale-revision sentence: what was expected and what is current, as far as the answer says. */
export function staleText(answer: Extract<DeleteAnswer, { kind: 'stale' }>, expected: bigint): string {
  const c = answer.conflict
  if (c === null) {
    return `Revision ${expected} is no longer current. The service did not say which revision is (${answer.failure.code}: ${answer.failure.message}).`
  }
  const current = c.currentRevision === 0n ? 'none' : `${c.currentRevision}${c.currentIsTombstone ? ', a tombstone' : ''}`
  return `Revision ${expected} is no longer current; the current revision is ${current}.`
}
