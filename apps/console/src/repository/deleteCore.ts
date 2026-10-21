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
import { RevisionConflictSchema, type RevisionConflict } from '@/gen/ai/protomolt/proto/repo/v2/errors_pb'
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

/** How a delete ended, when the service answered. */
export type DeleteAnswer =
  | { kind: 'deleted'; response: DeleteDocumentResponse }
  /** ABORTED: the expected revision is no longer current. */
  | { kind: 'stale'; conflict: RevisionConflict | null; failure: RpcFailure }
  /** PERMISSION_DENIED: the caller may not write this document. */
  | { kind: 'denied'; failure: RpcFailure }

/**
 * Sends the delete once. A stale revision and a refusal are answers the
 * screen shows; every other status is thrown as a failed call. Nothing here
 * retries.
 */
export async function deleteDocument(
  client: Client<typeof DocumentService>,
  request: DeleteDocumentRequest,
  signal: AbortSignal,
): Promise<DeleteAnswer> {
  try {
    return { kind: 'deleted', response: await client.deleteDocument(request, { signal }) }
  } catch (error) {
    if (!(error instanceof ConnectError)) throw error
    if (error.code === Code.Aborted) {
      const [conflict] = error.findDetails(RevisionConflictSchema)
      return { kind: 'stale', conflict: conflict ?? null, failure: rpcFailure(error) }
    }
    if (error.code === Code.PermissionDenied) return { kind: 'denied', failure: rpcFailure(error) }
    throw error
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
