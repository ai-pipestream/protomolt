/** Archive deletion uses the mutation receipt contract, never the document-delete shortcut. */
import { clone, create, toBinary } from '@bufbuild/protobuf'
import { Code, ConnectError, type Client } from '@connectrpc/connect'
import { ArchiveMutationKind, ArchiveMutationRequestSchema, ArchiveMutationState, type ArchiveMutationReceipt, type ArchiveMutationRequest } from '@/gen/ai/protomolt/proto/repo/archive/v1/archive_mutation_pb'
import { ArchiveMutationService } from '@/gen/ai/protomolt/proto/repo/archive/v1/archive_mutation_service_pb'
import { DeleteEntryRequestSchema } from '@/gen/ai/protomolt/proto/repo/archive/v1/archive_service_pb'
import { EntryAddressSchema, type EntryAddress } from '@/gen/ai/protomolt/proto/repo/archive/v1/entry_pb'
import { rpcFailure, type RpcFailure } from './rpcErrorCore'
import { Sha256 } from './sha256'

export function planArchiveDelete(address: EntryAddress, version: bigint, operationId: string): ArchiveMutationRequest {
  if (version <= 0n) throw new Error('Deleting an entry requires its observed positive version.')
  if (!operationId.trim()) throw new Error('Deleting an entry requires an operation ID.')
  for (const value of [address.accountId, address.archive, address.entryId]) {
    if (!value.trim()) throw new Error('Deleting an entry requires a complete archive address.')
  }
  return create(ArchiveMutationRequestSchema, {
    operationId,
    mutation: { case: 'deleteEntry', value: create(DeleteEntryRequestSchema, {
      address: clone(EntryAddressSchema, address), expectedVersion: version,
    }) },
  })
}

/** Existing receipts identify the address-only logical delete, not its version guard. */
export function archiveDeleteDigest(request: ArchiveMutationRequest): string {
  const command = clone(ArchiveMutationRequestSchema, request)
  command.operationId = ''
  if (command.mutation.case === 'deleteEntry') command.mutation.value.expectedVersion = undefined
  return new Sha256().update(toBinary(ArchiveMutationRequestSchema, command)).hex()
}

/** A missing or mismatched receipt never becomes successful deletion. */
export function requireArchiveDeleteReceipt(request: ArchiveMutationRequest, receipt: ArchiveMutationReceipt | undefined): ArchiveMutationReceipt {
  const address = request.mutation.case === 'deleteEntry' ? request.mutation.value.address : undefined
  if (!receipt || !address || receipt.operationId !== request.operationId
      || receipt.address?.accountId !== address.accountId || receipt.address.archive !== address.archive
      || receipt.address.entryId !== address.entryId || receipt.kind !== ArchiveMutationKind.DELETE_ENTRY
      || receipt.commandSha256 !== archiveDeleteDigest(request)) {
    throw new Error('ArchiveMutation returned a missing or mismatched delete receipt; the outcome remains unknown.')
  }
  if (receipt.state !== ArchiveMutationState.COMPLETED || !receipt.entryDeleted) {
    throw new Error('ArchiveMutation has not confirmed this entry deletion; retain the operation ID and check again.')
  }
  return receipt
}

export type ArchiveDeleteAnswer =
  | { kind: 'deleted'; receipt: ArchiveMutationReceipt }
  | { kind: 'stale' | 'refused' | 'unsettled'; failure: RpcFailure }
const UNSETTLED = new Set([Code.Unknown, Code.DeadlineExceeded, Code.Internal, Code.Unavailable, Code.Canceled])

export async function deleteArchiveEntry(client: Client<typeof ArchiveMutationService>, request: ArchiveMutationRequest, signal: AbortSignal): Promise<ArchiveDeleteAnswer> {
  try {
    const response = await client.archiveMutation(request, { signal })
    return { kind: 'deleted', receipt: requireArchiveDeleteReceipt(request, response.receipt) }
  } catch (error) {
    const failure = rpcFailure(error)
    if (!(error instanceof ConnectError) || UNSETTLED.has(error.code)) return { kind: 'unsettled', failure }
    return { kind: error.code === Code.Aborted ? 'stale' : 'refused', failure }
  }
}

export type ArchiveDeleteLookup =
  | { kind: 'deleted'; receipt: ArchiveMutationReceipt }
  | { kind: 'unknown' }
  | { kind: 'unavailable'; failure: RpcFailure }

/** NOT_FOUND is not evidence that a request still in flight cannot commit. */
export async function lookupArchiveDelete(client: Client<typeof ArchiveMutationService>, request: ArchiveMutationRequest, signal: AbortSignal): Promise<ArchiveDeleteLookup> {
  if (request.mutation.case !== 'deleteEntry' || !request.mutation.value.address) throw new Error('Not an entry delete')
  try {
    const response = await client.getArchiveMutation({ accountId: request.mutation.value.address.accountId, operationId: request.operationId }, { signal })
    return { kind: 'deleted', receipt: requireArchiveDeleteReceipt(request, response.receipt) }
  } catch (error) {
    if (error instanceof ConnectError && error.code === Code.NotFound) return { kind: 'unknown' }
    return { kind: 'unavailable', failure: rpcFailure(error) }
  }
}
