/**
 * Lookups by operation ID: GetOperationOutcome answers committed (an accession,
 * an unchanged match, or a recorded rejection) or no outcome yet, and an
 * UNAVAILABLE status means the lookup itself could not answer. The three are
 * never merged: "no outcome" does not mean the save failed, and "unavailable"
 * does not mean "no outcome".
 */
import { create } from '@bufbuild/protobuf'
import { Code, ConnectError, type Client } from '@connectrpc/connect'
import {
  GetOperationOutcomeRequestSchema,
  type DocumentService,
} from '@/gen/ai/protomolt/proto/repo/v2/document_service_pb'
import type { SaveRejection } from '@/gen/ai/protomolt/proto/repo/v2/errors_pb'
import type { Accession } from '@/gen/ai/protomolt/proto/repo/v2/manifest_pb'
import { instant } from './formatCore'
import { rpcFailure, type RpcFailure } from './rpcErrorCore'

export type OutcomeAnswer =
  | { state: 'committed'; accession: Accession; recordedAt: string | null; unchanged?: boolean }
  | { state: 'rejected'; rejection: SaveRejection; recordedAt: string | null }
  | { state: 'none' }
  | { state: 'unavailable'; failure: RpcFailure }

/** Refused before a request is sent; `field` names the blank input. */
export class OutcomeInputError extends Error {
  constructor(readonly field: string) {
    super(`${field} is blank.`)
    this.name = 'OutcomeInputError'
  }
}

/**
 * Asks for the outcome of one operation. Every status other than UNAVAILABLE
 * (for example PERMISSION_DENIED for an account the caller is not bound to)
 * is thrown, as a failed call rather than an answer.
 */
export async function lookupOutcome(
  client: Client<typeof DocumentService>,
  accountId: string,
  operationId: string,
  signal: AbortSignal,
): Promise<OutcomeAnswer> {
  if (accountId.trim() === '') throw new OutcomeInputError('Account ID')
  if (operationId.trim() === '') throw new OutcomeInputError('Operation ID')
  let response
  try {
    response = await client.getOperationOutcome(
      create(GetOperationOutcomeRequestSchema, { accountId, operationId }),
      { signal },
    )
  } catch (error) {
    if (ConnectError.from(error).code === Code.Unavailable) return { state: 'unavailable', failure: rpcFailure(error) }
    throw error
  }
  const state = response.state
  switch (state.case) {
    case 'none':
      return { state: 'none' }
    case 'committed': {
      const recordedAt = instant(state.value.recordedAt)
      const result = state.value.result
      if (result.case === 'accession') return { state: 'committed', accession: result.value, recordedAt, unchanged: false }
      if (result.case === 'unchanged') return { state: 'committed', accession: result.value, recordedAt, unchanged: true }
      if (result.case === 'rejection') return { state: 'rejected', rejection: result.value, recordedAt }
      throw new Error('GetOperationOutcome recorded an outcome with neither an accession, an unchanged match, nor a rejection.')
    }
    case undefined:
      throw new Error('GetOperationOutcome answered with neither committed nor none.')
  }
}
