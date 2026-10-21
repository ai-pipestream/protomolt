import { create } from '@bufbuild/protobuf'
import { Code, ConnectError } from '@connectrpc/connect'
import { describe, expect, it } from 'vitest'
import {
  DocumentService,
  GetOperationOutcomeResponseSchema,
  NoRecordedOutcomeSchema,
  RecordedOutcomeSchema,
  type GetOperationOutcomeRequest,
} from '@/gen/ai/protomolt/proto/repo/v2/document_service_pb'
import { RejectionReason, SaveRejectionSchema } from '@/gen/ai/protomolt/proto/repo/v2/errors_pb'
import { AccessionSchema } from '@/gen/ai/protomolt/proto/repo/v2/manifest_pb'
import { OutcomeInputError, lookupOutcome } from './lookupCore'
import { at, routerClients, testAddress } from './repositoryTestKit'

function server(answer: (request: GetOperationOutcomeRequest) => ReturnType<typeof create<typeof GetOperationOutcomeResponseSchema>>) {
  return routerClients((router) => router.service(DocumentService, { getOperationOutcome: (r) => answer(r) })).documents
}

const signal = () => new AbortController().signal

describe('lookupOutcome', () => {
  it('answers committed with the accession and when it was recorded', async () => {
    const seen: GetOperationOutcomeRequest[] = []
    const client = server((r) => {
      seen.push(r)
      return create(GetOperationOutcomeResponseSchema, {
        state: {
          case: 'committed',
          value: create(RecordedOutcomeSchema, {
            result: { case: 'accession', value: create(AccessionSchema, { address: testAddress(), revision: 7n }) },
            recordedAt: at('2026-10-01T00:00:00Z'),
          }),
        },
      })
    })
    const answer = await lookupOutcome(client, 'acct-a', 'op-1', signal())
    expect(answer.state).toBe('committed')
    if (answer.state !== 'committed') throw new Error('unreachable')
    expect(answer.accession.revision).toBe(7n)
    expect(answer.recordedAt).toBe('2026-10-01T00:00:00.000Z')
    expect(seen[0]).toMatchObject({ accountId: 'acct-a', operationId: 'op-1' })
  })

  it('answers a recorded rejection as its own state, not as committed', async () => {
    const client = server(() =>
      create(GetOperationOutcomeResponseSchema, {
        state: {
          case: 'committed',
          value: create(RecordedOutcomeSchema, {
            result: {
              case: 'rejection',
              value: create(SaveRejectionSchema, { reason: RejectionReason.VALIDATION_FAILED, message: 'title too long' }),
            },
            recordedAt: at('2026-10-01T00:00:00Z'),
          }),
        },
      }),
    )
    const answer = await lookupOutcome(client, 'acct-a', 'op-1', signal())
    expect(answer.state).toBe('rejected')
    if (answer.state !== 'rejected') throw new Error('unreachable')
    expect(answer.rejection.message).toBe('title too long')
  })

  it('answers none when nothing is recorded', async () => {
    const client = server(() =>
      create(GetOperationOutcomeResponseSchema, { state: { case: 'none', value: create(NoRecordedOutcomeSchema) } }),
    )
    expect(await lookupOutcome(client, 'acct-a', 'op-1', signal())).toEqual({ state: 'none' })
  })

  it('answers unavailable, distinct from none, when the lookup itself cannot answer', async () => {
    const client = server(() => {
      throw new ConnectError('outcome ledger unreachable', Code.Unavailable)
    })
    const answer = await lookupOutcome(client, 'acct-a', 'op-1', signal())
    expect(answer).toEqual({
      state: 'unavailable',
      failure: { code: 'UNAVAILABLE', codeNumber: Code.Unavailable, message: 'outcome ledger unreachable' },
    })
  })

  it('throws every other status as a failed call', async () => {
    const client = server(() => {
      throw new ConnectError('account acct-b is not bound to this caller', Code.PermissionDenied)
    })
    const error = await lookupOutcome(client, 'acct-b', 'op-1', signal()).catch((e) => e)
    expect(ConnectError.from(error).code).toBe(Code.PermissionDenied)
  })

  it('refuses an answer with no state', async () => {
    const client = server(() => create(GetOperationOutcomeResponseSchema))
    await expect(lookupOutcome(client, 'acct-a', 'op-1', signal())).rejects.toThrow('neither committed nor none')
  })

  it('refuses blank input by name before calling', async () => {
    const client = server(() => {
      throw new Error('must not be called')
    })
    await expect(lookupOutcome(client, ' ', 'op-1', signal())).rejects.toThrow(OutcomeInputError)
    await expect(lookupOutcome(client, 'acct-a', '', signal())).rejects.toThrow('Operation ID is blank')
  })
})
