import { create } from '@bufbuild/protobuf'
import { describe, expect, it } from 'vitest'
import {
  DocumentService,
  GetOperationOutcomeResponseSchema,
  NoRecordedOutcomeSchema,
  RecordedOutcomeSchema,
} from '@/gen/ai/protomolt/proto/repo/v2/document_service_pb'
import { FieldViolationSchema, RejectionReason, SaveRejectionSchema } from '@/gen/ai/protomolt/proto/repo/v2/errors_pb'
import { AccessionSchema } from '@/gen/ai/protomolt/proto/repo/v2/manifest_pb'
import { OutcomeInputError } from './lookupCore'
import { NO_OUTCOME_EXPLANATION, describeOutcome, inspectOutcome } from './outcomeCore'
import { at, routerClients, testAddress } from './repositoryTestKit'

const signal = () => new AbortController().signal

function server(response: ReturnType<typeof create<typeof GetOperationOutcomeResponseSchema>>) {
  return routerClients((router) => router.service(DocumentService, { getOperationOutcome: () => response })).documents
}

describe('describeOutcome', () => {
  it('shows a committed accession with its revision link', () => {
    const view = describeOutcome({
      state: 'committed',
      accession: create(AccessionSchema, {
        address: testAddress({ accountId: 'acct-a', docId: 'doc-1', graphAddressId: 'ds-1' }),
        revision: 7n,
        previousRevision: 6n,
      }),
      recordedAt: '2026-10-01T00:00:00.000Z',
    })
    expect(view).toMatchObject({
      kind: 'committed',
      docId: 'doc-1',
      datasourceId: 'ds-1',
      revision: '7',
      previousRevision: '6',
      newDocument: false,
      tombstone: false,
      recordedAt: '2026-10-01T00:00:00.000Z',
    })
    if (view.kind !== 'committed') throw new Error('unreachable')
    expect(view.route).toMatchObject({
      name: 'repository-document',
      query: { account: 'acct-a', doc: 'doc-1', node: 'ds-1', revision: '7' },
    })
  })

  it('marks a first revision as a new document and a tombstone as one', () => {
    const first = describeOutcome({
      state: 'committed',
      accession: create(AccessionSchema, { address: testAddress(), revision: 1n, previousRevision: 0n }),
      recordedAt: null,
    })
    expect(first).toMatchObject({ kind: 'committed', newDocument: true })
    const tombstone = describeOutcome({
      state: 'committed',
      accession: create(AccessionSchema, {
        address: testAddress(), revision: 3n, previousRevision: 2n, tombstone: true,
      }),
      recordedAt: null,
    })
    expect(tombstone).toMatchObject({ kind: 'committed', tombstone: true, newDocument: false })
  })

  it('shows a rejection with its reason label and violations', () => {
    const view = describeOutcome({
      state: 'rejected',
      rejection: create(SaveRejectionSchema, {
        reason: RejectionReason.VALIDATION_FAILED,
        message: 'the title is blank',
        violations: [create(FieldViolationSchema, {
          fieldPath: 'search_metadata.title', ruleId: 'string.min_len', message: 'must not be blank',
        })],
      }),
      recordedAt: '2026-10-02T00:00:00.000Z',
    })
    expect(view).toMatchObject({
      kind: 'rejected',
      reason: 'VALIDATION_FAILED',
      message: 'the title is blank',
      recordedAt: '2026-10-02T00:00:00.000Z',
    })
    if (view.kind !== 'rejected') throw new Error('unreachable')
    expect(view.violations).toEqual([
      { ruleId: 'string.min_len', fieldPath: 'search_metadata.title', message: 'must not be blank' },
    ])
  })

  it('explains the no-outcome state without calling it a failure', () => {
    const view = describeOutcome({ state: 'none' })
    expect(view).toEqual({ kind: 'none', explanation: NO_OUTCOME_EXPLANATION })
    expect(NO_OUTCOME_EXPLANATION).toContain('may still commit later')
  })

  it('keeps an unavailable lookup distinct from every outcome', () => {
    const failure = { code: 'unavailable', message: 'the ledger is away', detail: null } as never
    const view = describeOutcome({ state: 'unavailable', failure })
    expect(view).toMatchObject({ kind: 'unavailable', failure })
  })
})

describe('inspectOutcome', () => {
  it('answers the display form over binary Connect', async () => {
    const client = server(create(GetOperationOutcomeResponseSchema, {
      state: {
        case: 'committed',
        value: create(RecordedOutcomeSchema, {
          result: { case: 'accession', value: create(AccessionSchema, { address: testAddress(), revision: 2n }) },
          recordedAt: at('2026-10-03T00:00:00Z'),
        }),
      },
    }))
    const view = await inspectOutcome(client, 'acct-a', 'op-9', signal())
    expect(view).toMatchObject({ kind: 'committed', revision: '2', newDocument: true })
  })

  it('answers none for a recorded nothing', async () => {
    const client = server(create(GetOperationOutcomeResponseSchema, {
      state: { case: 'none', value: create(NoRecordedOutcomeSchema) },
    }))
    expect(await inspectOutcome(client, 'acct-a', 'op-9', signal())).toMatchObject({ kind: 'none' })
  })

  it('refuses blank input before any request', async () => {
    const client = server(create(GetOperationOutcomeResponseSchema, {
      state: { case: 'none', value: create(NoRecordedOutcomeSchema) },
    }))
    await expect(inspectOutcome(client, ' ', 'op', signal())).rejects.toBeInstanceOf(OutcomeInputError)
    await expect(inspectOutcome(client, 'acct', '', signal())).rejects.toBeInstanceOf(OutcomeInputError)
  })
})
