import { create } from '@bufbuild/protobuf'
import { Code, ConnectError } from '@connectrpc/connect'
import { describe, expect, it } from 'vitest'
import {
  DeleteDocumentResponseSchema,
  DocumentService,
  type DeleteDocumentRequest,
} from '@/gen/ai/protomolt/proto/repo/v2/document_service_pb'
import { RevisionConflictSchema } from '@/gen/ai/protomolt/proto/repo/v2/errors_pb'
import { AccessionSchema, ExpectedRevisionSchema } from '@/gen/ai/protomolt/proto/repo/v2/manifest_pb'
import { DeleteInputError, MAX_REASON_LENGTH, deleteDocument, planDelete, staleText } from './deleteCore'
import { routerClients, testAddress } from './repositoryTestKit'

const signal = () => new AbortController().signal

function server(answer: (request: DeleteDocumentRequest) => ReturnType<typeof create<typeof DeleteDocumentResponseSchema>>) {
  return routerClients((router) => router.service(DocumentService, { deleteDocument: (r) => answer(r) })).documents
}

describe('planDelete', () => {
  it('expects the exact current revision under the given operation ID', () => {
    const request = planDelete(testAddress(), 4n, 'op-del', 'duplicate scan')
    expect(request.expected?.expected).toEqual({ case: 'revision', value: 4n })
    expect(request).toMatchObject({ operationId: 'op-del', reason: 'duplicate scan' })
    expect(request.address?.docId).toBe('doc-1')
  })

  it('refuses a reason over the contract limit and a blank operation ID by name', () => {
    expect(() => planDelete(testAddress(), 4n, 'op', 'x'.repeat(MAX_REASON_LENGTH + 1))).toThrow(DeleteInputError)
    expect(() => planDelete(testAddress(), 4n, 'op', 'x'.repeat(MAX_REASON_LENGTH + 1))).toThrow('Reason: is 1025 characters')
    expect(() => planDelete(testAddress(), 4n, ' ', '')).toThrow('Operation ID: is blank')
  })
})

describe('deleteDocument', () => {
  it('answers deleted with the tombstone accession', async () => {
    const seen: DeleteDocumentRequest[] = []
    const answer = await deleteDocument(
      server((r) => {
        seen.push(r)
        return create(DeleteDocumentResponseSchema, {
          operationId: r.operationId,
          accession: create(AccessionSchema, { address: testAddress(), revision: 5n, previousRevision: 4n, tombstone: true }),
        })
      }),
      planDelete(testAddress(), 4n, 'op-del', ''),
      signal(),
    )
    expect(answer.kind).toBe('deleted')
    if (answer.kind !== 'deleted') throw new Error('unreachable')
    expect(answer.response.accession?.tombstone).toBe(true)
    expect(seen[0].expected?.expected).toEqual({ case: 'revision', value: 4n })
  })

  it('reads the current revision from the RevisionConflict detail of an ABORTED answer', async () => {
    const answer = await deleteDocument(
      server(() => {
        throw new ConnectError('expected revision 4, current 6', Code.Aborted, undefined, [
          {
            desc: RevisionConflictSchema,
            value: {
              address: testAddress(),
              expected: create(ExpectedRevisionSchema, { expected: { case: 'revision', value: 4n } }),
              currentRevision: 6n,
              currentIsTombstone: false,
            },
          },
        ])
      }),
      planDelete(testAddress(), 4n, 'op-del', ''),
      signal(),
    )
    expect(answer.kind).toBe('stale')
    if (answer.kind !== 'stale') throw new Error('unreachable')
    expect(answer.conflict?.currentRevision).toBe(6n)
    expect(staleText(answer, 4n)).toBe('Revision 4 is no longer current; the current revision is 6.')
  })

  it('says what it can when ABORTED carries no detail', async () => {
    const answer = await deleteDocument(
      server(() => {
        throw new ConnectError('concurrent write', Code.Aborted)
      }),
      planDelete(testAddress(), 4n, 'op-del', ''),
      signal(),
    )
    if (answer.kind !== 'stale') throw new Error('expected stale')
    expect(answer.conflict).toBeNull()
    expect(staleText(answer, 4n)).toContain('did not say which revision is (ABORTED: concurrent write)')
  })

  it('answers denied with the reason, and throws every other status', async () => {
    const denied = await deleteDocument(
      server(() => {
        throw new ConnectError('The caller may not delete this document', Code.PermissionDenied)
      }),
      planDelete(testAddress(), 4n, 'op-del', ''),
      signal(),
    )
    expect(denied).toMatchObject({ kind: 'denied', failure: { code: 'PERMISSION_DENIED', message: 'The caller may not delete this document' } })
    const error = await deleteDocument(
      server(() => {
        throw new ConnectError('catalog down', Code.Unavailable)
      }),
      planDelete(testAddress(), 4n, 'op-del', ''),
      signal(),
    ).catch((e) => e)
    expect(ConnectError.from(error).code).toBe(Code.Unavailable)
  })
})
