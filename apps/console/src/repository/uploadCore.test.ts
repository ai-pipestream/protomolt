// @vitest-environment jsdom
import { createHash } from 'node:crypto'
import { create, toBinary } from '@bufbuild/protobuf'
import { Code, ConnectError } from '@connectrpc/connect'
import { describe, expect, it } from 'vitest'
import {
  DocumentService,
  GetOperationOutcomeResponseSchema,
  NoRecordedOutcomeSchema,
  RecordedOutcomeSchema,
  SaveDocumentResponseSchema,
  UploadDocumentResponseSchema,
} from '@/gen/ai/protomolt/proto/repo/v2/document_service_pb'
import { ItemErrorSchema, RevisionConflictSchema, type ItemError } from '@/gen/ai/protomolt/proto/repo/v2/errors_pb'
import { AccessionSchema, ExpectedRevisionSchema } from '@/gen/ai/protomolt/proto/repo/v2/manifest_pb'
import { FakeUploadRequest, at, routerClients, testAddress, type FakeAnswer } from './repositoryTestKit'
import {
  HASH_SLICE_BYTES,
  UploadInputError,
  emptyUploadForm,
  fileSha256,
  followOutcome,
  planUpload,
  sendUpload,
  type UploadForm,
} from './uploadCore'

const BYTES = new Uint8Array(Array.from({ length: 300_000 }, (_, i) => (i * 7) & 0xff))
const SHA = createHash('sha256').update(BYTES).digest('hex')
const file = () => new File([BYTES], 'report.pdf', { type: 'application/pdf' })

function form(overrides: Partial<UploadForm> = {}): UploadForm {
  return { ...emptyUploadForm('acct-a'), datasourceId: 'ds-files', drive: 'intake', ...overrides }
}

/** The route's protobuf refusal: its HTTP status and a binary ItemError body. */
function itemError(status: number, code: Code, message: string, detail?: ItemError['detail']): FakeAnswer {
  return {
    status,
    headers: { 'content-type': 'application/proto', 'grpc-status': String(code), 'grpc-message': encodeURIComponent(message) },
    body: toBinary(ItemErrorSchema, create(ItemErrorSchema, { code, message, detail })),
  }
}

const saved = toBinary(
  UploadDocumentResponseSchema,
  create(UploadDocumentResponseSchema, {
    saved: create(SaveDocumentResponseSchema, {
      operationId: 'op-1',
      accession: create(AccessionSchema, { address: testAddress(), revision: 1n }),
    }),
  }),
)

function send(answer: FakeAnswer | 'network-error' | 'hang', signal = new AbortController().signal) {
  const plan = planUpload(form(), file(), 'op-1', SHA)
  const progress: number[] = []
  let request: FakeUploadRequest | null = null
  const result = sendUpload(
    plan,
    () => (request = new FakeUploadRequest(() => answer)),
    (sent) => progress.push(sent),
    signal,
  )
  return { result, progress, request: () => request! }
}

describe('fileSha256', () => {
  it('hashes the file slice by slice, reporting bytes hashed', async () => {
    const big = new Uint8Array(HASH_SLICE_BYTES * 2 + 17).map((_, i) => i & 0xff)
    const seen: number[] = []
    const digest = await fileSha256(new File([big], 'big.bin'), new AbortController().signal, (n) => seen.push(n))
    expect(digest).toBe(createHash('sha256').update(big).digest('hex'))
    expect(seen).toEqual([HASH_SLICE_BYTES, HASH_SLICE_BYTES * 2, big.length])
    expect(await fileSha256(new File([], 'empty'), new AbortController().signal)).toBe(createHash('sha256').digest('hex'))
  })

  it('stops hashing when the signal aborts', async () => {
    const controller = new AbortController()
    controller.abort(new Error('stopped'))
    await expect(fileSha256(file(), controller.signal)).rejects.toThrow('stopped')
  })
})

describe('planUpload', () => {
  it('carries the identity, the operation ID chosen before sending and the declared digest', () => {
    const plan = planUpload(form({ docId: 'doc 1', expectedRevision: '3', connectorId: 'fs' }), file(), 'op-1', SHA)
    const params = new URLSearchParams(plan.url.slice(plan.url.indexOf('?') + 1))
    expect(plan.url.startsWith('/v1/documents:upload?')).toBe(true)
    expect(Object.fromEntries(params)).toEqual({
      account_id: 'acct-a',
      datasource_id: 'ds-files',
      drive: 'intake',
      filename: 'report.pdf',
      operation_id: 'op-1',
      sha256: SHA,
      doc_id: 'doc 1',
      connector_id: 'fs',
      content_type: 'application/pdf',
      expected_revision: '3',
    })
    expect(plan.sizeBytes).toBe(BYTES.length)
  })

  it('sends no document ID, connector or expected revision when they are blank', () => {
    const params = new URLSearchParams(planUpload(form(), file(), 'op-1', SHA).url.split('?')[1])
    expect(params.has('doc_id')).toBe(false)
    expect(params.has('connector_id')).toBe(false)
    expect(params.has('expected_revision')).toBe(false)
  })

  it.each([
    [{ datasourceId: '' }, 'Datasource ID: is blank'],
    [{ drive: ' ' }, 'Drive: is blank'],
    [{ docId: '  ' }, 'Document ID: is only whitespace'],
    [{ docId: 'doc-1', expectedRevision: '0' }, 'Expected revision: must be greater than zero'],
    [{ docId: 'doc-1', expectedRevision: 'two' }, 'Expected revision: "two" is not a revision number'],
    [{ expectedRevision: '3' }, 'Expected revision: needs a Document ID'],
  ])('refuses %o by name', (overrides, message) => {
    expect(() => planUpload(form(overrides), file(), 'op-1', SHA)).toThrow(UploadInputError)
    expect(() => planUpload(form(overrides), file(), 'op-1', SHA)).toThrow(message)
  })
})

describe('sendUpload', () => {
  it('posts the file with credentials, asks for protobuf, reports bytes sent and decodes the saved answer', async () => {
    const { result, progress, request } = send({ status: 200, headers: { 'Content-Type': 'application/proto' }, body: saved })
    const answer = await result
    expect(answer.kind).toBe('saved')
    if (answer.kind !== 'saved') throw new Error('unreachable')
    expect(answer.response.saved?.accession?.revision).toBe(1n)
    const r = request()
    expect(r.method).toBe('POST')
    expect(r.withCredentials).toBe(true)
    expect(r.responseType).toBe('arraybuffer')
    expect(r.headers.accept).toBe('application/proto')
    expect(r.body?.size).toBe(BYTES.length)
    expect(progress[progress.length - 1]).toBe(BYTES.length)
    expect(progress.length).toBeGreaterThan(1)
  })

  it('treats HTTP 429 as a capacity refusal with the route status and message', async () => {
    const answer = await send(itemError(429, Code.ResourceExhausted, 'stack write budget exhausted')).result
    expect(answer).toEqual({
      kind: 'capacity',
      httpStatus: 429,
      failure: { code: 'RESOURCE_EXHAUSTED', codeNumber: Code.ResourceExhausted, message: 'stack write budget exhausted' },
    })
  })

  it('reports a 401 as unauthenticated, and a definite status as a refusal with its errors.proto detail', async () => {
    expect((await send(itemError(401, Code.Unauthenticated, 'Unauthorized')).result).kind).toBe('unauthenticated')
    const conflict = await send(
      itemError(409, Code.Aborted, 'expected revision 3, current 4', {
        case: 'conflict',
        value: create(RevisionConflictSchema, {
          address: testAddress(),
          expected: create(ExpectedRevisionSchema, { expected: { case: 'revision', value: 3n } }),
          currentRevision: 4n,
        }),
      }),
    ).result
    expect(conflict).toMatchObject({ kind: 'refused', httpStatus: 409, failure: { code: 'ABORTED', message: 'expected revision 3, current 4' } })
    if (conflict.kind !== 'refused') throw new Error('unreachable')
    expect(conflict.error?.detail.case).toBe('conflict')
    if (conflict.error?.detail.case !== 'conflict') throw new Error('unreachable')
    expect(conflict.error.detail.value.currentRevision).toBe(4n)
  })

  it.each([
    [Code.Unavailable, 503],
    [Code.Internal, 500],
    [Code.DeadlineExceeded, 504],
    [Code.Unknown, 500],
  ])('reports status %s as unsettled, since the save may still have committed', async (code, status) => {
    const answer = await send(itemError(status, code, 'transient')).result
    expect(answer).toMatchObject({ kind: 'unsettled', failure: { codeNumber: code } })
  })

  it('reports an answer without a protobuf error by its HTTP status as unsettled, never parsing the body', async () => {
    const answer = await send({ status: 502, headers: { 'Content-Type': 'text/html' }, body: new TextEncoder().encode('<html>bad gateway</html>') }).result
    expect(answer).toMatchObject({ kind: 'unsettled', failure: { code: 'HTTP 502', codeNumber: null } })
    if (answer.kind !== 'unsettled') throw new Error('unreachable')
    expect(answer.failure?.message).toContain('text/html')
  })

  it('reports a 200 it cannot read as unsettled rather than failed, since the route committed', async () => {
    expect(await send({ status: 200, headers: { 'Content-Type': 'application/json' }, body: new Uint8Array([1]) }).result).toMatchObject({
      kind: 'unsettled',
      reason: expect.stringContaining('application/json'),
    })
    expect(await send({ status: 200, headers: { 'Content-Type': 'application/proto' }, body: new Uint8Array([0xff, 0xff]) }).result).toMatchObject({
      kind: 'unsettled',
      reason: expect.stringContaining('does not decode'),
    })
  })

  it('reports a lost connection and a cancel as unsettled', async () => {
    expect(await send('network-error').result).toMatchObject({ kind: 'unsettled', reason: expect.stringContaining('connection failed') })
    const controller = new AbortController()
    const pending = send('hang', controller.signal)
    controller.abort()
    expect(await pending.result).toMatchObject({ kind: 'unsettled', reason: expect.stringContaining('cancelled') })
    expect(pending.request().aborted).toBe(true)
  })
})

describe('followOutcome', () => {
  const noSleep = { attempts: 3, intervalMs: 0, sleep: async () => {} }

  it('asks until committed and reports each answer', async () => {
    let calls = 0
    const client = routerClients((router) =>
      router.service(DocumentService, {
        getOperationOutcome() {
          calls++
          return calls < 2
            ? create(GetOperationOutcomeResponseSchema, { state: { case: 'none', value: create(NoRecordedOutcomeSchema) } })
            : create(GetOperationOutcomeResponseSchema, {
                state: {
                  case: 'committed',
                  value: create(RecordedOutcomeSchema, {
                    result: { case: 'accession', value: create(AccessionSchema, { address: testAddress(), revision: 1n }) },
                    recordedAt: at('2026-10-01T00:00:00Z'),
                  }),
                },
              })
        },
      }),
    ).documents
    const seen: string[] = []
    const last = await followOutcome(client, 'acct-a', 'op-1', new AbortController().signal, (a) => seen.push(a.state), noSleep)
    expect(seen).toEqual(['none', 'committed'])
    expect(last.state).toBe('committed')
  })

  it('stops after the attempts with the last answer, keeping unavailable distinct from none', async () => {
    const client = routerClients((router) =>
      router.service(DocumentService, {
        getOperationOutcome() {
          throw new ConnectError('ledger down', Code.Unavailable)
        },
      }),
    ).documents
    const seen: string[] = []
    const last = await followOutcome(client, 'acct-a', 'op-1', new AbortController().signal, (a) => seen.push(a.state), noSleep)
    expect(seen).toEqual(['unavailable', 'unavailable', 'unavailable'])
    expect(last.state).toBe('unavailable')
  })
})
