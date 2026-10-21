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
import { AccessionSchema } from '@/gen/ai/protomolt/proto/repo/v2/manifest_pb'
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
    [{ expectedRevision: '0' }, 'Expected revision: must be greater than zero'],
    [{ expectedRevision: 'two' }, 'Expected revision: "two" is not a revision number'],
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
    const answer = await send({ status: 429, headers: { 'grpc-status': '8', 'grpc-message': 'stack%20write%20budget%20exhausted' } }).result
    expect(answer).toEqual({
      kind: 'capacity',
      httpStatus: 429,
      failure: { code: 'RESOURCE_EXHAUSTED', codeNumber: Code.ResourceExhausted, message: 'stack write budget exhausted' },
    })
  })

  it('treats a 401 as a sign-in, and any other status as a refusal', async () => {
    expect((await send({ status: 401, headers: { 'grpc-status': '16', 'grpc-message': 'session%20expired' } }).result).kind).toBe('unauthenticated')
    const conflict = await send({ status: 409, headers: { 'grpc-status': '10', 'grpc-message': 'expected%20revision%203%2C%20current%204' } }).result
    expect(conflict).toMatchObject({ kind: 'refused', httpStatus: 409, failure: { code: 'ABORTED', message: 'expected revision 3, current 4' } })
  })

  it('reports a refusal without grpc-status by its HTTP status, never parsing the body', async () => {
    const answer = await send({ status: 400, headers: { 'Content-Type': 'application/json; charset=utf-8' }, body: new TextEncoder().encode('{"error":"x"}') }).result
    expect(answer).toMatchObject({ kind: 'refused', failure: { code: 'HTTP 400', codeNumber: null } })
    if (answer.kind !== 'refused') throw new Error('unreachable')
    expect(answer.failure.message).toContain('application/json')
  })

  it('refuses a 200 that is not protobuf', async () => {
    await expect(send({ status: 200, headers: { 'Content-Type': 'application/json' }, body: new Uint8Array([1]) }).result).rejects.toThrow(
      'reads only application/proto',
    )
  })

  it('reports a lost connection and a cancel as no answer, since the save may still commit', async () => {
    expect(await send('network-error').result).toMatchObject({ kind: 'no-answer', reason: expect.stringContaining('connection failed') })
    const controller = new AbortController()
    const pending = send('hang', controller.signal)
    controller.abort()
    expect(await pending.result).toMatchObject({ kind: 'no-answer', reason: expect.stringContaining('cancelled') })
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
