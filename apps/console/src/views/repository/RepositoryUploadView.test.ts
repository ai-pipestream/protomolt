// @vitest-environment jsdom
import { createHash } from 'node:crypto'
import { create, toBinary } from '@bufbuild/protobuf'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { DriveSchema, DriveService, ListDrivesResponseSchema } from '@/gen/ai/protomolt/proto/repo/v1/drive_pb'
import {
  DocumentService,
  GetOperationOutcomeResponseSchema,
  RecordedOutcomeSchema,
  SaveDocumentResponseSchema,
  UploadDocumentResponseSchema,
  type GetOperationOutcomeRequest,
} from '@/gen/ai/protomolt/proto/repo/v2/document_service_pb'
import { AccessionSchema } from '@/gen/ai/protomolt/proto/repo/v2/manifest_pb'
import { REPOSITORY_SIGN_IN, REPOSITORY_UPLOAD_REQUEST } from '@/repository/clients'
import { FakeUploadRequest, at, testAddress, type FakeAnswer } from '@/repository/repositoryTestKit'
import { mountRepositoryView, settle } from '@/repository/repositoryViewKit'
import RepositoryUploadView from './RepositoryUploadView.vue'

// A multi-megabyte file, so progress is reported over many steps.
const BYTES = new Uint8Array(3 * 1024 * 1024 + 123).map((_, i) => (i * 31) & 0xff)
const SHA = createHash('sha256').update(BYTES).digest('hex')

const savedBody = (operationId: string) =>
  toBinary(
    UploadDocumentResponseSchema,
    create(UploadDocumentResponseSchema, {
      saved: create(SaveDocumentResponseSchema, { operationId, accession: create(AccessionSchema, { address: testAddress(), revision: 5n }) }),
    }),
  )

function repository(outcomes: GetOperationOutcomeRequest[]) {
  return (router: Parameters<Parameters<typeof mountRepositoryView>[2]>[0]) => {
    router.service(DriveService, {
      listDrives: () => create(ListDrivesResponseSchema, { drives: [create(DriveSchema, { name: 'intake' })] }),
    })
    router.service(DocumentService, {
      getOperationOutcome(request) {
        outcomes.push(request)
        return create(GetOperationOutcomeResponseSchema, {
          state: {
            case: 'committed',
            value: create(RecordedOutcomeSchema, {
              result: { case: 'accession', value: create(AccessionSchema, { address: testAddress(), revision: 5n }) },
              recordedAt: at('2026-10-21T10:00:00Z'),
            }),
          },
        })
      },
    })
  }
}

type Answer = (request: FakeUploadRequest) => FakeAnswer | 'network-error' | 'hang'

const proto = (status: number, body?: Uint8Array, headers: Record<string, string> = {}): FakeAnswer => ({
  status,
  body,
  headers: { 'content-type': 'application/proto', ...headers },
})
const saved: Answer = (r) => proto(200, savedBody(r.params().get('operation_id')!))
const atCapacity: Answer = () => proto(429, undefined, { 'grpc-status': '8', 'grpc-message': 'stack%20write%20budget%20exhausted' })

async function mountUpload(answers: Answer[], signIn = vi.fn()) {
  const outcomes: GetOperationOutcomeRequest[] = []
  const queue = [...answers]
  const mounted = await mountRepositoryView(
    RepositoryUploadView,
    { name: 'repository-upload', query: { account: 'acct-a', datasource: 'ds-files' } },
    repository(outcomes),
    {
      [REPOSITORY_UPLOAD_REQUEST as symbol]: () => new FakeUploadRequest((r) => queue.shift()!(r)),
      [REPOSITORY_SIGN_IN as symbol]: signIn,
    },
  )
  const input = mounted.wrapper.get('[data-testid="up-file"]').element as HTMLInputElement
  Object.defineProperty(input, 'files', { value: [new File([BYTES], 'scan.pdf', { type: 'application/pdf' })] })
  await mounted.wrapper.get('[data-testid="up-file"]').trigger('change')
  ;(mounted.wrapper.vm as unknown as { form: { drive: string } }).form.drive = 'intake'
  await settle()
  return { ...mounted, outcomes, signIn }
}

afterEach(() => {
  FakeUploadRequest.sent = []
})

describe('RepositoryUploadView', () => {
  it('uploads a multi-MB file under a pre-chosen operation ID, follows the outcome and opens the revision', async () => {
    const { wrapper, router, outcomes } = await mountUpload([saved])
    await wrapper.get('[data-testid="up-start"]').trigger('click')
    await settle()
    const request = FakeUploadRequest.sent[0]
    const params = request.params()
    const operationId = params.get('operation_id')!
    expect(operationId).toMatch(/^[0-9a-f-]{36}$/)
    expect(wrapper.get('[data-testid="up-operation"]').text()).toBe(operationId)
    expect(params.get('sha256')).toBe(SHA)
    expect(params.get('drive')).toBe('intake')
    expect(params.get('datasource_id')).toBe('ds-files')
    expect(params.get('filename')).toBe('scan.pdf')
    expect(request.body?.size).toBe(BYTES.length)
    expect(wrapper.get('[data-testid="up-sent"]').text()).toBe(`${BYTES.length} of ${BYTES.length} bytes sent`)
    expect(outcomes[0]).toMatchObject({ accountId: 'acct-a', operationId })
    expect(wrapper.get('[data-testid="up-outcome-committed"]').text()).toContain('Committed as revision 5')
    await wrapper.get('[data-testid="up-open"]').trigger('click')
    await settle()
    expect(router.currentRoute.value.query).toMatchObject({ doc: 'doc-1', revision: '5' })
    wrapper.unmount()
  })

  it('shows a capacity refusal as nothing saved, and retries with the same operation ID', async () => {
    const { wrapper, outcomes } = await mountUpload([atCapacity, saved])
    await wrapper.get('[data-testid="up-start"]').trigger('click')
    await settle()
    const capacity = wrapper.get('[data-testid="up-capacity"]').text()
    expect(capacity).toContain('nothing was saved')
    expect(capacity).toContain('RESOURCE_EXHAUSTED: stack write budget exhausted')
    expect(wrapper.find('[data-testid="rpc-failure"]').exists()).toBe(false)
    expect(outcomes).toHaveLength(0)
    await wrapper.get('[data-testid="up-retry"]').trigger('click')
    await settle()
    const [first, second] = FakeUploadRequest.sent
    expect(second.params().get('operation_id')).toBe(first.params().get('operation_id'))
    expect(wrapper.find('[data-testid="up-outcome-committed"]').exists()).toBe(true)
    wrapper.unmount()
  })

  it('looks the outcome up when the connection drops before an answer', async () => {
    const { wrapper, outcomes } = await mountUpload([() => 'network-error'])
    await wrapper.get('[data-testid="up-start"]').trigger('click')
    await settle()
    expect(wrapper.get('[data-testid="up-no-answer"]').text()).toContain('may still commit')
    expect(outcomes).toHaveLength(1)
    expect(wrapper.find('[data-testid="up-outcome-committed"]').exists()).toBe(true)
    wrapper.unmount()
  })

  it('starts sign-in when the route answers 401', async () => {
    const { wrapper, signIn } = await mountUpload([() => proto(401, undefined, { 'grpc-status': '16', 'grpc-message': 'session%20expired' })])
    await wrapper.get('[data-testid="up-start"]').trigger('click')
    await settle()
    expect(signIn).toHaveBeenCalledTimes(1)
    wrapper.unmount()
  })

  it('names a blank required field and sends nothing', async () => {
    const { wrapper } = await mountUpload([])
    ;(wrapper.vm as unknown as { form: { drive: string } }).form.drive = ''
    await wrapper.get('[data-testid="up-start"]').trigger('click')
    await settle()
    expect(wrapper.get('[data-testid="up-input-error"]').text()).toBe('Drive: is blank')
    expect(FakeUploadRequest.sent).toHaveLength(0)
    wrapper.unmount()
  })
})
