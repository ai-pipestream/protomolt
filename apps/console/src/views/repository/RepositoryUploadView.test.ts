// @vitest-environment jsdom
import { createHash } from 'node:crypto'
import { create, toBinary } from '@bufbuild/protobuf'
import { Code } from '@connectrpc/connect'
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
import { ItemErrorSchema } from '@/gen/ai/protomolt/proto/repo/v2/errors_pb'
import { AccessionSchema } from '@/gen/ai/protomolt/proto/repo/v2/manifest_pb'
import { IntakeWriterScopeSchema } from '@/gen/ai/protomolt/proto/authz/v1/account_bindings_pb'
import { ConsoleSessionStatusSchema } from '@/gen/ai/protomolt/proto/authz/v1/console_session_pb'
import { REPOSITORY_SESSION, REPOSITORY_SIGN_IN, REPOSITORY_UPLOAD_REQUEST } from '@/repository/clients'
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
      answer: {
        case: 'saved',
        value: create(SaveDocumentResponseSchema, {
          operationId,
          accession: create(AccessionSchema, { address: testAddress(), revision: 5n, committedAt: at('2026-10-21T10:00:00Z') }),
        }),
      },
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
/** The route's protobuf refusal: its HTTP status and a binary ItemError body. */
const refusal = (status: number, code: Code, message: string): Answer => () =>
  proto(status, toBinary(ItemErrorSchema, create(ItemErrorSchema, { code, message })), {
    'grpc-status': String(code),
    'grpc-message': encodeURIComponent(message),
  })
const atCapacity = refusal(429, Code.ResourceExhausted, 'stack write budget exhausted')

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
  it('looks up a deep-linked operation only when the account came from the query', async () => {
    const linked: GetOperationOutcomeRequest[] = []
    const fromQuery = await mountRepositoryView(
      RepositoryUploadView,
      { name: 'repository-upload', query: { account: 'acct-a', operationId: 'op-linked' } },
      repository(linked),
      { [REPOSITORY_SIGN_IN as symbol]: vi.fn() },
    )
    expect(linked).toHaveLength(1)
    expect(linked[0]).toMatchObject({ accountId: 'acct-a', operationId: 'op-linked' })
    fromQuery.wrapper.unmount()

    const typed: GetOperationOutcomeRequest[] = []
    const typing = await mountRepositoryView(
      RepositoryUploadView,
      { name: 'repository-upload', query: { operationId: 'op-typed' } },
      repository(typed),
      { [REPOSITORY_SIGN_IN as symbol]: vi.fn() },
    )
    await typing.wrapper.get('[data-testid="up-account"] input').setValue('a')
    await settle()
    await typing.wrapper.get('[data-testid="up-account"] input').setValue('acct-a')
    await settle()
    expect(typed).toHaveLength(0)
    typing.wrapper.unmount()
  })

  it('uploads a multi-MB file under a pre-chosen operation ID and opens the revision the route committed', async () => {
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
    // The 200 answer names the revision; no outcome lookup is needed.
    expect(outcomes).toHaveLength(0)
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
    expect(outcomes).toHaveLength(0)
    wrapper.unmount()
  })

  it('looks the outcome up when the connection drops, and offers a resend under the same operation ID', async () => {
    const { wrapper, outcomes } = await mountUpload([() => 'network-error', saved])
    await wrapper.get('[data-testid="up-start"]').trigger('click')
    await settle()
    expect(wrapper.get('[data-testid="up-unsettled"]').text()).toContain('may or may not have committed')
    expect(outcomes).toHaveLength(1)
    expect(wrapper.find('[data-testid="up-outcome-committed"]').exists()).toBe(true)
    await wrapper.get('[data-testid="up-resend"]').trigger('click')
    await settle()
    const [first, second] = FakeUploadRequest.sent
    expect(second.params().get('operation_id')).toBe(first.params().get('operation_id'))
    wrapper.unmount()
  })

  it('shows a committed upload even when the outcome lookup would be refused', async () => {
    const { wrapper } = await mountUpload([saved])
    await wrapper.get('[data-testid="up-start"]').trigger('click')
    await settle()
    expect(wrapper.get('[data-testid="up-outcome-committed"]').text()).toContain('Committed as revision 5')
    expect(wrapper.find('[data-testid="rpc-failure"]').exists()).toBe(false)
    wrapper.unmount()
  })

  it('stops after Cancel while hashing and sends nothing', async () => {
    const { wrapper } = await mountUpload([saved])
    await wrapper.get('[data-testid="up-start"]').trigger('click')
    await wrapper.get('[data-testid="up-cancel"]').trigger('click')
    await settle()
    expect(FakeUploadRequest.sent).toHaveLength(0)
    wrapper.unmount()
  })

  it('asks nothing more of the server once the screen is gone', async () => {
    const { wrapper, outcomes } = await mountUpload([() => 'hang'])
    await wrapper.get('[data-testid="up-start"]').trigger('click')
    await settle()
    expect(FakeUploadRequest.sent).toHaveLength(1)
    wrapper.unmount()
    await settle()
    await new Promise((resolve) => setTimeout(resolve, 50))
    expect(FakeUploadRequest.sent[0].aborted).toBe(true)
    expect(outcomes).toHaveLength(0)
  })

  it('shows a 401 as a refusal with a Sign in button instead of navigating away', async () => {
    const { wrapper, signIn } = await mountUpload([refusal(401, Code.Unauthenticated, 'Unauthorized')])
    await wrapper.get('[data-testid="up-start"]').trigger('click')
    await settle()
    expect(signIn).not.toHaveBeenCalled()
    expect(wrapper.get('[data-testid="rpc-failure"]').text()).toContain('UNAUTHENTICATED')
    await wrapper.get('[data-testid="up-sign-in"]').trigger('click')
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

  it("tells a signed-in session whether its binding's writer scopes cover the upload", async () => {
    const status = async () =>
      create(ConsoleSessionStatusSchema, {
        authenticated: true,
        principal: 'ada',
        accountIds: ['acct-a'],
        intakeWriters: [
          create(IntakeWriterScopeSchema, { accountId: 'acct-a', datasourceId: 'ds-files', allowCreate: true }),
          create(IntakeWriterScopeSchema, { accountId: 'acct-a', datasourceId: 'ds-mail' }),
        ],
      })
    const { wrapper } = await mountRepositoryView(RepositoryUploadView, '/repository/upload?account=acct-a', repository([]), {
      [REPOSITORY_SESSION as symbol]: { status },
    })
    expect(wrapper.find('[data-testid="up-writer"]').exists()).toBe(false)
    await wrapper.get('[data-testid="up-datasource"] input').setValue('ds-files')
    expect(wrapper.get('[data-testid="up-writer"]').text()).toContain('may save as ds-files (create and update)')
    await wrapper.get('[data-testid="up-datasource"] input').setValue('ds-mail')
    const refused = wrapper.get('[data-testid="up-writer"]').text()
    expect(refused).toContain('no writer scope for this new document')
    expect(refused).toContain('WRITE_DENIED')
    expect(refused).toContain('ds-files (create and update); ds-mail (update only)')
    await wrapper.get('[data-testid="up-expected"] input').setValue('3')
    expect(wrapper.get('[data-testid="up-writer"]').text()).toContain('may save as ds-mail (update only)')
    wrapper.unmount()
  })

  it('names a session status read that failed instead of hiding it', async () => {
    const status = async () => {
      throw new Error('/console/session answered 503: sign-in is not configured')
    }
    const { wrapper } = await mountRepositoryView(RepositoryUploadView, '/repository/upload?account=acct-a', repository([]), {
      [REPOSITORY_SESSION as symbol]: { status },
    })
    expect(wrapper.get('[data-testid="up-session-failure"]').text()).toContain('sign-in is not configured')
    wrapper.unmount()
  })
})
