// @vitest-environment jsdom
import { create, toBinary } from '@bufbuild/protobuf'
import { Code } from '@connectrpc/connect'
import { afterEach, describe, expect, it } from 'vitest'
import { ArchiveService, GetEntryManifestResponseSchema, ListVersionsResponseSchema, UploadRenditionResponseSchema } from '@/gen/ai/protomolt/proto/repo/archive/v1/archive_service_pb'
import { EntryInfoSchema, VersionManifestSchema } from '@/gen/ai/protomolt/proto/repo/archive/v1/entry_pb'
import { StatusSchema } from '@/gen/google/rpc/status_pb'
import { REPOSITORY_UPLOAD_REQUEST } from '@/repository/clients'
import { FakeUploadRequest, type FakeAnswer } from '@/repository/repositoryTestKit'
import { mountRepositoryView, settle } from '@/repository/repositoryViewKit'
import RepositoryArchiveEntryView from './RepositoryArchiveEntryView.vue'

const ENTRY = { name: 'repository-archive-entry', query: { account: 'acct-a', archive: 'papers', entry: 'e-1' } }
const saved = (r: FakeUploadRequest): FakeAnswer => ({ status: 200, headers: { 'content-type': 'application/protobuf' },
  body: toBinary(UploadRenditionResponseSchema, create(UploadRenditionResponseSchema, {
    operationId: r.params().get('operation_id')!, version: 4n, replayed: true,
  })),
})
const refusal = (code: Code): FakeAnswer => ({ status: code === Code.ResourceExhausted ? 429 : 400,
  headers: { 'content-type': 'application/protobuf' },
  body: toBinary(StatusSchema, create(StatusSchema, { code, message: 'route refusal' })),
})

async function mountUpload(first: FakeAnswer | 'network-error' | 'hang') {
  let calls = 0
  const mounted = await mountRepositoryView(RepositoryArchiveEntryView, ENTRY, (r) => r.service(ArchiveService, {
    getEntryManifest: (request) => create(GetEntryManifestResponseSchema, {
      info: create(EntryInfoSchema, { address: request.address, currentVersion: 3n }),
      manifest: create(VersionManifestSchema, { address: request.address, version: 3n }),
    }),
    listVersions: () => create(ListVersionsResponseSchema),
  }), { [REPOSITORY_UPLOAD_REQUEST as symbol]: () => new FakeUploadRequest(r => ++calls === 1 ? first : saved(r)) })
  await mounted.wrapper.get('[data-testid="toggle-rendition-upload"]').trigger('click')
  const input = mounted.wrapper.get('[data-testid="rendition-up-file"]')
  Object.defineProperty(input.element, 'files', { value: [new File(['original bytes'], 'original.txt', { type: 'text/plain' })] })
  await input.trigger('change')
  return mounted
}
afterEach(() => { FakeUploadRequest.sent = [] })

describe('rendition upload intent', () => {
  it.each(['network-error', 'capacity', 'malformed'] as const)('retries the exact upload after %s, including navigation away and back', async (failure) => {
    const first = failure === 'capacity' ? refusal(Code.ResourceExhausted) : failure === 'malformed'
      ? { status: 200, headers: { 'content-type': 'application/protobuf' }, body: new Uint8Array([255]) } : failure
    const { wrapper, router } = await mountUpload(first)
    await wrapper.get('[data-testid="rendition-up-submit"]').trigger('click')
    await settle()
    expect(FakeUploadRequest.sent).toHaveLength(1)
    expect(wrapper.get('[data-testid="rendition-upload-fields"]').attributes('disabled')).toBeDefined()
    await router.push({ ...ENTRY, query: { ...ENTRY.query, entry: 'e-2' } })
    await settle()
    expect(wrapper.find('[data-testid="rendition-upload-committed"]').exists()).toBe(false)
    await router.push(ENTRY)
    await settle()
    await wrapper.get('[data-testid="toggle-rendition-upload"]').trigger('click')
    // Even a programmatic model edit cannot retarget the retained request.
    ;(wrapper.vm as unknown as { uploadForm: { rendition: string } }).uploadForm.rendition = 'changed'
    await wrapper.get('[data-testid="rendition-up-submit"]').trigger('click')
    await settle()
    const [a, b] = FakeUploadRequest.sent
    expect(b.url).toBe(a.url)
    expect(b.body).toBe(a.body)
    expect(b.headers).toEqual(a.headers)
    expect(wrapper.get('[data-testid="rendition-upload-committed"]').text()).toContain('replayed')
    expect(wrapper.get('[data-testid="rendition-upload-fields"]').attributes('disabled')).toBeUndefined()
    wrapper.unmount()
  })

  it('releases a definitely rejected upload so a corrected rendition uses a fresh operation', async () => {
    const { wrapper } = await mountUpload(refusal(Code.InvalidArgument))
    await wrapper.get('[data-testid="rendition-up-submit"]').trigger('click')
    await settle()
    expect(wrapper.get('[data-testid="rendition-upload-fields"]').attributes('disabled')).toBeUndefined()
    await wrapper.get('[data-testid="rendition-up-name"] input').setValue('corrected')
    await wrapper.get('[data-testid="rendition-up-submit"]').trigger('click')
    await settle()
    const [a, b] = FakeUploadRequest.sent
    expect(b.params().get('operation_id')).not.toBe(a.params().get('operation_id'))
    expect(b.params().get('rendition')).toBe('corrected')
    wrapper.unmount()
  })

  it('keeps an interrupted send pinned to its original entry', async () => {
    const { wrapper, router } = await mountUpload('hang')
    await wrapper.get('[data-testid="rendition-up-submit"]').trigger('click')
    await settle()
    await router.push({ ...ENTRY, query: { ...ENTRY.query, account: 'acct-b' } })
    await settle()
    expect(FakeUploadRequest.sent[0].aborted).toBe(true)
    await router.push(ENTRY)
    await settle()
    await wrapper.get('[data-testid="toggle-rendition-upload"]').trigger('click')
    await wrapper.get('[data-testid="rendition-up-submit"]').trigger('click')
    await settle()
    expect(FakeUploadRequest.sent[1].url).toBe(FakeUploadRequest.sent[0].url)
    wrapper.unmount()
  })
})
