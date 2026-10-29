// @vitest-environment jsdom
import { createHash } from 'node:crypto'
import { create, toBinary } from '@bufbuild/protobuf'
import { Code, ConnectError, type ConnectRouter } from '@connectrpc/connect'
import { afterEach, describe, expect, it, vi } from 'vitest'
import {
  BlobBagSchema,
  BlobSchema,
  BlobsSchema,
  DocumentSchema,
  SearchMetadataSchema,
  file_ai_protomolt_proto_repo_v1_document,
} from '@/gen/ai/protomolt/proto/repo/v1/document_pb'
import { RepositorySchemaAssetReferenceSchema } from '@/gen/ai/protomolt/proto/repo/v1/schema_asset_reference_pb'
import { ChangeKind, PartChangeSchema } from '@/gen/ai/protomolt/proto/repo/v2/events_pb'
import {
  BlobHeaderSchema,
  DocumentService,
  GetDocumentResponseSchema,
  GetManifestResponseSchema,
  GetSchemaDescriptorResponseSchema,
  ListRevisionsResponseSchema,
  PartDataSchema,
  ReadBlobResponseSchema,
  ReadCompleteSchema,
  ReadDocumentResponseSchema,
  RevisionSummarySchema,
  type GetManifestRequest,
  type ReadBlobRequest,
} from '@/gen/ai/protomolt/proto/repo/v2/document_service_pb'
import {
  DocumentManifestSchema,
  DocumentPart,
  PartManifestEntrySchema,
  PartSchemaBindingSchema,
  PartSelectorSchema,
  PartState,
  RevisionValidationSchema,
  SaveOwnerSchema,
  ValidationStatus,
} from '@/gen/ai/protomolt/proto/repo/v2/manifest_pb'
import RepositoryDocumentView from '@/views/repository/RepositoryDocumentView.vue'
import { at, descriptorSetOf, testAddress } from '@/repository/repositoryTestKit'
import { mountRepositoryView, settle } from '@/repository/repositoryViewKit'

const CORE = create(PartSelectorSchema, { selector: { case: 'part', value: DocumentPart.CORE } })
const BLOBS = create(PartSelectorSchema, { selector: { case: 'part', value: DocumentPart.BLOBS } })
const owner = create(SaveOwnerSchema, { owner: { case: 'intake', value: { datasourceId: 'ds-files' } } })

const coreBytes = toBinary(
  DocumentSchema,
  create(DocumentSchema, { docId: 'doc-1', searchMetadata: create(SearchMetadataSchema, { title: 'Quarterly report' }) }),
)
const DESCRIPTORS = descriptorSetOf(file_ai_protomolt_proto_repo_v1_document)
const DESCRIPTORS_SHA = createHash('sha256').update(DESCRIPTORS).digest('hex')
const TEXT = new TextEncoder().encode('line one\nline two\n'.repeat(4000))
const TEXT_SHA = createHash('sha256').update(TEXT).digest('hex')
const PDF = new Uint8Array(Array.from({ length: 300_000 }, (_, i) => (i * 13) & 0xff))
const PDF_SHA = createHash('sha256').update(PDF).digest('hex')

const SMALL = new TextEncoder().encode('twelve bytes')
const FILES: Record<string, { bytes: Uint8Array; sha: string; mime: string; name: string }> = {
  'blob-txt': { bytes: TEXT, sha: TEXT_SHA, mime: 'text/plain', name: 'notes.txt' },
  'blob-pdf': { bytes: PDF, sha: PDF_SHA, mime: 'application/pdf', name: 'report.pdf' },
  'blob-small': { bytes: SMALL, sha: createHash('sha256').update(SMALL).digest('hex'), mime: 'text/plain', name: 'small.txt' },
}

function validation() {
  return create(RevisionValidationSchema, {
    status: ValidationStatus.TYPED_VALID,
    candidateSha256: 'c'.repeat(64),
    validatorProfile: 'house-v1',
    partSchemas: [
      create(PartSchemaBindingSchema, {
        part: CORE,
        fragmentSchema: create(RepositorySchemaAssetReferenceSchema, {
          typeUrl: 'type.googleapis.com/ai.protomolt.proto.repo.v1.Document',
          descriptorSha256: DESCRIPTORS_SHA,
        }),
      }),
    ],
  })
}

/** A repository holding one document at revisions 1 to 3 (3 is current). */
function repository(seen: { manifests: GetManifestRequest[]; blobs: ReadBlobRequest[]; holdPartReads?: boolean }) {
  return (router: ConnectRouter) =>
    router.service(DocumentService, {
      getManifest(request) {
        seen.manifests.push(request)
        const selector = request.revision?.selector
        const revision = selector?.case === 'revision' ? selector.value : 3n
        if (revision > 3n) throw new ConnectError(`doc-1 has no revision ${revision}`, Code.NotFound)
        return create(GetManifestResponseSchema, {
          manifest: create(DocumentManifestSchema, {
            address: testAddress(),
            revision,
            parts: [
              create(PartManifestEntrySchema, { selector: CORE, state: PartState.PRESENT, sizeBytes: BigInt(coreBytes.length), sha256: 'a'.repeat(64), writtenInRevision: revision, owner }),
              create(PartManifestEntrySchema, { selector: BLOBS, state: PartState.PRESENT, sizeBytes: 90n, sha256: 'b'.repeat(64), writtenInRevision: 1n, owner }),
            ],
          }),
          validation: validation(),
        })
      },
      getDocument(request) {
        const selector = request.revision?.selector
        if (selector?.case === 'revision' && selector.value === 2n) {
          throw new ConnectError('stack read failed', Code.Unavailable)
        }
        return create(GetDocumentResponseSchema, {
          document: create(DocumentSchema, {
            blobBag: create(BlobBagSchema, {
              blobData: {
                case: 'blobs',
                value: create(BlobsSchema, {
                  blob: Object.entries(FILES).map(([blobId, f]) =>
                    create(BlobSchema, { blobId, filename: f.name, mimeType: f.mime, sizeBytes: BigInt(f.bytes.length) }),
                  ),
                }),
              },
            }),
          }),
          manifest: create(DocumentManifestSchema, { address: testAddress(), revision: 3n }),
          validation: validation(),
          metadata: { 'request.source': 'console-test', 'batch': '7' },
        })
      },
      listRevisions() {
        return create(ListRevisionsResponseSchema, {
          revisions: [3n, 2n, 1n].map((revision) =>
            create(RevisionSummarySchema, {
              revision,
              previousRevision: revision - 1n,
              committedAt: at(`2026-10-0${revision}T00:00:00Z`),
              operationId: `op-${revision}`,
              owner,
              validationStatus: ValidationStatus.TYPED_VALID,
              changes: [create(PartChangeSchema, { selector: CORE, kind: revision === 1n ? ChangeKind.ADDED : ChangeKind.REPLACED, sha256: 'a'.repeat(64) })],
            }),
          ),
        })
      },
      async *readDocument(request) {
        if (seen.holdPartReads) await new Promise((resolve) => setTimeout(resolve, 20))
        void request
        yield create(ReadDocumentResponseSchema, {
          frame: {
            case: 'manifest',
            value: create(DocumentManifestSchema, {
              address: testAddress(),
              revision: 3n,
              parts: [create(PartManifestEntrySchema, { selector: CORE, state: PartState.PRESENT, writtenInRevision: 3n, owner, contentIncluded: true })],
            }),
          },
        })
        yield create(ReadDocumentResponseSchema, { frame: { case: 'partData', value: create(PartDataSchema, { selector: CORE, offset: 0n, data: coreBytes, last: true }) } })
        yield create(ReadDocumentResponseSchema, { frame: { case: 'complete', value: create(ReadCompleteSchema) } })
      },
      getSchemaDescriptor(request) {
        return create(GetSchemaDescriptorResponseSchema, { revision: 3n, descriptorSha256: request.descriptorSha256, descriptorSet: DESCRIPTORS })
      },
      async *readBlob(request) {
        seen.blobs.push(request)
        const file = FILES[request.blobId]
        if (!file) throw new ConnectError(`no blob ${request.blobId}`, Code.NotFound)
        const total = BigInt(file.bytes.length)
        // The server's own range rule (DocumentStreams.prepareBlobRead).
        if (request.offset > total || request.length > total - request.offset) {
          throw new ConnectError(`Requested byte range [${request.offset}, +${request.length}) is outside blob length ${total}`, Code.InvalidArgument)
        }
        yield create(ReadBlobResponseSchema, {
          frame: { case: 'header', value: create(BlobHeaderSchema, { revision: 3n, totalLength: BigInt(file.bytes.length), sha256: file.sha, mimeType: file.mime, filename: file.name }) },
        })
        const start = Number(request.offset)
        const end = request.length === 0n ? file.bytes.length : Math.min(file.bytes.length, start + Number(request.length))
        for (let i = start; i < end; i += 32768) {
          yield create(ReadBlobResponseSchema, { frame: { case: 'data', value: file.bytes.subarray(i, Math.min(end, i + 32768)) } })
        }
      },
    })
}

const DOC = { name: 'repository-document', query: { account: 'acct-a', graph: 'intake:acct-a', node: 'ds-files', doc: 'doc-1' } }

function seenCalls() {
  return { manifests: [] as GetManifestRequest[], blobs: [] as ReadBlobRequest[], holdPartReads: false }
}

afterEach(() => {
  vi.restoreAllMocks()
  delete (globalThis as Record<string, unknown>).showSaveFilePicker
})

describe('RepositoryDocumentView', () => {
  it('shows the manifest parts, metadata and validation of the current revision, pinning it', async () => {
    const seen = seenCalls()
    const { wrapper } = await mountRepositoryView(RepositoryDocumentView, DOC, repository(seen))
    expect(seen.manifests[0].revision?.selector).toEqual({ case: 'current', value: true })
    expect(wrapper.get('[data-testid="revision-chip"]').text()).toBe('revision 3 (current)')
    const parts = wrapper.get('[data-testid="parts-table"]').text()
    expect(parts).toContain('CORE')
    expect(parts).toContain('PRESENT')
    expect(parts).toContain('a'.repeat(64))
    expect(parts).toContain('intake ds-files')
    const tabs = wrapper.findAll('.v-tab')
    await tabs.find((t) => t.text() === 'Metadata')!.trigger('click')
    await settle()
    expect(wrapper.get('[data-testid="metadata-table"]').text()).toContain('request.source')
    await tabs.find((t) => t.text() === 'Validation')!.trigger('click')
    await settle()
    const v = wrapper.get('[data-testid="validation-view"]').text()
    expect(v).toContain('TYPED_VALID')
    expect(v).toContain('CORE fragment')
    expect(v).toContain(DESCRIPTORS_SHA)
    wrapper.unmount()
  })

  it('reads and decodes the CORE part with the descriptor retained by digest', async () => {
    const { wrapper } = await mountRepositoryView(RepositoryDocumentView, DOC, repository(seenCalls()))
    await wrapper.get('[data-testid="read-part-0"]').trigger('click')
    await settle()
    const view = wrapper.get('[data-testid="part-view"]')
    expect(view.get('[data-testid="decoded-with"]').text()).toContain(DESCRIPTORS_SHA)
    expect(view.text()).toContain('doc_id')
    expect(view.text()).toContain('"Quarterly report"')
    wrapper.unmount()
  })

  it('lists history with part changes and opens an exact revision', async () => {
    const seen = seenCalls()
    const { wrapper, router } = await mountRepositoryView(RepositoryDocumentView, DOC, repository(seen))
    await wrapper.get('[data-testid="tab-history"]').trigger('click')
    await settle()
    const history = wrapper.get('[data-testid="history"]').text()
    expect(history).toContain('op-2')
    expect(history).toContain('ADDED CORE')
    expect(history).toContain('REPLACED CORE')
    await wrapper.get('[data-testid="open-revision-2"]').trigger('click')
    await settle()
    expect(router.currentRoute.value.query.revision).toBe('2')
    expect(seen.manifests[seen.manifests.length - 1].revision?.selector).toEqual({ case: 'revision', value: 2n })
    expect(wrapper.get('[data-testid="revision-chip"]').text()).toBe('revision 2')
    wrapper.unmount()
  })

  it('shows NOT_FOUND for an exact revision that does not exist, never the current one', async () => {
    const { wrapper } = await mountRepositoryView(RepositoryDocumentView, { ...DOC, query: { ...DOC.query, revision: '9' } }, repository(seenCalls()))
    expect(wrapper.get('[data-testid="rpc-failure"]').text()).toContain('NOT_FOUND')
    expect(wrapper.get('[data-testid="rpc-failure"]').text()).toContain('doc-1 has no revision 9')
    expect(wrapper.find('[data-testid="parts-table"]').exists()).toBe(false)
    wrapper.unmount()
  })

  it('refuses an incomplete call number in the route by name', async () => {
    const { wrapper } = await mountRepositoryView(RepositoryDocumentView, { ...DOC, query: { account: 'acct-a', doc: 'doc-1' } }, () => {})
    expect(wrapper.get('[data-testid="route-error"]').text()).toContain('Graph ID is blank')
    wrapper.unmount()
  })

  it('downloads a byte range of the pinned revision and says it is not checked against the whole-file digest', async () => {
    const seen = seenCalls()
    const saved: Blob[] = []
    vi.spyOn(URL, 'createObjectURL').mockImplementation((b) => {
      saved.push(b as Blob)
      return 'blob:test'
    })
    vi.spyOn(URL, 'revokeObjectURL').mockImplementation(() => {})
    vi.spyOn(HTMLAnchorElement.prototype, 'click').mockImplementation(() => {})
    const { wrapper } = await mountRepositoryView(RepositoryDocumentView, DOC, repository(seen))
    await wrapper.get('[data-testid="tab-files"]').trigger('click')
    await settle()
    await wrapper.get('[data-testid="select-blob-blob-pdf"]').trigger('click')
    await settle()
    await wrapper.get('[data-testid="range-offset"] input').setValue('1000')
    await wrapper.get('[data-testid="range-length"] input').setValue('100000')
    await wrapper.get('[data-testid="download"]').trigger('click')
    await settle()
    const request = seen.blobs[seen.blobs.length - 1]
    expect(request).toMatchObject({ blobId: 'blob-pdf', offset: 1000n, length: 100000n })
    expect(request.revision?.selector).toEqual({ case: 'revision', value: 3n })
    expect(wrapper.get('[data-testid="verification"]').text()).toContain('100000 bytes of a byte range')
    expect(wrapper.get('[data-testid="verification"]').text()).toContain(PDF_SHA)
    expect(new Uint8Array(await saved[0].arrayBuffer())).toEqual(PDF.subarray(1000, 101000))
    wrapper.unmount()
  })

  it('streams a full download to the chosen file and shows the verified SHA-256', async () => {
    const written: Uint8Array[] = []
    let closed = false
    ;(globalThis as Record<string, unknown>).showSaveFilePicker = async () => ({
      createWritable: async () => ({
        write: async (chunk: Uint8Array) => void written.push(chunk.slice()),
        close: async () => void (closed = true),
        abort: async () => {},
      }),
    })
    const { wrapper } = await mountRepositoryView(RepositoryDocumentView, DOC, repository(seenCalls()))
    await wrapper.get('[data-testid="tab-files"]').trigger('click')
    await settle()
    await wrapper.get('[data-testid="select-blob-blob-pdf"]').trigger('click')
    await settle()
    expect(wrapper.get('[data-testid="files"]').text()).toContain('straight to the file you choose')
    await wrapper.get('[data-testid="download"]').trigger('click')
    await settle()
    expect(wrapper.get('[data-testid="verification"]').text()).toContain(`hash to the stored SHA-256 ${PDF_SHA}`)
    expect(closed).toBe(true)
    expect(written.length).toBe(Math.ceil(PDF.length / 32768))
    expect(new Uint8Array(await new Blob(written as Uint8Array<ArrayBuffer>[]).arrayBuffer())).toEqual(PDF)
    wrapper.unmount()
  })

  it('previews the start of a text file as text', async () => {
    const { wrapper } = await mountRepositoryView(RepositoryDocumentView, DOC, repository(seenCalls()))
    await wrapper.get('[data-testid="tab-files"]').trigger('click')
    await settle()
    await wrapper.get('[data-testid="select-blob-blob-txt"]').trigger('click')
    await settle()
    await wrapper.get('[data-testid="preview"]').trigger('click')
    await settle()
    expect(wrapper.get('[data-testid="preview-text"]').text()).toContain('line one')
    expect(wrapper.get('[data-testid="verification"]').text()).toContain('65536 bytes of a byte range')
    wrapper.unmount()
  })

  it('previews a file shorter than the preview whole instead of asking past its end', async () => {
    const seen = seenCalls()
    const { wrapper } = await mountRepositoryView(RepositoryDocumentView, DOC, repository(seen))
    await wrapper.get('[data-testid="tab-files"]').trigger('click')
    await settle()
    await wrapper.get('[data-testid="select-blob-blob-small"]').trigger('click')
    await settle()
    await wrapper.get('[data-testid="preview"]').trigger('click')
    await settle()
    expect(wrapper.find('[data-testid="rpc-failure"]').exists()).toBe(false)
    expect(wrapper.get('[data-testid="preview-text"]').text()).toBe('twelve bytes')
    expect(seen.blobs[seen.blobs.length - 1].length).toBe(0n)
    wrapper.unmount()
  })

  it('drops the previous revision\'s metadata and files when the next one cannot be read', async () => {
    const { wrapper, router } = await mountRepositoryView(RepositoryDocumentView, DOC, repository(seenCalls()))
    const tabs = () => wrapper.findAll('.v-tab')
    await tabs().find((t) => t.text() === 'Metadata')!.trigger('click')
    await settle()
    expect(wrapper.get('[data-testid="metadata-table"]').text()).toContain('request.source')
    await router.push({ ...DOC, query: { ...DOC.query, revision: '2' } })
    await settle()
    expect(wrapper.find('[data-testid="metadata-table"]').exists()).toBe(false)
    expect(wrapper.text()).toContain('stack read failed')
    await tabs().find((t) => t.text() === 'Files')!.trigger('click')
    await settle()
    expect(wrapper.find('[data-testid^="blob-blob-"]').exists()).toBe(false)
    wrapper.unmount()
  })

  it('discards a part read still running when another revision opens', async () => {
    const seen = seenCalls()
    seen.holdPartReads = true
    const { wrapper, router } = await mountRepositoryView(RepositoryDocumentView, DOC, repository(seen))
    await wrapper.get('[data-testid="read-part-0"]').trigger('click')
    await router.push({ ...DOC, query: { ...DOC.query, revision: '1' } })
    await settle()
    await new Promise((resolve) => setTimeout(resolve, 40))
    await settle()
    expect(wrapper.get('[data-testid="revision-chip"]').text()).toBe('revision 1')
    expect(wrapper.find('[data-testid="part-view"]').exists()).toBe(false)
    wrapper.unmount()
  })

  it('shows a save dialog failure instead of dropping it', async () => {
    ;(globalThis as Record<string, unknown>).showSaveFilePicker = async () => {
      throw new DOMException('not allowed here', 'SecurityError')
    }
    const { wrapper } = await mountRepositoryView(RepositoryDocumentView, DOC, repository(seenCalls()))
    await wrapper.get('[data-testid="tab-files"]').trigger('click')
    await settle()
    await wrapper.get('[data-testid="select-blob-blob-pdf"]').trigger('click')
    await settle()
    await wrapper.get('[data-testid="download"]').trigger('click')
    await settle()
    expect(wrapper.get('[data-testid="rpc-failure"]').text()).toContain('not allowed here')
    wrapper.unmount()
  })

  it('opens directly on the tab named in the route query, updates the query on tab switches, inspects a retained schema descriptor, and jumps to its part', async () => {
    const seen = seenCalls()
    const { wrapper, router } = await mountRepositoryView(
      RepositoryDocumentView,
      { ...DOC, query: { ...DOC.query, tab: 'validation' } },
      repository(seen),
    )
    expect(wrapper.get('[data-testid="validation-view"]').text()).toContain('TYPED_VALID')
    expect(router.currentRoute.value.query.tab).toBe('validation')

    await wrapper.get('[data-testid="inspect-schema-0"]').trigger('click')
    await settle()
    const descriptorView = wrapper.get('[data-testid="validation-descriptor-view"]')
    expect(descriptorView.text()).toContain('Retained descriptor: CORE fragment')
    expect(descriptorView.text()).toContain('ai.protomolt.proto.repo.v1.Document')
    expect(descriptorView.get('[data-testid="validation-descriptor-verified"]').text()).toContain('SHA-256 verified')

    await wrapper.get('[data-testid="open-schema-part-0"]').trigger('click')
    await settle()
    expect(router.currentRoute.value.query.tab).toBeUndefined()
    const partView = wrapper.get('[data-testid="part-view"]')
    expect(partView.text()).toContain('"Quarterly report"')

    await wrapper.get('[data-testid="part-tree-filter"] input').setValue('Quarterly')
    await settle()
    expect(wrapper.get('[data-testid="part-tree-match-count"]').text()).toBe('1 match')
    expect(wrapper.get('[data-testid="part-decoded"]').text()).toContain('"Quarterly report"')
    expect(wrapper.get('[data-testid="part-decoded"]').text()).not.toContain('"doc-1"')

    await wrapper.get('[data-testid="part-tree-filter"] input').setValue('no-such-field-value')
    await settle()
    expect(wrapper.get('[data-testid="part-tree-no-matches"]').text()).toContain('No decoded fields or values match "no-such-field-value".')
    wrapper.unmount()
  })
})
