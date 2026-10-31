// @vitest-environment jsdom
import { createHash } from 'node:crypto'
import { create, toBinary } from '@bufbuild/protobuf'
import { Code, ConnectError, type ConnectRouter } from '@connectrpc/connect'
import { describe, expect, it } from 'vitest'
import { DocumentSchema, SearchMetadataSchema, file_ai_protomolt_proto_repo_v1_document } from '@/gen/ai/protomolt/proto/repo/v1/document_pb'
import { RepositorySchemaAssetReferenceSchema } from '@/gen/ai/protomolt/proto/repo/v1/schema_asset_reference_pb'
import {
  DocumentService,
  GetDocumentResponseSchema,
  GetManifestResponseSchema,
  GetSchemaDescriptorResponseSchema,
  ListLinksResponseSchema,
  ListRevisionsResponseSchema,
  PartDataSchema,
  ReadCompleteSchema,
  ReadDocumentResponseSchema,
  ResolvedLinkSchema,
  RevisionSummarySchema,
  StaticLinkStateSchema,
  type GetManifestRequest,
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
import { at, descriptorSetOf, testAddress } from '@/repository/repositoryTestKit'
import { mountRepositoryView, settle } from '@/repository/repositoryViewKit'
import RepositoryDocumentView from './RepositoryDocumentView.vue'

const CORE = create(PartSelectorSchema, { selector: { case: 'part', value: DocumentPart.CORE } })
const PARSED = create(PartSelectorSchema, { selector: { case: 'part', value: DocumentPart.PARSED } })
const owner = create(SaveOwnerSchema, { owner: { case: 'intake', value: { datasourceId: 'ds-files' } } })
const DESCRIPTORS = descriptorSetOf(file_ai_protomolt_proto_repo_v1_document)
const DESCRIPTORS_SHA = createHash('sha256').update(DESCRIPTORS).digest('hex')

/** CORE bytes per revision: the title changes at revision 3 and a category appears. */
const CORES: Record<string, Uint8Array> = {
  '1': toBinary(DocumentSchema, create(DocumentSchema, { docId: 'doc-1', searchMetadata: create(SearchMetadataSchema, { title: 'Draft', language: 'en' }) })),
  '3': toBinary(
    DocumentSchema,
    create(DocumentSchema, { docId: 'doc-1', searchMetadata: create(SearchMetadataSchema, { title: 'Final', language: 'en', category: 'report' }) }),
  ),
}

function manifest(revision: bigint) {
  const core = CORES[revision.toString()]
  return create(DocumentManifestSchema, {
    address: testAddress(),
    revision,
    committedAt: at(`2026-10-0${revision}T00:00:00Z`),
    parts: [
      create(PartManifestEntrySchema, {
        selector: CORE,
        state: PartState.PRESENT,
        sizeBytes: BigInt(core.length),
        sha256: createHash('sha256').update(core).digest('hex'),
        writtenInRevision: revision,
        owner,
        contentIncluded: true,
      }),
      // PARSED is present at 1 and deleted at 3.
      revision === 1n
        ? create(PartManifestEntrySchema, { selector: PARSED, state: PartState.PRESENT, sizeBytes: 0n, writtenInRevision: 1n, owner, contentIncluded: true })
        : create(PartManifestEntrySchema, { selector: PARSED, state: PartState.DELETED, deletedReason: 'reparse requested', writtenInRevision: 3n, owner }),
    ],
  })
}

function validation() {
  return create(RevisionValidationSchema, {
    status: ValidationStatus.TYPED_VALID,
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

const selected = (r: { revision?: { selector: { case?: string; value?: unknown } } }) => {
  const s = r.revision?.selector
  return s?.case === 'revision' ? (s.value as bigint) : 3n
}

function repository(seen: { manifests: GetManifestRequest[] }, options: { failLinksAt?: bigint } = {}) {
  return (router: ConnectRouter) =>
    router.service(DocumentService, {
      getManifest(request) {
        seen.manifests.push(request)
        const revision = selected(request)
        if (CORES[revision.toString()] === undefined) throw new ConnectError(`doc-1 has no revision ${revision}`, Code.NotFound)
        return create(GetManifestResponseSchema, { manifest: manifest(revision), validation: validation() })
      },
      getDocument: () => create(GetDocumentResponseSchema, { validation: validation() }),
      listRevisions: () =>
        create(ListRevisionsResponseSchema, {
          revisions: [3n, 1n].map((revision) =>
            create(RevisionSummarySchema, { revision, previousRevision: revision === 3n ? 1n : 0n, committedAt: at('2026-10-01T00:00:00Z'), owner }),
          ),
        }),
      getSchemaDescriptor: () => create(GetSchemaDescriptorResponseSchema, { descriptorSet: DESCRIPTORS }),
      async *readDocument(request) {
        const revision = selected(request)
        const wanted = request.selection?.selectors[0]
        const isCore = wanted?.selector.case === 'part' && wanted.selector.value === DocumentPart.CORE
        const data = isCore ? CORES[revision.toString()] : new Uint8Array()
        yield create(ReadDocumentResponseSchema, { frame: { case: 'manifest', value: manifest(revision) } })
        yield create(ReadDocumentResponseSchema, { frame: { case: 'partData', value: create(PartDataSchema, { selector: wanted, offset: 0n, data, last: true }) } })
        yield create(ReadDocumentResponseSchema, { frame: { case: 'complete', value: create(ReadCompleteSchema) } })
      },
      listLinks(request) {
        const revision = selected(request)
        if (revision === options.failLinksAt) throw new ConnectError('links index unavailable', Code.Unavailable)
        const target = revision === 1n ? 'doc-old' : 'doc-new'
        return create(ListLinksResponseSchema, {
          revision,
          links: [
            create(ResolvedLinkSchema, {
              fieldPath: 'structured_data.cites',
              relation: 'references',
              target: { case: 'staticTarget', value: create(StaticLinkStateSchema, { address: testAddress({ docId: target }) }) },
            }),
          ],
        })
      },
    })
}

const DOC = '/repository/document?account=acct-a&graph=intake:acct-a&node=ds-files&doc=doc-1'

describe('RepositoryDocumentView compare', () => {
  it('opens ?revision=3&compare=1 on the comparison of both exact revisions', async () => {
    const seen = { manifests: [] as GetManifestRequest[] }
    const { wrapper } = await mountRepositoryView(RepositoryDocumentView, `${DOC}&revision=3&compare=1`, repository(seen))
    expect(wrapper.find('[data-testid="compare"]').exists()).toBe(true)
    const revisions = seen.manifests.map(selected)
    expect(revisions).toContain(1n)
    expect(revisions.filter((r) => r === 3n).length).toBeGreaterThanOrEqual(1)
    expect(wrapper.get('[data-testid="compare-heading"]').text()).toContain('revision 1')
    expect(wrapper.get('[data-testid="compare-heading"]').text()).toContain('revision 3')

    const core = wrapper.get('[data-testid="compare-part-CORE"]').text()
    expect(core).toContain('changed')
    expect(core).toContain('sizeBytes, sha256, writtenInRevision')
    const parsed = wrapper.get('[data-testid="compare-part-PARSED"]').text()
    expect(parsed).toContain('state, writtenInRevision, deletedReason')
    expect(parsed).toContain('DELETED: reparse requested')

    const metadata = wrapper.get('[data-testid="compare-metadata"]').text()
    expect(metadata).toContain('search_metadata.title = "Draft"')
    expect(metadata).toContain('search_metadata.title = "Final"')
    expect(metadata).toContain('search_metadata.category = "report"')
    expect(wrapper.get('[data-testid="compare-metadata-added"]').text()).toBe('+2')
    expect(wrapper.get('[data-testid="compare-metadata-removed"]').text()).toBe('−1')
    expect(wrapper.get('[data-testid="compare-concepts-same"]').text()).toContain('concept tags are the same')
    const links = wrapper.get('[data-testid="compare-links"]').text()
    expect(links).toContain('doc-old')
    expect(links).toContain('doc-new')
    // PARSED is raw at 1 (no schema bound) and deleted at 3: the section says both.
    expect(wrapper.text()).toContain('PARSED is DELETED at revision 3.')
    expect(wrapper.get('[data-testid="compare-parsed"]').text()).toContain('(raw) Revision 1 binds no fragment schema to PARSED.')
    expect(wrapper.find('[data-testid="rpc-failure"]').exists()).toBe(false)
    wrapper.unmount()
  })

  it('starts, swaps and closes a comparison through the route', async () => {
    const seen = { manifests: [] as GetManifestRequest[] }
    const { wrapper, router } = await mountRepositoryView(RepositoryDocumentView, DOC, repository(seen))
    expect(wrapper.find('[data-testid="tab-compare"]').exists()).toBe(false)
    await wrapper.get('[data-testid="tab-history"]').trigger('click')
    await settle()
    expect(wrapper.find('[data-testid="compare-revision-3"]').exists()).toBe(false)
    await wrapper.get('[data-testid="compare-revision-1"]').trigger('click')
    await settle()
    // The shown side is pinned to the revision on screen, so a later save cannot change it.
    expect(router.currentRoute.value.query).toMatchObject({ revision: '3', compare: '1' })
    expect(wrapper.find('[data-testid="compare"]').exists()).toBe(true)

    await wrapper.get('[data-testid="compare-swap"]').trigger('click')
    await settle()
    expect(router.currentRoute.value.query).toMatchObject({ revision: '1', compare: '3' })
    expect(wrapper.get('[data-testid="compare-metadata-added"]').text()).toBe('+1')

    await wrapper.get('[data-testid="compare-close"]').trigger('click')
    await settle()
    expect(router.currentRoute.value.query.compare).toBeUndefined()
    expect(wrapper.find('[data-testid="compare"]').exists()).toBe(false)
    wrapper.unmount()
  })

  it('refuses a compare revision that is not a revision number, or is the one shown', async () => {
    const seen = { manifests: [] as GetManifestRequest[] }
    const bad = await mountRepositoryView(RepositoryDocumentView, `${DOC}&compare=latest`, repository(seen))
    expect(bad.wrapper.get('[data-testid="route-error"]').text()).toContain('Compare: Revision "latest" is not a whole number.')
    bad.wrapper.unmount()

    const same = await mountRepositoryView(RepositoryDocumentView, `${DOC}&revision=3&compare=3`, repository(seen))
    expect(same.wrapper.get('[data-testid="compare-same"]').text()).toContain('Revision 3 is the revision shown')
    same.wrapper.unmount()

    const typed = await mountRepositoryView(RepositoryDocumentView, `${DOC}&revision=3&compare=1`, repository(seen))
    await typed.wrapper.get('[data-testid="compare-input"] input').setValue('3')
    await typed.wrapper.get('[data-testid="compare-apply"]').trigger('click')
    expect(typed.wrapper.get('[data-testid="compare-input-error"]').text()).toContain('Revision 3 is the revision shown')
    expect(typed.router.currentRoute.value.query.compare).toBe('1')
    typed.wrapper.unmount()
  })

  it('shows a failed section by its code and still compares the rest', async () => {
    const seen = { manifests: [] as GetManifestRequest[] }
    const { wrapper } = await mountRepositoryView(RepositoryDocumentView, `${DOC}&revision=3&compare=1`, repository(seen, { failLinksAt: 1n }))
    const failures = wrapper.findAll('[data-testid="rpc-failure"]')
    expect(failures).toHaveLength(1)
    const failure = failures[0].text()
    expect(failure).toContain('Listing links at revision 1')
    expect(failure).toContain('UNAVAILABLE')
    expect(wrapper.find('[data-testid="compare-links"]').exists()).toBe(false)
    expect(wrapper.get('[data-testid="compare-metadata"]').text()).toContain('"Final"')
    wrapper.unmount()
  })

  it('names a revision that does not exist', async () => {
    const seen = { manifests: [] as GetManifestRequest[] }
    const { wrapper } = await mountRepositoryView(RepositoryDocumentView, `${DOC}&revision=3&compare=2`, repository(seen))
    const failure = wrapper.get('[data-testid="rpc-failure"]').text()
    expect(failure).toContain('NOT_FOUND')
    expect(failure).toContain('doc-1 has no revision 2')
    wrapper.unmount()
  })

  it('filters revision comparison by changed-only and line query', async () => {
    const seen = { manifests: [] as GetManifestRequest[] }
    const { wrapper } = await mountRepositoryView(RepositoryDocumentView, `${DOC}&revision=3&compare=1`, repository(seen))
    expect(wrapper.find('[data-testid="compare-concepts-same"]').exists()).toBe(true)

    await wrapper.get('[data-testid="compare-changed-only"] input').setValue(true)
    await settle()
    expect(wrapper.find('[data-testid="compare-concepts-same"]').exists()).toBe(false)
    expect(wrapper.get('[data-testid="compare-metadata"]').text()).not.toContain('language = "en"')
    expect(wrapper.get('[data-testid="compare-metadata"]').text()).toContain('title = "Final"')

    await wrapper.get('[data-testid="compare-line-filter"] input').setValue('category')
    await settle()
    expect(wrapper.get('[data-testid="compare-metadata"]').text()).toContain('category = "report"')
    expect(wrapper.get('[data-testid="compare-metadata"]').text()).not.toContain('title = "Final"')
    wrapper.unmount()
  })
})
