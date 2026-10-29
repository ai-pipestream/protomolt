// @vitest-environment jsdom
import { createHash } from 'node:crypto'
import { create } from '@bufbuild/protobuf'
import { Code, ConnectError, type ConnectRouter } from '@connectrpc/connect'
import { afterEach, describe, expect, it } from 'vitest'
import { CharacterizationEvidenceSchema, ClassificationSchema, ClassificationState } from '@/gen/ai/protomolt/proto/asset/v1/classification_pb'
import { BridgeKind, BridgeStatus } from '@/gen/ai/protomolt/proto/asset/v1/bridge_pb'
import { ContentClass, ContentProfileSchema, QualityScoreSchema } from '@/gen/ai/protomolt/proto/asset/v1/content_pb'
import { FormatFactSchema, PdfDocumentSchema } from '@/gen/ai/protomolt/proto/asset/v1/format_pb'
import { ArchiveSchema, ArchiveStatsSchema, RenditionStatsSchema, VersioningPolicy } from '@/gen/ai/protomolt/proto/repo/archive/v1/archive_pb'
import {
  ArchiveService,
  BridgeEntryResponseSchema,
  BridgeOutcomeSchema,
  ClassifyEntryResponseSchema,
  GetArchiveResponseSchema,
  GetArchiveStatsResponseSchema,
  GetEntryManifestResponseSchema,
  ListEntriesResponseSchema,
  ListVersionsResponseSchema,
  ReadRenditionResponseSchema,
  RenditionReadHeaderSchema,
  type GetEntryManifestRequest,
  type ListEntriesRequest,
  type ReadRenditionRequest,
} from '@/gen/ai/protomolt/proto/repo/archive/v1/archive_service_pb'
import {
  EntryAddressSchema,
  EntryInfoSchema,
  RenditionDescriptorSchema,
  RenditionManifestEntrySchema,
  RenditionState,
  VersionManifestSchema,
} from '@/gen/ai/protomolt/proto/repo/archive/v1/entry_pb'
import { at } from '@/repository/repositoryTestKit'
import { mountRepositoryView, settle } from '@/repository/repositoryViewKit'
import RepositoryArchiveEntryView from './RepositoryArchiveEntryView.vue'
import RepositoryArchiveView from './RepositoryArchiveView.vue'

const PDF = new Uint8Array(150_000).map((_, i) => (i * 3) & 0xff)
const PDF_SHA = createHash('sha256').update(PDF).digest('hex')
const address = (entryId: string) => create(EntryAddressSchema, { accountId: 'acct-a', archive: 'papers', entryId })
const original = create(RenditionDescriptorSchema, { name: 'original', mediaType: 'application/pdf' })

function manifest(version: bigint) {
  return create(VersionManifestSchema, {
    address: address('e-1'),
    version,
    totalBytes: BigInt(PDF.length),
    createdAt: at(`2026-10-0${version}T00:00:00Z`),
    renditions: [
      create(RenditionManifestEntrySchema, {
        rendition: original,
        state: RenditionState.PRESENT,
        sizeBytes: BigInt(PDF.length),
        sha256: PDF_SHA,
        contentProfile: create(ContentProfileSchema, {
          contentClass: ContentClass.OCR_TEXT,
          quality: create(QualityScoreSchema, { score: 0.91 }),
        }),
      }),
      create(RenditionManifestEntrySchema, {
        rendition: create(RenditionDescriptorSchema, { name: 'ocr' }),
        state: version === 1n ? RenditionState.PRESENT : RenditionState.DELETED,
        sizeBytes: version === 1n ? 512n : 0n,
        sha256: version === 1n ? 'f'.repeat(64) : '',
        deletedReason: version === 1n ? '' : 'RETENTION',
      }),
    ],
  })
}

interface Seen {
  entries: ListEntriesRequest[]
  manifests: GetEntryManifestRequest[]
  reads: ReadRenditionRequest[]
}

function archives(seen: Seen) {
  return (router: ConnectRouter) =>
    router.service(ArchiveService, {
      getArchive: (r) =>
        create(GetArchiveResponseSchema, {
          archive: create(ArchiveSchema, { name: r.archive, accountId: r.accountId, driveName: 'pipeline', versioning: VersioningPolicy.NONE, description: 'Scanned papers' }),
        }),
      getArchiveStats: (r) =>
        create(GetArchiveStatsResponseSchema, {
          stats: create(ArchiveStatsSchema, {
            archive: r.archive,
            entries: 2n,
            versions: 5n,
            currentBytes: 2048n,
            retainedBytes: 4096n,
            renditions: [create(RenditionStatsSchema, { renditionName: 'original', objectCount: 5n, totalBytes: 4096n })],
          }),
        }),
      listEntries(request) {
        seen.entries.push(request)
        const page = request.continuationToken === '' ? 1 : 2
        return create(ListEntriesResponseSchema, {
          entries: [create(EntryInfoSchema, { address: address(`e-${page}`), currentVersion: 3n, title: `Paper ${page}`, filename: `p${page}.pdf` })],
          nextContinuationToken: page === 1 ? 'c2' : '',
          totalCount: 2n,
        })
      },
      getEntryManifest(request) {
        seen.manifests.push(request)
        const version = request.version === 0n ? 3n : request.version
        if (version > 3n) throw new ConnectError(`e-1 has no version ${version}`, Code.NotFound)
        return create(GetEntryManifestResponseSchema, {
          info: create(EntryInfoSchema, { address: address('e-1'), currentVersion: 3n, title: 'Paper 1', filename: 'p1.pdf', contentType: 'application/pdf' }),
          manifest: manifest(version),
        })
      },
      listVersions: () => create(ListVersionsResponseSchema, { versions: [manifest(3n), manifest(2n), manifest(1n)] }),
      async *readRendition(request) {
        seen.reads.push(request)
        const size = BigInt(PDF.length)
        const length = request.length === 0n ? size - request.offset : request.length
        yield create(ReadRenditionResponseSchema, {
          frame: {
            case: 'header',
            value: create(RenditionReadHeaderSchema, { version: request.version, rendition: original, sizeBytes: size, sha256: PDF_SHA, offset: request.offset, length, wholeObject: request.offset === 0n && length === size }),
          },
        })
        const end = Number(request.offset + length)
        for (let i = Number(request.offset); i < end; i += 65536) {
          yield create(ReadRenditionResponseSchema, { frame: { case: 'data', value: PDF.subarray(i, Math.min(end, i + 65536)) } })
        }
      },
    })
}

const seen = (): Seen => ({ entries: [], manifests: [], reads: [] })

afterEach(() => {
  delete (globalThis as Record<string, unknown>).showSaveFilePicker
})

describe('RepositoryArchiveView, review cases', () => {
  it('offers every classification state the descriptor defines, VERIFIED and CONFLICTED included', async () => {
    const s = seen()
    const { wrapper } = await mountRepositoryView(RepositoryArchiveView, { name: 'repository-archive', query: { account: 'acct-a', archive: 'papers' } }, archives(s))
    const items = (wrapper.vm as unknown as { classificationItems: { title: string; value: number }[] }).classificationItems
    expect(items.map((i) => i.title)).toEqual(['Any classification', 'UNCLASSIFIED', 'DECLARED', 'IDENTIFIED', 'VERIFIED', 'CONFLICTED'])
    ;(wrapper.vm as unknown as { classification: ClassificationState }).classification = ClassificationState.CONFLICTED
    await settle()
    expect(s.entries[s.entries.length - 1].classificationState).toBe(ClassificationState.CONFLICTED)
    expect(wrapper.text()).toContain('live entries in the archive, before the filter')
    wrapper.unmount()
  })

  it('drops the previous archive rows as soon as another archive opens', async () => {
    let hold: (() => void) | null = null
    const { wrapper, router } = await mountRepositoryView(RepositoryArchiveView, { name: 'repository-archive', query: { account: 'acct-a', archive: 'papers' } }, (router) => {
      archives(seen())(router)
      router.service(ArchiveService, {
        getArchive: (r) => create(GetArchiveResponseSchema, { archive: create(ArchiveSchema, { name: r.archive, accountId: r.accountId }) }),
        getArchiveStats: (r) => create(GetArchiveStatsResponseSchema, { stats: create(ArchiveStatsSchema, { archive: r.archive }) }),
        async listEntries(request) {
          if (request.archive === 'letters') await new Promise<void>((resolve) => (hold = resolve))
          return create(ListEntriesResponseSchema, {
            entries: [create(EntryInfoSchema, { address: create(EntryAddressSchema, { accountId: 'acct-a', archive: request.archive, entryId: `${request.archive}-1` }) })],
          })
        },
      })
    })
    expect(wrapper.find('[data-testid="entry-papers-1"]').exists()).toBe(true)
    await router.push({ name: 'repository-archive', query: { account: 'acct-a', archive: 'letters' } })
    await settle()
    expect(wrapper.find('[data-testid="entry-papers-1"]').exists()).toBe(false)
    hold!()
    await settle()
    expect(wrapper.find('[data-testid="entry-letters-1"]').exists()).toBe(true)
    wrapper.unmount()
  })
})

describe('RepositoryArchiveView', () => {
  it('shows the archive, its stats and its entries, filtering by classification and paging', async () => {
    const s = seen()
    const { wrapper, router } = await mountRepositoryView(RepositoryArchiveView, { name: 'repository-archive', query: { account: 'acct-a', archive: 'papers' } }, archives(s))
    expect(wrapper.get('[data-testid="archive-info"]').text()).toContain('Scanned papers')
    const stats = wrapper.get('[data-testid="archive-stats"]').text()
    expect(stats).toContain('5')
    expect(stats).toContain('4.0 KiB')
    expect(wrapper.get('[data-testid="entry-table"]').text()).toContain('Paper 1')
    expect(s.entries[0]).toMatchObject({ accountId: 'acct-a', archive: 'papers', classificationState: ClassificationState.UNSPECIFIED, continuationToken: '' })

    ;(wrapper.vm as unknown as { classification: ClassificationState }).classification = ClassificationState.IDENTIFIED
    await settle()
    expect(s.entries[s.entries.length - 1]).toMatchObject({ classificationState: ClassificationState.IDENTIFIED, continuationToken: '' })
    await wrapper.get('[data-testid="entries-next"]').trigger('click')
    await settle()
    expect(s.entries[s.entries.length - 1]).toMatchObject({ classificationState: ClassificationState.IDENTIFIED, continuationToken: 'c2' })
    expect(wrapper.text()).toContain('Paper 2')

    await wrapper.get('[data-testid="entry-e-2"]').trigger('click')
    await settle()
    expect(router.currentRoute.value.query).toEqual({ account: 'acct-a', archive: 'papers', entry: 'e-2' })
    wrapper.unmount()
  })

  it("shows a scoped caller's refusal for another account's archive by its code", async () => {
    const { wrapper } = await mountRepositoryView(RepositoryArchiveView, { name: 'repository-archive', query: { account: 'acct-b', archive: 'papers' } }, (router) =>
      router.service(ArchiveService, {
        getArchive() {
          throw new ConnectError('account acct-b is not bound to this caller', Code.PermissionDenied)
        },
        getArchiveStats() {
          throw new ConnectError('account acct-b is not bound to this caller', Code.PermissionDenied)
        },
        listEntries() {
          throw new ConnectError('account acct-b is not bound to this caller', Code.PermissionDenied)
        },
      }),
    )
    const failures = wrapper.findAll('[data-testid="rpc-failure"]').map((f) => f.text())
    expect(failures).toHaveLength(3)
    for (const f of failures) expect(f).toContain('PERMISSION_DENIED')
    expect(wrapper.find('[data-testid="entry-table"]').exists()).toBe(false)
    wrapper.unmount()
  })
})

const ENTRY = { name: 'repository-archive-entry', query: { account: 'acct-a', archive: 'papers', entry: 'e-1' } }

describe('RepositoryArchiveEntryView', () => {
  it("shows the current version's renditions, readable only when present, and its versions", async () => {
    const s = seen()
    const { wrapper } = await mountRepositoryView(RepositoryArchiveEntryView, ENTRY, archives(s))
    expect(s.manifests[0].version).toBe(0n)
    expect(wrapper.get('[data-testid="version-chip"]').text()).toBe('version 3 (current)')
    const table = wrapper.get('[data-testid="rendition-table"]').text()
    expect(table).toContain(PDF_SHA)
    expect(table).toContain('RETENTION')
    expect(wrapper.find('[data-testid="select-rendition-original"]').exists()).toBe(true)
    expect(wrapper.find('[data-testid="select-rendition-ocr"]').exists()).toBe(false)
    expect(wrapper.get('[data-testid="versions"]').text()).toContain('shown')
    wrapper.unmount()
  })

  it('streams a whole rendition of the pinned version to disk and verifies its SHA-256', async () => {
    const written: Uint8Array[] = []
    ;(globalThis as Record<string, unknown>).showSaveFilePicker = async () => ({
      createWritable: async () => ({ write: async (c: Uint8Array) => void written.push(c.slice()), close: async () => {}, abort: async () => {} }),
    })
    const s = seen()
    const { wrapper } = await mountRepositoryView(RepositoryArchiveEntryView, ENTRY, archives(s))
    await wrapper.get('[data-testid="select-rendition-original"]').trigger('click')
    await settle()
    await wrapper.get('[data-testid="download"]').trigger('click')
    await settle()
    expect(s.reads[0]).toMatchObject({ version: 3n, rendition: 'original', offset: 0n, length: 0n })
    expect(wrapper.get('[data-testid="verification"]').text()).toContain(`hash to the stored SHA-256 ${PDF_SHA}`)
    expect(written.length).toBe(Math.ceil(PDF.length / 65536))
    expect(new Uint8Array(await new Blob(written as Uint8Array<ArrayBuffer>[]).arrayBuffer())).toEqual(PDF)
    wrapper.unmount()
  })

  it('opens an older version and reads its renditions at that version', async () => {
    const s = seen()
    const { wrapper, router } = await mountRepositoryView(RepositoryArchiveEntryView, ENTRY, archives(s))
    await wrapper.get('[data-testid="open-version-1"]').trigger('click')
    await settle()
    expect(router.currentRoute.value.query.version).toBe('1')
    expect(s.manifests[s.manifests.length - 1].version).toBe(1n)
    expect(wrapper.get('[data-testid="version-chip"]').text()).toBe('version 1')
    await wrapper.get('[data-testid="select-rendition-original"]').trigger('click')
    await settle()
    await wrapper.get('[data-testid="range-length"] input').setValue('100')
    await wrapper.get('[data-testid="preview"]').trigger('click')
    await settle()
    expect(s.reads[s.reads.length - 1]).toMatchObject({ version: 1n, length: 100n })
    expect(wrapper.get('[data-testid="preview-hex"]').text()).toContain('00000000')
    wrapper.unmount()
  })

  it('shows NOT_FOUND for a version that does not exist', async () => {
    const { wrapper } = await mountRepositoryView(RepositoryArchiveEntryView, { ...ENTRY, query: { ...ENTRY.query, version: '9' } }, archives(seen()))
    expect(wrapper.get('[data-testid="rpc-failure"]').text()).toContain('NOT_FOUND')
    expect(wrapper.find('[data-testid="rendition-table"]').exists()).toBe(false)
    wrapper.unmount()
  })

  it('displays rendition content profiles, compares two versions side by side, and toggles the upload panel', async () => {
    const s = seen()
    const { wrapper } = await mountRepositoryView(RepositoryArchiveEntryView, ENTRY, archives(s))
    expect(wrapper.get('[data-testid="rendition-profile-original"]').text()).toContain('OCR_TEXT · quality 0.91')

    await wrapper.get('[data-testid="compare-version-1"]').trigger('click')
    await settle()
    expect(wrapper.find('[data-testid="version-compare"]').exists()).toBe(true)
    const compareText = wrapper.get('[data-testid="version-compare-table"]').text()
    expect(compareText).toContain('ocr')
    expect(compareText).toContain('original')

    await wrapper.get('[data-testid="version-compare-changed-only"] input').setValue(true)
    await settle()
    expect(wrapper.get('[data-testid="version-compare-table"]').text()).toContain('ocr')
    expect(wrapper.get('[data-testid="version-compare-table"]').text()).not.toContain('unchanged')

    await wrapper.get('[data-testid="version-compare-close"]').trigger('click')
    await settle()
    expect(wrapper.find('[data-testid="version-compare"]').exists()).toBe(false)

    await wrapper.get('[data-testid="toggle-rendition-upload"]').trigger('click')
    await settle()
    expect(wrapper.find('[data-testid="rendition-upload-panel"]').exists()).toBe(true)
    wrapper.unmount()
  })

  it('invokes ClassifyEntry and BridgeEntry from the characterization workbench and inspects produced renditions', async () => {
    const s = seen()
    let classifiedDeclared: string | undefined
    let bridgedKinds: BridgeKind[] = []
    const { wrapper } = await mountRepositoryView(RepositoryArchiveEntryView, ENTRY, (router) => {
      archives(s)(router)
      router.service(ArchiveService, {
        getEntryManifest: (r) =>
          create(GetEntryManifestResponseSchema, {
            info: create(EntryInfoSchema, { address: address('e-1'), currentVersion: 3n, title: 'Paper 1', filename: 'p1.pdf', contentType: 'application/pdf' }),
            manifest: manifest(r.version === 0n ? 3n : r.version),
          }),
        listVersions: () => create(ListVersionsResponseSchema, { versions: [manifest(3n)] }),
        classifyEntry(request) {
          classifiedDeclared = request.declared?.format.case
          return create(ClassifyEntryResponseSchema, {
            classification: create(ClassificationSchema, {
              state: ClassificationState.VERIFIED,
              declared: request.declared,
              identified: create(FormatFactSchema, { format: { case: 'pdf', value: create(PdfDocumentSchema, { filename: 'p1.pdf' }) } }),
              evidence: [create(CharacterizationEvidenceSchema, { signal: 'magic-bytes', observation: '%PDF-1.7 header' })],
            }),
          })
        },
        bridgeEntry(request) {
          bridgedKinds = [...request.bridges]
          return create(BridgeEntryResponseSchema, {
            version: 3n,
            outcomes: [
              create(BridgeOutcomeSchema, {
                bridge: BridgeKind.DOCUMENT_TEXT,
                status: BridgeStatus.PRODUCED,
                rendition: 'original',
                detail: 'extracted 8 pages',
              }),
            ],
          })
        },
      })
    })

    await wrapper.get('[data-testid="toggle-classify-bridge"]').trigger('click')
    await settle()
    expect(wrapper.find('[data-testid="classify-bridge-panel"]').exists()).toBe(true)

    ;(wrapper.vm as unknown as { classifyForm: { formatCase: string; filename: string } }).classifyForm.formatCase = 'pdf'
    ;(wrapper.vm as unknown as { classifyForm: { formatCase: string; filename: string } }).classifyForm.filename = 'p1.pdf'
    await settle()
    await wrapper.get('[data-testid="classify-submit"]').trigger('click')
    await settle()
    expect(classifiedDeclared).toBe('pdf')
    expect(wrapper.get('[data-testid="classify-result"]').text()).toContain('VERIFIED')
    expect(wrapper.get('[data-testid="classify-result"]').text()).toContain('%PDF-1.7 header')

    ;(wrapper.vm as unknown as { selectedBridges: BridgeKind[] }).selectedBridges = [BridgeKind.DOCUMENT_TEXT]
    await settle()
    await wrapper.get('[data-testid="bridge-submit"]').trigger('click')
    await settle()
    expect(bridgedKinds).toEqual([BridgeKind.DOCUMENT_TEXT])
    expect(wrapper.get('[data-testid="bridge-outcomes-table"]').text()).toContain('DOCUMENT_TEXT')
    expect(wrapper.get('[data-testid="bridge-outcomes-table"]').text()).toContain('PRODUCED')
    await wrapper.get('[data-testid="bridge-inspect-original"]').trigger('click')
    await settle()
    expect((wrapper.vm as unknown as { selected: { name: string } | null }).selected?.name).toBe('original')
    wrapper.unmount()
  })

  it('syncs archive classification and text filters with the URL query and clears them via active filter chips', async () => {
    const s = seen()
    const { wrapper, router } = await mountRepositoryView(
      RepositoryArchiveView,
      { name: 'repository-archive', query: { account: 'acct-a', archive: 'papers', classification: 'VERIFIED', q: 'Paper 1' } },
      archives(s),
    )
    expect(s.entries[0].classificationState).toBe(ClassificationState.VERIFIED)
    expect(wrapper.find('[data-testid="archive-active-filters"]').exists()).toBe(true)
    expect(wrapper.get('[data-testid="archive-filter-classification"]').text()).toContain('VERIFIED')
    expect(wrapper.get('[data-testid="archive-filter-q"]').text()).toContain('Paper 1')

    await wrapper.get('[data-testid="archive-clear-filters"]').trigger('click')
    await settle()
    expect(router.currentRoute.value.query).toEqual({ account: 'acct-a', archive: 'papers' })
    expect(wrapper.find('[data-testid="archive-active-filters"]').exists()).toBe(false)
    wrapper.unmount()
  })
})
