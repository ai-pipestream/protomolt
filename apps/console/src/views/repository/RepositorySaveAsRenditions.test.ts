// @vitest-environment jsdom
import { createHash } from 'node:crypto'
import { create, fromBinary, toBinary } from '@bufbuild/protobuf'
import { anyPack, StringValueSchema } from '@bufbuild/protobuf/wkt'
import { Code, ConnectError, type ConnectRouter } from '@connectrpc/connect'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { IntakeWriterScopeSchema } from '@/gen/ai/protomolt/proto/authz/v1/account_bindings_pb'
import { ConsoleSessionStatusSchema } from '@/gen/ai/protomolt/proto/authz/v1/console_session_pb'
import {
  ParquetMaskStrategy,
  ParquetRendererOptionsSchema,
} from '@/gen/ai/protomolt/proto/emit/parquet/v1/parquet_renderer_pb'
import { RendererCapabilitySchema } from '@/gen/ai/protomolt/proto/emit/v1/renderer_pb'
import { ArchiveSchema, ArchiveStatsSchema, VersioningPolicy } from '@/gen/ai/protomolt/proto/repo/archive/v1/archive_pb'
import {
  ArchiveService,
  GetArchiveResponseSchema,
  GetArchiveStatsResponseSchema,
  GetEntryManifestResponseSchema,
  ListArchivesResponseSchema,
  ListEntriesResponseSchema,
  ListVersionsResponseSchema,
  ReadRenditionResponseSchema,
  RenditionReadHeaderSchema,
} from '@/gen/ai/protomolt/proto/repo/archive/v1/archive_service_pb'
import {
  EntryAddressSchema,
  EntryInfoSchema,
  RenditionDescriptorSchema,
  RenditionManifestEntrySchema,
  RenditionState,
  VersionManifestSchema,
} from '@/gen/ai/protomolt/proto/repo/archive/v1/entry_pb'
import {
  CancelRepositorySinkJobResponseSchema,
  GetRepositorySinkJobResponseSchema,
  ListRenditionProvenanceResponseSchema,
  ListSaveAsCapabilitiesResponseSchema,
  PublishRepositorySinkResponseSchema,
  RenditionProvenanceEntrySchema,
  RenditionProvenanceSchema,
  RepositorySinkJobSchema,
  RepositorySinkJobState,
  RepositorySinkService,
  SaveAsCapabilitySchema,
  SaveAsCompatibilityStatus,
  type CancelRepositorySinkJobRequest,
  type GetRepositorySinkJobRequest,
  type ListSaveAsCapabilitiesRequest,
  type PublishRepositorySinkRequest,
} from '@/gen/ai/protomolt/proto/repo/sink/v1/repository_sink_pb'
import { BlobBagSchema, BlobSchema, BlobsSchema, DocumentSchema } from '@/gen/ai/protomolt/proto/repo/v1/document_pb'
import {
  DocumentService,
  GetDocumentResponseSchema,
  GetManifestResponseSchema,
  GetSchemaDescriptorResponseSchema,
  ListRevisionsResponseSchema,
  PartDataSchema,
  ReadCompleteSchema,
  ReadDocumentResponseSchema,
} from '@/gen/ai/protomolt/proto/repo/v2/document_service_pb'
import {
  DocumentManifestSchema,
  DocumentPart,
  PartManifestEntrySchema,
  PartSchemaBindingSchema,
  PartSelectorSchema,
  PartState,
  RevisionValidationSchema,
  ValidationStatus,
} from '@/gen/ai/protomolt/proto/repo/v2/manifest_pb'
import { REPOSITORY_SESSION } from '@/repository/clients'
import { at, descriptorSetOf, testAddress } from '@/repository/repositoryTestKit'
import { mountRepositoryView, settle } from '@/repository/repositoryViewKit'
import RepositoryArchiveEntryView from './RepositoryArchiveEntryView.vue'
import RepositoryArchiveView from './RepositoryArchiveView.vue'
import RepositoryDocumentView from './RepositoryDocumentView.vue'

const PARQUET_BYTES = new Uint8Array([0x50, 0x41, 0x52, 0x31, 1, 2, 3, 4, 0x50, 0x41, 0x52, 0x31])
const PARQUET_SHA = createHash('sha256').update(PARQUET_BYTES).digest('hex')
const PROTO_DOC = create(DocumentSchema, {
  docId: 'doc-1',
  searchMetadata: { title: 'Quarterly metrics' },
})
const PROTO_BYTES = toBinary(DocumentSchema, PROTO_DOC)
const PROTO_SHA = createHash('sha256').update(PROTO_BYTES).digest('hex')
const DOC_DESCRIPTORS = descriptorSetOf(DocumentSchema.file)
const DOC_DESCRIPTORS_SHA = createHash('sha256').update(DOC_DESCRIPTORS).digest('hex')
const STRING_DESCRIPTORS = descriptorSetOf(StringValueSchema.file)
const STRING_DESCRIPTORS_SHA = createHash('sha256').update(STRING_DESCRIPTORS).digest('hex')

const entryAddr = (entryId = 'e-metrics') =>
  create(EntryAddressSchema, { accountId: 'acct-a', archive: 'datasets', entryId })

const parquetCap = create(RendererCapabilitySchema, {
  providerId: 'parquet-v1',
  providerVersion: '1.0.0',
  sourceProfile: 'tabular-scalar-v1',
  mediaType: 'application/vnd.apache.parquet',
  extension: 'parquet',
  optionsType: 'ai.protomolt.proto.emit.parquet.v1.ParquetRendererOptions',
  optionsFingerprint: 'fp-parquet-opts',
  lossDescription: 'Projects scalar top-level columns only; nested messages are omitted.',
  maxInputBytes: 4_194_304,
  maxOutputBytes: 1_048_576,
})

afterEach(() => {
  vi.restoreAllMocks()
  delete (globalThis as Record<string, unknown>).showSaveFilePicker
  document.body.innerHTML = ''
})

describe('Repository Save-as & Format Icons (#820)', () => {
  it('renders trusted format-family icons across RepositoryArchiveView without implying conversion support', async () => {
    const { wrapper } = await mountRepositoryView(
      RepositoryArchiveView,
      { name: 'repository-archive', query: { account: 'acct-a', archive: 'datasets' } },
      (router: ConnectRouter) => {
        router.service(ArchiveService, {
          getArchive: (r) =>
            create(GetArchiveResponseSchema, {
              archive: create(ArchiveSchema, { name: r.archive, accountId: r.accountId, versioning: VersioningPolicy.NONE }),
            }),
          getArchiveStats: (r) =>
            create(GetArchiveStatsResponseSchema, {
              stats: create(ArchiveStatsSchema, { archive: r.archive, entries: 3n, versions: 3n }),
            }),
          listEntries: () =>
            create(ListEntriesResponseSchema, {
              entries: [
                create(EntryInfoSchema, {
                  address: entryAddr('row-parquet'),
                  currentVersion: 1n,
                  title: 'Metrics slice',
                  filename: 'slice.parquet',
                  contentType: 'application/vnd.apache.parquet',
                }),
                create(EntryInfoSchema, {
                  address: entryAddr('row-hint'),
                  currentVersion: 1n,
                  title: 'Hinted PDF',
                  filename: 'scan.pdf',
                  contentType: 'application/octet-stream',
                }),
                create(EntryInfoSchema, {
                  address: entryAddr('row-unknown'),
                  currentVersion: 1n,
                  title: 'Custom binary',
                  filename: 'mystery.dat',
                  contentType: 'application/x-custom-vendor',
                }),
              ],
              totalCount: 3n,
            }),
        })
      },
    )

    const parquetRow = wrapper.get('[data-testid="entry-row-parquet"]')
    expect(parquetRow.get('[data-testid="format-icon"]').attributes('data-format-family')).toBe('parquet')
    expect(parquetRow.text()).toContain('Parquet')

    const hintRow = wrapper.get('[data-testid="entry-row-hint"]')
    expect(hintRow.get('[data-testid="format-icon"]').attributes('data-format-family')).toBe('pdf')
    expect(hintRow.get('[data-testid="format-extension-hint"]').text()).toBe('(filename hint)')

    const unknownRow = wrapper.get('[data-testid="entry-row-unknown"]')
    expect(unknownRow.get('[data-testid="format-icon"]').attributes('data-format-family')).toBe('unknown')
    expect(unknownRow.text()).toContain('Unknown format')
    wrapper.unmount()
  })

  it('shows provenance, stale derived rendition badge, verified download, and zero-provider Save-as state without inventing PDF or Markdown', async () => {
    const seenCapRequests: ListSaveAsCapabilitiesRequest[] = []
    const writtenChunks: Uint8Array[] = []
    ;(globalThis as Record<string, unknown>).showSaveFilePicker = async () => ({
      createWritable: async () => ({
        write: async (c: Uint8Array) => void writtenChunks.push(c.slice()),
        close: async () => {},
        abort: async () => {},
      }),
    })

    const { wrapper } = await mountRepositoryView(
      RepositoryArchiveEntryView,
      { name: 'repository-archive-entry', query: { account: 'acct-a', archive: 'datasets', entry: 'e-metrics' } },
      (router: ConnectRouter) => {
        router.service(ArchiveService, {
          listArchives: () =>
            create(ListArchivesResponseSchema, {
              archives: [create(ArchiveSchema, { accountId: 'acct-a', name: 'datasets' })],
            }),
          getEntryManifest: () =>
            create(GetEntryManifestResponseSchema, {
              info: create(EntryInfoSchema, {
                address: entryAddr(),
                currentVersion: 3n,
                title: 'Quarterly metrics',
                filename: 'metrics.pb',
                contentType: 'application/x-protobuf',
                sourceUri: 'javascript:alert(1)',
              }),
              manifest: create(VersionManifestSchema, {
                address: entryAddr(),
                version: 3n,
                totalBytes: BigInt(PROTO_BYTES.length + PARQUET_BYTES.length),
                renditions: [
                  create(RenditionManifestEntrySchema, {
                    rendition: create(RenditionDescriptorSchema, {
                      name: 'parsed',
                      mediaType: 'application/x-protobuf',
                    }),
                    state: RenditionState.PRESENT,
                    sizeBytes: BigInt(PROTO_BYTES.length),
                    sha256: PROTO_SHA,
                  }),
                  create(RenditionManifestEntrySchema, {
                    rendition: create(RenditionDescriptorSchema, {
                      name: 'parquet',
                      mediaType: 'application/vnd.apache.parquet',
                    }),
                    state: RenditionState.PRESENT,
                    sizeBytes: BigInt(PARQUET_BYTES.length),
                    sha256: PARQUET_SHA,
                  }),
                ],
              }),
            }),
          listVersions: () => create(ListVersionsResponseSchema, { versions: [] }),
          async *readRendition(req) {
            yield create(ReadRenditionResponseSchema, {
              frame: {
                case: 'header',
                value: create(RenditionReadHeaderSchema, {
                  version: req.version,
                  rendition: create(RenditionDescriptorSchema, {
                    name: 'parquet',
                    mediaType: 'application/vnd.apache.parquet',
                  }),
                  sizeBytes: BigInt(PARQUET_BYTES.length),
                  sha256: PARQUET_SHA,
                  offset: 0n,
                  length: BigInt(PARQUET_BYTES.length),
                  wholeObject: true,
                }),
              },
            })
            yield create(ReadRenditionResponseSchema, {
              frame: { case: 'data', value: PARQUET_BYTES },
            })
          },
        })
        router.service(RepositorySinkService, {
          listRenditionProvenance: () =>
            create(ListRenditionProvenanceResponseSchema, {
              entries: [
                create(RenditionProvenanceEntrySchema, {
                  rendition: create(RenditionDescriptorSchema, { name: 'parsed', mediaType: 'application/x-protobuf' }),
                  provenance: create(RenditionProvenanceSchema, {
                    authoritativeProtobuf: true,
                    sourceRevision: 3n,
                  }),
                  currentSourceRevision: 3n,
                  sourceStale: false,
                }),
                create(RenditionProvenanceEntrySchema, {
                  rendition: create(RenditionDescriptorSchema, {
                    name: 'parquet',
                    mediaType: 'application/vnd.apache.parquet',
                  }),
                  provenance: create(RenditionProvenanceSchema, {
                    authoritativeProtobuf: false,
                    providerId: 'parquet-v1',
                    providerVersion: '1.0.0',
                    sourceProfile: 'tabular-scalar-v1',
                    sourceRevision: 1n,
                    sourceEntry: entryAddr(),
                    destinationAddress: entryAddr(),
                    mappingId: 'map-q1',
                    policyId: 'pol-internal',
                    lossDescription: 'Scalar fields only; <script>alert(1)</script>',
                  }),
                  currentSourceRevision: 3n,
                  sourceStale: true,
                }),
              ],
            }),
          listSaveAsCapabilities: (req) => {
            seenCapRequests.push(req)
            return create(ListSaveAsCapabilitiesResponseSchema, {
              resolvedSourceRevision: 3n,
              sourceSchemaType: 'ai.protomolt.proto.repo.v1.Document',
              sourceSchemaFingerprint: 'fp-doc',
              sourceSizeBytes: BigInt(PROTO_BYTES.length),
              capabilities: [],
            })
          },
        })
      },
    )

    // Unsafe javascript: sourceUri must never become a clickable anchor
    expect(wrapper.find('[data-testid="rendition-external-link"]').exists()).toBe(false)

    // Authoritative parsed protobuf has Save-as button; derived parquet does NOT
    const parsedRow = wrapper.get('[data-testid="rendition-parsed"]')
    expect(parsedRow.get('[data-testid="format-badge-protobuf"]').text()).toBe('Authoritative')
    expect(wrapper.find('[data-testid="save-as-rendition-parsed"]').exists()).toBe(true)
    expect(wrapper.find('[data-testid="save-as-rendition-parquet"]').exists()).toBe(false)

    // Derived parquet shows Derived + Stale badges and escaped provenance metadata
    const parquetRow = wrapper.get('[data-testid="rendition-parquet"]')
    expect(parquetRow.get('[data-testid="format-badge-derived"]').text()).toBe('Derived')
    expect(parquetRow.get('[data-testid="format-badge-stale"]').text()).toBe('Stale')
    expect(parquetRow.get('[data-testid="rendition-stale-parquet"]').text()).toContain('r1')
    expect(parquetRow.get('[data-testid="rendition-stale-parquet"]').text()).toContain('r3')
    expect(parquetRow.text()).toContain('parquet-v1')
    expect(parquetRow.text()).toContain('map-q1')
    expect(parquetRow.text()).toContain('pol-internal')
    expect(parquetRow.get('[data-testid="rendition-loss-parquet"]').text()).toContain('<script>alert(1)</script>')
    expect(parquetRow.find('script').exists()).toBe(false)

    // Verified SHA-256 download of derived Parquet rendition
    await wrapper.get('[data-testid="select-rendition-parquet"]').trigger('click')
    await settle()
    await wrapper.get('[data-testid="download"]').trigger('click')
    await settle()
    expect(wrapper.get('[data-testid="verification"]').text()).toContain(PARQUET_SHA)
    expect(new Uint8Array(await new Blob(writtenChunks as Uint8Array<ArrayBuffer>[]).arrayBuffer())).toEqual(PARQUET_BYTES)

    // Open Save-as dialog on authoritative parsed rendition when zero providers are enabled
    await wrapper.get('[data-testid="save-as-rendition-parsed"]').trigger('click')
    await settle()

    expect(seenCapRequests).toHaveLength(1)
    const dialog = document.body.querySelector('[data-testid="save-as-dialog"]')!
    expect(dialog).not.toBeNull()
    expect(dialog.textContent).toContain('No Save-as conversion providers are enabled on this server.')
    expect(dialog.textContent).not.toContain('PDF')
    expect(dialog.textContent).not.toContain('Markdown')
    expect(dialog.textContent).not.toContain('Parquet')
    expect(dialog.querySelector('[data-testid="save-as-submit-btn"]')).toBeNull()
    wrapper.unmount()
  })

  it('submits binary ParquetRendererOptions, supports async job cancellation, and reconciles lost responses with identical operationId', async () => {
    const publishCalls: PublishRepositorySinkRequest[] = []
    const getJobCalls: GetRepositorySinkJobRequest[] = []
    const cancelCalls: CancelRepositorySinkJobRequest[] = []
    let publishBehavior: 'drop-network' | 'queued' = 'drop-network'
    let jobStateToReturn: RepositorySinkJobState = RepositorySinkJobState.UNRESOLVED

    const { wrapper } = await mountRepositoryView(
      RepositoryArchiveEntryView,
      { name: 'repository-archive-entry', query: { account: 'acct-a', archive: 'datasets', entry: 'e-metrics' } },
      (router: ConnectRouter) => {
        router.service(ArchiveService, {
          listArchives: () =>
            create(ListArchivesResponseSchema, {
              archives: [create(ArchiveSchema, { accountId: 'acct-a', name: 'datasets' })],
            }),
          getEntryManifest: () =>
            create(GetEntryManifestResponseSchema, {
              info: create(EntryInfoSchema, { address: entryAddr(), currentVersion: 2n, title: 'Metrics' }),
              manifest: create(VersionManifestSchema, {
                address: entryAddr(),
                version: 2n,
                renditions: [
                  create(RenditionManifestEntrySchema, {
                    rendition: create(RenditionDescriptorSchema, { name: 'parsed', mediaType: 'application/x-protobuf' }),
                    state: RenditionState.PRESENT,
                    sizeBytes: BigInt(PROTO_BYTES.length),
                    sha256: PROTO_SHA,
                  }),
                ],
              }),
            }),
          listVersions: () => create(ListVersionsResponseSchema, { versions: [] }),
        })
        router.service(RepositorySinkService, {
          listRenditionProvenance: () => create(ListRenditionProvenanceResponseSchema),
          listSaveAsCapabilities: () =>
            create(ListSaveAsCapabilitiesResponseSchema, {
              resolvedSourceRevision: 2n,
              sourceSchemaType: 'ai.protomolt.proto.repo.v1.Document',
              sourceSizeBytes: BigInt(PROTO_BYTES.length),
              capabilities: [
                create(SaveAsCapabilitySchema, {
                  capability: parquetCap,
                  status: SaveAsCompatibilityStatus.COMPATIBLE,
                }),
                create(SaveAsCapabilitySchema, {
                  capability: create(RendererCapabilitySchema, {
                    providerId: 'strict-avro',
                    providerVersion: '2.0.0',
                    sourceProfile: 'flat-only',
                    mediaType: 'application/vnd.apache.avro',
                    extension: 'avro',
                  }),
                  status: SaveAsCompatibilityStatus.INCOMPATIBLE,
                  incompatibilityReason: 'Nested Timestamp is unsupported by strict-avro.',
                }),
              ],
            }),
          publishRepositorySink: (req) => {
            publishCalls.push(req)
            if (publishBehavior === 'drop-network') {
              throw new ConnectError('upstream transport closed mid-commit', Code.Unavailable)
            }
            return create(PublishRepositorySinkResponseSchema, {
              job: create(RepositorySinkJobSchema, {
                operationId: req.operationId,
                accountId: 'acct-a',
                state: RepositorySinkJobState.RUNNING,
                frozenSourceRevision: 2n,
                destinationAddress: req.destination?.address,
                destinationRendition: req.destination?.rendition,
              }),
            })
          },
          getRepositorySinkJob: (req) => {
            getJobCalls.push(req)
            return create(GetRepositorySinkJobResponseSchema, {
              job: create(RepositorySinkJobSchema, {
                operationId: req.operationId,
                accountId: req.accountId,
                state: jobStateToReturn,
                frozenSourceRevision: 2n,
                destinationAddress: entryAddr(),
                destinationRendition: create(RenditionDescriptorSchema, {
                  name: 'parquet',
                  mediaType: 'application/vnd.apache.parquet',
                }),
                destinationVersion: jobStateToReturn === RepositorySinkJobState.SUCCEEDED ? 3n : 0n,
                bytesCommitted: jobStateToReturn === RepositorySinkJobState.SUCCEEDED ? BigInt(PARQUET_BYTES.length) : 0n,
                replayed: jobStateToReturn === RepositorySinkJobState.SUCCEEDED,
              }),
            })
          },
          cancelRepositorySinkJob: (req) => {
            cancelCalls.push(req)
            return create(CancelRepositorySinkJobResponseSchema, {
              job: create(RepositorySinkJobSchema, {
                operationId: req.operationId,
                accountId: req.accountId,
                state: RepositorySinkJobState.CANCELLED,
                frozenSourceRevision: 2n,
              }),
            })
          },
        })
      },
    )

    await wrapper.get('[data-testid="save-as-rendition-parsed"]').trigger('click')
    await settle()

    const body = document.body
    expect(body.querySelector('[data-testid="save-as-capability-parquet-v1"]')).not.toBeNull()
    expect(body.querySelector('[data-testid="save-as-incompatible-strict-avro"]')?.textContent).toContain(
      'Nested Timestamp is unsupported by strict-avro.',
    )

    // Populate Parquet options
    const colInput = body.querySelector('[data-testid="parquet-columns-input"] input') as HTMLInputElement
    colInput.value = 'doc_id, title'
    colInput.dispatchEvent(new Event('input', { bubbles: true }))

    const mappingInput = body.querySelector('[data-testid="parquet-mapping-id-input"] input') as HTMLInputElement
    mappingInput.value = 'map-sales'
    mappingInput.dispatchEvent(new Event('input', { bubbles: true }))
    await settle()

    // First submit drops network -> must NOT claim success!
    const submitBtn = body.querySelector('[data-testid="save-as-submit-btn"]') as HTMLButtonElement
    submitBtn.click()
    await settle()

    expect(publishCalls).toHaveLength(1)
    const firstReq = publishCalls[0]!
    expect(firstReq.destination?.expectedDestinationVersion).toBe(2n)
    const decodedOpts = fromBinary(ParquetRendererOptionsSchema, firstReq.conversion!.options)
    expect(decodedOpts.columns).toEqual(['doc_id', 'title'])
    expect(decodedOpts.mappingId).toBe('map-sales')
    expect(decodedOpts.maskStrategy).toBe(ParquetMaskStrategy.UNSPECIFIED)

    const errorAlert = body.querySelector('[data-testid="save-as-rpc-error"]')!
    expect(errorAlert.textContent).toContain('Outcome unresolved')
    expect(body.querySelector('[data-testid="save-as-job-receipt"]')).toBeNull()

    // Reconcile when still UNRESOLVED -> still must NOT claim success
    const reconcileBtn = body.querySelector('[data-testid="save-as-reconcile-btn"]') as HTMLButtonElement
    reconcileBtn.click()
    await settle()
    expect(getJobCalls).toHaveLength(1)
    expect(getJobCalls[0]!.operationId).toBe(firstReq.operationId)
    expect(body.querySelector('[data-testid="save-as-job-status"]')?.getAttribute('data-job-phase')).toBe('unresolved')
    expect(body.querySelector('[data-testid="save-as-job-receipt"]')).toBeNull()

    // Retry same operation -> preserves identical operationId and moves to RUNNING, then cancel!
    publishBehavior = 'queued'
    const retryBtn = body.querySelector('[data-testid="save-as-retry-job-btn"]') as HTMLButtonElement
    retryBtn.click()
    await settle()
    expect(publishCalls).toHaveLength(2)
    expect(publishCalls[1]!.operationId).toBe(firstReq.operationId)
    expect(body.querySelector('[data-testid="save-as-job-status"]')?.getAttribute('data-job-phase')).toBe('running')

    const cancelBtn = body.querySelector('[data-testid="save-as-cancel-job-btn"]') as HTMLButtonElement
    cancelBtn.click()
    await settle()
    expect(cancelCalls).toHaveLength(1)
    expect(cancelCalls[0]!.operationId).toBe(firstReq.operationId)
    expect(body.querySelector('[data-testid="save-as-job-status"]')?.getAttribute('data-job-phase')).toBe('cancelled')

    // Now test reconciling to SUCCEEDED on a fresh attempt
    ;(body.querySelector('[data-testid="save-as-close-btn"]') as HTMLButtonElement).click()
    await settle()
    publishBehavior = 'drop-network'
    jobStateToReturn = RepositorySinkJobState.SUCCEEDED
    await wrapper.get('[data-testid="save-as-rendition-parsed"]').trigger('click')
    await settle()
    ;(body.querySelector('[data-testid="save-as-submit-btn"]') as HTMLButtonElement).click()
    await settle()
    ;(body.querySelector('[data-testid="save-as-reconcile-btn"]') as HTMLButtonElement).click()
    await settle()
    expect(body.querySelector('[data-testid="save-as-job-status"]')?.getAttribute('data-job-phase')).toBe('succeeded')
    expect(body.querySelector('[data-testid="save-as-job-receipt"]')?.textContent).toContain('v3')
    wrapper.unmount()
  })

  it('disables Save-as actions with a clear explanation when the signed-in session is read-only', async () => {
    const readOnlySession = {
      status: async () =>
        create(ConsoleSessionStatusSchema, {
          authenticated: true,
          principal: 'auditor@example.com',
          accountIds: ['acct-a'],
          intakeWriters: [
            create(IntakeWriterScopeSchema, { accountId: 'acct-other', datasourceId: 'ds-other' }),
          ],
        }),
    }

    const { wrapper } = await mountRepositoryView(
      RepositoryArchiveEntryView,
      { name: 'repository-archive-entry', query: { account: 'acct-a', archive: 'datasets', entry: 'e-metrics' } },
      (router: ConnectRouter) => {
        router.service(ArchiveService, {
          getEntryManifest: () =>
            create(GetEntryManifestResponseSchema, {
              info: create(EntryInfoSchema, { address: entryAddr(), currentVersion: 1n, title: 'Metrics' }),
              manifest: create(VersionManifestSchema, {
                address: entryAddr(),
                version: 1n,
                renditions: [
                  create(RenditionManifestEntrySchema, {
                    rendition: create(RenditionDescriptorSchema, { name: 'parsed', mediaType: 'application/x-protobuf' }),
                    state: RenditionState.PRESENT,
                    sizeBytes: BigInt(PROTO_BYTES.length),
                    sha256: PROTO_SHA,
                  }),
                ],
              }),
            }),
          listVersions: () => create(ListVersionsResponseSchema, { versions: [] }),
        })
      },
      { [REPOSITORY_SESSION as symbol]: readOnlySession },
    )

    expect(wrapper.get('[data-testid="save-as-readonly-notice"]').text()).toContain('auditor@example.com')
    const saveAsBtn = wrapper.get('[data-testid="save-as-rendition-parsed"]')
    expect(saveAsBtn.attributes('disabled')).toBeDefined()
    expect(wrapper.find('[data-testid="select-rendition-parsed"]').exists()).toBe(true)
    wrapper.unmount()
  })

  it('keeps RepositoryDocumentView part metadata, raw bytes, and Download operable when retained fragment schema loading fails, and supports Save-as on decoded parts with Any occurrences', async () => {
    let failFragmentSchema = true
    const seenCapRequests: ListSaveAsCapabilitiesRequest[] = []
    const packedAny = anyPack(StringValueSchema, create(StringValueSchema, { value: 'nested-row' }))
    const docWithAny = create(DocumentSchema, {
      docId: 'doc-1',
      structuredData: packedAny,
      blobBag: create(BlobBagSchema, {
        blobData: {
          case: 'blobs',
          value: create(BlobsSchema, {
            blob: [
              create(BlobSchema, {
                blobId: 'attachment-1',
                filename: 'archive.parquet',
                mimeType: 'application/vnd.apache.parquet',
                sizeBytes: 64n,
              }),
            ],
          }),
        },
      }),
    })
    const docBytes = toBinary(DocumentSchema, docWithAny)
    const docSha = createHash('sha256').update(docBytes).digest('hex')
    const coreSelector = create(PartSelectorSchema, { selector: { case: 'part', value: DocumentPart.CORE } })

    const { wrapper } = await mountRepositoryView(
      RepositoryDocumentView,
      { name: 'repository-document', query: { account: 'acct-a', graph: 'intake:acct-a', node: 'ds-files', doc: 'doc-1' } },
      (router: ConnectRouter) => {
        router.service(ArchiveService, {
          listArchives: () =>
            create(ListArchivesResponseSchema, {
              archives: [create(ArchiveSchema, { accountId: 'acct-a', name: 'exports' })],
            }),
        })
        router.service(DocumentService, {
          getManifest: () =>
            create(GetManifestResponseSchema, {
              manifest: create(DocumentManifestSchema, {
                address: testAddress(),
                revision: 5n,
                parts: [
                  create(PartManifestEntrySchema, {
                    selector: coreSelector,
                    state: PartState.PRESENT,
                    sizeBytes: BigInt(docBytes.length),
                    sha256: docSha,
                    writtenInRevision: 5n,
                    contentIncluded: true,
                  }),
                ],
              }),
              validation: create(RevisionValidationSchema, {
                status: ValidationStatus.TYPED_VALID,
                partSchemas: [
                  create(PartSchemaBindingSchema, {
                    part: coreSelector,
                    fragmentSchema: {
                      typeUrl: `type.googleapis.com/${DocumentSchema.typeName}`,
                      descriptorSha256: DOC_DESCRIPTORS_SHA,
                    },
                  }),
                ],
                resolvedOccurrences: [
                  {
                    occurrence: { part: coreSelector, steps: [{ step: { case: 'fieldNumber', value: 4 } }] },
                    schema: {
                      typeUrl: packedAny.typeUrl,
                      descriptorSha256: STRING_DESCRIPTORS_SHA,
                    },
                    payloadSha256: createHash('sha256').update(packedAny.value).digest('hex'),
                    payloadLength: BigInt(packedAny.value.length),
                  },
                ],
              }),
            }),
          getDocument: () => create(GetDocumentResponseSchema, { document: docWithAny }),
          listRevisions: () => create(ListRevisionsResponseSchema),
          async *readDocument() {
            yield create(ReadDocumentResponseSchema, {
              frame: {
                case: 'manifest',
                value: create(DocumentManifestSchema, {
                  address: testAddress(),
                  revision: 5n,
                  parts: [
                    create(PartManifestEntrySchema, {
                      selector: coreSelector,
                      state: PartState.PRESENT,
                      sizeBytes: BigInt(docBytes.length),
                      sha256: docSha,
                      writtenInRevision: 5n,
                      contentIncluded: true,
                    }),
                  ],
                }),
              },
            })
            yield create(ReadDocumentResponseSchema, {
              frame: { case: 'partData', value: create(PartDataSchema, { selector: coreSelector, offset: 0n, data: docBytes, last: true }) },
            })
            yield create(ReadDocumentResponseSchema, { frame: { case: 'complete', value: create(ReadCompleteSchema) } })
          },
          getSchemaDescriptor: (req) => {
            if (failFragmentSchema) {
              throw new ConnectError('descriptor store offline', Code.Unavailable)
            }
            return create(GetSchemaDescriptorResponseSchema, {
              revision: 5n,
              descriptorSha256: req.descriptorSha256,
              descriptorSet: req.descriptorSha256 === DOC_DESCRIPTORS_SHA ? DOC_DESCRIPTORS : STRING_DESCRIPTORS,
            })
          },
        })
        router.service(RepositorySinkService, {
          listSaveAsCapabilities: (req) => {
            seenCapRequests.push(req)
            return create(ListSaveAsCapabilitiesResponseSchema, {
              resolvedSourceRevision: 5n,
              sourceSchemaType: DocumentSchema.typeName,
              sourceSizeBytes: BigInt(docBytes.length),
              capabilities: [
                create(SaveAsCapabilitySchema, {
                  capability: parquetCap,
                  status: SaveAsCompatibilityStatus.COMPATIBLE,
                }),
              ],
            })
          },
        })
      },
    )

    // Verify format icon on Files tab (BlobFiles)
    const tabs = wrapper.findAll('.v-tab')
    await tabs.find((t) => t.text() === 'Files')!.trigger('click')
    await settle()
    expect(wrapper.get('[data-testid="blob-attachment-1"] [data-testid="format-icon"]').attributes('data-format-family')).toBe('parquet')

    await tabs.find((t) => t.text() === 'Parts')!.trigger('click')
    await settle()

    // 1. Read part while fragment schema store is offline -> part-view MUST NOT blank!
    await wrapper.get('[data-testid="read-part-0"]').trigger('click')
    await settle()
    const failedSchemaView = wrapper.get('[data-testid="part-view"]')
    expect(failedSchemaView.text()).toContain('CORE')
    expect(failedSchemaView.text()).toContain('revision 5')
    expect(failedSchemaView.find('[data-testid="download-part"]').exists()).toBe(true)
    expect(failedSchemaView.get('[data-testid="part-schema-failure"]').text()).toContain('descriptor store offline')

    // 2. Restore schema store and read part -> decodes cleanly and exposes Save-as with Any occurrence options!
    failFragmentSchema = false
    await wrapper.get('[data-testid="read-part-0"]').trigger('click')
    await settle()
    const decodedView = wrapper.get('[data-testid="part-view"]')
    expect(decodedView.find('[data-testid="save-as-open-part"]').exists()).toBe(true)
    await decodedView.get('[data-testid="save-as-open-part"]').trigger('click')
    await settle()

    expect(seenCapRequests).toHaveLength(1)
    const capSource = seenCapRequests[0]!.source
    expect(capSource?.kind.case).toBe('repositoryRevision')
    if (capSource?.kind.case === 'repositoryRevision' && capSource.kind.value.target.case === 'documentPart') {
      expect(capSource.kind.value.target.value.revision?.selector).toEqual({ case: 'revision', value: 5n })
    }
    expect(document.body.querySelector('[data-testid="save-as-occurrence-picker"]')).not.toBeNull()
    expect(document.body.querySelector('[data-testid="save-as-capability-parquet-v1"]')).not.toBeNull()
    wrapper.unmount()
  })

  it('surfaces version conflicts (Code.Aborted) with an explicit Reload source action', async () => {
    let manifestCalls = 0
    const { wrapper } = await mountRepositoryView(
      RepositoryArchiveEntryView,
      { name: 'repository-archive-entry', query: { account: 'acct-a', archive: 'datasets', entry: 'e-metrics' } },
      (router: ConnectRouter) => {
        router.service(ArchiveService, {
          listArchives: () => create(ListArchivesResponseSchema, { archives: [create(ArchiveSchema, { accountId: 'acct-a', name: 'datasets' })] }),
          getEntryManifest: () => {
            manifestCalls += 1
            return create(GetEntryManifestResponseSchema, {
              info: create(EntryInfoSchema, { address: entryAddr(), currentVersion: BigInt(manifestCalls), title: 'Metrics' }),
              manifest: create(VersionManifestSchema, {
                address: entryAddr(),
                version: BigInt(manifestCalls),
                renditions: [
                  create(RenditionManifestEntrySchema, {
                    rendition: create(RenditionDescriptorSchema, { name: 'parsed', mediaType: 'application/x-protobuf' }),
                    state: RenditionState.PRESENT,
                    sizeBytes: BigInt(PROTO_BYTES.length),
                    sha256: PROTO_SHA,
                  }),
                ],
              }),
            })
          },
          listVersions: () => create(ListVersionsResponseSchema, { versions: [] }),
        })
        router.service(RepositorySinkService, {
          listRenditionProvenance: () => create(ListRenditionProvenanceResponseSchema),
          listSaveAsCapabilities: () =>
            create(ListSaveAsCapabilitiesResponseSchema, {
              resolvedSourceRevision: 1n,
              sourceSchemaType: DocumentSchema.typeName,
              sourceSizeBytes: BigInt(PROTO_BYTES.length),
              capabilities: [
                create(SaveAsCapabilitySchema, {
                  capability: parquetCap,
                  status: SaveAsCompatibilityStatus.COMPATIBLE,
                }),
              ],
            }),
          publishRepositorySink: () => {
            throw new ConnectError('Expected destination version 1 does not match current version 2', Code.Aborted)
          },
        })
      },
    )

    expect(manifestCalls).toBe(1)
    await wrapper.get('[data-testid="save-as-rendition-parsed"]').trigger('click')
    await settle()
    ;(document.body.querySelector('[data-testid="save-as-submit-btn"]') as HTMLButtonElement).click()
    await settle()

    const conflictAlert = document.body.querySelector('[data-testid="save-as-rpc-error"]')!
    expect(conflictAlert.textContent).toContain('Version conflict')
    expect(conflictAlert.textContent).toContain('Expected destination version 1 does not match current version 2')

    const reloadBtn = document.body.querySelector('[data-testid="save-as-reload-source-btn"]') as HTMLButtonElement
    reloadBtn.click()
    await settle()
    expect(manifestCalls).toBe(2)
    expect(wrapper.get('[data-testid="version-chip"]').text()).toBe('version 2 (current)')
    wrapper.unmount()
  })
})
