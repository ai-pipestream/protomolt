/**
 * Browser harness for the 5 Repository Console frontend improvement slices.
 * Mounts real Vue views with Vuetify and in-memory binary Connect routers in Chromium.
 */
import 'vuetify/styles'
import '@mdi/font/css/materialdesignicons.css'
import '@/styles/console.css'
import { createApp, h } from 'vue'
import { createVuetify } from 'vuetify'
import * as components from 'vuetify/components'
import * as directives from 'vuetify/directives'
import { create, toBinary } from '@bufbuild/protobuf'
import { anyPack, StringValueSchema } from '@bufbuild/protobuf/wkt'
import { CharacterizationEvidenceSchema, ClassificationSchema, ClassificationState } from '@/gen/ai/protomolt/proto/asset/v1/classification_pb'
import { BridgeKind, BridgeStatus } from '@/gen/ai/protomolt/proto/asset/v1/bridge_pb'
import { ContentClass, ContentProfileSchema, QualityDimensionSchema, QualityScoreSchema } from '@/gen/ai/protomolt/proto/asset/v1/content_pb'
import { FormatFactSchema, PdfDocumentSchema } from '@/gen/ai/protomolt/proto/asset/v1/format_pb'
import { IntakeWriterScopeSchema } from '@/gen/ai/protomolt/proto/authz/v1/account_bindings_pb'
import { ConsoleSessionStatusSchema } from '@/gen/ai/protomolt/proto/authz/v1/console_session_pb'
import { ArchiveSchema, ArchiveStatsSchema, VersioningPolicy } from '@/gen/ai/protomolt/proto/repo/archive/v1/archive_pb'
import {
  ArchiveService,
  BridgeEntryResponseSchema,
  BridgeOutcomeSchema,
  ClassifyEntryResponseSchema,
  CreateArchiveResponseSchema,
  GetArchiveResponseSchema,
  GetArchiveStatsResponseSchema,
  GetEntryManifestResponseSchema,
  ListArchivesResponseSchema,
  ListEntriesResponseSchema,
  ListVersionsResponseSchema,
  PutEntryResponseSchema,
} from '@/gen/ai/protomolt/proto/repo/archive/v1/archive_service_pb'
import {
  EntryAddressSchema,
  EntryInfoSchema,
  RenditionDescriptorSchema,
  RenditionManifestEntrySchema,
  RenditionState,
  VersionManifestSchema,
  WriteAttributionSchema,
} from '@/gen/ai/protomolt/proto/repo/archive/v1/entry_pb'
import { BlobBagSchema, BlobSchema, BlobsSchema, DocumentSchema, SearchMetadataSchema } from '@/gen/ai/protomolt/proto/repo/v1/document_pb'
import {
  CreateDriveResponseSchema,
  DriveProviderConfigSchema,
  DriveSchema,
  DriveService,
  DriveStatus,
  DriveType,
  GetDriveResponseSchema,
  ListDrivesResponseSchema,
  S3DriveConfigSchema,
  type Drive,
} from '@/gen/ai/protomolt/proto/repo/v1/drive_pb'
import {
  BacklinkSchema,
  BlobHeaderSchema,
  ConceptFilterEvidenceSchema,
  ConceptFilterSchema,
  DocumentFilterSchema,
  DocumentLookupSchema,
  DocumentService,
  DocumentSummarySchema,
  DynamicLinkSelectorSchema,
  GetDocumentResponseSchema,
  GetManifestResponseSchema,
  GetOperationOutcomeResponseSchema,
  GetSchemaDescriptorResponseSchema,
  ListBacklinksResponseSchema,
  ListDocumentsResponseSchema,
  ListLinksResponseSchema,
  ListRevisionsResponseSchema,
  MountedConceptSchemeSchema,
  ReadBlobResponseSchema,
  ReadDocumentResponseSchema,
  RecordedOutcomeSchema,
  ResolveLinkResponseSchema,
  ResolvedLinkSchema,
  RevisionSummarySchema,
  StaticLinkStateSchema,
} from '@/gen/ai/protomolt/proto/repo/v2/document_service_pb'
import {
  AccessionSchema,
  DocumentManifestSchema,
  DocumentPart,
  PartSelectorSchema,
  PartState,
  RevisionValidationSchema,
  ValidationStatus,
} from '@/gen/ai/protomolt/proto/repo/v2/manifest_pb'
import {
  ConceptExpansionSchema,
  ConceptNotationSchema,
  ConceptRefSchema,
  ConceptSchema,
  ConceptSchemeSchema,
  LangLabelSchema,
} from '@/gen/ai/protomolt/proto/types/v1/concepts_pb'
import { REPOSITORY_CLIENTS, REPOSITORY_SESSION, REPOSITORY_SIGN_IN } from '@/repository/clients'
import { at, descriptorSetOf, routerClients, testAddress } from '@/repository/repositoryTestKit'
import { repositoryRouter } from '@/repository/repositoryViewKit'
import { Sha256 } from '@/repository/sha256'
import { moltDark } from '@/styles/theme'
import RepositoryAccountView from '@/views/repository/RepositoryAccountView.vue'
import RepositoryArchiveEntryView from '@/views/repository/RepositoryArchiveEntryView.vue'
import RepositoryArchiveView from '@/views/repository/RepositoryArchiveView.vue'
import RepositoryDocumentView from '@/views/repository/RepositoryDocumentView.vue'
import RepositoryLookupView from '@/views/repository/RepositoryLookupView.vue'
import RepositoryUploadView from '@/views/repository/RepositoryUploadView.vue'

const hash = (bytes: Uint8Array) => new Sha256().update(bytes).hex()

/** Builds a 360x96 PNG badge in the browser canvas for Slice 3 verified raster image preview. */
function buildSamplePng(): Uint8Array {
  const canvas = document.createElement('canvas')
  canvas.width = 360
  canvas.height = 96
  const ctx = canvas.getContext('2d')
  if (ctx) {
    ctx.fillStyle = '#132a38'
    ctx.fillRect(0, 0, 360, 96)
    ctx.strokeStyle = '#38bdf8'
    ctx.lineWidth = 3
    ctx.strokeRect(4, 4, 352, 88)
    ctx.fillStyle = '#38bdf8'
    ctx.font = 'bold 16px monospace'
    ctx.fillText('PROTOMOLT VERIFIED BLOB PREVIEW', 22, 40)
    ctx.fillStyle = '#a7f3d0'
    ctx.font = '13px monospace'
    ctx.fillText('signature-stamp.png · image/png · SHA-256 OK', 22, 66)
  }
  const dataUrl = canvas.toDataURL('image/png')
  const b64 = dataUrl.slice(dataUrl.indexOf(',') + 1)
  return Uint8Array.from(atob(b64), (c) => c.charCodeAt(0))
}

const SAMPLE_JSON = new TextEncoder().encode(
  JSON.stringify(
    {
      pipeline: 'intake-enrichment-v2',
      documentId: 'doc-1',
      revision: 7,
      metrics: { latencyMs: 42, tokens: 1280, verified: true },
      tags: ['contracts', 'nda', 'verified'],
    },
    null,
    2,
  ),
)

function buildVuetify() {
  return createVuetify({
    components,
    directives,
    theme: { defaultTheme: 'moltDark', themes: { moltDark } },
  })
}

export async function mountDocumentImprovements(initialTab?: string) {
  const SAMPLE_PNG = buildSamplePng()
  const packed7 = anyPack(
    StringValueSchema,
    create(StringValueSchema, { value: 'Clause 7.2: Mutual non-disclosure obligations remain active for 5 years.' }),
  )
  const packed6 = anyPack(
    StringValueSchema,
    create(StringValueSchema, { value: 'Clause 7.1: Initial draft confidentiality obligations for 3 years.' }),
  )

  const jsonSha = hash(SAMPLE_JSON)
  const pngSha = hash(SAMPLE_PNG)

  const doc7 = create(DocumentSchema, {
    docId: 'doc-1',
    structuredData: packed7,
    searchMetadata: create(SearchMetadataSchema, {
      title: 'Master Mutual NDA — Acme Corp (Final)',
      documentType: 'contract/nda',
      sourceMimeType: 'application/pdf',
      metadata: {
        department: 'legal',
        counterparty: 'Acme Corp',
        status: 'executed',
      },
    }),
    blobBag: create(BlobBagSchema, {
      blobData: {
        case: 'blobs',
        value: create(BlobsSchema, {
          blob: [
            create(BlobSchema, {
              blobId: 'blob-json',
              filename: 'enrichment-receipt.json',
              mimeType: 'application/json',
              sizeBytes: BigInt(SAMPLE_JSON.byteLength),
              checksum: jsonSha,
            }),
            create(BlobSchema, {
              blobId: 'blob-png',
              filename: 'signature-stamp.png',
              mimeType: 'image/png',
              sizeBytes: BigInt(SAMPLE_PNG.byteLength),
              checksum: pngSha,
            }),
          ],
        }),
      },
    }),
  })

  const doc6 = create(DocumentSchema, {
    docId: 'doc-1',
    structuredData: packed6,
    searchMetadata: create(SearchMetadataSchema, {
      title: 'Master Mutual NDA — Acme Corp (Draft)',
      documentType: 'contract/nda',
      sourceMimeType: 'application/pdf',
      metadata: {
        department: 'legal',
        counterparty: 'Acme Corp',
        status: 'draft',
      },
    }),
  })

  const bytes7 = toBinary(DocumentSchema, doc7)
  const bytes6 = toBinary(DocumentSchema, doc6)
  const rootSet = descriptorSetOf(DocumentSchema.file)
  const anySet = descriptorSetOf(StringValueSchema.file)
  const selector = create(PartSelectorSchema, { selector: { case: 'part', value: DocumentPart.CORE } })

  const manifest7 = create(DocumentManifestSchema, {
    address: testAddress(),
    revision: 7n,
    committedAt: at('2026-10-27T18:30:00Z'),
    parts: [
      {
        selector,
        state: PartState.PRESENT,
        sizeBytes: BigInt(bytes7.length),
        sha256: hash(bytes7),
        writtenInRevision: 7n,
        contentIncluded: true,
      },
    ],
  })

  const manifest6 = create(DocumentManifestSchema, {
    address: testAddress(),
    revision: 6n,
    committedAt: at('2026-10-26T12:00:00Z'),
    parts: [
      {
        selector,
        state: PartState.PRESENT,
        sizeBytes: BigInt(bytes6.length),
        sha256: hash(bytes6),
        writtenInRevision: 6n,
        contentIncluded: true,
      },
    ],
  })

  const makeValidation = (packed: typeof packed7, coreBytes: Uint8Array) =>
    create(RevisionValidationSchema, {
      status: ValidationStatus.TYPED_VALID,
      candidateSha256: hash(coreBytes),
      partSchemas: [
        {
          part: selector,
          fragmentSchema: {
            typeUrl: `type.googleapis.com/${DocumentSchema.typeName}`,
            descriptorSha256: hash(rootSet),
          },
        },
      ],
      resolvedOccurrences: [
        {
          occurrence: { part: selector, steps: [{ step: { case: 'fieldNumber', value: 4 } }] },
          schema: { typeUrl: packed.typeUrl, descriptorSha256: hash(anySet) },
          payloadSha256: hash(packed.value),
          payloadLength: BigInt(packed.value.length),
        },
      ],
    })

  const validation7 = makeValidation(packed7, bytes7)
  const validation6 = makeValidation(packed6, bytes6)

  const clients = routerClients((router) =>
    router.service(DocumentService, {
      getManifest: (req) => {
        const rev = req.revision?.selector.case === 'revision' ? req.revision.selector.value : 7n
        return create(GetManifestResponseSchema, {
          manifest: rev === 6n ? manifest6 : manifest7,
          validation: rev === 6n ? validation6 : validation7,
        })
      },
      getDocument: (req) => {
        const rev = req.revision?.selector.case === 'revision' ? req.revision.selector.value : 7n
        return create(GetDocumentResponseSchema, {
          document: rev === 6n ? doc6 : doc7,
          manifest: rev === 6n ? manifest6 : manifest7,
          validation: rev === 6n ? validation6 : validation7,
        })
      },
      listRevisions: () =>
        create(ListRevisionsResponseSchema, {
          revisions: [
            create(RevisionSummarySchema, {
              revision: 7n,
              previousRevision: 6n,
              committedAt: at('2026-10-27T18:30:00Z'),
              validationStatus: ValidationStatus.TYPED_VALID,
              owner: { owner: { case: 'step', value: { moduleId: 'legal-enricher' } } },
            }),
            create(RevisionSummarySchema, {
              revision: 6n,
              previousRevision: 0n,
              committedAt: at('2026-10-26T12:00:00Z'),
              validationStatus: ValidationStatus.TYPED_VALID,
              owner: { owner: { case: 'step', value: { moduleId: 'intake-gateway' } } },
            }),
          ],
        }),
      listLinks: () =>
        create(ListLinksResponseSchema, {
          revision: 7n,
          links: [
            create(ResolvedLinkSchema, {
              fieldPath: 'structured_data.supersedes',
              ordinal: 0,
              relation: 'supersedes',
              label: 'Prior Mutual NDA (2024)',
              target: {
                case: 'staticTarget',
                value: create(StaticLinkStateSchema, {
                  address: testAddress({ docId: 'nda-2024-base' }),
                  pinnedRevision: 2n,
                }),
              },
            }),
            create(ResolvedLinkSchema, {
              fieldPath: 'structured_data.archived_draft',
              ordinal: 0,
              relation: 'replaces',
              label: 'Retired Early Draft',
              target: {
                case: 'staticTarget',
                value: create(StaticLinkStateSchema, {
                  address: testAddress({ docId: 'legacy-draft-001' }),
                  targetTombstoned: true,
                }),
              },
            }),
            create(ResolvedLinkSchema, {
              fieldPath: 'structured_data.related_agreements',
              ordinal: 0,
              relation: 'related',
              label: 'Active Commercial Contracts',
              target: {
                case: 'dynamicTarget',
                value: create(DynamicLinkSelectorSchema, {
                  filter: create(DocumentFilterSchema, {
                    concept: create(ConceptFilterSchema, {
                      concept: create(ConceptRefSchema, { schemeId: 'legal', conceptId: 'contracts' }),
                      expansion: create(ConceptExpansionSchema, { narrower: true }),
                    }),
                  }),
                }),
              },
            }),
          ],
        }),
      listBacklinks: () =>
        create(ListBacklinksResponseSchema, {
          backlinks: [
            create(BacklinkSchema, {
              address: testAddress({ docId: 'msa-2026-042' }),
              revision: 3n,
              fieldPath: 'structured_data.governing_nda',
              relation: 'references',
              pinnedRevision: 7n,
            }),
            create(BacklinkSchema, {
              address: testAddress({ docId: 'dpa-2026-009' }),
              revision: 2n,
              fieldPath: 'structured_data.parent_agreement',
              relation: 'amends',
            }),
          ],
        }),
      resolveLink: () =>
        create(ResolveLinkResponseSchema, {
          documents: [
            create(DocumentSummarySchema, {
              address: testAddress({ docId: 'msa-2026-042' }),
              revision: 3n,
              committedAt: at('2026-10-26T15:10:00Z'),
              lookup: create(DocumentLookupSchema, { title: 'Enterprise MSA — Globex Systems' }),
            }),
            create(DocumentSummarySchema, {
              address: testAddress({ docId: 'sow-2026-015' }),
              revision: 1n,
              committedAt: at('2026-10-28T11:00:00Z'),
              lookup: create(DocumentLookupSchema, { title: 'Statement of Work #15 — AI Platform' }),
            }),
          ],
          conceptEvidence: create(ConceptFilterEvidenceSchema, {
            expandedConcepts: 3,
            mountedSchemes: [create(MountedConceptSchemeSchema, { schemeId: 'legal', version: '2026.10' })],
          }),
        }),
      getSchemaDescriptor: (request) => {
        if (request.descriptorSha256 === hash(rootSet)) {
          return create(GetSchemaDescriptorResponseSchema, { descriptorSet: rootSet })
        }
        return create(GetSchemaDescriptorResponseSchema, { descriptorSet: anySet })
      },
      async *readDocument(req) {
        const rev = req.revision?.selector.case === 'revision' ? req.revision.selector.value : 7n
        const m = rev === 6n ? manifest6 : manifest7
        const b = rev === 6n ? bytes6 : bytes7
        yield create(ReadDocumentResponseSchema, { frame: { case: 'manifest', value: m } })
        yield create(ReadDocumentResponseSchema, { frame: { case: 'partData', value: { selector, offset: 0n, data: b, last: true } } })
        yield create(ReadDocumentResponseSchema, { frame: { case: 'complete', value: {} } })
      },
      async *readBlob(req) {
        const isPng = req.blobId === 'blob-png'
        const payload = isPng ? SAMPLE_PNG : SAMPLE_JSON
        const sha = isPng ? pngSha : jsonSha
        const offset = Number(req.offset)
        const length = req.length === 0n ? payload.byteLength - offset : Number(req.length)
        const slice = payload.subarray(offset, offset + length)
        const revision = req.revision?.selector.case === 'revision' ? req.revision.selector.value : 7n
        yield create(ReadBlobResponseSchema, {
          frame: {
            case: 'header',
            value: create(BlobHeaderSchema, {
              revision,
              totalLength: BigInt(payload.byteLength),
              sha256: sha,
              mimeType: isPng ? 'image/png' : 'application/json',
              filename: isPng ? 'signature-stamp.png' : 'enrichment-receipt.json',
            }),
          },
        })
        yield create(ReadBlobResponseSchema, { frame: { case: 'data', value: slice } })
      },
    }),
  )

  const router = repositoryRouter()
  const query: Record<string, string> = {
    account: 'acct-a',
    graph: 'intake:acct-a',
    node: 'ds-files',
    doc: 'doc-1',
    revision: '7',
  }
  if (initialTab) query.tab = initialTab
  await router.push({ name: 'repository-document', query })
  await router.isReady()

  const app = createApp({
    render: () =>
      h(components.VApp, {}, () =>
        h(components.VMain, { style: 'padding: 24px; max-width: 1240px; margin: auto;' }, () => h(RepositoryDocumentView)),
      ),
  })
  app
    .use(buildVuetify())
    .use(router)
    .provide(REPOSITORY_CLIENTS, clients)
    .provide(REPOSITORY_SIGN_IN, () => {})
    .mount('#app')
}

export async function mountAccountImprovements(initialUrl = '/repository/accounts/acct-a'): Promise<{ legalTaxonomyBase64: string }> {
  const label = (text: string, language = 'en') => create(LangLabelSchema, { text, language })
  const legalSchemeProto = create(ConceptSchemeSchema, {
    prefLabel: [label('Legal Matter Taxonomy', 'en')],
    concepts: [
      create(ConceptSchema, {
        id: 'contracts',
        prefLabel: [label('Commercial Contracts', 'en')],
        definition: [label('Binding commercial agreements and confidentiality instruments.', 'en')],
        notation: [create(ConceptNotationSchema, { value: 'LEGAL-100', datatypeIri: 'https://vocab.protomolt.ai/dt' })],
        iri: 'https://vocab.protomolt.ai/legal-taxonomy/contracts',
      }),
      create(ConceptSchema, {
        id: 'nda',
        prefLabel: [label('Non-Disclosure Agreements (NDA)', 'en')],
        broader: ['contracts'],
        notation: [create(ConceptNotationSchema, { value: 'LEGAL-110', datatypeIri: 'https://vocab.protomolt.ai/dt' })],
      }),
      create(ConceptSchema, {
        id: 'msa',
        prefLabel: [label('Master Services Agreements (MSA)', 'en')],
        broader: ['contracts'],
        notation: [create(ConceptNotationSchema, { value: 'LEGAL-120', datatypeIri: 'https://vocab.protomolt.ai/dt' })],
      }),
    ],
  })
  const legalBytes = toBinary(ConceptSchemeSchema, legalSchemeProto)
  let binary = ''
  for (const byte of legalBytes) binary += String.fromCharCode(byte)
  const legalTaxonomyBase64 = btoa(binary)

  const extraDrives: Drive[] = []
  const extraArchives: ReturnType<typeof create<typeof ArchiveSchema>>[] = []

  const clients = routerClients((router) => {
    router.service(DriveService, {
      listDrives: (r) =>
        create(ListDrivesResponseSchema, {
          drives: [
            create(DriveSchema, {
              driveId: 'legal-vault',
              name: 'legal-vault',
              accountId: r.accountId,
              driveType: DriveType.INTAKE,
              status: DriveStatus.ACTIVE,
              provider: 's3',
              bucket: 'acme-legal-vault',
              prefix: 'acct-a',
              createdAt: at('2026-01-15T09:00:00Z'),
            }),
            ...extraDrives,
          ],
        }),
      createDrive: (r) => {
        const d = create(DriveSchema, {
          driveId: `drv-${extraDrives.length + 2}`,
          name: r.name,
          accountId: r.accountId,
          driveType: r.driveType,
          status: DriveStatus.ACTIVE,
          provider: r.provider || 's3',
          bucket: r.bucket || 'acme-archive-store',
          prefix: r.prefix || 'acct-a/archives',
          createdAt: at('2026-10-29T18:00:00Z'),
        })
        extraDrives.push(d)
        return create(CreateDriveResponseSchema, { drive: d })
      },
      getDrive: (r) => {
        const name = r.coordinate.case === 'name' ? r.coordinate.value : 'legal-vault'
        return create(GetDriveResponseSchema, {
          drive: create(DriveSchema, {
            driveId: 'legal-vault',
            name,
            accountId: r.accountId || 'acct-a',
            driveType: DriveType.INTAKE,
            status: DriveStatus.ACTIVE,
            provider: 's3',
            bucket: 'acme-legal-vault',
            prefix: 'acct-a',
            region: 'us-east-1',
            credentialsRef: 'vault:aws/legal-vault-rw',
            createdAt: at('2026-01-15T09:00:00Z'),
            providerConfig: create(DriveProviderConfigSchema, {
              config: {
                case: 's3',
                value: create(S3DriveConfigSchema, {
                  endpointOverride: 'https://s3.us-east-1.acme.internal',
                  forcePathStyle: true,
                }),
              },
              options: {
                'checksum.algorithm': 'SHA256',
                'multipart.threshold': '16MB',
              },
            }),
            metadata: {
              compliance: 'SOC2-Type-II',
              owner: 'legal-ops',
              retention: '7-years',
            },
          }),
        })
      },
    })
    router.service(ArchiveService, {
      listArchives: (r) =>
        create(ListArchivesResponseSchema, {
          archives: [
            create(ArchiveSchema, {
              name: 'executed-agreements',
              accountId: r.accountId,
              driveName: 'legal-vault',
              versioning: VersioningPolicy.RETAINED,
              description: 'Signed agreements and OCR renditions',
            }),
            ...extraArchives,
          ],
        }),
      createArchive: (r) => {
        const a = create(ArchiveSchema, {
          name: r.archive?.name ?? 'regulatory-filings-2026',
          accountId: r.archive?.accountId ?? 'acct-a',
          driveName: r.archive?.driveName ?? 'legal-vault',
          versioning: r.archive?.versioning ?? VersioningPolicy.RETAINED,
          description: r.archive?.description ?? '2026 regulatory filings corpus',
          createdAt: at('2026-10-29T18:01:00Z'),
        })
        extraArchives.push(a)
        return create(CreateArchiveResponseSchema, { archive: a })
      },
    })
    router.service(DocumentService, {
      listDocuments: (r) =>
        create(ListDocumentsResponseSchema, {
          documents: [
            create(DocumentSummarySchema, {
              address: { accountId: r.accountId, graphId: 'intake:acct-a', graphAddressId: 'ds-legal', docId: 'nda-2026-001' },
              revision: 7n,
              committedAt: at('2026-10-27T18:30:00Z'),
              lookup: create(DocumentLookupSchema, {
                title: 'Master Mutual NDA — Acme Corp',
                documentType: 'contract/nda',
                sourceMimeType: 'application/pdf',
                datasourceId: 'ds-legal',
              }),
            }),
            create(DocumentSummarySchema, {
              address: { accountId: r.accountId, graphId: 'intake:acct-a', graphAddressId: 'ds-finance', docId: 'msa-2026-042' },
              revision: 3n,
              committedAt: at('2026-10-26T15:10:00Z'),
              lookup: create(DocumentLookupSchema, {
                title: 'Enterprise MSA — Globex Systems',
                documentType: 'contract/msa',
                sourceMimeType: 'application/pdf',
                datasourceId: 'ds-finance',
              }),
            }),
          ],
          conceptEvidence: r.filter?.concept
            ? create(ConceptFilterEvidenceSchema, {
                expandedConcepts: 3,
                mountedSchemes: [create(MountedConceptSchemeSchema, { schemeId: 'legal', version: '2026.10' })],
              })
            : undefined,
        }),
    })
  })

  const sessionStatus = create(ConsoleSessionStatusSchema, {
    authenticated: true,
    principal: 'legal-ops@acme.example',
    accountIds: ['acct-a'],
    intakeWriters: [
      create(IntakeWriterScopeSchema, {
        accountId: 'acct-a',
        datasourceId: 'ds-legal',
      }),
      create(IntakeWriterScopeSchema, {
        accountId: 'acct-a',
        datasourceId: 'ds-finance',
      }),
    ],
  })

  const router = repositoryRouter()
  await router.push(initialUrl)
  await router.isReady()

  const app = createApp({
    render: () =>
      h(components.VApp, {}, () =>
        h(components.VMain, { style: 'padding: 24px; max-width: 1240px; margin: auto;' }, () => h(RepositoryAccountView)),
      ),
  })
  app
    .use(buildVuetify())
    .use(router)
    .provide(REPOSITORY_CLIENTS, clients)
    .provide(REPOSITORY_SIGN_IN, () => {})
    .provide(REPOSITORY_SESSION, {
      status: async () => sessionStatus,
    })
    .mount('#app')

  return { legalTaxonomyBase64 }
}

export async function mountArchiveEntryImprovements() {
  const addr = create(EntryAddressSchema, { accountId: 'acct-a', archive: 'executed-agreements', entryId: 'nda-2026-001' })
  const origDesc = create(RenditionDescriptorSchema, { name: 'original', mediaType: 'application/pdf' })
  const ocrDesc = create(RenditionDescriptorSchema, { name: 'ocr', subKey: 'page-1', mediaType: 'text/plain' })

  const pdfShaV3 = '9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08'
  const pdfShaV1 = '4b227777d4dd1fc61c6f884f48641d02b4d121d3fd328cb08b5531fcacdabf8a'
  const ocrShaV3 = 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'

  const makeManifest = (version: bigint) =>
    create(VersionManifestSchema, {
      address: addr,
      version,
      totalBytes: version === 3n ? 248_512n : 240_000n,
      rootChecksum: version === 3n ? pdfShaV3 : pdfShaV1,
      createdAt: at(`2026-10-2${version}T14:20:00Z`),
      renditions: [
        create(RenditionManifestEntrySchema, {
          rendition: origDesc,
          state: RenditionState.PRESENT,
          sizeBytes: version === 3n ? 244_000n : 240_000n,
          sha256: version === 3n ? pdfShaV3 : pdfShaV1,
          writtenAt: at(`2026-10-2${version}T14:20:00Z`),
          writtenBy: create(WriteAttributionSchema, { module: 'intake-gateway', actor: 'legal-ops' }),
          contentProfile: create(ContentProfileSchema, {
            contentClass: ContentClass.INFORMATIONAL_TEXT,
            quality: create(QualityScoreSchema, { score: 0.97 }),
          }),
        }),
        ...(version >= 2n
          ? [
              create(RenditionManifestEntrySchema, {
                rendition: ocrDesc,
                state: RenditionState.PRESENT,
                sizeBytes: 4_512n,
                sha256: ocrShaV3,
                writtenAt: at(`2026-10-2${version}T14:21:00Z`),
                writtenBy: create(WriteAttributionSchema, { module: 'grparse-ocr', actor: 'pipeline' }),
                contentProfile: create(ContentProfileSchema, {
                  contentClass: ContentClass.OCR_TEXT,
                  quality: create(QualityScoreSchema, {
                    score: 0.92,
                    dimensions: [
                      create(QualityDimensionSchema, { name: 'character-confidence', score: 0.95, weight: 0.6 }),
                      create(QualityDimensionSchema, { name: 'dictionary-hit-rate', score: 0.88, weight: 0.4 }),
                    ],
                  }),
                }),
              }),
            ]
          : []),
      ],
    })

  const clients = routerClients((router) =>
    router.service(ArchiveService, {
      getArchive: (r) =>
        create(GetArchiveResponseSchema, {
          archive: create(ArchiveSchema, { name: r.archive, accountId: r.accountId, driveName: 'legal-vault' }),
        }),
      getArchiveStats: (r) =>
        create(GetArchiveStatsResponseSchema, {
          stats: create(ArchiveStatsSchema, { archive: r.archive, entries: 12n, versions: 28n }),
        }),
      getEntryManifest: (r) => {
        const ver = r.version === 0n ? 3n : r.version
        return create(GetEntryManifestResponseSchema, {
          info: create(EntryInfoSchema, {
            address: addr,
            entryUuid: '00000000-0000-5000-8000-000000000101',
            currentVersion: 3n,
            title: 'Master Mutual NDA — Acme Corp',
            filename: 'acme-mutual-nda-signed.pdf',
            contentType: 'application/pdf',
            sourceUri: 's3://acme-legal-drop/2026/10/acme-mutual-nda-signed.pdf',
            createdAt: at('2026-10-21T14:20:00Z'),
            updatedAt: at('2026-10-23T14:20:00Z'),
            classification: create(ClassificationSchema, {
              state: ClassificationState.VERIFIED,
              declared: create(FormatFactSchema, { format: { case: 'pdf', value: create(PdfDocumentSchema, { filename: 'acme-mutual-nda-signed.pdf' }) } }),
              identified: create(FormatFactSchema, { format: { case: 'pdf', value: create(PdfDocumentSchema, { filename: 'acme-mutual-nda-signed.pdf' }) } }),
            }),
            metadata: {
              department: 'legal',
              jurisdiction: 'US-DE',
              retentionClass: '7-years',
            },
          }),
          manifest: makeManifest(ver),
        })
      },
      listVersions: () =>
        create(ListVersionsResponseSchema, {
          versions: [makeManifest(3n), makeManifest(2n), makeManifest(1n)],
        }),
      classifyEntry: (req) =>
        create(ClassifyEntryResponseSchema, {
          classification: create(ClassificationSchema, {
            state: ClassificationState.VERIFIED,
            declared:
              req.declared ??
              create(FormatFactSchema, { format: { case: 'pdf', value: create(PdfDocumentSchema, { filename: 'acme-mutual-nda-signed.pdf' }) } }),
            identified: create(FormatFactSchema, {
              format: { case: 'pdf', value: create(PdfDocumentSchema, { filename: 'acme-mutual-nda-signed.pdf' }) },
            }),
            evidence: [
              create(CharacterizationEvidenceSchema, { signal: 'magic-bytes', observation: '%PDF-1.7 header at offset 0' }),
              create(CharacterizationEvidenceSchema, { signal: 'probe', observation: '14 pages, xref table intact, linearized=true' }),
            ],
          }),
        }),
      bridgeEntry: () =>
        create(BridgeEntryResponseSchema, {
          version: 4n,
          outcomes: [
            create(BridgeOutcomeSchema, {
              bridge: BridgeKind.DOCUMENT_TEXT,
              rendition: 'original',
              status: BridgeStatus.PRODUCED,
              detail: 'Extracted 14 pages (42,180 UTF-8 bytes)',
            }),
            create(BridgeOutcomeSchema, {
              bridge: BridgeKind.OCR_TEXT,
              rendition: 'ocr / page-1',
              status: BridgeStatus.UNCHANGED,
              detail: 'Matches existing SHA-256 e3b0c44298fc1c14',
            }),
          ],
        }),
      putEntry: (req) =>
        create(PutEntryResponseSchema, {
          version: 4n,
          rootChecksum: 'e4d909c290d0fb1ca068ffaddf22cbd0a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4',
          totalBytes: 249_024n,
          deduplicated: false,
          replayed: false,
          operationId: req.operationId || '018f4063-7d4b-7000-8000-000000000694',
        }),
    }),
  )

  const router = repositoryRouter()
  await router.push({
    name: 'repository-archive-entry',
    query: { account: 'acct-a', archive: 'executed-agreements', entry: 'nda-2026-001' },
  })
  await router.isReady()

  const app = createApp({
    render: () =>
      h(components.VApp, {}, () =>
        h(components.VMain, { style: 'padding: 24px; max-width: 1240px; margin: auto;' }, () => h(RepositoryArchiveEntryView)),
      ),
  })
  app
    .use(buildVuetify())
    .use(router)
    .provide(REPOSITORY_CLIENTS, clients)
    .provide(REPOSITORY_SIGN_IN, () => {})
    .mount('#app')
}

export async function mountArchiveImprovements(
  initialQuery: Record<string, string> = { account: 'acct-a', archive: 'executed-agreements' },
) {
  const clients = routerClients((router) =>
    router.service(ArchiveService, {
      getArchive: (r) =>
        create(GetArchiveResponseSchema, {
          archive: create(ArchiveSchema, {
            name: r.archive,
            accountId: r.accountId,
            driveName: 'legal-vault',
            versioning: VersioningPolicy.RETAINED,
            description: 'Signed agreements and OCR renditions',
            createdAt: at('2026-01-15T09:00:00Z'),
          }),
        }),
      getArchiveStats: (r) =>
        create(GetArchiveStatsResponseSchema, {
          stats: create(ArchiveStatsSchema, {
            archive: r.archive,
            entries: 3n,
            versions: 8n,
            currentBytes: 492_512n,
            retainedBytes: 980_000n,
          }),
        }),
      listEntries: () =>
        create(ListEntriesResponseSchema, {
          entries: [
            create(EntryInfoSchema, {
              address: create(EntryAddressSchema, { accountId: 'acct-a', archive: 'executed-agreements', entryId: 'nda-2026-001' }),
              currentVersion: 3n,
              title: 'Master Mutual NDA — Acme Corp',
              filename: 'acme-mutual-nda-signed.pdf',
              contentType: 'application/pdf',
              updatedAt: at('2026-10-23T14:20:00Z'),
              classification: create(ClassificationSchema, {
                state: ClassificationState.VERIFIED,
                declared: create(FormatFactSchema, { format: { case: 'pdf', value: create(PdfDocumentSchema, { filename: 'acme-mutual-nda-signed.pdf' }) } }),
                identified: create(FormatFactSchema, { format: { case: 'pdf', value: create(PdfDocumentSchema, { filename: 'acme-mutual-nda-signed.pdf' }) } }),
              }),
            }),
            create(EntryInfoSchema, {
              address: create(EntryAddressSchema, { accountId: 'acct-a', archive: 'executed-agreements', entryId: 'msa-2026-042' }),
              currentVersion: 2n,
              title: 'Enterprise MSA — Globex Systems',
              filename: 'globex-msa-2026.pdf',
              contentType: 'application/pdf',
              updatedAt: at('2026-10-26T15:10:00Z'),
              classification: create(ClassificationSchema, {
                state: ClassificationState.VERIFIED,
                declared: create(FormatFactSchema, { format: { case: 'pdf', value: create(PdfDocumentSchema, { filename: 'globex-msa-2026.pdf' }) } }),
                identified: create(FormatFactSchema, { format: { case: 'pdf', value: create(PdfDocumentSchema, { filename: 'globex-msa-2026.pdf' }) } }),
              }),
            }),
          ],
          totalCount: 2n,
        }),
    }),
  )

  const router = repositoryRouter()
  await router.push({ name: 'repository-archive', query: initialQuery })
  await router.isReady()

  const app = createApp({
    render: () =>
      h(components.VApp, {}, () =>
        h(components.VMain, { style: 'padding: 24px; max-width: 1240px; margin: auto;' }, () => h(RepositoryArchiveView)),
      ),
  })
  app
    .use(buildVuetify())
    .use(router)
    .provide(REPOSITORY_CLIENTS, clients)
    .provide(REPOSITORY_SIGN_IN, () => {})
    .mount('#app')
}

export async function mountLookupImprovements(
  initialQuery: Record<string, string> = { account: 'acct-a', op: '018f4063-7d4b-7000-8000-000000000199' },
) {
  const clients = routerClients((router) =>
    router.service(DocumentService, {
      getOperationOutcome: () =>
        create(GetOperationOutcomeResponseSchema, {
          state: {
            case: 'committed',
            value: create(RecordedOutcomeSchema, {
              result: {
                case: 'accession',
                value: create(AccessionSchema, {
                  address: {
                    accountId: 'acct-a',
                    graphId: 'intake:acct-a',
                    graphAddressId: 'ds-legal',
                    docId: 'nda-2026-001',
                  },
                  revision: 7n,
                  committedAt: at('2026-10-27T18:30:00Z'),
                }),
              },
              recordedAt: at('2026-10-27T18:30:00Z'),
            }),
          },
        }),
    }),
  )

  const sessionStatus = create(ConsoleSessionStatusSchema, {
    authenticated: true,
    principal: 'legal-ops@acme.example',
    accountIds: ['acct-a'],
    intakeWriters: [
      create(IntakeWriterScopeSchema, {
        accountId: 'acct-a',
        datasourceId: 'ds-legal',
      }),
    ],
  })

  const router = repositoryRouter()
  await router.push({ name: 'repository-lookup', query: initialQuery })
  await router.isReady()

  const app = createApp({
    render: () =>
      h(components.VApp, {}, () =>
        h(components.VMain, { style: 'padding: 24px; max-width: 1240px; margin: auto;' }, () => h(RepositoryLookupView)),
      ),
  })
  app
    .use(buildVuetify())
    .use(router)
    .provide(REPOSITORY_CLIENTS, clients)
    .provide(REPOSITORY_SIGN_IN, () => {})
    .provide(REPOSITORY_SESSION, {
      status: async () => sessionStatus,
    })
    .mount('#app')
}

export async function mountUploadImprovements() {
  const clients = routerClients((router) => {
    router.service(DriveService, {
      listDrives: () =>
        create(ListDrivesResponseSchema, {
          drives: [create(DriveSchema, { driveId: 'legal-vault', name: 'legal-vault', accountId: 'acct-a' })],
        }),
    })
    router.service(DocumentService, {
      getOperationOutcome: () => create(GetOperationOutcomeResponseSchema),
    })
  })

  const sessionStatus = create(ConsoleSessionStatusSchema, {
    authenticated: true,
    principal: 'legal-ops@acme.example',
    accountIds: ['acct-a'],
    intakeWriters: [
      create(IntakeWriterScopeSchema, {
        accountId: 'acct-a',
        datasourceId: 'ds-legal',
        allowCreate: true,
      }),
    ],
  })

  const router = repositoryRouter()
  await router.push({ name: 'repository-upload', query: { account: 'acct-a', datasource: 'ds-legal' } })
  await router.isReady()

  const app = createApp({
    render: () =>
      h(components.VApp, {}, () =>
        h(components.VMain, { style: 'padding: 24px; max-width: 1240px; margin: auto;' }, () => h(RepositoryUploadView)),
      ),
  })
  app
    .use(buildVuetify())
    .use(router)
    .provide(REPOSITORY_CLIENTS, clients)
    .provide(REPOSITORY_SIGN_IN, () => {})
    .provide(REPOSITORY_SESSION, {
      status: async () => sessionStatus,
    })
    .mount('#app')
}
