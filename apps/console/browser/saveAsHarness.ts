/** Browser fixture: real RepositoryArchiveEntryView + binary Connect clients over in-memory router. */
import 'vuetify/styles'
import '@mdi/font/css/materialdesignicons.css'
import '@/styles/console.css'
import { createApp, h } from 'vue'
import { createVuetify } from 'vuetify'
import * as components from 'vuetify/components'
import * as directives from 'vuetify/directives'
import { createMemoryHistory, createRouter } from 'vue-router'
import { create, fromBinary, toBinary } from '@bufbuild/protobuf'
import { timestampFromDate } from '@bufbuild/protobuf/wkt'
import RepositoryArchiveEntryView from '@/views/repository/RepositoryArchiveEntryView.vue'
import { IntakeWriterScopeSchema } from '@/gen/ai/protomolt/proto/authz/v1/account_bindings_pb'
import { ConsoleSessionStatusSchema } from '@/gen/ai/protomolt/proto/authz/v1/console_session_pb'
import { ParquetRendererOptionsSchema } from '@/gen/ai/protomolt/proto/emit/parquet/v1/parquet_renderer_pb'
import { RendererCapabilitySchema } from '@/gen/ai/protomolt/proto/emit/v1/renderer_pb'
import { ArchiveSchema } from '@/gen/ai/protomolt/proto/repo/archive/v1/archive_pb'
import {
  ArchiveService,
  GetEntryManifestResponseSchema,
  ListArchivesResponseSchema,
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
} from '@/gen/ai/protomolt/proto/repo/sink/v1/repository_sink_pb'
import { DocumentSchema } from '@/gen/ai/protomolt/proto/repo/v1/document_pb'
import { REPOSITORY_CLIENTS, REPOSITORY_SESSION, REPOSITORY_SIGN_IN } from '@/repository/clients'
import { routerClients } from '@/repository/repositoryTestKit'
import { Sha256 } from '@/repository/sha256'
import { moltDark } from '@/styles/theme'

export interface BrowserSaveAsHarnessOptions {
  parquetEnabled?: boolean
  staleDerived?: boolean
}

export async function mountSaveAsBrowserEntry(options: BrowserSaveAsHarnessOptions = {}) {
  const parquetEnabled = options.parquetEnabled ?? true
  const staleDerived = options.staleDerived ?? false

  const address = create(EntryAddressSchema, {
    accountId: 'acct-a',
    archive: 'datasets',
    entryId: 'customer-ledger',
  })
  const protoDoc = create(DocumentSchema, {
    docId: 'customer-ledger',
    searchMetadata: { title: 'Customer ledger snapshot' },
  })
  const protoBytes = toBinary(DocumentSchema, protoDoc)
  const protoSha = new Sha256().update(protoBytes).hex()

  const parquetBytes = new Uint8Array([0x50, 0x41, 0x52, 0x31, 10, 20, 30, 40, 0x50, 0x41, 0x52, 0x31])
  const parquetSha = new Sha256().update(parquetBytes).hex()

  let currentVersion = staleDerived ? 3n : 1n
  let hasDerivedParquet = staleDerived
  let derivedSourceRev = 1n
  let lastSubmittedColumns: string[] = []
  let lastMappingId = ''

  const buildManifest = (version: bigint) => {
    const renditions = [
      create(RenditionManifestEntrySchema, {
        rendition: create(RenditionDescriptorSchema, {
          name: 'parsed',
          mediaType: 'application/x-protobuf',
        }),
        state: RenditionState.PRESENT,
        sizeBytes: BigInt(protoBytes.length),
        sha256: protoSha,
        writtenAt: timestampFromDate(new Date('2026-11-14T00:00:00Z')),
      }),
    ]
    if (hasDerivedParquet) {
      renditions.push(
        create(RenditionManifestEntrySchema, {
          rendition: create(RenditionDescriptorSchema, {
            name: 'parquet',
            mediaType: 'application/vnd.apache.parquet',
          }),
          state: RenditionState.PRESENT,
          sizeBytes: BigInt(parquetBytes.length),
          sha256: parquetSha,
          writtenAt: timestampFromDate(new Date('2026-11-14T00:05:00Z')),
        }),
      )
    }
    return create(VersionManifestSchema, {
      address,
      version,
      totalBytes: renditions.reduce((acc, r) => acc + r.sizeBytes, 0n),
      renditions,
    })
  }

  const parquetCapability = create(RendererCapabilitySchema, {
    providerId: 'parquet-v1',
    providerVersion: '1.0.0',
    sourceProfile: 'tabular-scalar-v1',
    mediaType: 'application/vnd.apache.parquet',
    extension: 'parquet',
    optionsType: 'ai.protomolt.proto.emit.parquet.v1.ParquetRendererOptions',
    optionsFingerprint: 'fp-parquet-v1',
    lossDescription: 'Projects scalar top-level columns only; nested fields are omitted.',
    maxInputBytes: 4_194_304,
    maxOutputBytes: 1_048_576,
  })

  const clients = routerClients((router) => {
    router.service(ArchiveService, {
      listArchives: () =>
        create(ListArchivesResponseSchema, {
          archives: [create(ArchiveSchema, { accountId: 'acct-a', name: 'datasets' })],
        }),
      getEntryManifest: (req) => {
        const v = req.version === 0n ? currentVersion : req.version
        return create(GetEntryManifestResponseSchema, {
          info: create(EntryInfoSchema, {
            address,
            currentVersion,
            title: 'Customer ledger snapshot',
            filename: 'ledger.binpb',
            contentType: 'application/x-protobuf',
          }),
          manifest: buildManifest(v),
        })
      },
      listVersions: () => create(ListVersionsResponseSchema, { versions: [buildManifest(currentVersion)] }),
      async *readRendition(req) {
        const isParquet = req.rendition === 'parquet'
        const payload = isParquet ? parquetBytes : protoBytes
        const sha = isParquet ? parquetSha : protoSha
        const mediaType = isParquet ? 'application/vnd.apache.parquet' : 'application/x-protobuf'
        yield create(ReadRenditionResponseSchema, {
          frame: {
            case: 'header',
            value: create(RenditionReadHeaderSchema, {
              version: req.version,
              rendition: create(RenditionDescriptorSchema, { name: req.rendition, mediaType }),
              sizeBytes: BigInt(payload.length),
              sha256: sha,
              offset: 0n,
              length: BigInt(payload.length),
              wholeObject: true,
            }),
          },
        })
        yield create(ReadRenditionResponseSchema, {
          frame: { case: 'data', value: payload },
        })
      },
    })

    router.service(RepositorySinkService, {
      listRenditionProvenance: () => {
        const entries = [
          create(RenditionProvenanceEntrySchema, {
            rendition: create(RenditionDescriptorSchema, {
              name: 'parsed',
              mediaType: 'application/x-protobuf',
            }),
            provenance: create(RenditionProvenanceSchema, {
              authoritativeProtobuf: true,
              sourceRevision: currentVersion,
            }),
            currentSourceRevision: currentVersion,
            sourceStale: false,
          }),
        ]
        if (hasDerivedParquet) {
          entries.push(
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
                sourceRevision: derivedSourceRev,
                sourceEntry: address,
                destinationAddress: address,
                mappingId: lastMappingId || 'map-ledger',
                lossDescription: parquetCapability.lossDescription,
              }),
              currentSourceRevision: currentVersion,
              sourceStale: currentVersion !== derivedSourceRev,
            }),
          )
        }
        return create(ListRenditionProvenanceResponseSchema, { entries })
      },
      listSaveAsCapabilities: () =>
        create(ListSaveAsCapabilitiesResponseSchema, {
          resolvedSourceRevision: currentVersion,
          sourceSchemaType: DocumentSchema.typeName,
          sourceSchemaFingerprint: 'fp-doc-v1',
          sourceSizeBytes: BigInt(protoBytes.length),
          capabilities: parquetEnabled
            ? [
                create(SaveAsCapabilitySchema, {
                  capability: parquetCapability,
                  status: SaveAsCompatibilityStatus.COMPATIBLE,
                }),
              ]
            : [],
        }),
      publishRepositorySink: (req) => {
        const opts = fromBinary(ParquetRendererOptionsSchema, req.conversion!.options)
        lastSubmittedColumns = [...opts.columns]
        lastMappingId = opts.mappingId
        derivedSourceRev = currentVersion
        currentVersion += 1n
        hasDerivedParquet = true
        return create(PublishRepositorySinkResponseSchema, {
          job: create(RepositorySinkJobSchema, {
            operationId: req.operationId,
            accountId: 'acct-a',
            state: RepositorySinkJobState.SUCCEEDED,
            frozenSourceRevision: derivedSourceRev,
            destinationAddress: address,
            destinationRendition: create(RenditionDescriptorSchema, {
              name: req.destination?.rendition?.name || 'parquet',
              mediaType: 'application/vnd.apache.parquet',
            }),
            destinationVersion: currentVersion,
            bytesRendered: BigInt(parquetBytes.length),
            bytesCommitted: BigInt(parquetBytes.length),
          }),
        })
      },
    })
  })

  const writerSession = {
    status: async () =>
      create(ConsoleSessionStatusSchema, {
        authenticated: true,
        principal: 'operator@example.com',
        accountIds: ['acct-a'],
        intakeWriters: [
          create(IntakeWriterScopeSchema, { accountId: 'acct-a', datasourceId: 'datasets' }),
        ],
      }),
  }

  const router = createRouter({
    history: createMemoryHistory(),
    routes: ['repository-archive-entry', 'repository-archive', 'repository-document', 'repository', 'repository-account'].map(
      (name) => ({ path: `/${name}`, name, component: RepositoryArchiveEntryView }),
    ),
  })
  await router.push({
    name: 'repository-archive-entry',
    query: { account: 'acct-a', archive: 'datasets', entry: 'customer-ledger' },
  })

  const app = createApp({
    render: () =>
      h(components.VApp, {}, () =>
        h(components.VMain, { style: 'padding: 24px; max-width: 1200px; margin: auto;' }, () => h(RepositoryArchiveEntryView)),
      ),
  })
  app
    .use(createVuetify({ components, directives, theme: { defaultTheme: 'moltDark', themes: { moltDark } } }))
    .use(router)
    .provide(REPOSITORY_CLIENTS, clients)
    .provide(REPOSITORY_SESSION, writerSession)
    .provide(REPOSITORY_SIGN_IN, () => {})
    .mount('#app')

  ;(window as unknown as Record<string, unknown>).__saveAsState = () => ({
    lastSubmittedColumns,
    lastMappingId,
    parquetBytes: [...parquetBytes],
  })
}
