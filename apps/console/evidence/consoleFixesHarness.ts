/**
 * Browser harness for the console fix evidence specs (#699, #701, #702, #712, #713).
 * Mounts the real repository views with Vuetify and in-memory binary Connect routers in Chromium,
 * plus a strip that shows the current route and every RPC the view sent, so each screenshot
 * carries the request the fix is about.
 */
import 'vuetify/styles'
import '@mdi/font/css/materialdesignicons.css'
import '@/styles/console.css'
import { createApp, h, reactive, type Component } from 'vue'
import { createVuetify } from 'vuetify'
import * as components from 'vuetify/components'
import * as directives from 'vuetify/directives'
import { create } from '@bufbuild/protobuf'
import { BridgeKind, BridgeStatus } from '@/gen/ai/protomolt/proto/asset/v1/bridge_pb'
import { ArchiveSchema, ArchiveStatsSchema } from '@/gen/ai/protomolt/proto/repo/archive/v1/archive_pb'
import {
  ArchiveService,
  BridgeEntryResponseSchema,
  BridgeOutcomeSchema,
  GetArchiveResponseSchema,
  GetArchiveStatsResponseSchema,
  GetEntryManifestResponseSchema,
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
import {
  BacklinkSchema,
  DeleteDocumentResponseSchema,
  DocumentFilterSchema,
  DocumentLookupSchema,
  DocumentService,
  DocumentSummarySchema,
  DynamicLinkSelectorSchema,
  ListBacklinksResponseSchema,
  ListLinksResponseSchema,
  PurgeDocumentResponseSchema,
  ResolveLinkResponseSchema,
  ResolvedLinkSchema,
  StaticLinkStateSchema,
} from '@/gen/ai/protomolt/proto/repo/v2/document_service_pb'
import { AccessionSchema } from '@/gen/ai/protomolt/proto/repo/v2/manifest_pb'
import { REPOSITORY_CLIENTS, REPOSITORY_SIGN_IN } from '@/repository/clients'
import { at, routerClients, testAddress } from '@/repository/repositoryTestKit'
import { repositoryRouter } from '@/repository/repositoryViewKit'
import { moltDark } from '@/styles/theme'
import DocumentLinks from '@/components/repository/DocumentLinks.vue'
import RepositoryArchiveEntryView from '@/views/repository/RepositoryArchiveEntryView.vue'

/** Every RPC the mounted view sent, in order, as one line each. Specs read it through `window`. */
const rpcLog = reactive<string[]>([])
declare global {
  interface Window {
    consoleFixRpcLog: string[]
    consoleFixReleaseResolve: () => void
  }
}
window.consoleFixRpcLog = rpcLog

const ENTRY = { account: 'acct-a', archive: 'executed-agreements', entry: 'nda-2026-001' }
const PDF_SHA = '9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08'
const MARKDOWN_SHA = '2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae'

function evidenceStrip(router: ReturnType<typeof repositoryRouter>) {
  return h('div', { class: 'text-mono text-caption pa-3 mb-4', style: 'border: 1px dashed #38bdf8; border-radius: 6px;' }, [
    h('div', { 'data-testid': 'harness-route' }, `route: ${String(router.currentRoute.value.name)} ${router.currentRoute.value.fullPath}`),
    h('div', { class: 'mt-1' }, 'RPCs sent by the view:'),
    h('ol', { 'data-testid': 'harness-rpc-log', class: 'ml-6' }, rpcLog.map((line, i) => h('li', { key: i }, line))),
  ])
}

async function mount(view: Component, props: Record<string, unknown>, router: ReturnType<typeof repositoryRouter>, clients: ReturnType<typeof routerClients>) {
  const app = createApp({
    render: () =>
      h(components.VApp, {}, () =>
        h(components.VMain, { style: 'padding: 24px; max-width: 1240px; margin: auto;' }, () => [
          evidenceStrip(router),
          h(view, props),
        ]),
      ),
  })
  app
    .use(createVuetify({ components, directives, theme: { defaultTheme: 'moltDark', themes: { moltDark } } }))
    .use(router)
    .provide(REPOSITORY_CLIENTS, clients)
    .provide(REPOSITORY_SIGN_IN, () => {})
    .mount('#app')
}

/**
 * An archive entry whose current version 3 holds an `original` PDF and a stored `markdown` rendition.
 * Bridging advances the entry to version 4, as a server-side bridge run does.
 */
export async function mountArchiveEntry() {
  const addr = create(EntryAddressSchema, { accountId: ENTRY.account, archive: ENTRY.archive, entryId: ENTRY.entry })
  let head = 3n
  const manifest = (version: bigint) =>
    create(VersionManifestSchema, {
      address: addr,
      version,
      totalBytes: 248_512n,
      rootChecksum: PDF_SHA,
      createdAt: at(`2026-10-2${version}T14:20:00Z`),
      renditions: [
        create(RenditionManifestEntrySchema, {
          rendition: create(RenditionDescriptorSchema, { name: 'original', mediaType: 'application/pdf' }),
          state: RenditionState.PRESENT,
          sizeBytes: 244_000n,
          sha256: PDF_SHA,
          writtenAt: at(`2026-10-2${version}T14:20:00Z`),
          writtenBy: create(WriteAttributionSchema, { module: version === 4n ? 'grparse-bridge' : 'intake-gateway' }),
        }),
        create(RenditionManifestEntrySchema, {
          rendition: create(RenditionDescriptorSchema, { name: 'markdown', mediaType: 'text/markdown' }),
          state: RenditionState.PRESENT,
          sizeBytes: 4_512n,
          sha256: MARKDOWN_SHA,
          writtenAt: at('2026-10-23T14:21:00Z'),
          writtenBy: create(WriteAttributionSchema, { module: 'grparse-markdown' }),
        }),
      ],
    })

  const clients = routerClients((router) => {
    router.service(ArchiveService, {
      getArchive: (r) =>
        create(GetArchiveResponseSchema, {
          archive: create(ArchiveSchema, { name: r.archive, accountId: r.accountId, driveName: 'legal-vault' }),
        }),
      getArchiveStats: (r) =>
        create(GetArchiveStatsResponseSchema, { stats: create(ArchiveStatsSchema, { archive: r.archive, entries: 12n, versions: 28n }) }),
      getEntryManifest: (r) => {
        const version = r.version === 0n ? head : r.version
        rpcLog.push(`ArchiveService.GetEntryManifest version=${r.version === 0n ? 'current' : r.version} -> ${version}`)
        return create(GetEntryManifestResponseSchema, {
          info: create(EntryInfoSchema, {
            address: addr,
            entryUuid: '00000000-0000-5000-8000-000000000101',
            currentVersion: head,
            title: 'Master Mutual NDA - Acme Corp',
            filename: 'acme-mutual-nda-signed.pdf',
            contentType: 'application/pdf',
            metadata: { department: 'legal', jurisdiction: 'US-DE' },
          }),
          manifest: manifest(version),
        })
      },
      listVersions: () =>
        create(ListVersionsResponseSchema, { versions: Array.from({ length: Number(head) }, (_, i) => manifest(head - BigInt(i))) }),
      bridgeEntry: () => {
        head = 4n
        rpcLog.push('ArchiveService.BridgeEntry -> version 4 (original PRODUCED)')
        return create(BridgeEntryResponseSchema, {
          version: 4n,
          outcomes: [
            create(BridgeOutcomeSchema, {
              bridge: BridgeKind.DOCUMENT_TEXT,
              rendition: 'original',
              status: BridgeStatus.PRODUCED,
              detail: 'Extracted 14 pages into version 4',
            }),
          ],
        })
      },
      putEntry: (r) => {
        const renditions = r.renditions.map((p) => `${p.rendition?.name}(${p.data.byteLength} bytes)`).join(', ')
        rpcLog.push(`ArchiveService.PutEntry title="${r.title}" renditions=${r.renditions.length}${renditions ? ` [${renditions}]` : ''}`)
        return create(PutEntryResponseSchema, { version: 4n, rootChecksum: PDF_SHA, totalBytes: 248_512n, operationId: r.operationId })
      },
    })
    router.service(DocumentService, {
      deleteDocument: (r) => {
        const a = r.address
        const expected = r.expected?.expected.case === 'revision' ? r.expected.expected.value : 'none'
        rpcLog.push(`DocumentService.DeleteDocument ${a?.accountId} / ${a?.graphId} / ${a?.graphAddressId} / ${a?.docId} expected revision=${expected}`)
        return create(DeleteDocumentResponseSchema, {
          operationId: r.operationId,
          accession: create(AccessionSchema, { address: r.address, revision: 4n, previousRevision: 3n, tombstone: true }),
        })
      },
      purgeDocument: () => {
        rpcLog.push('DocumentService.PurgeDocument')
        return create(PurgeDocumentResponseSchema)
      },
    })
  })

  const router = repositoryRouter()
  await router.push({ name: 'repository-archive-entry', query: ENTRY })
  await router.isReady()
  await mount(RepositoryArchiveEntryView, {}, router, clients)
}

/**
 * A document with one static and one dynamic link. The first ResolveLink call is held until the
 * spec calls `window.consoleFixReleaseResolve()`, so Resolve all stays in flight while a newer
 * per-row resolve supersedes it.
 */
export async function mountDocumentLinks() {
  let release = () => {}
  const held = new Promise<void>((resolve) => { release = resolve })
  window.consoleFixReleaseResolve = () => release()
  let resolves = 0

  const clients = routerClients((router) =>
    router.service(DocumentService, {
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
                value: create(StaticLinkStateSchema, { address: testAddress({ docId: 'nda-2024-base' }), pinnedRevision: 2n }),
              },
            }),
            create(ResolvedLinkSchema, {
              fieldPath: 'structured_data.related_agreements',
              ordinal: 0,
              relation: 'related',
              label: 'Active Commercial Contracts',
              target: {
                case: 'dynamicTarget',
                value: create(DynamicLinkSelectorSchema, { filter: create(DocumentFilterSchema, { documentType: 'contract/msa' }) }),
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
            }),
          ],
        }),
      async resolveLink() {
        resolves++
        const call = resolves
        rpcLog.push(call === 1 ? 'DocumentService.ResolveLink #1 (Resolve all, held open)' : `DocumentService.ResolveLink #${call} (row resolve)`)
        if (call === 1) await held
        return create(ResolveLinkResponseSchema, {
          documents: [
            create(DocumentSummarySchema, {
              address: testAddress({ docId: 'msa-2026-042' }),
              revision: 3n,
              committedAt: at('2026-10-26T15:10:00Z'),
              lookup: create(DocumentLookupSchema, { title: 'Enterprise MSA - Globex Systems' }),
            }),
          ],
        })
      },
    }),
  )

  const router = repositoryRouter()
  await router.push({ name: 'repository-document', query: { account: 'acct-a', graph: 'intake:acct-a', node: 'ds-files', doc: 'doc-1', revision: '7' } })
  await router.isReady()
  await mount(DocumentLinks, { address: testAddress(), revision: 7n, tombstone: false }, router, clients)
}
