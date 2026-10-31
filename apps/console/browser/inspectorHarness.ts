/** Browser fixture: actual document view and binary Connect clients, no live account. */
import 'vuetify/styles'
import '@mdi/font/css/materialdesignicons.css'
import '@/styles/console.css'
import { createApp, h } from 'vue'
import { createVuetify } from 'vuetify'
import * as components from 'vuetify/components'
import * as directives from 'vuetify/directives'
import { createMemoryHistory, createRouter } from 'vue-router'
import { create, toBinary } from '@bufbuild/protobuf'
import { anyPack, StringValueSchema } from '@bufbuild/protobuf/wkt'
import RepositoryDocumentView from '@/views/repository/RepositoryDocumentView.vue'
import { DocumentSchema } from '@/gen/ai/protomolt/proto/repo/v1/document_pb'
import { DocumentService, GetDocumentResponseSchema, GetManifestResponseSchema, GetSchemaDescriptorResponseSchema, ListRevisionsResponseSchema, ReadDocumentResponseSchema } from '@/gen/ai/protomolt/proto/repo/v2/document_service_pb'
import { DocumentManifestSchema, DocumentPart, PartSelectorSchema, PartState, RevisionValidationSchema, ValidationStatus } from '@/gen/ai/protomolt/proto/repo/v2/manifest_pb'
import { REPOSITORY_CLIENTS } from '@/repository/clients'
import { descriptorSetOf, routerClients, testAddress } from '@/repository/repositoryTestKit'
import { Sha256 } from '@/repository/sha256'
import { moltDark } from '@/styles/theme'

export async function mountInspector() {
  const packed = anyPack(StringValueSchema, create(StringValueSchema, { value: 'This Any was decoded with its retained schema.' }))
  const document = create(DocumentSchema, { docId: 'doc-1', structuredData: packed, searchMetadata: { title: 'Protobuf inspection example' } })
  const bytes = new Uint8Array([...toBinary(DocumentSchema, document), 0xa0, 0x38, 1]) // unknown field 900
  const rootSet = descriptorSetOf(DocumentSchema.file)
  const anySet = descriptorSetOf(StringValueSchema.file)
  const hash = (bytes: Uint8Array) => new Sha256().update(bytes).hex()
  const selector = create(PartSelectorSchema, { selector: { case: 'part', value: DocumentPart.CORE } })
  const manifest = create(DocumentManifestSchema, { address: testAddress(), revision: 7n, parts: [{ selector, state: PartState.PRESENT, sizeBytes: BigInt(bytes.length), sha256: hash(bytes), writtenInRevision: 7n, contentIncluded: true }] })
  const validation = create(RevisionValidationSchema, {
    status: ValidationStatus.TYPED_VALID,
    partSchemas: [{ part: selector, fragmentSchema: { typeUrl: `type.googleapis.com/${DocumentSchema.typeName}`, descriptorSha256: hash(rootSet) } }],
    resolvedOccurrences: [{ occurrence: { part: selector, steps: [{ step: { case: 'fieldNumber', value: 4 } }] }, schema: { typeUrl: packed.typeUrl, descriptorSha256: hash(anySet) }, payloadSha256: hash(packed.value), payloadLength: BigInt(packed.value.length) }],
  })
  const clients = routerClients(router => router.service(DocumentService, {
    getManifest: () => create(GetManifestResponseSchema, { manifest, validation }),
    getDocument: () => create(GetDocumentResponseSchema, { document, manifest, validation }),
    listRevisions: () => create(ListRevisionsResponseSchema),
    getSchemaDescriptor: request => {
      if (request.revision?.selector.case !== 'revision' || request.revision.selector.value !== 7n) throw new Error('Unpinned descriptor request')
      if (![hash(rootSet), hash(anySet)].includes(request.descriptorSha256)) throw new Error('Unexpected descriptor')
      return create(GetSchemaDescriptorResponseSchema, { descriptorSet: request.descriptorSha256 === hash(rootSet) ? rootSet : anySet })
    },
    async *readDocument() {
      yield create(ReadDocumentResponseSchema, { frame: { case: 'manifest', value: manifest } })
      yield create(ReadDocumentResponseSchema, { frame: { case: 'partData', value: { selector, offset: 0n, data: bytes, last: true } } })
      yield create(ReadDocumentResponseSchema, { frame: { case: 'complete', value: {} } })
    },
  }))
  const router = createRouter({ history: createMemoryHistory(), routes: ['repository-document', 'repository', 'repository-account'].map(name => ({ path: `/${name}`, name, component: RepositoryDocumentView })) })
  await router.push({ name: 'repository-document', query: { account: 'acct-a', graph: 'intake:acct-a', node: 'ds-files', doc: 'doc-1', revision: '7' } })
  const app = createApp({ render: () => h(components.VApp, {}, () => h(components.VMain, { style: 'padding: 24px; max-width: 1200px; margin: auto;' }, () => h(RepositoryDocumentView))) })
  app.use(createVuetify({ components, directives, theme: { defaultTheme: 'moltDark', themes: { moltDark } } })).use(router).provide(REPOSITORY_CLIENTS, clients).mount('#app')
  return [...bytes]
}
