<template>
  <div>
    <v-card class="mb-4">
      <v-card-title class="text-subtitle-1 d-flex align-center flex-wrap ga-2">
        <v-btn icon="mdi-arrow-left" size="small" variant="text" aria-label="Account documents"
               :to="call ? { name: 'repository-account', params: { accountId: call.accountId } } : { name: 'repository' }" />
        <v-icon size="small">mdi-file-document-outline</v-icon>
        <span class="text-mono" data-testid="doc-id">{{ call?.docId }}</span>
        <v-chip v-if="manifest?.tombstone" size="small" color="warning" variant="tonal">tombstone</v-chip>
        <v-chip v-if="pinned !== null" size="small" variant="tonal" class="text-mono" data-testid="revision-chip">
          revision {{ pinned }}{{ requested === null ? ' (current)' : '' }}
        </v-chip>
        <v-spacer />
        <v-btn v-if="requested !== null && call" size="small" variant="text" data-testid="open-current" @click="openRevision(null)">
          Open current revision
        </v-btn>
        <v-btn v-if="uploadRoute" size="small" variant="tonal" prepend-icon="mdi-upload" data-testid="upload-revision" :to="uploadRoute">
          Upload a new revision
        </v-btn>
        <v-btn v-if="deletable" size="small" variant="tonal" color="error" prepend-icon="mdi-delete-outline"
               data-testid="delete-document" @click="deleting = true">
          Delete
        </v-btn>
      </v-card-title>
      <v-card-text v-if="call" class="text-caption text-mono pt-0">
        account {{ call.accountId }} · graph {{ call.graphId }} · graph address {{ call.graphAddressId }}
      </v-card-text>
    </v-card>

    <DeleteDocumentDialog
      v-if="deleting && deletable && address && pinned !== null"
      :address="address"
      :revision="pinned"
      @close="deleting = false"
      @reload="reloadAfterStale"
      @deleted="openTombstone"
    />
    <v-alert v-if="deletedNotice" type="success" variant="tonal" density="compact" class="mb-3" data-testid="deleted-notice">
      {{ deletedNotice }}
    </v-alert>

    <v-alert v-if="routeError" type="warning" variant="tonal" density="compact" class="mb-3" data-testid="route-error">
      {{ routeError }}
    </v-alert>
    <RpcFailureAlert v-if="manifestFailure" :failure="manifestFailure" title="Reading the manifest" />
    <RpcFailureAlert v-if="tombstoneFailure" :failure="tombstoneFailure" title="Looking for a tombstone in the history" />
    <v-alert v-if="latestTombstone !== null" type="info" variant="tonal" density="compact" class="mb-3" data-testid="tombstone-hint">
      This document's current revision is a tombstone.
      <v-btn size="small" variant="tonal" class="ml-2" data-testid="open-tombstone" @click="openRevision(latestTombstone)">
        Open revision {{ latestTombstone }}
      </v-btn>
    </v-alert>
    <v-progress-linear v-if="loading" indeterminate class="mb-2" />

    <template v-if="manifest && validation">
      <v-tabs v-model="tab" density="compact" class="mb-2">
        <v-tab value="parts">Parts</v-tab>
        <v-tab value="metadata">Metadata</v-tab>
        <v-tab value="validation">Validation</v-tab>
        <v-tab value="links" data-testid="tab-links">Links</v-tab>
        <v-tab value="history" data-testid="tab-history">History</v-tab>
        <v-tab value="files" data-testid="tab-files">Files</v-tab>
      </v-tabs>

      <v-window v-model="tab">
        <v-window-item value="parts">
          <v-card class="mb-4">
            <v-card-text>
              <div v-if="manifest.tombstone" class="text-body-2 text-medium-emphasis">
                A tombstone revision holds no parts. Earlier revisions keep their bytes; open one from History.
              </div>
              <v-table v-else density="compact" data-testid="parts-table">
                <thead>
                  <tr>
                    <th>Part</th>
                    <th>State</th>
                    <th>Size</th>
                    <th>SHA-256</th>
                    <th>Written in</th>
                    <th>Provenance</th>
                    <th />
                  </tr>
                </thead>
                <tbody>
                  <tr v-for="(row, i) in parts" :key="row.label">
                    <td>
                      {{ row.label }}
                      <v-chip v-if="row.opaque" size="x-small" color="warning" variant="tonal" class="ml-1">opaque</v-chip>
                    </td>
                    <td>
                      {{ row.state }}
                      <div v-if="row.deletedReason" class="text-caption text-medium-emphasis">{{ row.deletedReason }}</div>
                    </td>
                    <td class="text-mono">{{ byteSize(row.sizeBytes) }}</td>
                    <td class="text-mono text-caption">{{ row.sha256 }}</td>
                    <td class="text-mono">{{ row.writtenInRevision }}</td>
                    <td class="text-caption">{{ row.owner }}</td>
                    <td>
                      <v-btn v-if="row.state === 'PRESENT'" size="x-small" variant="tonal" :loading="partBusy === i"
                             :data-testid="`read-part-${i}`" @click="readPartAt(i)">Read</v-btn>
                    </td>
                  </tr>
                </tbody>
              </v-table>
              <v-checkbox v-model="allowOpaque" density="compact" hide-details class="mt-2"
                          label="Allow opaque payloads (the read is refused otherwise when a part holds opaque content)" />
            </v-card-text>
          </v-card>

          <RpcFailureAlert v-if="partFailure" :failure="partFailure" title="Reading the part" />
          <v-card v-if="partView" class="mb-4" data-testid="part-view">
            <v-card-title class="text-subtitle-2 d-flex align-center flex-wrap ga-2">
              {{ partView.label }}
              <span class="text-caption text-medium-emphasis">revision {{ partView.revision }} · {{ byteSize(partView.size) }}</span>
            </v-card-title>
            <v-card-text>
              <template v-if="partView.decoded.kind === 'decoded'">
                <div class="text-caption text-medium-emphasis mb-2" data-testid="decoded-with">
                  Decoded as {{ partView.decoded.typeUrl }} with the descriptor retained at
                  <span class="text-mono">{{ partView.decoded.descriptorSha256 }}</span>
                </div>
                <div class="pmv">
                  <ProtoMessageNode v-for="(node, n) in partView.tree" :key="n" :node="node" :depth="0" />
                </div>
              </template>
              <template v-else>
                <v-alert type="info" variant="tonal" density="compact" class="mb-2" data-testid="raw-reason">
                  {{ partView.decoded.reason }} Showing the raw bytes.
                </v-alert>
                <div v-if="partView.size > 4096n" class="text-caption text-medium-emphasis mb-1">
                  The first 4096 of {{ partView.size }} bytes:
                </div>
                <pre class="text-mono text-caption hexdump">{{ partView.hex.join('\n') }}</pre>
              </template>
            </v-card-text>
          </v-card>
        </v-window-item>

        <v-window-item value="metadata">
          <v-card class="mb-4">
            <v-card-text>
              <RpcFailureAlert v-if="documentFailure" :failure="documentFailure" title="Reading metadata and files" />
              <v-table v-if="metadata.length > 0" density="compact" data-testid="metadata-table">
                <thead><tr><th>Key</th><th>Value</th></tr></thead>
                <tbody>
                  <tr v-for="m in metadata" :key="m.key"><td class="text-mono">{{ m.key }}</td><td>{{ m.value }}</td></tr>
                </tbody>
              </v-table>
              <div v-else-if="!documentFailure" class="text-body-2 text-medium-emphasis">This revision carries no request metadata.</div>
            </v-card-text>
          </v-card>
        </v-window-item>

        <v-window-item value="validation">
          <v-card class="mb-4" data-testid="validation-view">
            <v-card-text>
              <v-table density="compact" class="mb-3">
                <tbody>
                  <tr><th>Status</th><td>{{ validationSummary!.status }}</td></tr>
                  <tr><th>Candidate SHA-256</th><td class="text-mono text-caption">{{ validationSummary!.candidateSha256 }}</td></tr>
                  <tr><th>Validator profile</th><td>{{ validationSummary!.validatorProfile }}</td></tr>
                  <tr><th>Write authority</th><td>{{ validationSummary!.authority }}</td></tr>
                </tbody>
              </v-table>
              <div class="text-subtitle-2 mb-1">Schema artifacts</div>
              <v-table v-if="validationSummary!.schemas.length > 0" density="compact" class="mb-3" data-testid="schema-table">
                <thead><tr><th>Bound to</th><th>Type URL</th><th>Descriptor SHA-256</th><th>Metadata SHA-256</th></tr></thead>
                <tbody>
                  <tr v-for="(s, i) in validationSummary!.schemas" :key="i">
                    <td>{{ s.usage }}</td>
                    <td class="text-mono text-caption">{{ s.typeUrl }}</td>
                    <td class="text-mono text-caption">{{ s.descriptorSha256 }}</td>
                    <td class="text-mono text-caption">{{ s.metadataSha256 }}</td>
                  </tr>
                </tbody>
              </v-table>
              <div v-else class="text-body-2 text-medium-emphasis mb-3">No schema artifacts are bound.</div>
              <template v-if="validationSummary!.opaque.length > 0">
                <div class="text-subtitle-2 mb-1">Opaque values</div>
                <v-table density="compact" class="mb-3">
                  <thead><tr><th>In</th><th>Type URL</th><th>SHA-256</th><th>Length</th><th>Owner</th></tr></thead>
                  <tbody>
                    <tr v-for="(o, i) in validationSummary!.opaque" :key="i">
                      <td>{{ o.at }}</td><td class="text-mono text-caption">{{ o.typeUrl }}</td>
                      <td class="text-mono text-caption">{{ o.sha256 }}</td><td>{{ o.length }}</td><td>{{ o.owner }}</td>
                    </tr>
                  </tbody>
                </v-table>
              </template>
              <template v-if="validationSummary!.rules.length > 0">
                <div class="text-subtitle-2 mb-1">Rules not evaluated</div>
                <v-table density="compact">
                  <thead><tr><th>Rule</th><th>Field</th><th>State</th></tr></thead>
                  <tbody>
                    <tr v-for="(r, i) in validationSummary!.rules" :key="i">
                      <td class="text-mono">{{ r.ruleId }}</td><td class="text-mono">{{ r.fieldPath }}</td><td>{{ r.state }}</td>
                    </tr>
                  </tbody>
                </v-table>
              </template>
            </v-card-text>
          </v-card>
        </v-window-item>

        <v-window-item value="links">
          <DocumentLinks :address="address!" :revision="pinned!" :tombstone="manifest.tombstone" />
        </v-window-item>

        <v-window-item value="history">
          <RevisionHistory :address="address!" :shown-revision="pinned" @open="openRevision" />
        </v-window-item>

        <v-window-item value="files">
          <BlobFiles :address="address!" :revision="pinned!" :blobs="blobs" :failure="documentFailure" />
        </v-window-item>
      </v-window>
    </template>
  </div>
</template>

<script setup lang="ts">
import { computed, ref, shallowRef, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { create, type DescMessage, type Message } from '@bufbuild/protobuf'
import { Code, ConnectError } from '@connectrpc/connect'
import ProtoMessageNode from '@/components/ProtoMessageNode.vue'
import BlobFiles from '@/components/repository/BlobFiles.vue'
import DeleteDocumentDialog from '@/components/repository/DeleteDocumentDialog.vue'
import DocumentLinks from '@/components/repository/DocumentLinks.vue'
import RevisionHistory from '@/components/repository/RevisionHistory.vue'
import RpcFailureAlert from '@/components/repository/RpcFailureAlert.vue'
import type { NodeAddress } from '@/gen/ai/protomolt/proto/repo/v1/address_pb'
import {
  GetDocumentRequestSchema,
  GetManifestRequestSchema,
  ListRevisionsRequestSchema,
} from '@/gen/ai/protomolt/proto/repo/v2/document_service_pb'
import {
  DocumentPart,
  PartSelectionSchema,
  PartSelectorSchema,
  type Accession,
  type DocumentManifest,
  type RevisionValidation,
} from '@/gen/ai/protomolt/proto/repo/v2/manifest_pb'
import type { ViewNode } from '@/lib/message-view/messageTree'
import { useRepositoryClients } from '@/repository/clients'
import { decodedTree } from '@/repository/decodedTreeCore'
import {
  blobRows,
  metadataEntries,
  nodeAddress,
  parseRevision,
  partRows,
  revisionSelector,
  validationView,
  type BlobRow,
  type CallNumber,
} from '@/repository/documentCore'
import { byteSize, hexDump } from '@/repository/formatCore'
import { decodePart, readPart, type DecodedPart } from '@/repository/partReadCore'
import { rpcFailure, type RpcFailure } from '@/repository/rpcErrorCore'
import { useCallScope } from '@/repository/useCallScope'

const clients = useRepositoryClients()
const route = useRoute()
const router = useRouter()
const readScope = useCallScope()
const partScope = useCallScope()

const tab = ref('parts')
const loading = ref(false)
const routeError = ref<string | null>(null)
const call = ref<CallNumber | null>(null)
const address = shallowRef<NodeAddress | null>(null)
const requested = ref<bigint | null>(null)
const pinned = ref<bigint | null>(null)
const manifest = shallowRef<DocumentManifest | null>(null)
const validation = shallowRef<RevisionValidation | null>(null)
const manifestFailure = ref<RpcFailure | null>(null)
const metadata = ref<{ key: string; value: string }[]>([])
const blobs = ref<BlobRow[]>([])
const documentFailure = ref<RpcFailure | null>(null)

const parts = computed(() => (manifest.value ? partRows(manifest.value) : []))

// Only the current revision of a live document can be tombstoned.
const deleting = ref(false)
const deletedNotice = ref<string | null>(null)
let deletedRevision: bigint | null = null
const deletable = computed(() => manifest.value !== null && !manifest.value.tombstone && requested.value === null)

function reloadAfterStale(tombstone: bigint | null) {
  deleting.value = false
  if (tombstone !== null) openRevision(tombstone)
  else void load()
}

function openTombstone(accession: Accession) {
  deleting.value = false
  deletedRevision = accession.revision
  deletedNotice.value = `Deleted: revision ${accession.revision} is a tombstone${accession.previousRevision > 0n ? ` after revision ${accession.previousRevision}` : ''}. The bytes of earlier revisions stay.`
  openRevision(accession.revision)
}

// The upload route saves intake documents, whose call number is (doc, datasource,
// account, "intake:<account>"); a new revision of the current one expects it.
const uploadRoute = computed(() => {
  const c = call.value
  if (c === null || manifest.value === null || requested.value !== null) return null
  if (c.graphId !== `intake:${c.accountId}`) return null
  return {
    name: 'repository-upload',
    query: { account: c.accountId, datasource: c.graphAddressId, doc: c.docId, expected: manifest.value.revision.toString() },
  }
})
const validationSummary = computed(() => (validation.value ? validationView(validation.value) : null))

const allowOpaque = ref(false)
const partBusy = ref<number | null>(null)
const partFailure = ref<RpcFailure | null>(null)
const partView = shallowRef<{
  label: string
  revision: bigint
  size: bigint
  decoded: DecodedPart
  tree: ViewNode[]
  hex: string[]
} | null>(null)

function queryText(name: string): string {
  const value = route.query[name]
  return typeof value === 'string' ? value : ''
}

async function load() {
  // A new route starts from nothing: earlier reads, a part read still running
  // and the previous revision's metadata and files all go.
  const signal = readScope.next()
  partScope.next()
  partBusy.value = null
  // A delete dialog belongs to the document it opened on.
  deleting.value = false
  routeError.value = null
  manifestFailure.value = null
  documentFailure.value = null
  partFailure.value = null
  partView.value = null
  manifest.value = null
  validation.value = null
  pinned.value = null
  metadata.value = []
  blobs.value = []
  latestTombstone.value = null
  tombstoneFailure.value = null
  call.value = null
  address.value = null
  const callNumber: CallNumber = {
    accountId: queryText('account'),
    graphId: queryText('graph'),
    graphAddressId: queryText('node'),
    docId: queryText('doc'),
  }
  let target: NodeAddress
  try {
    target = nodeAddress(callNumber)
    requested.value = parseRevision(queryText('revision'))
  } catch (error) {
    routeError.value = (error as Error).message
    return
  }
  address.value = target
  call.value = callNumber
  // The deletion notice belongs to the tombstone it opened, and goes with any other page.
  if (requested.value !== deletedRevision) {
    deletedNotice.value = null
    deletedRevision = null
  }
  loading.value = true
  let revision: bigint
  try {
    const answer = await clients.documents.getManifest(
      create(GetManifestRequestSchema, { address: target, revision: revisionSelector(requested.value) }),
      { signal },
    )
    if (signal.aborted) return
    if (answer.manifest === undefined || answer.validation === undefined) {
      throw new Error('GetManifest answered without a manifest or validation record.')
    }
    if (requested.value !== null && answer.manifest.revision !== requested.value) {
      throw new Error(`GetManifest answered for revision ${answer.manifest.revision}; revision ${requested.value} was asked for.`)
    }
    manifest.value = answer.manifest
    validation.value = answer.validation
    // Every later read pins the revision this manifest answered for, so a
    // newer save cannot mix into the screen.
    revision = answer.manifest.revision
    pinned.value = revision
  } catch (error) {
    if (signal.aborted) return
    manifestFailure.value = rpcFailure(error)
    loading.value = false
    // A current read of a tombstoned document is NOT_FOUND; the history still
    // names its tombstone, which opens as an exact revision.
    if (requested.value === null && ConnectError.from(error).code === Code.NotFound) void findTombstone(target, signal)
    return
  }
  try {
    const document = await clients.documents.getDocument(
      create(GetDocumentRequestSchema, {
        address: target,
        revision: revisionSelector(revision),
        selection: create(PartSelectionSchema, {
          selectors: [create(PartSelectorSchema, { selector: { case: 'part', value: DocumentPart.BLOBS } })],
        }),
        allowOpaque: allowOpaque.value,
      }),
      { signal },
    )
    if (signal.aborted) return
    metadata.value = metadataEntries(document.metadata)
    blobs.value = blobRows(document.document)
  } catch (error) {
    if (!signal.aborted) documentFailure.value = rpcFailure(error)
  } finally {
    // Only the newest load owns the indicator.
    if (!signal.aborted) loading.value = false
  }
}

const latestTombstone = ref<bigint | null>(null)
const tombstoneFailure = ref<RpcFailure | null>(null)

async function findTombstone(target: NodeAddress, signal: AbortSignal) {
  try {
    const page = await clients.documents.listRevisions(create(ListRevisionsRequestSchema, { address: target, pageSize: 1 }), { signal })
    if (signal.aborted) return
    const newest = page.revisions[0]
    if (newest?.tombstone) latestTombstone.value = newest.revision
  } catch (error) {
    if (!signal.aborted) tombstoneFailure.value = rpcFailure(error)
  }
}

async function readPartAt(index: number) {
  const entry = manifest.value?.parts[index]
  if (!entry?.selector || !address.value || pinned.value === null || !validation.value) return
  const signal = partScope.next()
  partBusy.value = index
  partFailure.value = null
  partView.value = null
  try {
    const part = await readPart(clients.documents, address.value, pinned.value, entry.selector, allowOpaque.value, signal)
    const decoded = await decodePart(clients.documents, address.value, part.revision, validation.value, entry.selector, part.bytes, signal)
    if (signal.aborted) return
    partView.value = {
      label: parts.value[index].label,
      revision: part.revision,
      size: BigInt(part.bytes.length),
      decoded,
      tree: decoded.kind === 'decoded' ? decodedTree(decoded.desc as DescMessage, decoded.message as Message) : [],
      hex: decoded.kind === 'raw' ? hexDump(part.bytes.subarray(0, 4096)) : [],
    }
  } catch (error) {
    if (!signal.aborted) partFailure.value = rpcFailure(error)
  } finally {
    if (!signal.aborted) partBusy.value = null
  }
}

function openRevision(revision: bigint | null) {
  const query = { ...route.query }
  if (revision === null) delete query.revision
  else query.revision = revision.toString()
  void router.push({ name: 'repository-document', query })
}

watch(() => route.fullPath, () => void load(), { immediate: true })
</script>

<style scoped>
.hexdump {
  white-space: pre;
  overflow-x: auto;
}
.pmv {
  font-family: var(--pm-font-mono, 'JetBrains Mono Variable', monospace);
  font-size: 0.82rem;
  line-height: 1.7;
}
</style>
