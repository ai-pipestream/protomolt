<template>
  <div data-testid="links">
    <v-card class="mb-4">
      <v-card-title class="text-subtitle-2">Outbound links</v-card-title>
      <v-card-text>
        <div v-if="tombstone" class="text-body-2 text-medium-emphasis">A tombstone revision holds no links.</div>
        <template v-else>
          <RpcFailureAlert v-if="linksFailure" :failure="linksFailure" title="Listing links" />
          <v-table v-if="links.length > 0" density="compact" data-testid="links-table">
            <thead>
              <tr>
                <th>Field</th>
                <th>Relation</th>
                <th>Target</th>
                <th>Status</th>
                <th />
              </tr>
            </thead>
            <tbody>
              <template v-for="row in links" :key="row.key">
                <tr :data-testid="`link-${row.fieldPath}-${row.ordinal}`">
                  <td>
                    <div class="text-mono">{{ row.fieldPath }}</div>
                    <div v-if="row.label !== null" class="text-caption text-medium-emphasis">{{ row.label }}</div>
                  </td>
                  <td>
                    <a v-if="row.relationHref !== null" :href="row.relationHref" target="_blank" rel="noopener noreferrer"
                       class="text-mono text-caption" data-testid="relation-link">{{ row.relation }}</a>
                    <v-chip v-else size="x-small" variant="tonal" class="text-mono" data-testid="relation">{{ row.relation }}</v-chip>
                  </td>
                  <td>
                    <template v-if="row.target.kind === 'static'">
                      <v-chip size="x-small" variant="outlined" class="mr-1">static</v-chip>
                      <span class="text-mono text-caption">
                        {{ row.target.call.graphId }} / {{ row.target.call.graphAddressId }} / {{ row.target.call.docId }}
                      </span>
                      <span v-if="row.target.pinnedRevision !== null" class="text-caption text-medium-emphasis">
                        pinned to revision {{ row.target.pinnedRevision }}
                      </span>
                    </template>
                    <template v-else>
                      <v-chip size="x-small" variant="outlined" class="mr-1">dynamic</v-chip>
                      <div v-for="(c, i) in row.target.conditions" :key="i" class="text-caption text-mono">{{ c }}</div>
                      <div v-if="row.target.order !== null" class="text-caption text-medium-emphasis">ordered by {{ row.target.order }}</div>
                    </template>
                  </td>
                  <td>
                    <v-chip v-if="row.target.kind === 'static'" size="x-small" variant="tonal" :color="statusColor(row.target.status)"
                            :data-testid="`link-status-${row.fieldPath}-${row.ordinal}`">{{ row.target.status }}</v-chip>
                  </td>
                  <td>
                    <v-btn v-if="row.target.kind === 'static' && row.target.status !== 'UNAVAILABLE'" size="x-small" variant="tonal"
                           :data-testid="`open-link-${row.fieldPath}-${row.ordinal}`"
                           @click="openCall(row.target.call, row.target.pinnedRevision)">Open</v-btn>
                    <v-btn v-else-if="row.target.kind === 'dynamic'" size="x-small" variant="tonal"
                           :loading="resolving?.key === row.key && resolving.loading"
                           :data-testid="`resolve-link-${row.fieldPath}-${row.ordinal}`" @click="resolve(row, true)">
                      Resolve dynamic target
                    </v-btn>
                  </td>
                </tr>
                <tr v-if="resolving?.key === row.key">
                  <td colspan="5" data-testid="resolved-targets">
                    <RpcFailureAlert v-if="resolving.failure" :failure="resolving.failure" title="Resolving the link" />
                    <ConceptEvidence v-if="resolving.evidence" :evidence="resolving.evidence" class="mb-2" />
                    <v-table v-if="resolving.rows.length > 0" density="compact" hover>
                      <thead>
                        <tr><th>Document</th><th>Title</th><th>Revision</th><th>Committed</th></tr>
                      </thead>
                      <tbody>
                        <tr v-for="doc in resolving.rows" :key="doc.key" class="cursor-pointer"
                            :data-testid="`resolved-${doc.docId}`" @click="openRow(doc)">
                          <td>
                            <div class="text-mono">{{ doc.docId }}</div>
                            <div class="text-caption text-medium-emphasis text-mono">{{ doc.graphId }} / {{ doc.graphAddressId }}</div>
                          </td>
                          <td>{{ doc.title ?? '(no title)' }}</td>
                          <td class="text-mono">{{ doc.revision }}</td>
                          <td class="text-mono text-caption">{{ doc.committedAt ?? '' }}</td>
                        </tr>
                      </tbody>
                    </v-table>
                    <div v-else-if="!resolving.loading && !resolving.failure" class="text-body-2 text-medium-emphasis"
                         data-testid="no-resolved-targets">No current document matches the selector.</div>
                    <v-btn v-if="resolving.nextToken && !resolving.loading" size="small" variant="text" data-testid="resolve-more"
                           @click="resolve(row, false)">More targets</v-btn>
                  </td>
                </tr>
              </template>
            </tbody>
          </v-table>
          <div v-else-if="linksLoaded && !linksFailure" class="text-body-2 text-medium-emphasis" data-testid="no-links">
            Revision {{ revision }} holds no links.
          </div>
        </template>
      </v-card-text>
    </v-card>

    <v-card class="mb-4">
      <v-card-title class="text-subtitle-2">Inbound backlinks</v-card-title>
      <v-card-text>
        <div class="text-caption text-medium-emphasis mb-2">
          Static links into this document from the current revision of each linking document you can read.
        </div>
        <RpcFailureAlert v-if="backlinksFailure" :failure="backlinksFailure" title="Listing backlinks" />
        <v-table v-if="backlinks.length > 0" density="compact" hover data-testid="backlinks-table">
          <thead>
            <tr>
              <th>Linking document</th>
              <th>Revision</th>
              <th>Field</th>
              <th>Relation</th>
              <th>Pinned to</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="row in backlinks" :key="row.key" class="cursor-pointer"
                :data-testid="`backlink-${row.call.docId}-${row.fieldPath}`" @click="openCall(row.call, row.revision)">
              <td>
                <div class="text-mono">{{ row.call.docId }}</div>
                <div class="text-caption text-medium-emphasis text-mono">{{ row.call.graphId }} / {{ row.call.graphAddressId }}</div>
              </td>
              <td class="text-mono">{{ row.revision }}</td>
              <td class="text-mono">{{ row.fieldPath }}</td>
              <td>
                <a v-if="row.relationHref !== null" :href="row.relationHref" target="_blank" rel="noopener noreferrer"
                   class="text-mono text-caption" data-testid="relation-link" @click.stop>{{ row.relation }}</a>
                <v-chip v-else size="x-small" variant="tonal" class="text-mono" data-testid="relation">{{ row.relation }}</v-chip>
              </td>
              <td class="text-mono">{{ row.pinnedRevision === null ? 'current' : `revision ${row.pinnedRevision}` }}</td>
            </tr>
          </tbody>
        </v-table>
        <div v-else-if="backlinksLoaded && !backlinksLoading && !backlinksFailure" class="text-body-2 text-medium-emphasis"
             data-testid="no-backlinks">No document you can read links here.</div>
        <v-progress-linear v-if="backlinksLoading" indeterminate class="my-2" />
        <v-btn v-if="backlinksNext && !backlinksLoading" size="small" variant="text" data-testid="backlinks-more" @click="loadBacklinks(false)">
          More backlinks
        </v-btn>
      </v-card-text>
    </v-card>
  </div>
</template>

<script setup lang="ts">
import { onMounted, ref, shallowRef, watch } from 'vue'
import { useRouter } from 'vue-router'
import ConceptEvidence from './ConceptEvidence.vue'
import RpcFailureAlert from './RpcFailureAlert.vue'
import type { NodeAddress } from '@/gen/ai/protomolt/proto/repo/v1/address_pb'
import { useRepositoryClients } from '@/repository/clients'
import type { CallNumber } from '@/repository/documentCore'
import {
  backlinkRow,
  documentRoute,
  evidenceView,
  linkRows,
  listBacklinksRequest,
  listLinksRequest,
  resolveLinkRequest,
  type BacklinkRow,
  type EvidenceView,
  type LinkRow,
  type TargetStatus,
} from '@/repository/linksCore'
import { documentRow, type DocumentRow } from '@/repository/listingCore'
import { isOwnCancellation, rpcFailure, type RpcFailure } from '@/repository/rpcErrorCore'
import { useCallScope } from '@/repository/useCallScope'

const props = defineProps<{ address: NodeAddress; revision: bigint; tombstone: boolean }>()

const clients = useRepositoryClients()
const router = useRouter()
const linksScope = useCallScope()
const resolveScope = useCallScope()
const backlinksScope = useCallScope()

const links = shallowRef<LinkRow[]>([])
const linksLoaded = ref(false)
const linksFailure = ref<RpcFailure | null>(null)

/** The one dynamic link whose targets are open, with the pages read so far. */
const resolving = shallowRef<{
  key: string
  rows: DocumentRow[]
  nextToken: string
  evidence: EvidenceView | null
  loading: boolean
  failure: RpcFailure | null
} | null>(null)

const backlinks = shallowRef<BacklinkRow[]>([])
const backlinksNext = ref('')
const backlinksLoaded = ref(false)
const backlinksLoading = ref(false)
const backlinksFailure = ref<RpcFailure | null>(null)

function statusColor(status: TargetStatus): string {
  return status === 'RESOLVED' ? 'success' : status === 'TOMBSTONED' ? 'warning' : 'error'
}

async function loadLinks() {
  const signal = linksScope.next()
  resolveScope.next()
  resolving.value = null
  links.value = []
  linksLoaded.value = false
  linksFailure.value = null
  if (props.tombstone) return
  try {
    const answer = await clients.documents.listLinks(listLinksRequest(props.address, props.revision), { signal })
    if (signal.aborted) return
    if (answer.revision !== props.revision) {
      throw new Error(`ListLinks answered for revision ${answer.revision}; revision ${props.revision} was asked for.`)
    }
    links.value = linkRows(answer)
    linksLoaded.value = true
  } catch (error) {
    if (!isOwnCancellation(error, signal)) linksFailure.value = rpcFailure(error)
  }
}

async function resolve(row: LinkRow, first: boolean) {
  const signal = resolveScope.next()
  const earlier = first || resolving.value?.key !== row.key ? null : resolving.value
  resolving.value = { key: row.key, rows: earlier?.rows ?? [], nextToken: '', evidence: earlier?.evidence ?? null, loading: true, failure: null }
  try {
    const page = await clients.documents.resolveLink(
      resolveLinkRequest(props.address, props.revision, row, earlier?.nextToken ?? ''),
      { signal },
    )
    if (signal.aborted) return
    resolving.value = {
      key: row.key,
      rows: [...(earlier?.rows ?? []), ...page.documents.map(documentRow)],
      nextToken: page.nextPageToken,
      evidence: evidenceView(page.conceptEvidence) ?? earlier?.evidence ?? null,
      loading: false,
      failure: null,
    }
  } catch (error) {
    if (isOwnCancellation(error, signal)) return
    resolving.value = { ...resolving.value!, nextToken: earlier?.nextToken ?? '', loading: false, failure: rpcFailure(error) }
  }
}

async function loadBacklinks(first: boolean) {
  const signal = backlinksScope.next()
  backlinksLoading.value = true
  backlinksFailure.value = null
  if (first) {
    backlinks.value = []
    backlinksNext.value = ''
    backlinksLoaded.value = false
  }
  try {
    const page = await clients.documents.listBacklinks(listBacklinksRequest(props.address, first ? '' : backlinksNext.value), { signal })
    if (signal.aborted) return
    const more = page.backlinks.map(backlinkRow)
    backlinks.value = first ? more : [...backlinks.value, ...more]
    backlinksNext.value = page.nextPageToken
    backlinksLoaded.value = true
  } catch (error) {
    if (!isOwnCancellation(error, signal)) backlinksFailure.value = rpcFailure(error)
  } finally {
    if (!signal.aborted) backlinksLoading.value = false
  }
}

function openCall(call: CallNumber, revision: bigint | null) {
  void router.push(documentRoute(call, revision))
}

function openRow(row: DocumentRow) {
  openCall({ accountId: props.address.accountId, graphId: row.graphId, graphAddressId: row.graphAddressId, docId: row.docId }, null)
}

onMounted(() => {
  void loadLinks()
  void loadBacklinks(true)
})
watch(() => [props.address, props.revision], () => {
  void loadLinks()
  void loadBacklinks(true)
})
</script>
