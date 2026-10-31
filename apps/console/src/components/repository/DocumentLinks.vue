<template>
  <div data-testid="links">
    <v-card v-if="!tombstone && (links.length > 0 || backlinks.length > 0)" class="mb-4" data-testid="link-neighborhood-card">
      <v-card-title class="text-subtitle-2 d-flex align-center flex-wrap ga-2">
        <span>1-hop link neighborhood</span>
        <span class="text-caption text-medium-emphasis">
          {{ backlinks.length }} inbound · {{ links.length }} outbound
        </span>
        <v-spacer />
        <v-btn
          v-if="hasDynamicLinks"
          size="x-small"
          variant="tonal"
          color="primary"
          prepend-icon="mdi-sync"
          :loading="resolvingAll"
          data-testid="resolve-all-dynamic-links"
          @click="resolveAllDynamic"
        >
          Resolve all dynamic links
        </v-btn>
      </v-card-title>
      <v-card-text>
        <div class="link-graph-wrapper" role="region" aria-label="1-hop document link neighborhood graph">
          <svg
            :viewBox="`0 0 ${neighborhoodGraph.width} ${neighborhoodGraph.height}`"
            class="link-graph-svg"
            data-testid="link-neighborhood-graph"
          >
            <defs>
              <marker id="link-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
                <path d="M 0 1 L 10 5 L 0 9 z" fill="#78909c" />
              </marker>
            </defs>
            <text x="125" y="18" text-anchor="middle" class="graph-col-header">INBOUND BACKLINKS</text>
            <text x="380" y="18" text-anchor="middle" class="graph-col-header">CURRENT DOCUMENT</text>
            <text x="635" y="18" text-anchor="middle" class="graph-col-header">OUTBOUND TARGETS</text>

            <g v-for="edge in neighborhoodGraph.edges" :key="edge.id" :data-testid="`graph-edge-${edge.id}`">
              <line
                :x1="edge.x1"
                :y1="edge.y1"
                :x2="edge.x2"
                :y2="edge.y2"
                stroke="#78909c"
                stroke-width="1.5"
                :stroke-dasharray="edge.dashed ? '4 3' : undefined"
                marker-end="url(#link-arrow)"
              />
              <text :x="edge.labelX" :y="edge.labelY" text-anchor="middle" class="graph-edge-label">
                {{ edge.relation }}
              </text>
            </g>

            <g
              v-for="node in neighborhoodGraph.nodes"
              :key="node.id"
              :transform="`translate(${node.x - 92}, ${node.y - 22})`"
              :class="{ 'graph-node-clickable': node.call !== null && node.column !== 'current' }"
              :data-testid="`graph-node-${node.docId}`"
              @click="onGraphNodeClick(node)"
            >
              <rect
                width="184"
                height="44"
                rx="6"
                :fill="graphNodeFill(node.status)"
                :stroke="graphNodeStroke(node.status)"
                stroke-width="1.5"
              />
              <text x="92" y="18" text-anchor="middle" class="graph-node-title">
                {{ truncateGraphText(node.docId, 23) }}
              </text>
              <text x="92" y="34" text-anchor="middle" class="graph-node-sub">
                {{ truncateGraphText(node.subtitle, 26) }}
              </text>
            </g>
          </svg>
        </div>
      </v-card-text>
    </v-card>

    <v-card class="mb-4">
      <v-card-title class="text-subtitle-2 d-flex align-center flex-wrap ga-2">
        <span>Outbound links</span>
        <v-spacer />
        <v-btn
          v-if="hasDynamicLinks && backlinks.length === 0 && links.length === 0"
          size="x-small"
          variant="tonal"
          :loading="resolvingAll"
          @click="resolveAllDynamic"
        >
          Resolve all dynamic links
        </v-btn>
      </v-card-title>
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
                    <v-chip
                      v-else
                      size="x-small"
                      variant="tonal"
                      :color="dynamicStatusColor(row.key)"
                      :data-testid="`link-status-${row.fieldPath}-${row.ordinal}`"
                    >
                      {{ dynamicStatusLabel(row.key) }}
                    </v-chip>
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
import { computed, onMounted, ref, shallowRef, watch } from 'vue'
import { useRouter } from 'vue-router'
import ConceptEvidence from './ConceptEvidence.vue'
import RpcFailureAlert from './RpcFailureAlert.vue'
import type { NodeAddress } from '@/gen/ai/protomolt/proto/repo/v1/address_pb'
import { useRepositoryClients } from '@/repository/clients'
import { callNumberOf, type CallNumber } from '@/repository/documentCore'
import {
  backlinkRow,
  buildLinkNeighborhoodGraph,
  documentRoute,
  evidenceView,
  linkRows,
  listBacklinksRequest,
  listLinksRequest,
  resolveLinkRequest,
  type BacklinkRow,
  type EvidenceView,
  type LinkGraphNode,
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
const resolvedDynamicMap = ref<Record<string, DocumentRow[]>>({})
const resolvingAll = ref(false)
// The resolve-all call that owns `resolvingAll`; any newer resolve or reload supersedes it.
let resolvingAllSignal: AbortSignal | null = null

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

const hasDynamicLinks = computed(() => links.value.some((r) => r.target.kind === 'dynamic'))
const neighborhoodGraph = computed(() =>
  buildLinkNeighborhoodGraph(callNumberOf(props.address), props.revision, links.value, backlinks.value, resolvedDynamicMap.value),
)

function statusColor(status: TargetStatus): string {
  return status === 'RESOLVED' ? 'success' : status === 'TOMBSTONED' ? 'warning' : 'error'
}

function dynamicStatusColor(key: string): string {
  const rows = resolvedDynamicMap.value[key]
  if (rows === undefined) return 'default'
  return rows.length > 0 ? 'success' : 'warning'
}

function dynamicStatusLabel(key: string): string {
  const rows = resolvedDynamicMap.value[key]
  if (rows === undefined) return 'DYNAMIC'
  return rows.length > 0 ? `RESOLVED (${rows.length})` : 'EMPTY (0)'
}

function graphNodeFill(status: LinkGraphNode['status']): string {
  if (status === 'CURRENT') return 'rgba(245, 166, 35, 0.16)'
  if (status === 'RESOLVED') return 'rgba(46, 125, 50, 0.15)'
  if (status === 'TOMBSTONED') return 'rgba(237, 108, 2, 0.15)'
  if (status === 'UNAVAILABLE') return 'rgba(211, 47, 47, 0.15)'
  return 'rgba(120, 144, 156, 0.14)'
}

function graphNodeStroke(status: LinkGraphNode['status']): string {
  if (status === 'CURRENT') return '#f5a623'
  if (status === 'RESOLVED') return '#4caf50'
  if (status === 'TOMBSTONED') return '#ff9800'
  if (status === 'UNAVAILABLE') return '#ef5350'
  return '#78909c'
}

function truncateGraphText(value: string, max: number): string {
  return value.length <= max ? value : `${value.slice(0, max - 1)}…`
}

function onGraphNodeClick(node: LinkGraphNode) {
  if (node.call !== null && node.column !== 'current') {
    openCall(node.call, node.revision)
  }
}

async function loadLinks() {
  const signal = linksScope.next()
  resolveScope.next()
  resolving.value = null
  resolvedDynamicMap.value = {}
  resolvingAll.value = false
  resolvingAllSignal = null
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
  resolvingAll.value = false
  resolvingAllSignal = null
  const earlier = first || resolving.value?.key !== row.key ? null : resolving.value
  resolving.value = { key: row.key, rows: earlier?.rows ?? [], nextToken: '', evidence: earlier?.evidence ?? null, loading: true, failure: null }
  try {
    const page = await clients.documents.resolveLink(
      resolveLinkRequest(props.address, props.revision, row, earlier?.nextToken ?? ''),
      { signal },
    )
    if (signal.aborted) return
    const nextRows = [...(earlier?.rows ?? []), ...page.documents.map(documentRow)]
    resolving.value = {
      key: row.key,
      rows: nextRows,
      nextToken: page.nextPageToken,
      evidence: evidenceView(page.conceptEvidence) ?? earlier?.evidence ?? null,
      loading: false,
      failure: null,
    }
    resolvedDynamicMap.value = { ...resolvedDynamicMap.value, [row.key]: nextRows }
  } catch (error) {
    if (isOwnCancellation(error, signal)) return
    resolving.value = { ...resolving.value!, nextToken: earlier?.nextToken ?? '', loading: false, failure: rpcFailure(error) }
  }
}

async function resolveAllDynamic() {
  const signal = resolveScope.next()
  resolvingAll.value = true
  resolvingAllSignal = signal
  try {
    const dynRows = links.value.filter((r) => r.target.kind === 'dynamic')
    for (const row of dynRows) {
      const page = await clients.documents.resolveLink(resolveLinkRequest(props.address, props.revision, row, ''), { signal })
      if (signal.aborted) return
      const rows = page.documents.map(documentRow)
      resolvedDynamicMap.value = { ...resolvedDynamicMap.value, [row.key]: rows }
    }
  } catch (error) {
    if (!isOwnCancellation(error, signal)) linksFailure.value = rpcFailure(error)
  } finally {
    if (resolvingAllSignal === signal) {
      resolvingAll.value = false
      resolvingAllSignal = null
    }
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

<style scoped>
.link-graph-wrapper {
  overflow-x: auto;
  border: 1px solid rgba(120, 144, 156, 0.25);
  border-radius: 6px;
  padding: 8px;
}

.link-graph-svg {
  width: 100%;
  min-width: 640px;
  height: auto;
  display: block;
}

.graph-col-header {
  font-size: 10px;
  font-weight: 600;
  letter-spacing: 0.06em;
  fill: rgba(255, 255, 255, 0.55);
}

.graph-edge-label {
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
  font-size: 10px;
  fill: #90a4ae;
}

.graph-node-title {
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
  font-size: 11px;
  font-weight: 600;
  fill: currentColor;
}

.graph-node-sub {
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
  font-size: 10px;
  fill: rgba(255, 255, 255, 0.68);
}

.graph-node-clickable {
  cursor: pointer;
}
</style>
