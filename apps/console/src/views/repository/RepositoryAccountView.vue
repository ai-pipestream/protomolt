<template>
  <div>
    <v-card class="mb-4">
      <v-card-title class="text-subtitle-1 d-flex align-center flex-wrap ga-2">
        <v-btn icon="mdi-arrow-left" size="small" variant="text" :to="{ name: 'repository' }" aria-label="All accounts" />
        <v-icon size="small">mdi-domain</v-icon>
        <span class="text-mono">{{ accountId }}</span>
        <v-spacer />
        <v-btn size="small" variant="tonal" prepend-icon="mdi-upload" data-testid="account-upload"
               :to="{ name: 'repository-upload', query: { account: accountId } }">
          Upload
        </v-btn>
        <v-btn size="small" variant="tonal" prepend-icon="mdi-magnify" :to="{ name: 'repository-lookup', query: { account: accountId } }">
          Look up
        </v-btn>
      </v-card-title>
    </v-card>

    <v-card class="mb-4">
      <v-card-title class="text-subtitle-2">Drives</v-card-title>
      <v-card-text>
        <RpcFailureAlert v-if="drivesFailure" :failure="drivesFailure" title="Listing drives" />
        <v-table v-if="drives.length > 0" density="compact" data-testid="drive-table">
          <thead>
            <tr>
              <th>Name</th>
              <th>Type</th>
              <th>Status</th>
              <th>Provider</th>
              <th>Location</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="drive in drives" :key="drive.driveId">
              <td>
                <div>{{ drive.name }}</div>
                <div class="text-caption text-medium-emphasis text-mono">{{ drive.driveId }}</div>
              </td>
              <td>{{ driveType(drive.driveType) }}</td>
              <td>{{ driveStatus(drive.status) }}</td>
              <td>{{ drive.provider }}</td>
              <td class="text-mono text-caption">{{ drive.bucket }}{{ drive.prefix ? `/${drive.prefix}` : '' }}</td>
            </tr>
          </tbody>
        </v-table>
        <div v-else-if="!drivesLoading && !drivesFailure" class="text-body-2 text-medium-emphasis">This account has no drives.</div>
        <v-btn v-if="drivesNext" size="small" variant="text" class="mt-2" :loading="drivesLoading" @click="loadDrives(false)">
          More drives
        </v-btn>
      </v-card-text>
    </v-card>

    <v-card class="mb-4">
      <v-card-title class="text-subtitle-2">Archives</v-card-title>
      <v-card-text>
        <RpcFailureAlert v-if="archivesFailure" :failure="archivesFailure" title="Listing archives" />
        <v-list v-if="archives.length > 0" density="compact" data-testid="archive-list">
          <v-list-item
            v-for="a in archives"
            :key="a.name"
            :to="{ name: 'repository-archive', query: { account: accountId, archive: a.name } }"
            :title="a.name"
            :subtitle="a.description || `drive ${a.driveName}`"
            prepend-icon="mdi-archive-outline"
          />
        </v-list>
        <div v-else-if="!archivesLoading && !archivesFailure" class="text-body-2 text-medium-emphasis">This account has no archives.</div>
      </v-card-text>
    </v-card>

    <v-card class="mb-4">
      <v-card-title class="text-subtitle-2">Documents</v-card-title>
      <v-card-text>
        <v-row dense>
          <v-col cols="12" sm="6" md="3">
            <v-text-field v-model="query.graphId" label="Graph ID (scope)" density="compact" hide-details />
          </v-col>
          <v-col cols="12" sm="6" md="3">
            <v-text-field v-model="query.graphAddressId" label="Graph address ID (scope)" density="compact" hide-details />
          </v-col>
          <v-col cols="6" md="2">
            <v-select v-model="query.sortField" :items="sortItems" label="Sort by" density="compact" hide-details data-testid="sort-field" />
          </v-col>
          <v-col cols="6" md="2">
            <v-switch v-model="query.descending" label="Descending" density="compact" hide-details color="primary"
                      :disabled="query.sortField === DocumentSortField.UNSPECIFIED" />
          </v-col>
          <v-col cols="6" md="2">
            <v-switch v-model="query.includeTombstoned" label="Tombstoned" density="compact" hide-details color="primary"
                      data-testid="include-tombstoned" />
          </v-col>
        </v-row>

        <v-row dense class="mt-1" data-testid="concept-filter">
          <v-col cols="12" sm="6" md="3">
            <v-text-field v-model="query.filter.concept.schemeId" label="Concept scheme ID" density="compact" hide-details
                          data-testid="concept-scheme" />
          </v-col>
          <v-col cols="12" sm="6" md="3">
            <v-text-field v-model="query.filter.concept.conceptId" label="Concept ID" density="compact" hide-details
                          data-testid="concept-id" />
          </v-col>
          <v-col cols="6" md="3">
            <v-checkbox v-model="query.filter.concept.narrower" label="Include narrower concepts" density="compact" hide-details
                        data-testid="concept-narrower" />
          </v-col>
          <v-col cols="6" md="3">
            <v-checkbox v-model="query.filter.concept.closeMatch" label="Include close matches" density="compact" hide-details
                        data-testid="concept-close-match" />
          </v-col>
        </v-row>

        <v-expansion-panels variant="accordion" class="my-3">
          <v-expansion-panel title="Filters" data-testid="filter-panel">
            <v-expansion-panel-text>
              <v-row dense>
                <v-col v-for="f in TEXT_FILTERS" :key="f.field" cols="12" sm="6" md="4">
                  <v-text-field v-model="query.filter.text[f.field]" :label="f.label" density="compact" hide-details
                                :data-testid="`filter-${f.field}`" />
                </v-col>
                <v-col cols="12" sm="6" md="4">
                  <v-text-field v-model="query.filter.committedAfter" type="datetime-local" label="Committed at or after"
                                density="compact" hide-details />
                </v-col>
                <v-col cols="12" sm="6" md="4">
                  <v-text-field v-model="query.filter.committedBefore" type="datetime-local" label="Committed before"
                                density="compact" hide-details />
                </v-col>
                <v-col cols="12" sm="6" md="2">
                  <v-select v-model="query.filter.owner.kind" :items="ownerKinds" label="Owner" density="compact" hide-details />
                </v-col>
                <v-col cols="12" sm="6" md="2">
                  <v-text-field v-model="query.filter.owner.id" :disabled="query.filter.owner.kind === 'any'"
                                :label="query.filter.owner.kind === 'module' ? 'Module ID' : 'Datasource ID'" density="compact" hide-details />
                </v-col>
              </v-row>
              <div class="text-caption text-medium-emphasis mt-3 mb-1">Metadata (each pair must match exactly)</div>
              <v-row v-for="(pair, i) in query.filter.metadata" :key="i" dense>
                <v-col cols="5"><v-text-field v-model="pair.key" :label="`Key ${i + 1}`" density="compact" hide-details /></v-col>
                <v-col cols="5"><v-text-field v-model="pair.value" :label="`Value ${i + 1}`" density="compact" hide-details /></v-col>
                <v-col cols="2">
                  <v-btn icon="mdi-close" size="small" variant="text" :aria-label="`Remove metadata pair ${i + 1}`"
                         @click="query.filter.metadata.splice(i, 1)" />
                </v-col>
              </v-row>
              <v-btn size="small" variant="text" prepend-icon="mdi-plus" @click="query.filter.metadata.push({ key: '', value: '' })">
                Add metadata pair
              </v-btn>
            </v-expansion-panel-text>
          </v-expansion-panel>
        </v-expansion-panels>

        <div class="d-flex align-center ga-2 mb-3">
          <v-btn color="primary" size="small" :loading="listing" data-testid="apply-query" @click="applyQuery">List documents</v-btn>
          <v-btn size="small" variant="text" @click="clearFilters">Clear filters</v-btn>
          <v-spacer />
          <v-text-field v-model.number="query.pageSize" type="number" label="Page size" density="compact" hide-details style="max-width: 120px" />
        </div>

        <v-alert v-if="inputError" type="warning" variant="tonal" density="compact" class="mb-3" data-testid="input-error">
          {{ inputError }}
        </v-alert>
        <RpcFailureAlert v-if="listFailure" :failure="listFailure" title="Listing documents" />
        <ConceptEvidence v-if="evidence" :evidence="evidence" class="mb-2" />

        <v-table v-if="rows.length > 0" density="compact" hover data-testid="document-table">
          <thead>
            <tr>
              <th>Document</th>
              <th>Title</th>
              <th>Type</th>
              <th>MIME type</th>
              <th>Owner</th>
              <th>Revision</th>
              <th>Committed</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="row in rows" :key="row.key" class="cursor-pointer" :data-testid="`doc-row-${row.docId}`" @click="openRow(row)">
              <td>
                <div class="text-mono">{{ row.docId }}</div>
                <div class="text-caption text-medium-emphasis text-mono">{{ row.graphId }} / {{ row.graphAddressId }}</div>
              </td>
              <td>
                <v-chip v-if="row.tombstone" size="x-small" color="warning" variant="tonal" class="mr-1">tombstone</v-chip>
                <span v-if="row.title !== null">{{ row.title }}</span>
                <span v-else class="text-medium-emphasis">(no title)</span>
              </td>
              <td>{{ row.documentType ?? '' }}</td>
              <td class="text-mono text-caption">{{ row.sourceMimeType ?? '' }}</td>
              <td class="text-caption">{{ row.owner ?? '' }}</td>
              <td class="text-mono">{{ row.revision }}</td>
              <td class="text-mono text-caption">{{ row.committedAt ?? '' }}</td>
            </tr>
          </tbody>
        </v-table>
        <div v-else-if="listed && !listing && !listFailure" class="text-body-2 text-medium-emphasis" data-testid="no-documents">
          No documents match.
        </div>

        <div v-if="listed" class="d-flex align-center ga-2 mt-3">
          <v-btn size="small" variant="text" prepend-icon="mdi-chevron-left" :disabled="!cursor.hasPrevious || listing"
                 data-testid="page-previous" @click="previousPage">Previous</v-btn>
          <span class="text-caption">Page {{ cursor.page }}</span>
          <v-btn size="small" variant="text" append-icon="mdi-chevron-right" :disabled="!cursor.hasNext || listing"
                 data-testid="page-next" @click="nextPage">Next</v-btn>
        </div>
      </v-card-text>
    </v-card>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, ref, shallowRef, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { create } from '@bufbuild/protobuf'
import ConceptEvidence from '@/components/repository/ConceptEvidence.vue'
import RpcFailureAlert from '@/components/repository/RpcFailureAlert.vue'
import type { Archive } from '@/gen/ai/protomolt/proto/repo/archive/v1/archive_pb'
import { ListArchivesRequestSchema } from '@/gen/ai/protomolt/proto/repo/archive/v1/archive_service_pb'
import { DriveStatusSchema, DriveTypeSchema, ListDrivesRequestSchema, type Drive } from '@/gen/ai/protomolt/proto/repo/v1/drive_pb'
import { DocumentSortField } from '@/gen/ai/protomolt/proto/repo/v2/document_service_pb'
import { useRepositoryClients } from '@/repository/clients'
import { enumLabel } from '@/repository/formatCore'
import { evidenceView, type EvidenceView } from '@/repository/linksCore'
import {
  ListingInputError,
  PageCursor,
  TEXT_FILTERS,
  buildListRequest,
  documentRow,
  emptyFilterForm,
  listingQueryFromRoute,
  listingRouteQuery,
  newListingQuery,
  sameRouteQuery,
  type DocumentRow,
  type ListingQuery,
} from '@/repository/listingCore'
import { isOwnCancellation, rpcFailure, type RpcFailure } from '@/repository/rpcErrorCore'
import { useCallScope } from '@/repository/useCallScope'

const clients = useRepositoryClients()
const route = useRoute()
const router = useRouter()
const accountId = computed(() => String(route.params.accountId))

const sortItems = [
  { title: 'Call number', value: DocumentSortField.UNSPECIFIED },
  { title: 'Committed at', value: DocumentSortField.COMMITTED_AT },
  { title: 'Title', value: DocumentSortField.TITLE },
]
const ownerKinds = [
  { title: 'Any owner', value: 'any' },
  { title: 'Intake datasource', value: 'datasource' },
  { title: 'Step module', value: 'module' },
]

// Drives
const drivesScope = useCallScope()
const drives = ref<Drive[]>([])
const drivesNext = ref('')
const drivesLoading = ref(false)
const drivesFailure = ref<RpcFailure | null>(null)

function driveType(value: number) {
  return enumLabel(DriveTypeSchema, value)
}
function driveStatus(value: number) {
  return enumLabel(DriveStatusSchema, value)
}

async function loadDrives(first: boolean) {
  const signal = drivesScope.next()
  drivesLoading.value = true
  drivesFailure.value = null
  try {
    const page = await clients.drives.listDrives(
      create(ListDrivesRequestSchema, { accountId: accountId.value, limit: 100, continuationToken: first ? '' : drivesNext.value }),
      { signal },
    )
    drives.value = first ? page.drives : [...drives.value, ...page.drives]
    drivesNext.value = page.nextContinuationToken
  } catch (error) {
    if (!isOwnCancellation(error, signal)) drivesFailure.value = rpcFailure(error)
  } finally {
    drivesLoading.value = false
  }
}

// Archives
const archivesScope = useCallScope()
const archives = shallowRef<Archive[]>([])
const archivesLoading = ref(false)
const archivesFailure = ref<RpcFailure | null>(null)

async function loadArchives() {
  const signal = archivesScope.next()
  archivesLoading.value = true
  archivesFailure.value = null
  archives.value = []
  try {
    const all: Archive[] = []
    let token = ''
    do {
      const page = await clients.archives.listArchives(
        create(ListArchivesRequestSchema, { accountId: accountId.value, limit: 100, continuationToken: token }),
        { signal },
      )
      all.push(...page.archives)
      token = page.nextContinuationToken
    } while (token !== '')
    if (!signal.aborted) archives.value = all
  } catch (error) {
    if (!signal.aborted) archivesFailure.value = rpcFailure(error)
  } finally {
    if (!signal.aborted) archivesLoading.value = false
  }
}

// Documents
const listScope = useCallScope()
const query = reactive(newListingQuery(accountId.value))
const cursor = reactive(new PageCursor()) as PageCursor
const rows = shallowRef<DocumentRow[]>([])
const listing = ref(false)
const listed = ref(false)
const listFailure = ref<RpcFailure | null>(null)
const inputError = ref<string | null>(null)
const evidence = shallowRef<EvidenceView | null>(null)

// The query the page tokens were issued for. Paging uses it, not the form,
// so editing the form changes nothing until List documents is clicked.
let listedQuery: ListingQuery = structuredQuery(query)

function structuredQuery(source: ListingQuery): ListingQuery {
  return {
    ...source,
    filter: {
      text: { ...source.filter.text },
      committedAfter: source.filter.committedAfter,
      committedBefore: source.filter.committedBefore,
      metadata: source.filter.metadata.map((m) => ({ ...m })),
      owner: { ...source.filter.owner },
      concept: { ...source.filter.concept },
    },
  }
}

async function loadPage() {
  inputError.value = null
  listFailure.value = null
  let request
  try {
    request = buildListRequest(listedQuery, cursor.token)
  } catch (error) {
    if (error instanceof ListingInputError) {
      inputError.value = error.message
      return
    }
    throw error
  }
  const signal = listScope.next()
  listing.value = true
  try {
    const page = await clients.documents.listDocuments(request, { signal })
    rows.value = page.documents.map(documentRow)
    evidence.value = evidenceView(page.conceptEvidence)
    cursor.answered(page.nextPageToken)
    listed.value = true
  } catch (error) {
    if (!isOwnCancellation(error, signal)) {
      rows.value = []
      evidence.value = null
      listFailure.value = rpcFailure(error)
    }
  } finally {
    listing.value = false
  }
}

// A listed query lives in the route, so a reload, the back button and a shared
// link list it again. List documents writes the form there; the route watcher
// below then lists from page 1. Listing the query already shown lists it again.
function applyQuery() {
  // A form the server would refuse stays out of the route; it is named here instead.
  try {
    buildListRequest(query, '')
  } catch (error) {
    if (!(error instanceof ListingInputError)) throw error
    inputError.value = error.message
    return
  }
  const next = listingRouteQuery(query)
  if (sameRouteQuery(next, route.query)) {
    listedQuery = structuredQuery(query)
    cursor.reset()
    void loadPage()
    return
  }
  void router.push({ name: 'repository-account', params: { accountId: accountId.value }, query: next })
}

/** Lists the query the route names from page 1, or names the route value it cannot read. */
function listFromRoute() {
  cursor.reset()
  rows.value = []
  evidence.value = null
  try {
    Object.assign(query, listingQueryFromRoute(accountId.value, route.query))
  } catch (error) {
    if (!(error instanceof ListingInputError)) throw error
    Object.assign(query, newListingQuery(accountId.value))
    listedQuery = structuredQuery(query)
    listFailure.value = null
    listed.value = false
    inputError.value = `The link's listing query cannot be read: ${error.message}`
    return
  }
  listedQuery = structuredQuery(query)
  void loadPage()
}
function clearFilters() {
  query.filter = emptyFilterForm()
}
function nextPage() {
  cursor.forward()
  void loadPage()
}
function previousPage() {
  cursor.back()
  void loadPage()
}

function openRow(row: DocumentRow) {
  const query: Record<string, string> = { account: accountId.value, graph: row.graphId, node: row.graphAddressId, doc: row.docId }
  // A current read of a tombstoned document is NOT_FOUND; its tombstone
  // revision itself opens, with the history behind it.
  if (row.tombstone) query.revision = row.revision
  void router.push({ name: 'repository-document', query })
}

function loadAll() {
  void loadDrives(true)
  void loadArchives()
  listFromRoute()
}

onMounted(loadAll)

// The same view serves every account; a different account starts over, and a
// new listing query in the route lists again from page 1. Other routes (a
// document this screen opened) leave the screen as it is.
let shownAccount = accountId.value
watch(
  () => route.fullPath,
  () => {
    if (route.name !== 'repository-account') return
    if (accountId.value !== shownAccount) {
      shownAccount = accountId.value
      listed.value = false
      drives.value = []
      drivesNext.value = ''
      loadAll()
      return
    }
    listFromRoute()
  },
)

// Call number order is always ascending; leaving a sort field clears Descending
// so it cannot linger behind a disabled switch.
watch(
  () => query.sortField,
  (field) => {
    if (field === DocumentSortField.UNSPECIFIED) query.descending = false
  },
)
</script>
