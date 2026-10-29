<template>
  <div>
    <v-card class="mb-4">
      <v-card-title class="text-subtitle-1 d-flex align-center flex-wrap ga-2">
        <v-btn icon="mdi-arrow-left" size="small" variant="text" aria-label="Account"
               :to="accountId ? { name: 'repository-account', params: { accountId } } : { name: 'repository' }" />
        <v-icon size="small">mdi-archive-outline</v-icon>
        <span class="text-mono" data-testid="archive-name">{{ archiveName }}</span>
        <span class="text-caption text-medium-emphasis text-mono">{{ accountId }}</span>
      </v-card-title>
      <v-card-text v-if="archive" class="text-body-2 pt-0" data-testid="archive-info">
        {{ archive.description }}
        <div class="text-caption text-medium-emphasis">
          drive {{ archive.driveName }} · versioning {{ versioning(archive.versioning) }} · created {{ instant(archive.createdAt) ?? 'not recorded' }}
        </div>
      </v-card-text>
    </v-card>

    <v-alert v-if="routeError" type="warning" variant="tonal" density="compact" class="mb-3" data-testid="route-error">{{ routeError }}</v-alert>
    <RpcFailureAlert v-if="archiveFailure" :failure="archiveFailure" title="Reading the archive" />

    <v-card v-if="stats" class="mb-4" data-testid="archive-stats">
      <v-card-title class="text-subtitle-2">Stats</v-card-title>
      <v-card-text>
        <div class="d-flex flex-wrap ga-6 mb-2">
          <div><div class="text-caption text-medium-emphasis">Entries</div><div class="text-h6">{{ stats.entries }}</div></div>
          <div><div class="text-caption text-medium-emphasis">Versions</div><div class="text-h6">{{ stats.versions }}</div></div>
          <div><div class="text-caption text-medium-emphasis">Current bytes</div><div class="text-h6">{{ byteSize(stats.currentBytes) }}</div></div>
          <div><div class="text-caption text-medium-emphasis">Retained bytes</div><div class="text-h6">{{ byteSize(stats.retainedBytes) }}</div></div>
        </div>
        <v-table v-if="stats.renditions.length > 0" density="compact" class="mb-2">
          <thead><tr><th scope="col">Rendition</th><th scope="col">Objects</th><th scope="col">Bytes</th></tr></thead>
          <tbody>
            <tr v-for="r in stats.renditions" :key="r.renditionName">
              <td class="text-mono">{{ r.renditionName }}</td><td>{{ r.objectCount }}</td><td>{{ byteSize(r.totalBytes) }}</td>
            </tr>
          </tbody>
        </v-table>
        <div class="text-caption">
          <span v-for="c in stats.classificationStates" :key="c.state" class="mr-3">{{ classificationState(c.state) }}: {{ c.count }}</span>
        </div>
      </v-card-text>
    </v-card>
    <RpcFailureAlert v-if="statsFailure" :failure="statsFailure" title="Reading archive stats" />

    <v-card v-if="!routeError" class="mb-4">
      <v-card-title class="text-subtitle-2 d-flex align-center flex-wrap ga-2">
        Entries
        <v-spacer />
        <v-text-field
          v-model="entrySearch"
          label="Filter entries"
          density="compact"
          hide-details
          clearable
          autocorrect="off"
          autocapitalize="off"
          spellcheck="false"
          style="max-width: 220px"
          data-testid="entry-search-filter"
        />
        <v-select v-model="classification" :items="classificationItems" label="Classification" density="compact" hide-details
                  style="max-width: 240px" data-testid="classification-filter" />
        <v-btn
          size="small"
          variant="tonal"
          prepend-icon="mdi-upload"
          data-testid="toggle-new-entry-upload"
          @click="showNewEntryPanel = !showNewEntryPanel"
        >
          {{ showNewEntryPanel ? 'Hide upload' : 'Upload new entry' }}
        </v-btn>
      </v-card-title>
      <v-card-text>
        <div
          v-if="classification !== ClassificationState.UNSPECIFIED || (entrySearch ?? '').trim() !== ''"
          class="d-flex align-center flex-wrap ga-1 mb-3"
          data-testid="archive-active-filters"
        >
          <span class="text-caption text-medium-emphasis mr-1">Active URL filters:</span>
          <v-chip
            v-if="classification !== ClassificationState.UNSPECIFIED"
            size="small"
            color="primary"
            variant="tonal"
            closable
            class="text-mono"
            data-testid="archive-filter-classification"
            @click:close="classification = ClassificationState.UNSPECIFIED"
          >
            classification: {{ classificationState(classification) }}
          </v-chip>
          <v-chip
            v-if="(entrySearch ?? '').trim() !== ''"
            size="small"
            color="primary"
            variant="tonal"
            closable
            class="text-mono"
            data-testid="archive-filter-q"
            @click:close="entrySearch = ''"
          >
            q: {{ (entrySearch ?? '').trim() }}
          </v-chip>
          <v-btn
            size="x-small"
            variant="text"
            data-testid="archive-clear-filters"
            @click="clearArchiveFilters"
          >
            Clear filters
          </v-btn>
        </div>

        <v-sheet v-if="showNewEntryPanel" rounded border class="pa-3 mb-3" data-testid="new-entry-upload-panel">
          <div class="text-subtitle-2 mb-2">Upload new archive entry</div>
          <v-row density="compact">
            <v-col cols="12" md="4">
              <v-text-field
                v-model="newEntryForm.entryId"
                label="Entry ID"
                density="compact"
                hint="Auto-suggested when a file is chosen"
                persistent-hint
                autocorrect="off"
                autocapitalize="off"
                spellcheck="false"
                data-testid="new-entry-id"
              />
            </v-col>
            <v-col cols="12" md="3">
              <v-text-field
                v-model="newEntryForm.rendition"
                label="Rendition name"
                density="compact"
                autocorrect="off"
                autocapitalize="off"
                spellcheck="false"
                data-testid="new-entry-rendition"
              />
            </v-col>
            <v-col cols="12" md="3">
              <v-text-field
                v-model="newEntryForm.contentType"
                label="Content type (optional)"
                density="compact"
                autocorrect="off"
                autocapitalize="off"
                spellcheck="false"
                data-testid="new-entry-type"
              />
            </v-col>
            <v-col cols="12" md="2">
              <v-text-field
                v-model="newEntryForm.module"
                label="Writer module"
                density="compact"
                autocorrect="off"
                autocapitalize="off"
                spellcheck="false"
                data-testid="new-entry-module"
              />
            </v-col>
          </v-row>
          <div class="d-flex align-center flex-wrap ga-3 mt-2">
            <input type="file" data-testid="new-entry-file" @change="pickNewEntryFile" />
            <span v-if="newEntryFile" class="text-caption text-mono">
              {{ newEntryFile.name }} · {{ byteSize(newEntryFile.size) }}
            </span>
            <v-btn
              size="small"
              color="primary"
              :loading="newEntryBusy"
              :disabled="newEntryBusy"
              data-testid="new-entry-submit"
              @click="submitNewEntryUpload"
            >
              Upload new entry
            </v-btn>
          </div>
          <v-alert
            v-if="newEntryError"
            type="warning"
            variant="tonal"
            density="compact"
            class="mt-2"
            data-testid="new-entry-error"
          >
            {{ newEntryError }}
          </v-alert>
          <v-alert
            v-if="newEntryCommitted"
            type="success"
            variant="tonal"
            density="compact"
            class="mt-2"
            data-testid="new-entry-committed"
          >
            <div class="d-flex align-center flex-wrap ga-2">
              <span>
                Committed entry <span class="text-mono">{{ newEntryCommitted.entryId }}</span> (version {{ newEntryCommitted.version }}, rendition
                <span class="text-mono">{{ newEntryCommitted.rendition }}</span>).
              </span>
              <router-link
                v-if="newEntryOperationId"
                :to="{ name: 'repository-lookup', query: { account: accountId, op: newEntryOperationId } }"
                class="text-caption text-mono"
                data-testid="new-entry-op-link"
              >
                op: {{ newEntryOperationId }}
              </router-link>
              <v-btn
                size="x-small"
                variant="tonal"
                data-testid="new-entry-open"
                @click="openEntry(newEntryCommitted.entryId)"
              >
                Open entry
              </v-btn>
            </div>
          </v-alert>
        </v-sheet>

        <RpcFailureAlert v-if="entriesFailure" :failure="entriesFailure" title="Listing entries" />
        <v-table v-if="filteredEntries.length > 0" density="compact" hover data-testid="entry-table">
          <thead>
            <tr>
              <th scope="col">Entry</th>
              <th scope="col">Title</th>
              <th scope="col">File</th>
              <th scope="col">Type</th>
              <th scope="col">Version</th>
              <th scope="col">Classification</th>
              <th scope="col">Updated</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="e in filteredEntries" :key="e.entryId" class="cursor-pointer" :data-testid="`entry-${e.entryId}`" @click="openEntry(e.entryId)">
              <td class="text-mono">{{ e.entryId }}</td>
              <td>{{ e.title }}</td>
              <td>{{ e.filename }}</td>
              <td class="text-mono text-caption">{{ e.contentType }}</td>
              <td class="text-mono">{{ e.currentVersion }}</td>
              <td class="text-caption">{{ e.classification }}</td>
              <td class="text-mono text-caption">{{ e.updatedAt ?? '' }}</td>
            </tr>
          </tbody>
        </v-table>
        <div v-else-if="listed && !listing && !entriesFailure" class="text-body-2 text-medium-emphasis" data-testid="no-entries">No entries match.</div>
        <div v-if="listed" class="d-flex align-center ga-2 mt-3">
          <v-btn size="small" variant="text" prepend-icon="mdi-chevron-left" :disabled="!cursor.hasPrevious || listing"
                 data-testid="entries-previous" @click="previousPage">Previous</v-btn>
          <span class="text-caption">Page {{ cursor.page }}<span v-if="totalCount !== null"> · {{ totalCount }} live entries in the archive<span v-if="classification !== ClassificationState.UNSPECIFIED">, before the filter</span></span></span>
          <v-btn size="small" variant="text" append-icon="mdi-chevron-right" :disabled="!cursor.hasNext || listing"
                 data-testid="entries-next" @click="nextPage">Next</v-btn>
        </div>
      </v-card-text>
    </v-card>
  </div>
</template>

<script setup lang="ts">
import { computed, inject, onMounted, reactive, ref, shallowRef, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { create } from '@bufbuild/protobuf'
import RpcFailureAlert from '@/components/repository/RpcFailureAlert.vue'
import { ClassificationState, ClassificationStateSchema } from '@/gen/ai/protomolt/proto/asset/v1/classification_pb'
import { VersioningPolicySchema, type Archive, type ArchiveStats } from '@/gen/ai/protomolt/proto/repo/archive/v1/archive_pb'
import {
  GetArchiveRequestSchema,
  GetArchiveStatsRequestSchema,
  ListEntriesRequestSchema,
} from '@/gen/ai/protomolt/proto/repo/archive/v1/archive_service_pb'
import {
  entryRow,
  planRenditionUpload,
  sendRenditionUpload,
  suggestEntryIdFromFilename,
  type EntryRow,
} from '@/repository/archiveCore'
import { REPOSITORY_UPLOAD_REQUEST, useRepositoryClients } from '@/repository/clients'
import { byteSize, enumLabel, instant } from '@/repository/formatCore'
import { PageCursor } from '@/repository/listingCore'
import { isOwnCancellation, rpcFailure, type RpcFailure } from '@/repository/rpcErrorCore'
import { fileSha256, newOperationId } from '@/repository/uploadCore'
import { useCallScope } from '@/repository/useCallScope'

const ENTRIES_PAGE_SIZE = 50

const clients = useRepositoryClients()
const newRequest = inject(REPOSITORY_UPLOAD_REQUEST, () => new XMLHttpRequest())
const route = useRoute()
const router = useRouter()
const infoScope = useCallScope()
const listScope = useCallScope()
const uploadScope = useCallScope()

function queryText(name: string): string {
  const value = route.query[name]
  return typeof value === 'string' ? value : ''
}
const accountId = computed(() => queryText('account'))
const archiveName = computed(() => queryText('archive'))

const routeError = ref<string | null>(null)
const archive = shallowRef<Archive | null>(null)
const archiveFailure = ref<RpcFailure | null>(null)
const stats = shallowRef<ArchiveStats | null>(null)
const statsFailure = ref<RpcFailure | null>(null)

// Every state the descriptor defines, so a state added to the proto is
// filterable without a console change. UNSPECIFIED means no filter.
const classificationItems = ClassificationStateSchema.values.map((v) => ({
  title: v.number === ClassificationState.UNSPECIFIED ? 'Any classification' : enumLabel(ClassificationStateSchema, v.number),
  value: v.number as ClassificationState,
}))

function parseClassificationQuery(raw: string): ClassificationState {
  const trimmed = raw.trim().toUpperCase()
  if (trimmed === '') return ClassificationState.UNSPECIFIED
  for (const v of ClassificationStateSchema.values) {
    const label = enumLabel(ClassificationStateSchema, v.number).toUpperCase()
    if (label === trimmed || String(v.number) === trimmed) {
      return v.number as ClassificationState
    }
  }
  return ClassificationState.UNSPECIFIED
}

const classification = ref<ClassificationState>(parseClassificationQuery(queryText('classification')))
const entrySearch = ref(queryText('q'))
const cursor = reactive(new PageCursor()) as PageCursor
const entries = shallowRef<EntryRow[]>([])
const filteredEntries = computed(() => {
  const needle = (entrySearch.value ?? '').trim().toLowerCase()
  if (needle === '') return entries.value
  return entries.value.filter(
    (e) =>
      e.entryId.toLowerCase().includes(needle) ||
      e.title.toLowerCase().includes(needle) ||
      e.filename.toLowerCase().includes(needle) ||
      e.contentType.toLowerCase().includes(needle),
  )
})
const totalCount = ref<string | null>(null)
const listing = ref(false)
const listed = ref(false)
const entriesFailure = ref<RpcFailure | null>(null)
// The filter the page tokens were issued for.
let listedClassification = classification.value
let loadedAccount = ''
let loadedArchive = ''

// Upload new archive entry state (#689).
const showNewEntryPanel = ref(false)
const newEntryForm = reactive({
  entryId: '',
  rendition: 'original',
  contentType: '',
  module: 'console',
})
const lastAutoEntryId = ref('')
const newEntryFile = shallowRef<File | null>(null)
const newEntryBusy = ref(false)
const newEntryError = ref<string | null>(null)
const newEntryOperationId = ref<string | null>(null)
const newEntryCommitted = ref<{ entryId: string; version: string; rendition: string } | null>(null)

function pickNewEntryFile(event: Event) {
  const files = (event.target as HTMLInputElement).files
  const picked = files && files.length > 0 ? files[0] : null
  newEntryFile.value = picked
  newEntryError.value = null
  newEntryCommitted.value = null
  if (picked !== null) {
    const currentId = newEntryForm.entryId.trim()
    if (currentId === '' || currentId === lastAutoEntryId.value) {
      const suggested = suggestEntryIdFromFilename(picked.name)
      newEntryForm.entryId = suggested
      lastAutoEntryId.value = suggested
    }
  }
}

async function submitNewEntryUpload() {
  newEntryError.value = null
  newEntryCommitted.value = null
  newEntryOperationId.value = null
  const chosen = newEntryFile.value
  if (chosen === null) {
    newEntryError.value = 'Choose a file to upload as the new archive entry.'
    return
  }
  const entryId = newEntryForm.entryId.trim()
  if (entryId === '') {
    newEntryError.value = 'Entry ID is required.'
    return
  }
  const signal = uploadScope.next()
  newEntryBusy.value = true
  try {
    const opId = newOperationId()
    newEntryOperationId.value = opId
    const sha256 = await fileSha256(chosen, signal)
    if (signal.aborted) return
    const plan = planRenditionUpload(
      {
        accountId: accountId.value,
        archive: archiveName.value,
        entryId,
        rendition: newEntryForm.rendition,
        subKey: '',
        contentType: newEntryForm.contentType,
        module: newEntryForm.module,
        expectedVersion: '',
      },
      chosen,
      opId,
      sha256,
    )
    const outcome = await sendRenditionUpload(plan, newRequest, () => {}, signal)
    if (signal.aborted) return
    if (outcome.kind === 'saved') {
      const committedVersion = outcome.response.version || 1n
      newEntryCommitted.value = {
        entryId,
        version: committedVersion.toString(),
        rendition: newEntryForm.rendition.trim() || 'original',
      }
      void loadInfo()
      void loadEntries()
    } else {
      newEntryError.value = `${outcome.failure.code}: ${outcome.failure.message}`
    }
  } catch (error) {
    if (!signal.aborted) {
      newEntryError.value = error instanceof Error ? error.message : String(error)
    }
  } finally {
    if (!signal.aborted) newEntryBusy.value = false
  }
}

function versioning(value: number) {
  return enumLabel(VersioningPolicySchema, value)
}
function classificationState(value: number) {
  return enumLabel(ClassificationStateSchema, value)
}

function syncArchiveQuery() {
  if (route.name !== 'repository-archive') return
  if (accountId.value.trim() === '' || archiveName.value.trim() === '') return
  const nextQuery: Record<string, string> = {
    account: accountId.value,
    archive: archiveName.value,
  }
  if (classification.value !== ClassificationState.UNSPECIFIED) {
    nextQuery.classification = classificationState(classification.value)
  }
  const q = (entrySearch.value ?? '').trim()
  if (q !== '') {
    nextQuery.q = q
  }
  const currClass = queryText('classification')
  const currQ = queryText('q')
  if ((nextQuery.classification ?? '') === currClass && (nextQuery.q ?? '') === currQ) {
    return
  }
  void router.replace({ name: 'repository-archive', query: nextQuery })
}

function clearArchiveFilters() {
  classification.value = ClassificationState.UNSPECIFIED
  entrySearch.value = ''
}

async function loadInfo() {
  const signal = infoScope.next()
  archive.value = null
  stats.value = null
  archiveFailure.value = null
  statsFailure.value = null
  const target = { accountId: accountId.value, archive: archiveName.value }
  const [info, counts] = await Promise.allSettled([
    clients.archives.getArchive(create(GetArchiveRequestSchema, target), { signal }),
    clients.archives.getArchiveStats(create(GetArchiveStatsRequestSchema, target), { signal }),
  ])
  if (signal.aborted) return
  if (info.status === 'fulfilled') archive.value = info.value.archive ?? null
  else archiveFailure.value = rpcFailure(info.reason)
  if (counts.status === 'fulfilled') stats.value = counts.value.stats ?? null
  else statsFailure.value = rpcFailure(counts.reason)
}

async function loadEntries() {
  const signal = listScope.next()
  listing.value = true
  entriesFailure.value = null
  try {
    const page = await clients.archives.listEntries(
      create(ListEntriesRequestSchema, {
        accountId: accountId.value,
        archive: archiveName.value,
        classificationState: listedClassification,
        limit: ENTRIES_PAGE_SIZE,
        continuationToken: cursor.token,
      }),
      { signal },
    )
    if (signal.aborted) return
    entries.value = page.entries.map(entryRow)
    totalCount.value = page.totalCount.toString()
    cursor.answered(page.nextContinuationToken)
    listed.value = true
  } catch (error) {
    if (isOwnCancellation(error, signal) || signal.aborted) return
    entries.value = []
    entriesFailure.value = rpcFailure(error)
  } finally {
    if (!signal.aborted) listing.value = false
  }
}

function nextPage() {
  cursor.forward()
  void loadEntries()
}
function previousPage() {
  cursor.back()
  void loadEntries()
}

function openEntry(entryId: string) {
  void router.push({ name: 'repository-archive-entry', query: { account: accountId.value, archive: archiveName.value, entry: entryId } })
}

function load() {
  // Another archive starts from nothing: the previous one's rows must not stay clickable.
  listScope.next()
  entries.value = []
  totalCount.value = null
  listed.value = false
  entriesFailure.value = null
  routeError.value = null
  loadedAccount = accountId.value
  loadedArchive = archiveName.value
  if (accountId.value.trim() === '' || archiveName.value.trim() === '') {
    routeError.value = 'An archive needs an account ID and an archive name.'
    return
  }
  classification.value = parseClassificationQuery(queryText('classification'))
  entrySearch.value = queryText('q')
  listedClassification = classification.value
  cursor.reset()
  void loadInfo()
  void loadEntries()
}

watch(classification, (next) => {
  syncArchiveQuery()
  if (next === listedClassification) return
  listedClassification = next
  cursor.reset()
  void loadEntries()
})
watch(entrySearch, () => {
  syncArchiveQuery()
})
watch(
  () => route.fullPath,
  () => {
    if (accountId.value !== loadedAccount || archiveName.value !== loadedArchive) {
      load()
      return
    }
    const routeClass = parseClassificationQuery(queryText('classification'))
    const routeQ = queryText('q')
    if (classification.value !== routeClass) classification.value = routeClass
    if ((entrySearch.value ?? '') !== routeQ) entrySearch.value = routeQ
  },
)
onMounted(load)
</script>
