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
          <thead><tr><th>Rendition</th><th>Objects</th><th>Bytes</th></tr></thead>
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
      <v-card-title class="text-subtitle-2 d-flex align-center ga-2">
        Entries
        <v-spacer />
        <v-select v-model="classification" :items="classificationItems" label="Classification" density="compact" hide-details
                  style="max-width: 240px" data-testid="classification-filter" />
      </v-card-title>
      <v-card-text>
        <RpcFailureAlert v-if="entriesFailure" :failure="entriesFailure" title="Listing entries" />
        <v-table v-if="entries.length > 0" density="compact" hover data-testid="entry-table">
          <thead>
            <tr><th>Entry</th><th>Title</th><th>File</th><th>Type</th><th>Version</th><th>Classification</th><th>Updated</th></tr>
          </thead>
          <tbody>
            <tr v-for="e in entries" :key="e.entryId" class="cursor-pointer" :data-testid="`entry-${e.entryId}`" @click="openEntry(e.entryId)">
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
          <span class="text-caption">Page {{ cursor.page }}<span v-if="totalCount !== null"> · {{ totalCount }} entries</span></span>
          <v-btn size="small" variant="text" append-icon="mdi-chevron-right" :disabled="!cursor.hasNext || listing"
                 data-testid="entries-next" @click="nextPage">Next</v-btn>
        </div>
      </v-card-text>
    </v-card>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, ref, shallowRef, watch } from 'vue'
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
import { entryRow, type EntryRow } from '@/repository/archiveCore'
import { useRepositoryClients } from '@/repository/clients'
import { byteSize, enumLabel, instant } from '@/repository/formatCore'
import { PageCursor } from '@/repository/listingCore'
import { isOwnCancellation, rpcFailure, type RpcFailure } from '@/repository/rpcErrorCore'
import { useCallScope } from '@/repository/useCallScope'

const ENTRIES_PAGE_SIZE = 50

const clients = useRepositoryClients()
const route = useRoute()
const router = useRouter()
const infoScope = useCallScope()
const listScope = useCallScope()

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

const classificationItems = [
  { title: 'Any classification', value: ClassificationState.UNSPECIFIED },
  { title: 'Unclassified', value: ClassificationState.UNCLASSIFIED },
  { title: 'Declared', value: ClassificationState.DECLARED },
  { title: 'Identified', value: ClassificationState.IDENTIFIED },
]
const classification = ref<ClassificationState>(ClassificationState.UNSPECIFIED)
const cursor = reactive(new PageCursor()) as PageCursor
const entries = shallowRef<EntryRow[]>([])
const totalCount = ref<string | null>(null)
const listing = ref(false)
const listed = ref(false)
const entriesFailure = ref<RpcFailure | null>(null)
// The filter the page tokens were issued for.
let listedClassification = classification.value

function versioning(value: number) {
  return enumLabel(VersioningPolicySchema, value)
}
function classificationState(value: number) {
  return enumLabel(ClassificationStateSchema, value)
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
  routeError.value = null
  if (accountId.value.trim() === '' || archiveName.value.trim() === '') {
    routeError.value = 'An archive needs an account ID and an archive name.'
    return
  }
  listedClassification = classification.value
  cursor.reset()
  void loadInfo()
  void loadEntries()
}

watch(classification, (next) => {
  listedClassification = next
  cursor.reset()
  void loadEntries()
})
watch(() => route.fullPath, load)
onMounted(load)
</script>
