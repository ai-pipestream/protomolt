<template>
  <v-container fluid class="pa-6">
    <RegistryHeaderBar @global-mode="globalMode = $event" />

    <v-card variant="flat" border>
      <v-card-title class="text-subtitle-1 d-flex align-center">
        <v-icon size="small" class="mr-2">mdi-format-list-bulleted</v-icon>
        Subjects
        <v-chip v-if="!loading && !error" size="x-small" variant="tonal" class="ml-2">
          {{ filtered.length }}<template v-if="filter"> / {{ rows.length }}</template>
        </v-chip>
        <v-spacer />
        <v-text-field
          ref="searchField"
          v-model="filter"
          label="Search subjects  ( / )"
          prepend-inner-icon="mdi-magnify"
          density="compact"
          variant="outlined"
          hide-details
          clearable
          style="max-width: 320px"
          @keydown.escape="filter = ''"
        />
        <v-btn
          icon="mdi-refresh"
          variant="text"
          size="small"
          class="ml-2"
          :loading="loading"
          aria-label="Reload subjects"
          @click="load"
        />
      </v-card-title>

      <!-- Error envelope, rendered meaningfully -->
      <v-alert v-if="error" type="error" variant="tonal" density="compact" class="ma-4">
        <div class="font-weight-medium">Could not load subjects</div>
        <div class="text-caption">{{ error }}</div>
        <template #append>
          <v-btn size="small" variant="text" @click="load">Retry</v-btn>
        </template>
      </v-alert>

      <!-- First-load skeleton -->
      <div v-else-if="loading && !rows.length" class="pa-4">
        <v-skeleton-loader type="table-row@6" />
      </div>

      <!-- Empty registry -->
      <v-empty-state
        v-else-if="!rows.length"
        icon="mdi-file-code-outline"
        title="No subjects yet"
        text="Nothing has been registered. Publish a schema (or use the Compatibility panel of any subject) and it will appear here."
      />

      <!-- No match -->
      <v-empty-state
        v-else-if="!filtered.length"
        icon="mdi-magnify-remove-outline"
        :title="`No subjects match “${filter}”`"
      >
        <template #actions>
          <v-btn variant="tonal" size="small" @click="filter = ''">Clear search</v-btn>
        </template>
      </v-empty-state>

      <v-table v-else density="comfortable" hover>
        <thead>
          <tr>
            <th>Subject</th>
            <th class="text-right">Versions</th>
            <th class="text-right">Latest</th>
            <th class="text-right" style="width: 1%"></th>
          </tr>
        </thead>
        <tbody>
          <tr
            v-for="row in filtered"
            :key="row.subject"
            class="subject-row"
            @click="open(row.subject)"
          >
            <td>
              <router-link
                class="subject-link"
                :to="subjectRoute(row.subject)"
                @click.stop
              >
                <v-icon size="x-small" class="mr-1 text-medium-emphasis">mdi-file-code-outline</v-icon>
                <span class="font-mono">{{ row.subject }}</span>
              </router-link>
            </td>
            <td class="text-right">
              <v-skeleton-loader v-if="row.versionCount === null" type="text" width="40" class="d-inline-block" />
              <v-chip v-else size="x-small" variant="tonal">{{ row.versionCount }}</v-chip>
            </td>
            <td class="text-right text-medium-emphasis">
              <template v-if="row.latestVersion !== null">v{{ row.latestVersion }}</template>
            </td>
            <td class="text-right">
              <v-icon size="small" class="text-medium-emphasis">mdi-chevron-right</v-icon>
            </td>
          </tr>
        </tbody>
      </v-table>
    </v-card>
  </v-container>
</template>

<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref } from 'vue'
import { useRouter } from 'vue-router'
import RegistryHeaderBar from '../components/RegistryHeaderBar.vue'
import { errorMessage, registryApi, type CompatibilityMode } from '../services/api'
import { subjectRoute } from '../services/routes'

interface SubjectRow {
  subject: string
  versionCount: number | null
  latestVersion: number | null
}

const router = useRouter()
const rows = ref<SubjectRow[]>([])
const filter = ref('')
const loading = ref(false)
const error = ref('')
const globalMode = ref<CompatibilityMode | null>(null)
const searchField = ref<{ focus?: () => void } | null>(null)

const filtered = computed(() => {
  const q = (filter.value ?? '').trim().toLowerCase()
  if (!q) return rows.value
  return rows.value.filter((r) => r.subject.toLowerCase().includes(q))
})

async function load() {
  loading.value = true
  error.value = ''
  try {
    const subjects = await registryApi.listSubjects()
    rows.value = subjects.map((subject) => ({ subject, versionCount: null, latestVersion: null }))
    // Version counts fan out in parallel; a failing subject just keeps its
    // placeholder rather than failing the whole list.
    await Promise.all(
      rows.value.map(async (row) => {
        try {
          const versions = await registryApi.listVersions(row.subject)
          row.versionCount = versions.length
          row.latestVersion = versions.length ? versions[versions.length - 1] : null
        } catch {
          /* row stays unknown */
        }
      }),
    )
  } catch (e) {
    error.value = errorMessage(e)
  } finally {
    loading.value = false
  }
}

function open(subject: string) {
  router.push(subjectRoute(subject))
}

// "/" focuses search from anywhere on the page (unless already typing).
function onKeydown(e: KeyboardEvent) {
  if (e.key !== '/' || e.ctrlKey || e.metaKey || e.altKey) return
  const target = e.target as HTMLElement | null
  if (target && ['INPUT', 'TEXTAREA', 'SELECT'].includes(target.tagName)) return
  e.preventDefault()
  searchField.value?.focus?.()
}

onMounted(() => {
  load()
  window.addEventListener('keydown', onKeydown)
})
onUnmounted(() => window.removeEventListener('keydown', onKeydown))
</script>

<style scoped>
.subject-row {
  cursor: pointer;
}

.subject-link {
  color: rgb(var(--v-theme-primary));
  text-decoration: none;
}

.subject-link:hover {
  text-decoration: underline;
}

.font-mono {
  font-family: ui-monospace, 'SF Mono', 'JetBrains Mono', Menlo, Consolas, monospace;
  font-size: 13px;
}
</style>
