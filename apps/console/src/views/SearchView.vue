<template>
  <div>
    <v-card variant="flat" border class="mb-4">
      <v-card-title class="text-subtitle-1 d-flex align-center">
        <v-icon size="small" class="mr-2">mdi-magnify</v-icon>
        Search
        <v-spacer />
        <v-chip v-if="profile" size="small" variant="tonal" class="text-mono">
          via {{ profile }}
        </v-chip>
      </v-card-title>
      <v-card-text class="text-body-2 text-medium-emphasis pt-0">
        Query the subjects a search node serves: term matching over the mapping's text
        fields, nearest chunks over its vectors, or both fused.
      </v-card-text>
    </v-card>

    <v-alert v-if="probed && !profile" type="info" variant="tonal" class="mb-4">
      <p class="mb-2">
        No registered service exposes the search contract
        (<code>{{ SEARCH_SERVICE }}</code>) yet.
      </p>
      <p class="mb-0">
        Register the platform node that serves search — its gRPC target is enough —
        and this page finds it by contract, whatever the profile is named.
      </p>
      <template #append>
        <v-btn size="small" variant="tonal" :to="{ name: 'services' }">
          Register a service
        </v-btn>
      </template>
    </v-alert>

    <template v-if="profile">
      <v-card variant="flat" border class="mb-4">
        <v-card-text>
          <v-row dense>
            <v-col cols="12" md="3">
              <v-select v-model="subject" :items="subjectNames" label="Subject"
                        density="compact" hide-details />
            </v-col>
            <v-col cols="12" md="5">
              <v-text-field v-model="query" label="Query" density="compact" hide-details
                            @keydown.enter="run" />
            </v-col>
            <v-col cols="6" md="2">
              <v-select v-model="lane" :items="lanes" item-title="label" item-value="value"
                        label="Lane" density="compact" hide-details />
            </v-col>
            <v-col cols="3" md="1">
              <v-text-field v-model.number="k" label="Hits" type="number" density="compact"
                            hide-details />
            </v-col>
            <v-col cols="3" md="1" class="d-flex align-center">
              <v-btn color="primary" block :loading="searching" :disabled="!subject || !query"
                     @click="run">
                Search
              </v-btn>
            </v-col>
          </v-row>
          <p v-if="subjectInfo && !subjectInfo.hasVectorLane" class="text-caption
             text-medium-emphasis mb-0 mt-2">
            This subject has no vector lane — lexical is the lane that answers.
          </p>
        </v-card-text>
      </v-card>

      <v-alert v-if="error" type="error" variant="tonal" density="compact" class="mb-4">
        {{ error }}
      </v-alert>

      <template v-if="hits !== null">
        <p class="text-caption text-medium-emphasis mb-2">
          {{ hits.length ? `${hits.length} hit${hits.length === 1 ? '' : 's'}, best first`
              : 'Nothing matched — a different lane or broader query may.' }}
        </p>
        <v-card v-for="(hit, i) in hits" :key="`${hit.docId}-${hit.chunkId ?? i}`"
                variant="flat" border class="mb-2">
          <v-card-text class="py-3">
            <div class="d-flex align-center flex-wrap ga-2 mb-1">
              <span class="text-body-2 text-mono font-weight-medium">{{ hit.docId }}</span>
              <v-chip v-if="hit.chunkId" size="x-small" variant="tonal">
                chunk {{ chunkOrdinal(hit.chunkId) }}
              </v-chip>
              <v-spacer />
              <span class="text-caption text-medium-emphasis text-mono">
                score {{ (hit.score ?? 0).toFixed(4) }}
              </span>
            </div>
            <dl class="stored">
              <template v-for="(value, field) in hit.stored ?? {}" :key="field">
                <dt class="text-caption text-medium-emphasis text-mono">{{ field }}</dt>
                <dd class="text-body-2">{{ renderStored(value) }}</dd>
              </template>
            </dl>
          </v-card-text>
        </v-card>
      </template>
    </template>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import {
  SEARCH_SERVICE,
  findSearchProfile,
  listSubjects,
  renderStored,
  search,
  type SearchHit,
  type SearchLane,
  type SearchSubject,
} from '../services/search'

const profile = ref<string | null>(null)
const probed = ref(false)
const subjects = ref<SearchSubject[]>([])
const subject = ref<string | null>(null)
const query = ref('')
const k = ref(10)
const lane = ref<SearchLane>('SEARCH_LANE_LEXICAL')
const searching = ref(false)
const error = ref('')
const hits = ref<SearchHit[] | null>(null)

const lanes = [
  { label: 'Lexical', value: 'SEARCH_LANE_LEXICAL' },
  { label: 'Vector', value: 'SEARCH_LANE_VECTOR' },
  { label: 'Hybrid', value: 'SEARCH_LANE_HYBRID' },
]

const subjectNames = computed(() => subjects.value.map((s) => s.subject))
const subjectInfo = computed(() =>
  subjects.value.find((s) => s.subject === subject.value) ?? null)

onMounted(async () => {
  try {
    profile.value = await findSearchProfile()
    if (profile.value) {
      subjects.value = await listSubjects(profile.value)
      subject.value = subjects.value[0]?.subject ?? null
    }
  } catch (e) {
    error.value = (e as Error).message
  } finally {
    probed.value = true
  }
})

async function run() {
  if (!profile.value || !subject.value || !query.value) return
  searching.value = true
  error.value = ''
  try {
    hits.value = await search(profile.value, {
      mappingSubject: subject.value,
      query: query.value,
      k: k.value,
      lane: lane.value,
    })
  } catch (e) {
    hits.value = null
    error.value = (e as Error).message
  } finally {
    searching.value = false
  }
}

/** "<doc>#<digest>#<ordinal>" — the ordinal is the part a person can use. */
function chunkOrdinal(chunkId: string): string {
  const at = chunkId.lastIndexOf('#')
  return at >= 0 ? chunkId.slice(at + 1) : chunkId
}
</script>

<style scoped>
.stored {
  display: grid;
  grid-template-columns: minmax(120px, max-content) 1fr;
  column-gap: 16px;
  row-gap: 2px;
  align-items: baseline;
}
.stored dd {
  margin: 0;
  overflow-wrap: anywhere;
}
</style>
