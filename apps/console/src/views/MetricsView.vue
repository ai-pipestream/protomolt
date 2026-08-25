<template>
  <div>
    <v-card variant="flat" border class="mb-4">
      <v-card-title class="text-subtitle-1 d-flex align-center">
        <v-icon size="small" class="mr-2">mdi-chart-box-outline</v-icon>
        Metrics
        <v-spacer />
        <v-chip v-if="profile" size="small" variant="tonal" class="text-mono">
          via {{ profile }}
        </v-chip>
      </v-card-title>
      <v-card-text class="text-body-2 text-medium-emphasis pt-0">
        Measures over a subject's indexed rows, grouped by its dimensions. The mapping
        declares what is queryable; describing a subject fills the pickers from it.
      </v-card-text>
    </v-card>

    <v-alert v-if="probed && !profile" type="info" variant="tonal" class="mb-4">
      <p class="mb-2">
        No registered service exposes the metric contract
        (<code>{{ METRIC_SERVICE }}</code>) yet.
      </p>
      <p class="mb-0">
        Register the platform's metrics node — its gRPC target is enough — and this
        page finds it by contract, whatever the profile is named.
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
            <v-col cols="12" md="4">
              <v-text-field v-model="subject" label="Mapping subject" density="compact"
                            hide-details placeholder="orders-value"
                            @keydown.enter="describe" />
            </v-col>
            <v-col cols="12" md="2" class="d-flex align-center">
              <v-btn variant="tonal" block :loading="describing" :disabled="!subject"
                     @click="describe">
                Describe
              </v-btn>
            </v-col>
          </v-row>
          <v-row v-if="mapping" dense class="mt-1">
            <v-col cols="12" md="4">
              <v-select v-model="chosenMeasures" :items="measureNames" label="Measures"
                        multiple chips closable-chips density="compact" hide-details />
            </v-col>
            <v-col cols="12" md="4">
              <v-select v-model="chosenDimensions" :items="dimensionNames" label="Group by"
                        multiple chips closable-chips density="compact" hide-details />
            </v-col>
            <v-col cols="6" md="2">
              <v-text-field v-model.number="limit" label="Row limit" type="number"
                            density="compact" hide-details />
            </v-col>
            <v-col cols="6" md="2" class="d-flex align-center">
              <v-btn color="primary" block :loading="querying" :disabled="!chosenMeasures.length"
                     @click="run">
                Query
              </v-btn>
            </v-col>
          </v-row>
        </v-card-text>
      </v-card>

      <v-alert v-if="error" type="error" variant="tonal" density="compact" class="mb-4">
        {{ error }}
      </v-alert>

      <template v-if="result">
        <p class="text-caption text-medium-emphasis mb-2">
          {{ rowCaption }}
        </p>
        <v-card v-if="result.rows.length" variant="flat" border class="mb-2">
          <v-table density="comfortable" class="metric-table">
            <thead>
              <tr>
                <th v-for="dimension in result.dimensions" :key="dimension"
                    class="text-caption">{{ dimension }}</th>
                <th v-for="measure in result.measures" :key="measure"
                    class="text-caption text-right">{{ measure }}</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="(row, i) in result.rows" :key="i">
                <td v-for="dimension in result.dimensions" :key="dimension" class="text-mono">
                  {{ row.dimensions?.[dimension] ?? '' }}
                </td>
                <td v-for="measure in result.measures" :key="measure" class="measure-cell">
                  <div class="measure">
                    <span class="value tabular">{{
                      renderMeasure(row.measures?.[measure] ?? 0) }}</span>
                    <span class="bar" :style="{ width: barWidth(measure, row) }"
                          aria-hidden="true" />
                  </div>
                </td>
              </tr>
            </tbody>
          </v-table>
        </v-card>
        <details v-if="result.physicalPlan" class="text-caption text-medium-emphasis mb-4">
          <summary>How the engine ran it</summary>
          <pre class="plan">{{ result.physicalPlan }}</pre>
        </details>
      </template>
    </template>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref, shallowRef } from 'vue'
import { errorMessage } from '../services/api'
import {
  METRIC_SERVICE,
  describeMapping,
  dimensions,
  findMetricsProfile,
  measures,
  queryMetrics,
  renderMeasure,
  type MetricMapping,
  type MetricRow,
  type MetricsResult,
} from '../services/metrics'

const profile = ref<string | null>(null)
const probed = ref(false)
const subject = ref('')
const mapping = shallowRef<MetricMapping | null>(null)
const chosenMeasures = ref<string[]>([])
const chosenDimensions = ref<string[]>([])
const limit = ref(100)
const describing = ref(false)
const querying = ref(false)
const error = ref('')
// The rendered table's columns are the QUERIED members, frozen with the rows:
// editing the pickers must not grow columns the engine never answered.
const result = shallowRef<(MetricsResult
    & { measures: string[]; dimensions: string[] }) | null>(null)

const measureNames = computed(() =>
  mapping.value ? measures(mapping.value).map((m) => m.name) : [])
const dimensionNames = computed(() =>
  mapping.value ? dimensions(mapping.value).map((m) => m.name) : [])
const rowCaption = computed(() => {
  const count = result.value?.rows.length ?? 0
  return count ? `${count} row${count === 1 ? '' : 's'}`
      : 'No rows matched — the subject may be empty or entirely filtered.'
})

onMounted(async () => {
  try {
    profile.value = await findMetricsProfile()
  } catch (e) {
    error.value = errorMessage(e)
  } finally {
    probed.value = true
  }
})

async function describe() {
  if (!profile.value || !subject.value) return
  describing.value = true
  error.value = ''
  result.value = null
  try {
    mapping.value = await describeMapping(profile.value, subject.value)
    // Start with the first measure chosen: one click from a described subject
    // to a first answer.
    chosenMeasures.value = measureNames.value.slice(0, 1)
    chosenDimensions.value = []
  } catch (e) {
    mapping.value = null
    error.value = errorMessage(e)
  } finally {
    describing.value = false
  }
}

async function run() {
  if (!profile.value || !subject.value) return
  querying.value = true
  error.value = ''
  try {
    const queried = await queryMetrics(profile.value, {
      mappingSubject: subject.value,
      measures: chosenMeasures.value,
      dimensions: chosenDimensions.value,
      limit: limit.value,
    })
    result.value = {
      ...queried,
      measures: [...chosenMeasures.value],
      dimensions: [...chosenDimensions.value],
    }
  } catch (e) {
    result.value = null
    error.value = errorMessage(e)
  } finally {
    querying.value = false
  }
}

/** Data-bar width, proportional to the column's largest magnitude. */
function barWidth(measure: string, row: MetricRow): string {
  const rows = result.value?.rows ?? []
  const largest = Math.max(...rows.map((r) => Math.abs(r.measures?.[measure] ?? 0)))
  if (!largest) return '0%'
  const share = Math.abs(row.measures?.[measure] ?? 0) / largest
  return `${(share * 100).toFixed(1)}%`
}
</script>

<style scoped>
/* One measure, one hue: the value is text in text ink; the bar behind it is
   redundant magnitude, so it never has to pass a text-contrast bar. */
.measure-cell {
  min-width: 160px;
}
.measure {
  display: grid;
  grid-template-columns: 1fr;
  align-items: center;
  justify-items: end;
  gap: 2px;
}
.measure .value {
  font-family: var(--molt-mono);
  font-size: 0.85rem;
}
.tabular {
  font-variant-numeric: tabular-nums;
}
.measure .bar {
  justify-self: end;
  height: 4px;
  border-radius: 2px;
  background: rgb(var(--v-theme-primary));
  opacity: 0.55;
}
.plan {
  font-family: var(--molt-mono);
  font-size: 0.75rem;
  white-space: pre-wrap;
  overflow-wrap: anywhere;
  margin-top: 4px;
}
</style>
