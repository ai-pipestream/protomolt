<template>
  <div>
    <div class="d-flex align-center flex-wrap ga-3 mb-4">
      <v-select
        v-model="leftVersion"
        :items="versionItems"
        label="From version"
        density="compact"
        variant="outlined"
        hide-details
        style="max-width: 180px"
      />
      <v-icon class="text-medium-emphasis">mdi-arrow-right</v-icon>
      <v-select
        v-model="rightVersion"
        :items="versionItems"
        label="To version"
        density="compact"
        variant="outlined"
        hide-details
        style="max-width: 180px"
      />
      <v-btn
        variant="tonal"
        color="primary"
        size="small"
        prepend-icon="mdi-swap-horizontal"
        @click="swap"
      >
        Swap
      </v-btn>
      <v-spacer />
      <template v-if="diff && !loading">
        <v-chip size="small" color="success" variant="tonal">+{{ stats.added }}</v-chip>
        <v-chip size="small" color="error" variant="tonal">−{{ stats.removed }}</v-chip>
      </template>
    </div>

    <v-alert v-if="error" type="error" variant="tonal" density="compact" class="mb-4">
      {{ error }}
      <template #append>
        <v-btn size="small" variant="text" @click="compute">Retry</v-btn>
      </template>
    </v-alert>

    <v-skeleton-loader v-else-if="loading" type="paragraph@3" />

    <v-alert
      v-else-if="diff && !stats.added && !stats.removed"
      type="success"
      variant="tonal"
      density="compact"
    >
      v{{ leftVersion }} and v{{ rightVersion }} have identical schema text.
    </v-alert>

    <div v-else-if="diff" class="diff-view">
      <table class="diff-table">
        <tbody>
          <tr v-for="(line, i) in diff" :key="i" :class="`diff-${line.op}`">
            <td class="diff-gutter">{{ line.left ?? '' }}</td>
            <td class="diff-gutter">{{ line.right ?? '' }}</td>
            <td class="diff-sign">{{ line.op === 'add' ? '+' : line.op === 'del' ? '−' : '' }}</td>
            <td class="diff-text">{{ line.text || ' ' }}</td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { errorMessage, registryApi } from '../services/api'
import { diffLines, diffStats, type DiffLine } from '../services/textDiff'

const props = defineProps<{
  subject: string
  versions: number[]
}>()

const leftVersion = ref<number | null>(null)
const rightVersion = ref<number | null>(null)
const diff = ref<DiffLine[] | null>(null)
const loading = ref(false)
const error = ref('')

const schemaCache = new Map<number, string>()

const versionItems = computed(() =>
  [...props.versions].reverse().map((v) => ({ title: `v${v}`, value: v })),
)

const stats = computed(() => diffStats(diff.value ?? []))

async function schemaFor(version: number): Promise<string> {
  const cached = schemaCache.get(version)
  if (cached !== undefined) return cached
  const envelope = await registryApi.getVersion(props.subject, version)
  schemaCache.set(version, envelope.schema)
  return envelope.schema
}

async function compute() {
  if (leftVersion.value == null || rightVersion.value == null) return
  loading.value = true
  error.value = ''
  try {
    const [oldText, newText] = await Promise.all([
      schemaFor(leftVersion.value),
      schemaFor(rightVersion.value),
    ])
    diff.value = diffLines(oldText, newText)
  } catch (e) {
    error.value = errorMessage(e)
    diff.value = null
  } finally {
    loading.value = false
  }
}

function swap() {
  const l = leftVersion.value
  leftVersion.value = rightVersion.value
  rightVersion.value = l
}

watch(
  () => [props.subject, props.versions] as const,
  () => {
    schemaCache.clear()
    const versions = props.versions
    if (versions.length >= 2) {
      leftVersion.value = versions[versions.length - 2]
      rightVersion.value = versions[versions.length - 1]
    } else if (versions.length === 1) {
      leftVersion.value = versions[0]
      rightVersion.value = versions[0]
    }
  },
  { immediate: true },
)

watch([leftVersion, rightVersion], compute, { immediate: true })
</script>

<style scoped>
.diff-view {
  overflow-x: auto;
  border: 1px solid rgba(var(--v-theme-on-surface), 0.12);
  border-radius: 6px;
}

.diff-table {
  border-collapse: collapse;
  width: 100%;
  font-family: ui-monospace, 'SF Mono', 'JetBrains Mono', Menlo, Consolas, monospace;
  font-size: 12.5px;
  line-height: 1.55;
}

.diff-gutter {
  user-select: none;
  text-align: right;
  padding: 0 8px;
  width: 1%;
  min-width: 34px;
  color: rgba(var(--v-theme-on-surface), 0.35);
}

.diff-sign {
  user-select: none;
  width: 1%;
  padding: 0 4px;
  text-align: center;
  font-weight: 700;
}

.diff-text {
  white-space: pre;
  padding: 0 16px 0 6px;
  width: 99%;
}

.diff-add {
  background: rgba(var(--v-theme-success), 0.12);
}

.diff-add .diff-sign {
  color: rgb(var(--v-theme-success));
}

.diff-del {
  background: rgba(var(--v-theme-error), 0.12);
}

.diff-del .diff-sign {
  color: rgb(var(--v-theme-error));
}
</style>
