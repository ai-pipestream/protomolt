<template>
  <v-container fluid class="pa-6">
    <!-- Header -->
    <div class="d-flex align-center flex-wrap ga-2 mb-4">
      <v-btn
        icon="mdi-arrow-left"
        variant="text"
        size="small"
        aria-label="Back to subjects"
        :to="{ name: 'schema-registry-subjects' }"
      />
      <v-icon color="primary">mdi-file-code-outline</v-icon>
      <h2 class="text-h6 font-mono">{{ subject }}</h2>
      <CompatibilityBadge
        v-if="effectiveMode"
        :mode="effectiveMode"
        :subject="subject"
        :inherited="modeInherited"
        @changed="onModeChanged"
      />
      <v-spacer />
      <v-btn
        icon="mdi-refresh"
        variant="text"
        size="small"
        :loading="loading"
        aria-label="Reload subject"
        @click="load"
      />
    </div>

    <!-- Subject-level error envelope -->
    <v-alert v-if="error" type="error" variant="tonal" density="compact" class="mb-4">
      <div class="font-weight-medium">Could not load subject</div>
      <div class="text-caption">{{ error }}</div>
      <template #append>
        <v-btn size="small" variant="text" @click="load">Retry</v-btn>
      </template>
    </v-alert>

    <v-row v-else>
      <!-- Version timeline -->
      <v-col cols="12" md="3" lg="2">
        <v-card variant="flat" border>
          <v-card-title class="text-subtitle-2">
            <v-icon size="small" class="mr-1">mdi-history</v-icon>
            Versions
          </v-card-title>
          <v-skeleton-loader v-if="loading && !versions.length" type="list-item@4" />
          <v-list v-else density="compact" nav>
            <v-list-item
              v-for="version in versionsDesc"
              :key="version"
              :active="version === selectedVersion"
              rounded
              @click="selectVersion(version)"
            >
              <template #prepend>
                <v-icon size="small">
                  {{ version === latestVersion ? 'mdi-star-circle-outline' : 'mdi-circle-small' }}
                </v-icon>
              </template>
              <v-list-item-title>
                v{{ version }}
                <span v-if="version === latestVersion" class="text-caption text-medium-emphasis">
                  latest
                </span>
              </v-list-item-title>
            </v-list-item>
          </v-list>
        </v-card>
      </v-col>

      <!-- Tabs -->
      <v-col cols="12" md="9" lg="10">
        <v-card variant="flat" border>
          <v-tabs v-model="tab" density="compact" color="primary">
            <v-tab value="schema" prepend-icon="mdi-code-braces">Schema</v-tab>
            <v-tab value="diff" prepend-icon="mdi-vector-difference" :disabled="versions.length < 2">
              Diff
            </v-tab>
            <v-tab value="types" prepend-icon="mdi-file-tree">Types</v-tab>
            <v-tab value="try" prepend-icon="mdi-play-box-outline">Try it</v-tab>
            <v-tab value="compat" prepend-icon="mdi-shield-sync-outline">Compatibility</v-tab>
          </v-tabs>
          <v-divider />

          <v-window v-model="tab" class="pa-4">
            <!-- Schema -->
            <v-window-item value="schema">
              <v-skeleton-loader v-if="envelopeLoading" type="paragraph@3" />
              <template v-else-if="envelope">
                <div class="d-flex align-center flex-wrap ga-2 mb-3">
                  <v-chip size="small" variant="tonal">v{{ envelope.version }}</v-chip>
                  <v-chip size="small" variant="tonal">global id {{ envelope.id }}</v-chip>
                  <v-chip size="small" variant="tonal">{{ envelope.schemaType }}</v-chip>
                  <v-spacer />
                  <v-btn
                    size="small"
                    variant="text"
                    prepend-icon="mdi-content-copy"
                    @click="copySchema"
                  >
                    Copy
                  </v-btn>
                </div>

                <template v-if="envelope.references.length">
                  <div class="text-caption text-medium-emphasis mb-1">References</div>
                  <div class="d-flex flex-wrap ga-2 mb-3">
                    <v-chip
                      v-for="reference in envelope.references"
                      :key="`${reference.subject}@${reference.version}`"
                      size="small"
                      variant="outlined"
                      color="primary"
                      :to="subjectRoute(reference.subject, reference.version)"
                    >
                      <v-icon start size="x-small">mdi-link-variant</v-icon>
                      {{ reference.name }} → {{ reference.subject }} v{{ reference.version }}
                    </v-chip>
                  </div>
                </template>

                <ProtoSource :code="envelope.schema" />
              </template>
              <v-alert v-else-if="envelopeError" type="error" variant="tonal" density="compact">
                {{ envelopeError }}
              </v-alert>
            </v-window-item>

            <!-- Diff -->
            <v-window-item value="diff">
              <VersionDiffPanel :subject="subject" :versions="versions" />
            </v-window-item>

            <!-- Type explorer -->
            <v-window-item value="types">
              <TypeExplorerPanel
                :model="descriptorModel"
                :loading="descriptorLoading"
                :error="descriptorError"
                @reload="loadDescriptor(true)"
              />
            </v-window-item>

            <!-- Try-it composer -->
            <v-window-item value="try">
              <TryItPanel
                :model="descriptorModel"
                :loading="descriptorLoading"
                :error="descriptorError"
                @reload="loadDescriptor(true)"
              />
            </v-window-item>

            <!-- Compatibility check -->
            <v-window-item value="compat">
              <CompatCheckPanel
                :subject="subject"
                :latest-schema="latestEnvelope?.schema ?? ''"
                :latest-references="latestEnvelope?.references ?? []"
                :effective-mode="effectiveMode"
                :mode-inherited="modeInherited"
                @mode-changed="onModeChanged"
                @registered="load"
              />
            </v-window-item>
          </v-window>
        </v-card>
      </v-col>
    </v-row>
  </v-container>
</template>

<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import CompatCheckPanel from '../components/CompatCheckPanel.vue'
import CompatibilityBadge from '../components/CompatibilityBadge.vue'
import ProtoSource from '../components/ProtoSource.vue'
import TryItPanel from '../components/TryItPanel.vue'
import TypeExplorerPanel from '../components/TypeExplorerPanel.vue'
import VersionDiffPanel from '../components/VersionDiffPanel.vue'
import { toast } from '@/composables/useToast'
import {
  errorMessage,
  registryApi,
  RegistryError,
  type CompatibilityMode,
  type SchemaVersion,
} from '../services/api'
import { buildDescriptorModel, type DescriptorModel } from '../services/descriptorModel'
import { subjectRoute } from '../services/routes'

const route = useRoute()
const router = useRouter()

const subject = computed(() => String(route.params.subject ?? ''))

const versions = ref<number[]>([])
const selectedVersion = ref<number | null>(null)
const envelope = ref<SchemaVersion | null>(null)
const latestEnvelope = ref<SchemaVersion | null>(null)
const loading = ref(false)
const error = ref('')
const envelopeLoading = ref(false)
const envelopeError = ref('')

const effectiveMode = ref<CompatibilityMode | null>(null)
const modeInherited = ref(true)

// The active tab lives in the URL (?tab=) so views are deep-linkable.
const TABS = ['schema', 'diff', 'types', 'try', 'compat']
const tab = ref(TABS.includes(String(route.query.tab)) ? String(route.query.tab) : 'schema')
watch(tab, (t) => {
  router.replace({ query: { ...route.query, tab: t === 'schema' ? undefined : t } })
})

const descriptorModel = ref<DescriptorModel | null>(null)
const descriptorLoading = ref(false)
const descriptorError = ref('')

const versionsDesc = computed(() => [...versions.value].reverse())
const latestVersion = computed(() =>
  versions.value.length ? versions.value[versions.value.length - 1] : null,
)

async function load() {
  loading.value = true
  error.value = ''
  try {
    versions.value = await registryApi.listVersions(subject.value)
    const queryVersion = Number(route.query.v)
    const wanted =
      Number.isInteger(queryVersion) && versions.value.includes(queryVersion)
        ? queryVersion
        : latestVersion.value
    await Promise.all([selectVersion(wanted, false), loadConfig(), loadLatestEnvelope()])
    // A registration invalidates the cached descriptor set.
    descriptorModel.value = null
    if (tab.value === 'types' || tab.value === 'try') await loadDescriptor(true)
  } catch (e) {
    error.value = errorMessage(e)
    versions.value = []
  } finally {
    loading.value = false
  }
}

async function selectVersion(version: number | null, updateQuery = true) {
  if (version == null) return
  selectedVersion.value = version
  if (updateQuery) {
    router.replace({ query: { ...route.query, v: String(version) } })
  }
  envelopeLoading.value = true
  envelopeError.value = ''
  try {
    envelope.value = await registryApi.getVersion(subject.value, version)
  } catch (e) {
    envelope.value = null
    envelopeError.value = errorMessage(e)
  } finally {
    envelopeLoading.value = false
  }
}

async function loadLatestEnvelope() {
  try {
    latestEnvelope.value = await registryApi.getVersion(subject.value, 'latest')
  } catch {
    latestEnvelope.value = null
  }
}

async function loadConfig() {
  try {
    const subjectMode = await registryApi.subjectConfig(subject.value)
    if (subjectMode) {
      effectiveMode.value = subjectMode
      modeInherited.value = false
    } else {
      effectiveMode.value = await registryApi.globalConfig()
      modeInherited.value = true
    }
  } catch (e) {
    // Config is decoration on this page — don't fail the whole view.
    if (!(e instanceof RegistryError)) throw e
  }
}

function onModeChanged(mode: CompatibilityMode) {
  effectiveMode.value = mode
  modeInherited.value = false
}

async function loadDescriptor(force = false) {
  if (descriptorModel.value && !force) return
  descriptorLoading.value = true
  descriptorError.value = ''
  try {
    const bytes = await registryApi.descriptorSet(subject.value)
    descriptorModel.value = buildDescriptorModel(bytes)
  } catch (e) {
    descriptorModel.value = null
    descriptorError.value = errorMessage(e)
  } finally {
    descriptorLoading.value = false
  }
}

async function copySchema() {
  if (!envelope.value) return
  try {
    await navigator.clipboard.writeText(envelope.value.schema)
    toast.success('Schema copied')
  } catch {
    toast.error('Clipboard unavailable')
  }
}

// Descriptor sets load lazily, first time a descriptor-driven tab opens.
watch(tab, (t) => {
  if (t === 'types' || t === 'try') loadDescriptor()
})

watch(subject, () => {
  tab.value = 'schema'
  descriptorModel.value = null
  descriptorError.value = ''
  envelope.value = null
  load()
})

load()
</script>

<style scoped>
.font-mono {
  font-family: ui-monospace, 'SF Mono', 'JetBrains Mono', Menlo, Consolas, monospace;
}
</style>
