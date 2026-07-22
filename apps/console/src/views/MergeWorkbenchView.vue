<template>
  <v-container fluid class="pa-6">
    <v-card variant="flat" border class="mb-4">
      <v-card-title class="text-subtitle-1 d-flex align-center">
        <v-btn icon="mdi-arrow-left" variant="text" size="small" class="mr-2"
               :to="{ name: 'schema-registry-subjects' }" aria-label="Back to subjects" />
        <v-icon size="small" class="mr-2">mdi-set-merge</v-icon>
        Merge schemas
        <v-spacer />
        <span class="text-caption text-medium-emphasis">
          validate &rarr; resolve &rarr; emit
        </span>
      </v-card-title>
      <v-card-text class="text-body-2 text-medium-emphasis pt-0">
        Pick two message types; the clash report is computed from the descriptors alone.
        Decide each clash, and one move emits the merged schema with its join and union
        mappings — registrable like any hand-written proto.
      </v-card-text>
    </v-card>

    <!-- Sources -->
    <v-row dense>
      <v-col v-for="(source, i) in sources" :key="i" cols="12" md="6">
        <v-card variant="flat" border>
          <v-card-title class="text-subtitle-2">Source {{ i + 1 }}</v-card-title>
          <v-card-text>
            <v-select
              v-model="source.subject"
              :items="subjects"
              label="Subject"
              density="compact"
              variant="outlined"
              @update:model-value="onSubjectPicked(i)"
            />
            <v-select
              v-model="source.type"
              :items="source.types"
              label="Message type"
              density="compact"
              variant="outlined"
              :disabled="!source.subject"
              @update:model-value="onTypePicked(i)"
            />
            <v-text-field
              v-model="source.name"
              label="Scope name (prefixes renames)"
              density="compact"
              variant="outlined"
              :disabled="!source.type"
              @update:model-value="analyze"
            />
          </v-card-text>
        </v-card>
      </v-col>
    </v-row>

    <v-text-field
      v-model="mergedName"
      label="Merged type name"
      density="compact"
      variant="outlined"
      class="mt-2"
      style="max-width: 480px"
      :disabled="!ready"
      @update:model-value="analyze"
    />

    <v-alert v-if="error" type="error" variant="tonal" density="compact" class="mb-4">
      {{ error }}
    </v-alert>

    <!-- Clash report -->
    <v-card v-if="report" variant="flat" border class="mb-4">
      <v-card-title class="text-subtitle-2 d-flex align-center">
        <v-icon size="small" class="mr-2">mdi-source-branch-check</v-icon>
        Field analysis
        <v-chip size="x-small" variant="tonal" class="ml-2"
                :color="hardClashes.length ? 'warning' : 'success'">
          {{ hardClashes.length }} clash{{ hardClashes.length === 1 ? '' : 'es' }},
          {{ coalesced.length }} shared
        </v-chip>
      </v-card-title>
      <v-card-text>
        <p v-if="!report.clashes.length" class="text-body-2 text-medium-emphasis">
          No overlapping field names — the merge is a clean union of both shapes.
        </p>

        <div v-for="clash in report.clashes" :key="clash.field" class="clash-row">
          <div class="d-flex align-center mb-1">
            <code class="mr-2">{{ clash.field }}</code>
            <v-chip size="x-small" variant="tonal"
                    :color="clash.kind === 'coalesced' ? 'success' : 'warning'">
              {{ clash.kind === 'coalesced' ? 'shared — natural join key' : clash.kind }}
            </v-chip>
          </div>
          <div class="text-caption text-medium-emphasis mb-2">
            <span v-for="(origin, j) in clash.origins" :key="origin.source">
              <template v-if="j > 0"> &middot; </template>
              {{ origin.source }}: <code>{{ origin.type }}</code>
            </span>
          </div>
          <template v-if="clash.kind !== 'coalesced'">
            <v-btn-toggle
              v-model="actionOf[clash.field]"
              density="compact"
              variant="outlined"
              divided
              mandatory
              class="mb-2"
              @update:model-value="onResolutionChanged"
            >
              <v-btn size="small" value="rename">Rename both</v-btn>
              <v-btn v-for="origin in clash.origins" :key="origin.source" size="small"
                     :value="'prefer:' + origin.source">
                Keep {{ origin.source }}'s
              </v-btn>
            </v-btn-toggle>
            <div v-if="actionOf[clash.field] === 'rename'" class="d-flex ga-2 mb-2">
              <v-text-field
                v-for="origin in clash.origins"
                :key="origin.source"
                v-model="renameOf[clash.field][origin.source]"
                :label="origin.source + ' becomes'"
                density="compact"
                variant="outlined"
                hide-details
                style="max-width: 240px"
                @update:model-value="onResolutionChanged"
              />
            </div>
          </template>
        </div>
      </v-card-text>
    </v-card>

    <!-- Emitted result -->
    <v-card v-if="report?.resolved" variant="flat" border>
      <v-card-title class="text-subtitle-2 d-flex align-center">
        <v-icon size="small" class="mr-2">mdi-file-code-outline</v-icon>
        <code>{{ report.file }}</code>
        <v-spacer />
        <v-btn size="small" variant="tonal" prepend-icon="mdi-content-copy"
               class="mr-2" @click="copy(report.protoSource ?? '', 'proto source')">
          Copy proto
        </v-btn>
        <v-btn size="small" color="primary" prepend-icon="mdi-database-plus"
               :loading="registering" @click="register">
          Register
        </v-btn>
      </v-card-title>
      <v-card-text>
        <ProtoSource :code="report.protoSource ?? ''" />
        <v-row dense class="mt-2">
          <v-col cols="12" md="6">
            <div class="text-subtitle-2 mb-1 d-flex align-center">
              The defined join
              <v-btn size="x-small" variant="text" icon="mdi-content-copy" class="ml-1"
                     aria-label="Copy join rules"
                     @click="copy((report.joinRules ?? []).join('\n'), 'join rules')" />
            </div>
            <div class="text-caption text-medium-emphasis mb-1">
              One ruleset reading every source at once.
            </div>
            <code v-for="rule in report.joinRules" :key="rule" class="rule-line">{{ rule }}</code>
          </v-col>
          <v-col cols="12" md="6">
            <div class="text-subtitle-2 mb-1">The defined union</div>
            <div class="text-caption text-medium-emphasis mb-1">
              One ruleset per source, mapping it alone onto the merged shape.
            </div>
            <template v-for="(rules, sourceName) in report.unionRules" :key="sourceName">
              <div class="text-caption font-weight-medium mt-1">{{ sourceName }}</div>
              <code v-for="rule in rules.rules ?? []" :key="rule" class="rule-line">{{ rule }}</code>
            </template>
          </v-col>
        </v-row>
      </v-card-text>
    </v-card>
  </v-container>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import ProtoSource from '../components/ProtoSource.vue'
import { errorMessage, registryApi } from '../services/api'
import { buildDescriptorModel, type DescriptorModel } from '../services/descriptorModel'
import {
  defaultResolutions,
  isHardClash,
  mergedNameFor,
  mergedReferences,
  mergeRequest,
  scopeNameFor,
  unresolvedFields,
  type MergeClash,
  type MergeReport,
  type ResolutionChoice,
} from '../services/mergeWorkbench'
import { toast } from '../composables/useToast'

interface SourceState {
  subject: string | null
  descriptorSetBase64: string
  model: DescriptorModel | null
  types: string[]
  type: string | null
  name: string
}

const router = useRouter()
const route = useRoute()
const subjects = ref<string[]>([])
const sources = reactive<SourceState[]>([
  { subject: null, descriptorSetBase64: '', model: null, types: [], type: null, name: '' },
  { subject: null, descriptorSetBase64: '', model: null, types: [], type: null, name: '' },
])
const mergedName = ref('')
const report = ref<MergeReport | null>(null)
const error = ref('')
const registering = ref(false)

/** Per-field UI state: 'rename' or 'prefer:<source>'; rename target names. */
const actionOf = reactive<Record<string, string>>({})
const renameOf = reactive<Record<string, Record<string, string>>>({})

const ready = computed(() => sources.every((s) => s.subject && s.type && s.name))
const hardClashes = computed(() => (report.value?.clashes ?? []).filter(isHardClash))
const coalesced = computed(() =>
  (report.value?.clashes ?? []).filter((c) => !isHardClash(c)))

onMounted(async () => {
  try {
    subjects.value = await registryApi.listSubjects()
  } catch (e) {
    error.value = errorMessage(e)
    return
  }
  // Deep links: ?left=<subject>&leftType=<type>&right=<subject>&rightType=<type>&name=<merged>
  const query = route.query
  const seeds = [
    { subject: query.left, type: query.leftType },
    { subject: query.right, type: query.rightType },
  ]
  for (let i = 0; i < seeds.length; i++) {
    if (typeof seeds[i].subject === 'string') {
      sources[i].subject = String(seeds[i].subject)
      await onSubjectPicked(i)
      if (typeof seeds[i].type === 'string' && sources[i].types.includes(String(seeds[i].type))) {
        sources[i].type = String(seeds[i].type)
        onTypePicked(i)
      }
    }
  }
  if (typeof query.name === 'string') {
    mergedName.value = String(query.name)
    void analyze()
  }
})

async function onSubjectPicked(i: number) {
  const source = sources[i]
  source.type = null
  source.types = []
  if (!source.subject) return
  try {
    const bytes = await registryApi.descriptorSet(source.subject)
    source.descriptorSetBase64 = toBase64(bytes)
    source.model = buildDescriptorModel(bytes)
    source.types = messageTypes(source.model)
    error.value = ''
  } catch (e) {
    error.value = errorMessage(e)
  }
}

function onTypePicked(i: number) {
  const source = sources[i]
  if (source.type) {
    source.name = scopeNameFor(source.type, sources.filter((_, j) => j !== i).map((s) => s.name))
  }
  if (ready.value && !mergedName.value) {
    mergedName.value = mergedNameFor(sources.map((s) => ({
      name: s.name, type: s.type ?? '', descriptorSetBase64: s.descriptorSetBase64,
    })))
  }
  void analyze()
}

/** The current resolutions, derived from the per-field UI state. */
function resolutions(): Record<string, ResolutionChoice> {
  const out: Record<string, ResolutionChoice> = {}
  for (const clash of hardClashes.value) {
    const action = actionOf[clash.field]
    if (!action) continue
    if (action === 'rename') {
      out[clash.field] = { action: 'rename', names: { ...renameOf[clash.field] } }
    } else if (action.startsWith('prefer:')) {
      out[clash.field] = { action: 'prefer', source: action.slice('prefer:'.length) }
    }
  }
  return out
}

async function analyze() {
  if (!ready.value || !mergedName.value) return
  report.value = null
  const body = mergeRequest(mergedName.value, sources.map((s) => ({
    name: s.name, type: s.type ?? '', descriptorSetBase64: s.descriptorSetBase64,
  })), {}, true)
  const first = await callMerge(body)
  if (!first) return
  // Seed the UI state from the suggested defaults, then emit if nothing blocks.
  for (const clash of first.clashes.filter(isHardClash)) {
    if (!actionOf[clash.field]) {
      actionOf[clash.field] = 'rename'
      renameOf[clash.field] = { ...(clash.suggested.names ?? {}) }
    }
  }
  report.value = first
  await emit(first.clashes)
}

async function onResolutionChanged() {
  if (report.value) await emit(report.value.clashes)
}

async function emit(clashes: MergeClash[]) {
  const chosen = { ...defaultResolutions(clashes), ...resolutions() }
  if (unresolvedFields(clashes, chosen).length) return
  const body = mergeRequest(mergedName.value, sources.map((s) => ({
    name: s.name, type: s.type ?? '', descriptorSetBase64: s.descriptorSetBase64,
  })), chosen, false)
  const emitted = await callMerge(body)
  if (emitted) report.value = emitted
}

async function callMerge(body: unknown): Promise<MergeReport | null> {
  try {
    const response = await fetch('/api/serve/grpc-json/ProtoMoltService/MergeSchemas', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(body),
    })
    const payload = await response.json()
    if (!response.ok) {
      error.value = payload?.message ?? payload?.error ?? `HTTP ${response.status}`
      return null
    }
    error.value = ''
    return {
      resolved: payload.resolved ?? false,
      clashes: payload.clashes ?? [],
      type: payload.type,
      file: payload.file,
      protoSource: payload.protoSource,
      descriptorSetBase64: payload.descriptorSetBase64,
      joinRules: payload.joinRules,
      unionRules: payload.unionRules,
    }
  } catch (e) {
    error.value = errorMessage(e)
    return null
  }
}

async function register() {
  const emitted = report.value
  if (!emitted?.resolved || !emitted.file || !emitted.protoSource) return
  registering.value = true
  try {
    const versions = new Map<string, number>()
    for (const subject of sources.map((s) => s.subject)) {
      if (subject) {
        const list = await registryApi.listVersions(subject)
        versions.set(subject, list[list.length - 1])
      }
    }
    const references = mergedReferences(emitted.protoSource, (subject) => {
      return versions.get(subject) ?? -1
    })
    await registryApi.register(emitted.file, emitted.protoSource, references)
    toast.success(`Registered ${emitted.file}`)
    await router.push({
      name: 'schema-registry-subject',
      params: { subject: emitted.file },
    })
  } catch (e) {
    error.value = errorMessage(e)
  } finally {
    registering.value = false
  }
}

function copy(text: string, what: string) {
  void navigator.clipboard.writeText(text)
  toast.success(`Copied ${what}`)
}

function toBase64(bytes: Uint8Array): string {
  let binary = ''
  const chunk = 0x8000
  for (let i = 0; i < bytes.length; i += chunk) {
    binary += String.fromCharCode(...bytes.subarray(i, i + chunk))
  }
  return btoa(binary)
}

function messageTypes(model: DescriptorModel): string[] {
  const names: string[] = []
  const walk = (nodes: { kind: string; typeName: string; children?: unknown }[]) => {
    for (const node of nodes) {
      if (node.kind === 'message') names.push(node.typeName)
      if (Array.isArray(node.children)) walk(node.children as typeof nodes)
    }
  }
  for (const file of model.files) walk(file.types)
  return names.sort()
}
</script>

<style scoped>
.clash-row {
  border-top: 1px solid rgba(var(--v-border-color), var(--v-border-opacity));
  padding-top: 12px;
  margin-top: 12px;
}
.clash-row:first-of-type {
  border-top: none;
  margin-top: 0;
  padding-top: 0;
}
.rule-line {
  display: block;
  font-size: 0.8rem;
  line-height: 1.5;
}
</style>
