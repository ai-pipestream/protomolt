<template>
  <section class="pa-4" aria-label="Start workflow authoring">
    <v-progress-linear v-if="loading" indeterminate class="mb-3" />
    <v-alert v-if="error" type="error" variant="tonal" class="mb-3">{{ error }}</v-alert>
    <template v-if="template">
      <div class="text-subtitle-2">Configured workflow authoring template</div>
      <div class="text-caption text-medium-emphasis mb-2">
        The coordinator supplies the result contract, policy, checks, and {{ template.template.leaseSeconds }}-second lease.
      </div>
      <div class="text-caption mb-1">Required checks</div>
      <div v-for="check in template.template.spec.requiredChecks" :key="check.name" class="text-body-2">
        {{ check.name }}: {{ check.description }}
      </div>
      <v-select v-model="workerId" :items="availableWorkers" label="Author worker" class="mt-3" />
      <v-textarea v-model="objective" label="Authoring objective"
                  rows="3" auto-grow />
      <v-btn color="primary" :loading="starting"
             :disabled="starting || !workerId || !objective.trim() || [...objective].length > 4096"
             @click="start">Start workflow authoring</v-btn>
    </template>
    <div v-if="saved.length" class="mt-4">
      <div class="text-subtitle-2">Saved authoring starts</div>
      <div v-for="record in saved" :key="record.request.taskId" class="my-2">
        <div class="text-caption text-mono">{{ record.request.taskId }} · {{ record.request.workerId }}</div>
        <div class="text-body-2">{{ record.request.objective }}</div>
        <div v-if="record.response" class="text-caption">Original offer recorded. See the task for current progress.</div>
        <v-btn v-else size="small" variant="tonal" :disabled="starting" @click="retry(record.request)">
          Retry saved start
        </v-btn>
      </div>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import type { WorkerSummary } from '../services/tasks'
import {
  WorkflowAuthoringApiError,
  WorkflowAuthoringApi,
  WorkflowAuthoringStartStore,
  type AuthoringStartRequest,
  type AuthoringTemplateResponse,
  type SavedAuthoringStart,
} from '../services/workflowAuthoring'

const props = defineProps<{ workers: WorkerSummary[]; principal: string }>()
const emit = defineEmits<{ started: [taskId: string] }>()
const loading = ref(false)
const starting = ref(false)
const error = ref('')
const template = ref<AuthoringTemplateResponse | null>(null)
const saved = ref<SavedAuthoringStart[]>([])
const workerId = ref('')
const objective = ref('')
const availableWorkers = computed(() => props.workers
  .filter((worker) => worker.connected && worker.admitted)
  .map((worker) => worker.workerId))
const api = computed(() => new WorkflowAuthoringApi(new WorkflowAuthoringStartStore(props.principal)))
let generation = 0
let draftVersion = 0
let controller: AbortController | null = null

watch([workerId, objective], () => { draftVersion++ })
watch(() => props.principal, load)
onMounted(load)
onBeforeUnmount(() => { generation++; controller?.abort() })

async function load() {
  const current = ++generation
  const currentPrincipal = props.principal
  controller?.abort()
  controller = new AbortController()
  loading.value = true
  starting.value = false
  template.value = null
  saved.value = []
  error.value = ''
  if (!props.principal) { loading.value = false; return }
  try {
    const client = api.value
    saved.value = client.starts.list()
    const loaded = await client.template(controller.signal)
    if (current !== generation || currentPrincipal !== props.principal) return
    template.value = loaded
    objective.value = loaded.template.spec.objective
    workerId.value = availableWorkers.value[0] ?? ''
  } catch (failure) {
    if (current === generation && currentPrincipal === props.principal
        && (failure as Error).name !== 'AbortError') error.value = displayError(failure)
  } finally {
    if (current === generation && currentPrincipal === props.principal) loading.value = false
  }
}

async function start() {
  const configured = template.value
  const selected = workerId.value
  const guidance = objective.value.trim()
  if (!configured || !availableWorkers.value.includes(selected) || !guidance || [...guidance].length > 4096) return
  let previous: SavedAuthoringStart | undefined
  try {
    saved.value = api.value.starts.list()
    previous = saved.value.find((record) => !record.response
      && record.request.workerId === selected
      && record.request.objective === guidance
      && record.request.templateSha256 === configured.templateSha256)
  } catch (failure) {
    error.value = displayError(failure)
    return
  }
  const request = previous?.request ?? api.value.newRequest(selected, guidance, configured.templateSha256)
  await retry(request)
}

async function retry(request: AuthoringStartRequest) {
  if (starting.value) return
  const current = generation
  const currentPrincipal = props.principal
  const currentDraft = draftVersion
  const client = api.value
  starting.value = true
  error.value = ''
  try {
    const result = await client.start(request)
    if (current !== generation || currentPrincipal !== props.principal) return
    saved.value = client.starts.list()
    if (currentDraft === draftVersion) emit('started', result.request.taskId)
  } catch (failure) {
    if (current === generation && currentPrincipal === props.principal) {
      try { saved.value = client.starts.list() } catch { /* Keep original error. */ }
      if (currentDraft === draftVersion) error.value = displayError(failure)
    }
  } finally {
    if (current === generation && currentPrincipal === props.principal) starting.value = false
  }
}

function displayError(failure: unknown): string {
  if (failure instanceof WorkflowAuthoringApiError) {
    if (failure.status === 403) return 'This console account cannot start workflow authoring.'
    if (failure.status === 409) return 'The task UUID is already bound to a different start.'
    if (failure.status === 412) return 'The authoring template or selected worker is no longer available.'
    return `Authoring request failed (${failure.message}). Retry the saved request if the outcome is uncertain.`
  }
  return failure instanceof Error ? failure.message : 'The authoring request failed.'
}
</script>
