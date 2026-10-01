<template>
  <section class="workflow-launch-panel pa-4" aria-label="Accepted workflow launch">
    <div class="d-flex align-center ga-2 mb-2">
      <v-icon icon="mdi-rocket-launch-outline" size="small" color="primary" />
      <strong>Launch accepted workflow</strong>
    </div>
    <v-progress-linear v-if="loading" indeterminate color="primary" class="mb-3" />
    <v-alert v-if="denied" type="warning" variant="tonal" class="mb-3">
      This console account does not have workflow launch permission.
    </v-alert>
    <v-alert v-else-if="notLaunchable" type="info" variant="tonal" class="mb-3">
      This accepted task is not a launchable authored workflow.
    </v-alert>
    <v-alert v-else-if="error" type="error" variant="tonal" class="mb-3">
      {{ error }}
    </v-alert>

    <template v-if="accepted && contract && !denied && !notLaunchable">
      <div class="text-caption text-medium-emphasis mb-1">Pinned input message</div>
      <div class="text-body-2 text-mono mb-2">{{ contract.inputType }}</div>
      <div class="text-caption text-medium-emphasis mb-3">
        Descriptor SHA-256: <span class="text-mono">{{ contract.descriptors.sha256 }}</span>
      </div>
      <v-textarea
        v-model="inputJson"
        label="Protobuf JSON input"
        aria-label="Protobuf JSON input"
        rows="8"
        auto-grow
        spellcheck="false"
        class="text-mono"
      />
      <div class="d-flex flex-wrap align-center ga-2 mb-3">
        <v-btn variant="tonal" prepend-icon="mdi-check-circle-outline"
               :loading="preparing" :disabled="preparing || launching || !inputJson.trim()"
               @click="prepareInput">Prepare input</v-btn>
        <v-btn color="primary" prepend-icon="mdi-rocket-launch"
               :loading="launching" :disabled="!prepared || launching || preparing"
               @click="launchPrepared">Launch workflow</v-btn>
      </div>
      <div v-if="prepared" class="text-caption mb-3">
        Prepared input SHA-256: <span class="text-mono">{{ prepared.input.sha256 }}</span>.
        Launching will submit this prepared input.
      </div>
      <div v-if="saved.length" class="saved-launches">
        <div class="text-subtitle-2 mb-2">Saved launch requests</div>
        <div v-for="record in saved" :key="record.intent.launchId" class="saved-launch mb-3 pa-3">
          <div class="text-caption text-mono">{{ record.intent.launchId }}</div>
          <div class="text-caption">Input SHA-256: {{ record.intent.input.sha256 }}</div>
          <template v-if="record.result">
            <div class="text-body-2 mt-2">Job ID: <span class="text-mono">{{ record.result.jobId }}</span></div>
            <div class="text-caption">Authorization SHA-256:
              <span class="text-mono">{{ record.result.authorization.sha256 }}</span>
            </div>
            <div class="text-caption text-medium-emphasis">Launch submitted. Refresh to read job progress.</div>
          </template>
          <div v-if="statuses[record.intent.launchId]" class="text-body-2 mt-2">
            <template v-if="'notAuthorized' in statuses[record.intent.launchId]!">
              No launch authorization was observed. Retry the same saved request if its outcome is uncertain.
            </template>
            <template v-else-if="'authorizedNotQueued' in statuses[record.intent.launchId]!">
              Launch authorized; no job was observed. Retry the same saved request to finish submission.
            </template>
            <template v-else-if="jobStatus(record.intent.launchId)">
              <strong>{{ stateText(jobStatus(record.intent.launchId)!.state) }}</strong>
              · attempt {{ jobStatus(record.intent.launchId)!.attempt }}
              / {{ jobStatus(record.intent.launchId)!.maxAttempts }}
              <span v-if="jobStatus(record.intent.launchId)!.completedAt">
                · finished {{ jobStatus(record.intent.launchId)!.completedAt }}
              </span>
            </template>
          </div>
          <div v-if="statusErrors[record.intent.launchId]" class="text-caption text-error mt-2">
            {{ statusErrors[record.intent.launchId] }}
          </div>
          <v-btn size="small" variant="tonal" class="mt-2 mr-2"
                 :loading="statusLoading[record.intent.launchId]"
                 :disabled="!sameAccepted(record.intent.acceptance) || statusLoading[record.intent.launchId]"
                 @click="refreshStatus(record.intent)">Refresh status</v-btn>
          <v-btn v-if="canRetry(record)" size="small" variant="outlined" class="mt-2"
                 :disabled="launching || !sameAccepted(record.intent.acceptance)"
                 @click="retry(record.intent)">Retry saved launch</v-btn>
          <div v-if="!sameAccepted(record.intent.acceptance)" class="text-caption text-warning mt-1">
            This saved request refers to an older acceptance and cannot be retried here.
          </div>
        </div>
      </div>
    </template>
  </section>
</template>

<script setup lang="ts">
import { onBeforeUnmount, onMounted, ref, watch } from 'vue'
import {
  sameAcceptance,
  sameArtifact,
  newLaunchId,
  workflowLaunchApi,
  WorkflowLaunchApiError,
  type AcceptedWorkflow,
  type LaunchInputContract,
  type LaunchIntent,
  type PreparedLaunchInput,
  type SavedLaunch,
  type WorkflowLaunchJobState,
  type WorkflowLaunchJobStatus,
  type WorkflowLaunchStatus,
} from '../services/workflowLaunch'

const props = defineProps<{ taskId: string }>()
const loading = ref(false)
const preparing = ref(false)
const launching = ref(false)
const denied = ref(false)
const notLaunchable = ref(false)
const error = ref('')
const accepted = ref<AcceptedWorkflow | null>(null)
const contract = ref<LaunchInputContract | null>(null)
const inputJson = ref('{}')
const prepared = ref<PreparedLaunchInput | null>(null)
const saved = ref<SavedLaunch[]>([])
const statuses = ref<Record<string, WorkflowLaunchStatus>>({})
const statusErrors = ref<Record<string, string>>({})
const statusLoading = ref<Record<string, boolean>>({})
const statusVersions = new Map<string, number>()
let generation = 0
let draftVersion = 0
let controller: AbortController | null = null

watch(inputJson, () => {
  draftVersion++
  prepared.value = null
})
watch(() => props.taskId, load)
onMounted(load)
onBeforeUnmount(() => {
  generation++
  controller?.abort()
})

function sameAccepted(value: AcceptedWorkflow): boolean {
  return accepted.value !== null && sameAcceptance(accepted.value, value)
}

async function load() {
  const current = ++generation
  controller?.abort()
  controller = new AbortController()
  loading.value = true
  preparing.value = false
  launching.value = false
  denied.value = false
  notLaunchable.value = false
  error.value = ''
  accepted.value = null
  contract.value = null
  prepared.value = null
  statuses.value = {}
  statusErrors.value = {}
  statusLoading.value = {}
  statusVersions.clear()
  try {
    saved.value = workflowLaunchApi.intents.list(props.taskId)
    const selected = await workflowLaunchApi.accepted(props.taskId, controller.signal)
    if (current !== generation || selected.taskId !== props.taskId) return
    const pinned = await workflowLaunchApi.contract(selected, controller.signal)
    if (current !== generation) return
    if (!sameAcceptance(selected, pinned.acceptance) || !pinned.inputType
        || !/^[0-9a-f]{64}$/.test(pinned.descriptors.sha256)) {
      throw new Error('The launch contract does not match the accepted task.')
    }
    accepted.value = selected
    contract.value = pinned
  } catch (failure) {
    if (current !== generation) return
    if (failure instanceof WorkflowLaunchApiError && failure.status === 403) denied.value = true
    else if (failure instanceof WorkflowLaunchApiError && failure.status === 412) notLaunchable.value = true
    else if ((failure as Error).name !== 'AbortError') error.value = displayError(failure)
  } finally {
    if (current === generation) loading.value = false
  }
}

async function prepareInput() {
  const selector = accepted.value
  if (!selector || !inputJson.value.trim()) return
  const current = generation
  const currentDraft = draftVersion
  preparing.value = true
  error.value = ''
  try {
    const result = await workflowLaunchApi.prepare(selector, inputJson.value)
    if (current !== generation || currentDraft !== draftVersion) return
    if (!sameAcceptance(selector, result.acceptance) || result.input.mediaType !== 'application/x-protobuf'
        || result.input.redacted || !/^[0-9a-f]{64}$/.test(result.input.sha256)) {
      throw new Error('Prepared input does not match the selected acceptance.')
    }
    prepared.value = result
  } catch (failure) {
    if (current === generation && currentDraft === draftVersion) error.value = displayError(failure)
  } finally {
    if (current === generation) preparing.value = false
  }
}

async function launchPrepared() {
  const selected = accepted.value
  const input = prepared.value
  if (!selected || !input || launching.value || !sameAcceptance(selected, input.acceptance)) return
  try {
    saved.value = workflowLaunchApi.intents.list(props.taskId)
  } catch (failure) {
    error.value = displayError(failure)
    return
  }
  const pending = saved.value.find((record) => !record.result
    && sameAcceptance(record.intent.acceptance, selected)
    && sameArtifact(record.intent.input, input.input))
  if (pending) {
    prepared.value = null
    await retry(pending.intent)
    return
  }
  const intent: LaunchIntent = {
    launchId: newLaunchId(),
    acceptance: { ...selected },
    input: { ...input.input },
  }
  prepared.value = null
  await retry(intent)
}

async function retry(intent: LaunchIntent) {
  if (launching.value || !sameAccepted(intent.acceptance)) return
  const current = generation
  statusVersions.set(intent.launchId, (statusVersions.get(intent.launchId) ?? 0) + 1)
  delete statuses.value[intent.launchId]
  delete statusErrors.value[intent.launchId]
  delete statusLoading.value[intent.launchId]
  launching.value = true
  error.value = ''
  try {
    // The complete request is durable before the first network call.
    workflowLaunchApi.intents.persist(intent)
    saved.value = workflowLaunchApi.intents.list(props.taskId)
    await workflowLaunchApi.launch(intent)
    if (current === generation) saved.value = workflowLaunchApi.intents.list(props.taskId)
  } catch (failure) {
    if (current === generation) {
      try { saved.value = workflowLaunchApi.intents.list(props.taskId) } catch { /* Keep the storage error below. */ }
      error.value = displayError(failure)
    }
  } finally {
    if (current === generation) launching.value = false
  }
}

function canRetry(record: SavedLaunch): boolean {
  const status = statuses.value[record.intent.launchId]
  return !record.result || status !== undefined && !('job' in status)
}

function jobStatus(id: string): WorkflowLaunchJobStatus | null {
  const status = statuses.value[id]
  return status && 'job' in status ? status.job : null
}

function stateText(state: WorkflowLaunchJobState): string {
  switch (state) {
    case 'WORKFLOW_LAUNCH_JOB_STATE_QUEUED': return 'Queued for execution'
    case 'WORKFLOW_LAUNCH_JOB_STATE_RUNNING': return 'Running'
    case 'WORKFLOW_LAUNCH_JOB_STATE_WAITING': return 'Waiting for external completion'
    case 'WORKFLOW_LAUNCH_JOB_STATE_COMPLETED': return 'Execution completed'
    case 'WORKFLOW_LAUNCH_JOB_STATE_FAILED': return 'Execution failed'
    case 'WORKFLOW_LAUNCH_JOB_STATE_DEAD': return 'Execution exhausted its retry budget'
  }
}

async function refreshStatus(intent: LaunchIntent) {
  if (!sameAccepted(intent.acceptance)) return
  const id = intent.launchId
  const current = generation
  const version = (statusVersions.get(id) ?? 0) + 1
  statusVersions.set(id, version)
  statusLoading.value[id] = true
  delete statusErrors.value[id]
  try {
    const observation = await workflowLaunchApi.status(intent)
    if (current !== generation || statusVersions.get(id) !== version) return
    statuses.value[id] = observation
  } catch (failure) {
    if (current === generation && statusVersions.get(id) === version) {
      statusErrors.value[id] = displayError(failure)
    }
  } finally {
    if (current === generation && statusVersions.get(id) === version) {
      statusLoading.value[id] = false
    }
  }
}

function displayError(failure: unknown): string {
  if (failure instanceof WorkflowLaunchApiError) {
    if (failure.status === 403) return 'This console account cannot launch workflows.'
    if (failure.status === 412) return 'The accepted workflow or prepared input is no longer current.'
    return `Launch request failed (${failure.message}). Retry the saved request if its outcome is uncertain.`
  }
  return failure instanceof Error ? failure.message : 'The launch request failed.'
}
</script>

<style scoped>
.text-mono { font-family: 'JetBrains Mono Variable', monospace; overflow-wrap: anywhere; }
.saved-launch { border: 1px solid rgba(var(--v-border-color), var(--v-border-opacity)); border-radius: 8px; }
</style>
