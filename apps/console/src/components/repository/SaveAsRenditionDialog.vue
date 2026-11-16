<template>
  <v-dialog
    :model-value="open"
    max-width="760"
    :persistent="submitting || cancelling || reconciling"
    @update:model-value="onDialogToggle"
    @keydown.esc="onEscapeKey"
  >
    <v-card
      v-if="open"
      role="dialog"
      aria-modal="true"
      aria-labelledby="save-as-dialog-title"
      data-testid="save-as-dialog"
    >
      <v-card-title id="save-as-dialog-title" class="d-flex align-center justify-space-between flex-wrap ga-2">
        <span>Save as derived rendition</span>
        <v-chip size="small" variant="tonal" label data-testid="save-as-pinned-revision">
          Source r{{ activeRevisionLabel }}
        </v-chip>
      </v-card-title>

      <v-card-subtitle class="text-wrap" data-testid="save-as-source-summary">
        {{ sourceLabel }}
        <template v-if="capabilitiesView && capabilitiesView.sourceSchemaType !== ''">
          · <code>{{ capabilitiesView.sourceSchemaType }}</code>
          · {{ byteSize(capabilitiesView.sourceSizeBytes) }}
        </template>
      </v-card-subtitle>

      <v-card-text>
        <!-- Optional unpacked Any occurrence selector for v2 document parts -->
        <div
          v-if="sourceKind === 'document-part' && occurrenceOptions.length > 0"
          class="mb-4"
          data-testid="save-as-occurrence-picker"
        >
          <v-select
            v-model="selectedOccurrencePath"
            :items="occurrenceSelectItems"
            item-title="title"
            item-value="value"
            label="Protobuf message target within part"
            density="compact"
            variant="outlined"
            hide-details="auto"
            :disabled="lockedByActiveAttempt"
            @update:model-value="onOccurrenceChanged"
          />
        </div>

        <v-progress-linear
          v-if="loadingCapabilities"
          indeterminate
          class="mb-3"
          aria-label="Loading Save-as conversion capabilities"
          data-testid="save-as-capabilities-loading"
        />

        <RpcFailureAlert
          v-if="capabilitiesFailure"
          :failure="capabilitiesFailure"
          title="Discovering Save-as capabilities"
          class="mb-3"
          data-testid="save-as-capabilities-failure"
          @sign-in="signIn"
        />

        <template v-if="!loadingCapabilities && capabilitiesView !== null">
          <!-- Zero-provider state: never invent PDF, Markdown, or Parquet when absent -->
          <v-alert
            v-if="!capabilitiesView.hasAnyProviders"
            type="info"
            variant="tonal"
            class="mb-3"
            data-testid="save-as-empty-providers"
          >
            {{ capabilitiesView.emptyStateMessage }}
          </v-alert>

          <template v-else>
            <div v-if="capabilitiesView.compatible.length === 0" class="mb-3">
              <v-alert
                type="warning"
                variant="tonal"
                data-testid="save-as-no-compatible"
              >
                None of the enabled conversion providers are compatible with this protobuf source.
              </v-alert>
            </div>

            <div v-if="capabilitiesView.compatible.length > 0" class="mb-4">
              <div class="text-subtitle-2 mb-2">Compatible target formats</div>
              <div
                role="radiogroup"
                aria-label="Compatible target formats"
                class="d-flex flex-column ga-2"
                data-testid="save-as-compatible-list"
              >
                <v-sheet
                  v-for="cap in capabilitiesView.compatible"
                  :key="cap.key"
                  rounded
                  border
                  class="pa-3 capability-card"
                  :class="{ 'capability-card--selected': selectedCapabilityKey === cap.key }"
                  :data-testid="`save-as-capability-${cap.providerId}`"
                >
                  <label class="d-flex align-start ga-3 capability-card__label">
                    <input
                      type="radio"
                      name="save-as-capability"
                      :value="cap.key"
                      :checked="selectedCapabilityKey === cap.key"
                      :disabled="lockedByActiveAttempt"
                      class="mt-1"
                      @change="selectCapability(cap)"
                    />
                    <div class="flex-grow-1">
                      <div class="d-flex align-center justify-space-between flex-wrap ga-2">
                        <FormatIcon :media-type="cap.mediaType" :filename="cap.extension" show-media-type />
                        <span class="text-caption">
                          <code>{{ cap.providerId }}</code> · v{{ cap.providerVersion }}
                        </span>
                      </div>
                      <div class="text-caption mt-1">
                        Profile: <code>{{ cap.sourceProfile }}</code>
                        <template v-if="cap.extension !== ''"> · Extension: <code>{{ cap.extension }}</code></template>
                        <template v-if="cap.maxOutputBytes > 0n"> · Max output: {{ byteSize(cap.maxOutputBytes) }}</template>
                      </div>
                      <div
                        v-if="cap.lossDescription !== ''"
                        class="text-body-2 mt-1 capability-loss"
                        data-testid="save-as-loss-description"
                      >
                        Fidelity note: {{ cap.lossDescription }}
                      </div>
                    </div>
                  </label>
                </v-sheet>
              </div>
            </div>

            <div v-if="capabilitiesView.incompatible.length > 0" class="mb-4" data-testid="save-as-incompatible-list">
              <div class="text-subtitle-2 mb-2">Incompatible providers for this source</div>
              <v-sheet
                v-for="cap in capabilitiesView.incompatible"
                :key="cap.key"
                rounded
                border
                class="pa-2 mb-2 capability-card capability-card--incompatible"
                :data-testid="`save-as-incompatible-${cap.providerId}`"
              >
                <div class="d-flex align-center justify-space-between flex-wrap ga-2">
                  <FormatIcon :media-type="cap.mediaType" :filename="cap.extension" show-media-type />
                  <v-chip size="x-small" color="warning" variant="tonal" label>Incompatible</v-chip>
                </div>
                <div class="text-caption mt-1">
                  <code>{{ cap.providerId }}</code> ({{ cap.sourceProfile }}): {{ cap.incompatibilityReason }}
                </div>
              </v-sheet>
            </div>
          </template>

          <!-- Provider options + destination form when a compatible capability is selected -->
          <template v-if="selectedCapability !== null">
            <v-divider class="my-3" />

            <SaveAsParquetOptions
              v-if="selectedCapability.optionsKind === 'parquet'"
              :form="parquetForm"
              :disabled="lockedByActiveAttempt"
              @edited="markDraftEdited"
            />

            <div v-else class="mb-4" data-testid="save-as-default-options">
              <div class="text-subtitle-2 mb-1">Provider options</div>
              <div class="text-caption mb-2">
                Uses default conversion options for <code>{{ selectedCapability.optionsType || 'google.protobuf.Empty' }}</code>.
              </div>
              <v-row density="compact">
                <v-col cols="12" sm="6">
                  <v-text-field
                    v-model="genericMappingId"
                    label="Mapping ID (optional)"
                    density="compact"
                    variant="outlined"
                    hide-details="auto"
                    :disabled="lockedByActiveAttempt"
                    @update:model-value="markDraftEdited"
                  />
                </v-col>
                <v-col cols="12" sm="6">
                  <v-text-field
                    v-model="genericPolicyId"
                    label="Policy ID (optional)"
                    density="compact"
                    variant="outlined"
                    hide-details="auto"
                    :disabled="lockedByActiveAttempt"
                    @update:model-value="markDraftEdited"
                  />
                </v-col>
              </v-row>
            </div>

            <v-divider class="my-3" />

            <RpcFailureAlert
              v-if="archivesFailure"
              :failure="archivesFailure"
              title="Listing destination archives"
              class="mb-3"
              data-testid="save-as-archives-failure"
              @sign-in="signIn"
            />

            <SaveAsDestinationFields
              :form="destinationForm"
              :disabled="lockedByActiveAttempt"
              :archives="availableArchives"
              @edited="markDraftEdited"
              @rendition-name-edited="onRenditionNameEdited"
            />
          </template>
        </template>

        <!-- Client validation error -->
        <v-alert
          v-if="inputError !== null"
          type="error"
          variant="tonal"
          role="alert"
          class="mt-3"
          data-testid="save-as-input-error"
        >
          {{ inputError }}
        </v-alert>

        <SaveAsJobPanel
          :entry="activeJob"
          @refresh="refreshJobStatus"
          @retry="retrySameOperation"
          @cancel="cancelActiveJob"
          @reload-source="emit('reloadSource')"
          @sign-in="signIn"
          @open-entry="closeDialog"
        />
      </v-card-text>

      <v-card-actions class="justify-end ga-2 px-4 pb-4">
        <v-btn
          variant="text"
          :disabled="submitting || cancelling || reconciling"
          data-testid="save-as-close-btn"
          @click="closeDialog"
        >
          {{ jobSummary?.phase === 'succeeded' ? 'Done' : 'Close' }}
        </v-btn>
        <v-btn
          v-if="selectedCapability !== null && jobSummary?.phase !== 'succeeded'"
          color="primary"
          variant="flat"
          :loading="submitting"
          :disabled="loadingCapabilities || lockedByActiveAttempt"
          data-testid="save-as-submit-btn"
          @click="submitSaveAs"
        >
          Save rendition
        </v-btn>
      </v-card-actions>
    </v-card>
  </v-dialog>
</template>

<script setup lang="ts">
import { create } from '@bufbuild/protobuf'
import { computed, inject, onBeforeUnmount, reactive, ref, shallowRef, watch } from 'vue'
import RpcFailureAlert from '@/components/repository/RpcFailureAlert.vue'
import FormatIcon from '@/components/repository/FormatIcon.vue'
import SaveAsDestinationFields from '@/components/repository/SaveAsDestinationFields.vue'
import SaveAsJobPanel from '@/components/repository/SaveAsJobPanel.vue'
import SaveAsParquetOptions from '@/components/repository/SaveAsParquetOptions.vue'
import { ParquetMaskStrategy } from '@/gen/ai/protomolt/proto/emit/parquet/v1/parquet_renderer_pb'
import { ListArchivesRequestSchema } from '@/gen/ai/protomolt/proto/repo/archive/v1/archive_service_pb'
import type { PublishRepositorySinkRequest, SinkSource } from '@/gen/ai/protomolt/proto/repo/sink/v1/repository_sink_pb'
import { REPOSITORY_SIGN_IN, useRepositoryClients } from '@/repository/clients'
import { byteSize } from '@/repository/formatCore'
import { isOwnCancellation, rpcFailure, type RpcFailure } from '@/repository/rpcErrorCore'
import {
  buildArchiveRenditionSinkSource,
  buildDocumentPartSinkSource,
  buildListSaveAsCapabilitiesRequest,
  buildPublishSaveAsRequest,
  describeSaveAsCapabilities,
  SaveAsInputError,
  type ArchiveRenditionSourceInput,
  type DocumentPartSourceInput,
  type ParquetOptionsFormInput,
  type SaveAsCapabilitiesView,
  type SaveAsCapabilityItem,
  type SaveAsDestinationInput,
} from '@/repository/saveAsCore'
import {
  type ClassifiedSinkError,
  type SinkJobSummary,
} from '@/repository/saveAsJobCore'
import { SaveAsJobCapacityError, useSaveAsJobs } from '@/repository/saveAsJobs'
import {
  type ResolvedOccurrenceOption,
} from '@/repository/saveAsProvenanceCore'
import { newOperationId } from '@/repository/uploadCore'
import { useCallScope } from '@/repository/useCallScope'

const props = withDefaults(
  defineProps<{
    open: boolean
    sourceKind: 'archive-rendition' | 'document-part'
    sourceLabel: string
    accountId: string
    archiveSource?: ArchiveRenditionSourceInput | null
    documentSource?: Omit<DocumentPartSourceInput, 'occurrenceFieldPath'> | null
    occurrenceOptions?: readonly ResolvedOccurrenceOption[]
    defaultDestinationArchive?: string
    defaultDestinationEntryId?: string
    defaultExpectedDestinationVersion?: bigint | null
  }>(),
  {
    archiveSource: null,
    documentSource: null,
    occurrenceOptions: () => [],
    defaultDestinationArchive: '',
    defaultDestinationEntryId: '',
    defaultExpectedDestinationVersion: null,
  },
)

const emit = defineEmits<{
  close: []
  saved: [job: SinkJobSummary]
  reloadSource: []
}>()

const clients = useRepositoryClients()
const signIn = inject(REPOSITORY_SIGN_IN, () => {})
const capScope = useCallScope()
const archivesScope = useCallScope()
const jobs = useSaveAsJobs()

const loadingCapabilities = ref(false)
const capabilitiesFailure = ref<RpcFailure | null>(null)
const archivesFailure = ref<RpcFailure | null>(null)
const capabilitiesView = shallowRef<SaveAsCapabilitiesView | null>(null)
const selectedCapabilityKey = ref<string>('')
const selectedOccurrencePath = ref<string>('')
const availableArchives = ref<string[]>([])
const userEditedRenditionName = ref(false)

const parquetForm = reactive<ParquetOptionsFormInput>({
  columnsText: '',
  maskedSensitivityClassesText: '',
  maskStrategy: ParquetMaskStrategy.UNSPECIFIED,
  maxOutputBytesText: '',
  mappingId: '',
  policyId: '',
})
const genericMappingId = ref('')
const genericPolicyId = ref('')

const destinationForm = reactive<SaveAsDestinationInput>({
  accountId: '',
  archive: '',
  entryId: '',
  renditionName: '',
  subKey: '',
  preconditionMode: 'unconditional',
  expectedDestinationVersionText: '',
})

const inputError = ref<string | null>(null)
const activeOperationId = ref<string | null>(null)
// The job itself (calls, polling, operation ID and summary) lives in the host store, so it
// survives this dialog unmounting; these views only read the active entry.
const activeJob = computed(() =>
  activeOperationId.value === null ? undefined : jobs.find(activeOperationId.value),
)
const submitting = computed(() => activeJob.value?.submitting ?? false)
const reconciling = computed(() => activeJob.value?.reconciling ?? false)
const cancelling = computed(() => activeJob.value?.cancelling ?? false)
const classifiedError = computed<ClassifiedSinkError | null>(() => activeJob.value?.error ?? null)
const frozenRequest = computed<PublishRepositorySinkRequest | null>(() => activeJob.value?.request ?? null)
const jobSummary = computed<SinkJobSummary | null>(() => activeJob.value?.summary ?? null)

const occurrenceSelectItems = computed(() => [
  { title: 'Entire part root message', value: '' },
  ...props.occurrenceOptions.map((opt) => ({
    title: `${opt.fieldPath} (${opt.typeName} · ${byteSize(opt.payloadSizeBytes)})`,
    value: opt.fieldPath,
  })),
])

const activeRevisionLabel = computed(() => {
  if (props.sourceKind === 'archive-rendition' && props.archiveSource) {
    return props.archiveSource.revision.toString()
  }
  if (props.sourceKind === 'document-part' && props.documentSource) {
    return props.documentSource.revision.toString()
  }
  return '0'
})

const selectedCapability = computed<SaveAsCapabilityItem | null>(() => {
  const view = capabilitiesView.value
  if (view === null) return null
  return view.compatible.find((c) => c.key === selectedCapabilityKey.value) ?? null
})

const lockedByActiveAttempt = computed(() => {
  if (submitting.value || reconciling.value || cancelling.value) return true
  const j = jobSummary.value
  if (j !== null && (j.phase === 'queued' || j.phase === 'running' || j.phase === 'unresolved')) {
    return true
  }
  if (classifiedError.value?.kind === 'unsettled') {
    return true
  }
  return false
})

function releaseActiveJob() {
  const entry = activeJob.value
  if (entry === undefined) return
  if (entry.dialogOpen && entry.summary !== null && !lockedByActiveAttempt.value) {
    // Settled and already shown here: nothing left for the banner to keep.
    jobs.dismiss(entry.operationId)
  } else {
    jobs.setDialogOpen(entry.operationId, false)
  }
}

function resetAttemptState() {
  releaseActiveJob()
  inputError.value = null
  activeOperationId.value = null
}

function markDraftEdited() {
  if (!lockedByActiveAttempt.value) {
    resetAttemptState()
  }
}

function onRenditionNameEdited() {
  userEditedRenditionName.value = true
  markDraftEdited()
}

function selectCapability(cap: SaveAsCapabilityItem) {
  selectedCapabilityKey.value = cap.key
  if (!userEditedRenditionName.value || destinationForm.renditionName.trim() === '') {
    destinationForm.renditionName = cap.defaultRenditionName
  }
  markDraftEdited()
}

function buildCurrentSinkSource(): SinkSource {
  if (props.sourceKind === 'archive-rendition') {
    if (props.archiveSource === null) {
      throw new SaveAsInputError('Source rendition: missing archive rendition coordinates.')
    }
    return buildArchiveRenditionSinkSource(props.archiveSource)
  }
  if (props.documentSource === null) {
    throw new SaveAsInputError('Source part: missing document part coordinates.')
  }
  return buildDocumentPartSinkSource({
    ...props.documentSource,
    occurrenceFieldPath: selectedOccurrencePath.value,
  })
}

async function loadArchives() {
  const acct = props.accountId.trim()
  if (acct === '') {
    availableArchives.value = []
    return
  }
  const signal = archivesScope.next()
  archivesFailure.value = null
  try {
    const names: string[] = []
    let token = ''
    do {
      const page = await clients.archives.listArchives(
        create(ListArchivesRequestSchema, { accountId: acct, limit: 100, continuationToken: token }),
        { signal },
      )
      names.push(...page.archives.map((a) => a.name))
      token = page.nextContinuationToken
    } while (token !== '')
    availableArchives.value = names
  } catch (error) {
    // The archive can still be typed by hand, so the dialog stays usable; the operator is
    // told the list is missing and why.
    if (!isOwnCancellation(error, signal)) {
      archivesFailure.value = rpcFailure(error)
    }
  }
}

async function loadCapabilities() {
  capabilitiesFailure.value = null
  capabilitiesView.value = null
  selectedCapabilityKey.value = ''
  resetAttemptState()

  let source: SinkSource
  try {
    source = buildCurrentSinkSource()
  } catch (error) {
    inputError.value = error instanceof Error ? error.message : String(error)
    return
  }

  const signal = capScope.next()
  loadingCapabilities.value = true
  try {
    const response = await clients.repositorySink.listSaveAsCapabilities(
      buildListSaveAsCapabilitiesRequest(source),
      { signal },
    )
    const view = describeSaveAsCapabilities(response)
    capabilitiesView.value = view
    if (view.compatible.length > 0) {
      const first = view.compatible[0]!
      selectedCapabilityKey.value = first.key
      if (!userEditedRenditionName.value || destinationForm.renditionName.trim() === '') {
        destinationForm.renditionName = first.defaultRenditionName
      }
    }
  } catch (error) {
    if (!isOwnCancellation(error, signal)) {
      capabilitiesFailure.value = rpcFailure(error)
    }
  } finally {
    loadingCapabilities.value = false
  }
}

function initializeDialogState() {
  resetAttemptState()
  userEditedRenditionName.value = false
  selectedOccurrencePath.value = ''
  parquetForm.columnsText = ''
  parquetForm.maskedSensitivityClassesText = ''
  parquetForm.maskStrategy = ParquetMaskStrategy.UNSPECIFIED
  parquetForm.maxOutputBytesText = ''
  parquetForm.mappingId = ''
  parquetForm.policyId = ''
  genericMappingId.value = ''
  genericPolicyId.value = ''

  destinationForm.accountId = props.accountId
  destinationForm.archive = props.defaultDestinationArchive
  destinationForm.entryId = props.defaultDestinationEntryId
  destinationForm.renditionName = ''
  destinationForm.subKey = ''

  const expectedVersion = props.defaultExpectedDestinationVersion ?? 0n
  if (expectedVersion > 0n) {
    destinationForm.preconditionMode = 'expected-version'
    destinationForm.expectedDestinationVersionText = expectedVersion.toString()
  } else if (props.sourceKind === 'document-part') {
    destinationForm.preconditionMode = 'create-only'
    destinationForm.expectedDestinationVersionText = ''
  } else {
    destinationForm.preconditionMode = 'unconditional'
    destinationForm.expectedDestinationVersionText = ''
  }

  void loadArchives()
  void loadCapabilities()
}

function onOccurrenceChanged() {
  void loadCapabilities()
}

async function submitSaveAs() {
  if (submitting.value || selectedCapability.value === null) return
  inputError.value = null
  releaseActiveJob()
  activeOperationId.value = null

  let request: PublishRepositorySinkRequest
  const opId = newOperationId()
  try {
    const source = buildCurrentSinkSource()
    request = buildPublishSaveAsRequest({
      operationId: opId,
      source,
      capability: selectedCapability.value,
      destination: {
        ...destinationForm,
        accountId: props.accountId,
      },
      parquetOptions: parquetForm,
      genericMappingId: genericMappingId.value,
      genericPolicyId: genericPolicyId.value,
      async: true,
    })
  } catch (error) {
    if (error instanceof SaveAsInputError) {
      inputError.value = error.message
      return
    }
    throw error
  }

  activeOperationId.value = opId
  try {
    await jobs.publish(clients.repositorySink, request, true)
  } catch (error) {
    if (error instanceof SaveAsJobCapacityError) {
      // Nothing was sent and nothing is tracked under this operation ID.
      activeOperationId.value = null
      inputError.value = error.message
      return
    }
    throw error
  }
}

async function retrySameOperation() {
  const request = frozenRequest.value
  if (submitting.value || request === null) return
  await jobs.publish(clients.repositorySink, request, true)
}

async function refreshJobStatus() {
  if (activeOperationId.value !== null) await jobs.refresh(activeOperationId.value)
}

async function cancelActiveJob() {
  if (activeOperationId.value !== null) {
    await jobs.cancel(activeOperationId.value, 'Cancelled from console Save-as dialog')
  }
}

function closeDialog() {
  if (submitting.value || cancelling.value || reconciling.value) return
  if (activeOperationId.value !== null) jobs.setDialogOpen(activeOperationId.value, false)
  emit('close')
}

function onDialogToggle(nextOpen: boolean) {
  if (!nextOpen) {
    closeDialog()
  }
}

function onEscapeKey() {
  closeDialog()
}

watch(
  () => props.open,
  (isOpen) => {
    if (isOpen) {
      initializeDialogState()
    } else if (activeOperationId.value !== null) {
      jobs.setDialogOpen(activeOperationId.value, false)
    }
  },
  { immediate: true },
)

// Emit `saved` once per observed commit; a cancel that finds the job already committed does not.
watch(
  () => [activeJob.value?.summary?.phase, activeJob.value?.updatedBy] as const,
  ([phase, updatedBy], [previousPhase]) => {
    const summary = jobSummary.value
    if (phase === 'succeeded' && previousPhase !== 'succeeded' && updatedBy !== 'cancel' && summary !== null) {
      emit('saved', summary)
    }
  },
)

onBeforeUnmount(() => {
  if (activeOperationId.value !== null) jobs.setDialogOpen(activeOperationId.value, false)
})
</script>

<style scoped>
.capability-card {
  border-color: rgba(var(--v-theme-on-surface), 0.22);
  transition: border-color 0.12s ease, background-color 0.12s ease;
}

.capability-card--selected {
  border-color: rgb(var(--v-theme-primary));
  background-color: rgba(var(--v-theme-primary), 0.06);
}

.capability-card--incompatible {
  opacity: 0.85;
}

.capability-card__label {
  cursor: pointer;
}

.capability-loss {
  color: rgb(var(--v-theme-on-surface));
  opacity: 0.88;
}
</style>
