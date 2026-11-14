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

            <div v-if="selectedCapability.optionsKind === 'parquet'" class="mb-4" data-testid="save-as-parquet-options">
              <div class="text-subtitle-2 mb-2">Parquet projection &amp; masking options</div>
              <v-row density="compact">
                <v-col cols="12" sm="6">
                  <v-text-field
                    v-model="parquetForm.columnsText"
                    label="Projected columns (optional)"
                    hint="Comma-separated top-level field names; leave blank for all scalar columns"
                    persistent-hint
                    density="compact"
                    variant="outlined"
                    :disabled="lockedByActiveAttempt"
                    data-testid="parquet-columns-input"
                    @update:model-value="markDraftEdited"
                  />
                </v-col>
                <v-col cols="12" sm="6">
                  <v-text-field
                    v-model="parquetForm.maxOutputBytesText"
                    label="Max output bytes (optional)"
                    hint="Whole bytes; leave blank for provider ceiling"
                    persistent-hint
                    density="compact"
                    variant="outlined"
                    inputmode="numeric"
                    :disabled="lockedByActiveAttempt"
                    data-testid="parquet-max-bytes-input"
                    @update:model-value="markDraftEdited"
                  />
                </v-col>
                <v-col cols="12" sm="6">
                  <v-text-field
                    v-model="parquetForm.maskedSensitivityClassesText"
                    label="Masked sensitivity classes (optional)"
                    hint="Comma-separated sensitivity classes (e.g. PII, SECRET)"
                    persistent-hint
                    density="compact"
                    variant="outlined"
                    :disabled="lockedByActiveAttempt"
                    data-testid="parquet-sensitivity-input"
                    @update:model-value="markDraftEdited"
                  />
                </v-col>
                <v-col cols="12" sm="6">
                  <v-select
                    v-model="parquetForm.maskStrategy"
                    :items="maskStrategyItems"
                    item-title="title"
                    item-value="value"
                    label="Mask strategy"
                    density="compact"
                    variant="outlined"
                    hide-details="auto"
                    :disabled="lockedByActiveAttempt"
                    data-testid="parquet-mask-strategy-select"
                    @update:model-value="markDraftEdited"
                  />
                </v-col>
                <v-col cols="12" sm="6">
                  <v-text-field
                    v-model="parquetForm.mappingId"
                    label="Mapping ID (optional)"
                    density="compact"
                    variant="outlined"
                    hide-details="auto"
                    :disabled="lockedByActiveAttempt"
                    data-testid="parquet-mapping-id-input"
                    @update:model-value="markDraftEdited"
                  />
                </v-col>
                <v-col cols="12" sm="6">
                  <v-text-field
                    v-model="parquetForm.policyId"
                    label="Policy ID (optional)"
                    density="compact"
                    variant="outlined"
                    hide-details="auto"
                    :disabled="lockedByActiveAttempt"
                    data-testid="parquet-policy-id-input"
                    @update:model-value="markDraftEdited"
                  />
                </v-col>
              </v-row>
            </div>

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

            <div class="mb-3" data-testid="save-as-destination">
              <div class="text-subtitle-2 mb-2">Archive destination</div>
              <v-row density="compact">
                <v-col cols="12" sm="4">
                  <v-combobox
                    v-if="availableArchives.length > 0"
                    v-model="destinationForm.archive"
                    :items="availableArchives"
                    label="Destination archive"
                    density="compact"
                    variant="outlined"
                    hide-details="auto"
                    :disabled="lockedByActiveAttempt"
                    data-testid="save-as-archive-input"
                    @update:model-value="markDraftEdited"
                  />
                  <v-text-field
                    v-else
                    v-model="destinationForm.archive"
                    label="Destination archive"
                    density="compact"
                    variant="outlined"
                    hide-details="auto"
                    :disabled="lockedByActiveAttempt"
                    data-testid="save-as-archive-input"
                    @update:model-value="markDraftEdited"
                  />
                </v-col>
                <v-col cols="12" sm="4">
                  <v-text-field
                    v-model="destinationForm.entryId"
                    label="Destination entry ID"
                    density="compact"
                    variant="outlined"
                    hide-details="auto"
                    :disabled="lockedByActiveAttempt"
                    data-testid="save-as-entry-input"
                    @update:model-value="markDraftEdited"
                  />
                </v-col>
                <v-col cols="12" sm="4">
                  <v-text-field
                    v-model="destinationForm.renditionName"
                    label="Rendition slot name"
                    density="compact"
                    variant="outlined"
                    hide-details="auto"
                    :disabled="lockedByActiveAttempt"
                    data-testid="save-as-rendition-input"
                    @update:model-value="onRenditionNameEdited"
                  />
                </v-col>
                <v-col cols="12" sm="6">
                  <v-select
                    v-model="destinationForm.preconditionMode"
                    :items="preconditionModeItems"
                    item-title="title"
                    item-value="value"
                    label="Destination version precondition"
                    density="compact"
                    variant="outlined"
                    hide-details="auto"
                    :disabled="lockedByActiveAttempt"
                    data-testid="save-as-precondition-select"
                    @update:model-value="markDraftEdited"
                  />
                </v-col>
                <v-col v-if="destinationForm.preconditionMode === 'expected-version'" cols="12" sm="6">
                  <v-text-field
                    v-model="destinationForm.expectedDestinationVersionText"
                    label="Expected destination version"
                    density="compact"
                    variant="outlined"
                    hide-details="auto"
                    inputmode="numeric"
                    :disabled="lockedByActiveAttempt"
                    data-testid="save-as-expected-version-input"
                    @update:model-value="markDraftEdited"
                  />
                </v-col>
              </v-row>
            </div>
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

        <!-- RPC / Conflict / Unsettled alert -->
        <v-alert
          v-if="classifiedError !== null"
          :type="classifiedError.kind === 'unsettled' ? 'warning' : 'error'"
          variant="tonal"
          :role="classifiedError.kind === 'unsettled' ? 'status' : 'alert'"
          class="mt-3"
          data-testid="save-as-rpc-error"
        >
          <div class="font-weight-medium">
            <template v-if="classifiedError.kind === 'unsettled'">
              Outcome unresolved ({{ classifiedError.codeLabel }}) — rendition commit is not yet confirmed
            </template>
            <template v-else-if="classifiedError.kind === 'conflict'">
              Version conflict ({{ classifiedError.codeLabel }})
            </template>
            <template v-else>
              Save-as failed ({{ classifiedError.codeLabel }})
            </template>
          </div>
          <div class="text-body-2 mt-1">{{ classifiedError.message }}</div>
          <div v-if="activeOperationId !== null" class="text-caption mt-1">
            Operation ID: <code>{{ activeOperationId }}</code>
          </div>
          <div class="d-flex flex-wrap ga-2 mt-2">
            <v-btn
              v-if="classifiedError.canReconcile && activeOperationId !== null"
              size="small"
              variant="tonal"
              :loading="reconciling"
              data-testid="save-as-reconcile-btn"
              @click="refreshJobStatus"
            >
              Refresh status
            </v-btn>
            <v-btn
              v-if="classifiedError.canReconcile && frozenRequest !== null"
              size="small"
              variant="outlined"
              :loading="submitting"
              data-testid="save-as-retry-same-btn"
              @click="retrySameOperation"
            >
              Retry same operation
            </v-btn>
            <v-btn
              v-if="classifiedError.kind === 'conflict'"
              size="small"
              variant="tonal"
              data-testid="save-as-reload-source-btn"
              @click="emit('reloadSource')"
            >
              Reload source
            </v-btn>
            <v-btn
              v-if="classifiedError.kind === 'unauthenticated'"
              size="small"
              variant="tonal"
              @click="signIn"
            >
              Sign in
            </v-btn>
          </div>
        </v-alert>

        <!-- Sink job status card -->
        <v-alert
          v-if="jobSummary !== null"
          :type="jobAlertType"
          variant="tonal"
          role="status"
          aria-live="polite"
          class="mt-3"
          data-testid="save-as-job-status"
          :data-job-phase="jobSummary.phase"
        >
          <div class="d-flex align-center justify-space-between flex-wrap ga-2">
            <span class="font-weight-medium">Job status: {{ jobSummary.stateLabel }}</span>
            <span class="text-caption">Operation <code>{{ jobSummary.operationId }}</code></span>
          </div>

          <v-progress-linear
            v-if="jobSummary.phase === 'queued' || jobSummary.phase === 'running'"
            indeterminate
            class="my-2"
            aria-label="Sink conversion job in progress"
          />

          <div v-if="jobSummary.phase === 'succeeded'" class="text-body-2 mt-1" data-testid="save-as-job-receipt">
            Committed <code>{{ jobSummary.destinationRendition?.name }}</code>
            ({{ byteSize(jobSummary.bytesCommitted) }})
            at version <strong>v{{ jobSummary.destinationVersion.toString() }}</strong>
            on <code>{{ jobSummary.destinationAddress?.archive }} / {{ jobSummary.destinationAddress?.entryId }}</code>.
          </div>

          <div v-if="jobSummary.phase === 'unresolved'" class="text-body-2 mt-1">
            The server has not yet confirmed whether this rendition committed. Refresh status or retry the same operation before assuming completion.
          </div>

          <div v-if="jobSummary.failureMessage" class="text-body-2 mt-1" data-testid="save-as-job-failure">
            {{ jobSummary.failureMessage }}
            <span v-if="jobSummary.failureReasonCode" class="text-caption">({{ jobSummary.failureReasonCode }})</span>
          </div>

          <div class="d-flex flex-wrap ga-2 mt-2">
            <v-btn
              v-if="jobSummary.canReconcile"
              size="small"
              variant="tonal"
              :loading="reconciling"
              data-testid="save-as-refresh-job-btn"
              @click="refreshJobStatus"
            >
              Refresh status
            </v-btn>
            <v-btn
              v-if="jobSummary.canCancel"
              size="small"
              color="warning"
              variant="tonal"
              :loading="cancelling"
              data-testid="save-as-cancel-job-btn"
              @click="cancelActiveJob"
            >
              Cancel job
            </v-btn>
            <v-btn
              v-if="jobSummary.canRetrySameOperation && frozenRequest !== null"
              size="small"
              variant="outlined"
              :loading="submitting"
              data-testid="save-as-retry-job-btn"
              @click="retrySameOperation"
            >
              Retry same operation
            </v-btn>
            <v-btn
              v-if="jobSummary.isConflict"
              size="small"
              variant="tonal"
              data-testid="save-as-conflict-reload-btn"
              @click="emit('reloadSource')"
            >
              Reload source
            </v-btn>
            <v-btn
              v-if="jobSummary.phase === 'succeeded' && destinationEntryLink !== null"
              size="small"
              variant="tonal"
              :to="destinationEntryLink"
              data-testid="save-as-open-entry-btn"
              @click="closeDialog"
            >
              Open destination entry
            </v-btn>
          </div>
        </v-alert>
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
import type { RouteLocationRaw } from 'vue-router'
import RpcFailureAlert from '@/components/repository/RpcFailureAlert.vue'
import FormatIcon from '@/components/repository/FormatIcon.vue'
import { ParquetMaskStrategy } from '@/gen/ai/protomolt/proto/emit/parquet/v1/parquet_renderer_pb'
import { ListArchivesRequestSchema } from '@/gen/ai/protomolt/proto/repo/archive/v1/archive_service_pb'
import type { PublishRepositorySinkRequest, SinkSource } from '@/gen/ai/protomolt/proto/repo/sink/v1/repository_sink_pb'
import { REPOSITORY_SIGN_IN, useRepositoryClients } from '@/repository/clients'
import { byteSize } from '@/repository/formatCore'
import { isOwnCancellation, rpcFailure, type RpcFailure } from '@/repository/rpcErrorCore'
import {
  buildArchiveRenditionSinkSource,
  buildCancelRepositorySinkJobRequest,
  buildDocumentPartSinkSource,
  buildGetRepositorySinkJobRequest,
  buildListSaveAsCapabilitiesRequest,
  buildPublishSaveAsRequest,
  classifySinkRpcError,
  describeSaveAsCapabilities,
  describeSinkJob,
  SaveAsInputError,
  type ArchiveRenditionSourceInput,
  type ClassifiedSinkError,
  type DocumentPartSourceInput,
  type ParquetOptionsFormInput,
  type ResolvedOccurrenceOption,
  type SaveAsCapabilitiesView,
  type SaveAsCapabilityItem,
  type SaveAsDestinationInput,
  type SinkJobSummary,
} from '@/repository/saveAsCore'
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
const jobScope = useCallScope()

const loadingCapabilities = ref(false)
const capabilitiesFailure = ref<RpcFailure | null>(null)
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

const submitting = ref(false)
const reconciling = ref(false)
const cancelling = ref(false)
const inputError = ref<string | null>(null)
const classifiedError = ref<ClassifiedSinkError | null>(null)
const activeOperationId = ref<string | null>(null)
const frozenRequest = shallowRef<PublishRepositorySinkRequest | null>(null)
const jobSummary = shallowRef<SinkJobSummary | null>(null)
let pollTimer: ReturnType<typeof setTimeout> | null = null

const maskStrategyItems = [
  { title: 'Unspecified (none unless classes set)', value: ParquetMaskStrategy.UNSPECIFIED },
  { title: 'Redact (null out masked columns)', value: ParquetMaskStrategy.REDACT },
  { title: 'Remove (omit masked columns)', value: ParquetMaskStrategy.REMOVE },
]

const preconditionModeItems = [
  { title: 'Match expected destination version', value: 'expected-version' as const },
  { title: 'Create new archive entry only', value: 'create-only' as const },
  { title: 'Any current destination version', value: 'unconditional' as const },
]

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

const jobAlertType = computed<'info' | 'success' | 'warning' | 'error'>(() => {
  const j = jobSummary.value
  if (j === null) return 'info'
  if (j.phase === 'succeeded') return 'success'
  if (j.phase === 'queued' || j.phase === 'running') return 'info'
  if (j.phase === 'unresolved' || j.phase === 'cancelled') return 'warning'
  return 'error'
})

const destinationEntryLink = computed<RouteLocationRaw | null>(() => {
  const j = jobSummary.value
  if (j === null || j.phase !== 'succeeded') return null
  const addr = j.destinationAddress
  if (addr === null) return null
  return {
    name: 'repository-archive-entry',
    query: {
      account: addr.accountId,
      archive: addr.archive,
      entry: addr.entryId,
      version: j.destinationVersion > 0n ? j.destinationVersion.toString() : undefined,
    },
  }
})

function clearPollTimer() {
  if (pollTimer !== null) {
    clearTimeout(pollTimer)
    pollTimer = null
  }
}

function scheduleJobPoll() {
  clearPollTimer()
  const j = jobSummary.value
  if (!props.open || j === null || (j.phase !== 'queued' && j.phase !== 'running')) return
  pollTimer = setTimeout(() => {
    pollTimer = null
    void refreshJobStatus()
  }, 500)
}

function resetAttemptState() {
  clearPollTimer()
  inputError.value = null
  classifiedError.value = null
  activeOperationId.value = null
  frozenRequest.value = null
  jobSummary.value = null
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
    if (destinationForm.archive === '' && names.length > 0) {
      destinationForm.archive = names[0]!
    }
  } catch {
    // Non-fatal: operator can still type the destination archive directly.
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

function applyJobUpdate(summary: SinkJobSummary, emitOnSuccess: boolean) {
  const alreadySucceeded = jobSummary.value?.phase === 'succeeded'
  jobSummary.value = summary
  if (summary.phase === 'succeeded') {
    classifiedError.value = null
    clearPollTimer()
    if (emitOnSuccess && !alreadySucceeded) {
      emit('saved', summary)
    }
  } else if (summary.phase === 'queued' || summary.phase === 'running') {
    classifiedError.value = null
    scheduleJobPoll()
  } else {
    clearPollTimer()
  }
}

async function submitSaveAs() {
  if (submitting.value || selectedCapability.value === null) return
  inputError.value = null
  classifiedError.value = null
  jobSummary.value = null

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
  frozenRequest.value = request
  await executePublish(request)
}

async function retrySameOperation() {
  if (submitting.value || frozenRequest.value === null) return
  classifiedError.value = null
  await executePublish(frozenRequest.value)
}

async function executePublish(request: PublishRepositorySinkRequest) {
  const signal = jobScope.next()
  submitting.value = true
  try {
    const response = await clients.repositorySink.publishRepositorySink(request, { signal })
    if (response.job === undefined) {
      classifiedError.value = {
        kind: 'unsettled',
        codeLabel: 'EmptyJobResponse',
        message: 'Server returned an empty job envelope; refresh status to inspect recorded state.',
        canReconcile: true,
      }
      return
    }
    applyJobUpdate(describeSinkJob(response.job), true)
  } catch (error) {
    if (!isOwnCancellation(error, signal)) {
      classifiedError.value = classifySinkRpcError(error)
    }
  } finally {
    submitting.value = false
  }
}

async function refreshJobStatus() {
  const opId = activeOperationId.value
  if (opId === null || reconciling.value) return
  const signal = jobScope.next()
  reconciling.value = true
  try {
    const response = await clients.repositorySink.getRepositorySinkJob(
      buildGetRepositorySinkJobRequest(props.accountId, opId),
      { signal },
    )
    if (response.job !== undefined) {
      applyJobUpdate(describeSinkJob(response.job), true)
    }
  } catch (error) {
    if (!isOwnCancellation(error, signal)) {
      classifiedError.value = classifySinkRpcError(error)
    }
  } finally {
    reconciling.value = false
  }
}

async function cancelActiveJob() {
  const opId = activeOperationId.value
  if (opId === null || cancelling.value) return
  clearPollTimer()
  const signal = jobScope.next()
  cancelling.value = true
  try {
    const response = await clients.repositorySink.cancelRepositorySinkJob(
      buildCancelRepositorySinkJobRequest(props.accountId, opId, 'Cancelled from console Save-as dialog'),
      { signal },
    )
    if (response.job !== undefined) {
      applyJobUpdate(describeSinkJob(response.job), false)
    }
  } catch (error) {
    if (!isOwnCancellation(error, signal)) {
      classifiedError.value = classifySinkRpcError(error)
    }
  } finally {
    cancelling.value = false
  }
}

function closeDialog() {
  if (submitting.value || cancelling.value || reconciling.value) return
  clearPollTimer()
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
    } else {
      clearPollTimer()
    }
  },
  { immediate: true },
)

onBeforeUnmount(() => {
  clearPollTimer()
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
