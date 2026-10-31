<template>
  <div>
    <v-card class="mb-4">
      <v-card-title class="text-subtitle-1 d-flex align-center ga-2">
        <v-btn icon="mdi-arrow-left" size="small" variant="text" aria-label="Account documents"
               :to="form.accountId ? { name: 'repository-account', params: { accountId: form.accountId } } : { name: 'repository' }" />
        <v-icon size="small">mdi-upload</v-icon>
        Upload a file
      </v-card-title>
      <v-card-text class="text-body-2 text-medium-emphasis pt-0">
        The file streams to the repository host and is saved as an intake document's raw file. The operation ID is chosen
        before anything is sent, so the outcome can be looked up even if the answer is lost.
      </v-card-text>
    </v-card>

    <v-card class="mb-4">
      <v-card-text>
        <div v-if="availableWriterScopes.length > 0" class="d-flex align-center flex-wrap ga-1 mb-2" data-testid="up-writer-picks">
          <span class="text-caption text-medium-emphasis mr-1">Session writer scopes:</span>
          <v-chip
            v-for="ws in availableWriterScopes"
            :key="`${ws.accountId}/${ws.datasourceId}/${ws.connectorId ?? ''}`"
            size="x-small"
            color="primary"
            variant="tonal"
            class="text-mono"
            :data-testid="`up-pick-scope-${ws.datasourceId}`"
            @click="pickWriterScope(ws.accountId, ws.datasourceId, ws.connectorId)"
          >
            {{ ws.accountId }} / {{ scopeLabel(ws) }}
          </v-chip>
        </div>
        <v-row density="compact">
          <v-col cols="12" md="4">
            <v-text-field v-model="form.accountId" label="Account ID" density="compact"
                          autocorrect="off" autocapitalize="off" spellcheck="false" data-testid="up-account" />
          </v-col>
          <v-col cols="12" md="4">
            <v-text-field v-model="form.datasourceId" label="Datasource ID" density="compact"
                          autocorrect="off" autocapitalize="off" spellcheck="false" data-testid="up-datasource" />
          </v-col>
          <v-col cols="12" md="4">
            <v-combobox v-model="form.drive" :items="driveNames" label="Drive" density="compact" data-testid="up-drive" />
          </v-col>
          <v-col cols="12" md="4">
            <v-text-field v-model="form.docId" label="Document ID (blank: derived from the file's SHA-256)" density="compact"
                          autocorrect="off" autocapitalize="off" spellcheck="false" data-testid="up-doc" />
          </v-col>
          <v-col cols="12" md="3">
            <v-text-field
              v-model="pathPrefix"
              label="Auto-ID path prefix (optional)"
              density="compact"
              placeholder="e.g. reports/2026"
              autocorrect="off"
              autocapitalize="off"
              spellcheck="false"
              data-testid="upload-path-prefix"
            />
          </v-col>
          <v-col cols="12" md="2">
            <v-text-field v-model="form.expectedRevision" label="Expected revision (blank: a new document)" density="compact"
                          autocorrect="off" autocapitalize="off" spellcheck="false" data-testid="up-expected" />
          </v-col>
          <v-col cols="12" md="1">
            <v-text-field v-model="form.connectorId" label="Connector ID" density="compact"
                          autocorrect="off" autocapitalize="off" spellcheck="false" data-testid="up-connector" />
          </v-col>
          <v-col cols="12" md="2"><v-text-field v-model="form.contentType" label="Content type" density="compact" /></v-col>
        </v-row>
        <v-alert v-if="writer" :type="writer.covered ? 'info' : 'warning'" variant="tonal" density="compact" class="mb-3"
                 data-testid="up-writer">
          <template v-if="writer.covered">
            Your session (<span class="text-mono">{{ writer.principal }}</span>) may save as {{ writer.covered }}.
          </template>
          <template v-else>
            Your session (<span class="text-mono">{{ writer.principal }}</span>) has no writer scope for this
            {{ writer.creating ? 'new document' : 'revision' }}, so the host will refuse it with WRITE_DENIED.
            <span v-if="writer.scopes.length > 0">In account {{ form.accountId }} it may save as: {{ writer.scopes.join('; ') }}.</span>
            <span v-else>Its account binding lists no intake writer in account {{ form.accountId }}.</span>
          </template>
        </v-alert>
        <div v-if="sessionFailure" class="text-caption text-medium-emphasis mb-2" data-testid="up-session-failure">
          Your session's writer scopes could not be read: {{ sessionFailure }}
        </div>
        <v-sheet
          rounded
          border
          class="pa-3 mb-2"
          :class="{ 'bg-surface-light': dragActive }"
          data-testid="upload-dropzone"
          @dragover.prevent="dragActive = true"
          @dragleave.prevent="dragActive = false"
          @drop.prevent="dropFiles"
        >
          <div class="d-flex align-center flex-wrap ga-2">
            <v-icon size="small" color="primary">mdi-cloud-upload-outline</v-icon>
            <span class="text-caption text-medium-emphasis">Drag &amp; drop one or more files here, or choose from disk:</span>
            <input ref="fileInput" type="file" multiple :disabled="busy || batchBusy" data-testid="up-file" @change="pickFile" />
          </div>
          <div v-if="file && batchQueue.length <= 1" class="d-flex align-center flex-wrap ga-2 text-caption text-mono mt-1">
            <span>{{ file.name }} · {{ byteSize(file.size) }} · {{ file.type || 'no type' }}</span>
            <v-chip
              size="x-small"
              color="primary"
              variant="tonal"
              data-testid="up-use-filename"
              @click="fillDocIdFromFilename"
            >
              Use filename as Document ID ({{ deriveDocIdFromFilename(pathPrefix, file.name) }})
            </v-chip>
          </div>
        </v-sheet>

        <v-sheet v-if="batchQueue.length > 1" rounded border class="pa-3 mb-3" data-testid="batch-upload-panel">
          <div class="d-flex align-center flex-wrap ga-2 mb-2">
            <span class="text-subtitle-2">Batch upload queue ({{ batchQueue.length }} files)</span>
            <v-spacer />
            <v-btn
              size="small"
              color="primary"
              :loading="batchBusy"
              :disabled="busy || batchBusy || batchQueue.every((item) => item.status === 'SAVED')"
              data-testid="batch-upload-submit"
              @click="startBatchUpload"
            >
              Upload all ({{ batchQueue.length }})
            </v-btn>
          </div>
          <v-table density="compact" data-testid="batch-upload-table">
            <thead>
              <tr>
                <th scope="col">File</th>
                <th scope="col">Size</th>
                <th scope="col">Document ID</th>
                <th scope="col">Status</th>
                <th scope="col">Outcome</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="(item, idx) in batchQueue" :key="idx" :data-testid="`batch-row-${idx}`">
                <td class="text-mono text-caption">{{ item.file.name }}</td>
                <td class="text-mono text-caption">{{ byteSize(item.file.size) }}</td>
                <td>
                  <v-text-field
                    v-model="item.docId"
                    :disabled="batchBusy || item.plan !== null"
                    density="compact"
                    hide-details
                    variant="underlined"
                    autocorrect="off"
                    autocapitalize="off"
                    spellcheck="false"
                    :data-testid="`batch-doc-id-${idx}`"
                  />
                </td>
                <td>
                  <v-chip
                    size="x-small"
                    :color="item.status === 'SAVED' ? 'success' : item.status === 'FAILED' ? 'error' : item.status === 'READY' ? 'default' : 'primary'"
                    variant="tonal"
                    class="text-mono"
                    :data-testid="`batch-status-${idx}`"
                  >
                    {{ item.status }}
                  </v-chip>
                </td>
                <td class="text-caption">
                  <div class="d-flex align-center flex-wrap ga-2">
                    <span v-if="item.message" class="text-mono">{{ item.message }}</span>
                    <router-link
                      v-if="item.operationId"
                      :to="{ name: 'repository-lookup', query: { account: item.plan?.accountId, op: item.operationId } }"
                      class="text-mono text-caption"
                      :data-testid="`batch-op-${idx}`"
                    >
                      {{ item.operationId.slice(0, 12) }}
                    </router-link>
                    <v-btn
                      v-if="item.address && item.revision !== null"
                      size="x-small"
                      variant="tonal"
                      :to="revisionRoute(item.address, item.revision)"
                      :data-testid="`batch-open-${idx}`"
                    >
                      Open
                    </v-btn>
                  </div>
                </td>
              </tr>
            </tbody>
          </v-table>
        </v-sheet>

        <RpcFailureAlert v-if="drivesFailure" :failure="drivesFailure" title="Listing drives" class="mt-2" />
        <v-alert v-if="inputError" type="warning" variant="tonal" density="compact" class="mt-2" data-testid="up-input-error">
          {{ inputError }}
        </v-alert>
        <div class="d-flex ga-2 mt-3">
          <v-btn color="primary" size="small" :disabled="busy || batchBusy" data-testid="up-start" @click="start">Upload</v-btn>
          <v-btn v-if="busy" size="small" variant="text" data-testid="up-cancel" @click="cancel">Cancel</v-btn>
        </div>
      </v-card-text>
    </v-card>

    <v-card v-if="plan || phase === 'hashing'" class="mb-4" data-testid="up-progress">
      <v-card-text>
        <div v-if="plan" class="mb-2">
          Operation ID <span class="text-mono" data-testid="up-operation">{{ plan.operationId }}</span>
          <div class="text-caption text-mono text-medium-emphasis">SHA-256 {{ plan.sha256 }}</div>
        </div>
        <div v-if="phase === 'hashing'" class="text-caption">Hashing the file: {{ hashed }} of {{ file?.size }} bytes</div>
        <template v-if="sent !== null">
          <v-progress-linear :model-value="sentPercent" class="mb-1" />
          <div class="text-caption text-mono" data-testid="up-sent">{{ sent }} of {{ plan?.sizeBytes }} bytes sent</div>
        </template>

        <v-alert v-if="result?.kind === 'capacity'" type="warning" variant="tonal" density="compact" class="mt-3" data-testid="up-capacity">
          <div class="font-weight-medium">The repository is at capacity; nothing was saved</div>
          <div class="text-body-2">{{ result.failure.code }}: {{ result.failure.message }}</div>
          <v-btn size="small" variant="tonal" class="mt-2" data-testid="up-retry" :disabled="busy" @click="send">
            Retry with the same operation ID
          </v-btn>
        </v-alert>
        <template v-if="result?.kind === 'refused'">
          <RpcFailureAlert :failure="result.failure" :title="`The upload route refused the file (HTTP ${result.httpStatus})`" class="mt-3" />
          <div v-if="result.error?.detail.case === 'conflict'" class="text-body-2" data-testid="up-conflict">
            Revision {{ result.error.detail.value.expected?.expected.value ?? '(none)' }} was expected; the current revision is
            {{ result.error.detail.value.currentRevision === 0n ? 'none (no such document)' : result.error.detail.value.currentRevision }}.
          </div>
          <div v-if="result.error?.detail.case === 'rejection'" class="text-body-2" data-testid="up-rejection">
            {{ rejectionReason(result.error.detail.value.reason) }}: {{ result.error.detail.value.message }}
            <div v-for="(v, i) in result.error.detail.value.violations" :key="i" class="text-caption text-mono">
              {{ v.ruleId }} {{ v.fieldPath }} {{ v.message }}
            </div>
          </div>
        </template>
        <template v-if="result?.kind === 'unauthenticated'">
          <RpcFailureAlert :failure="result.failure" title="The upload route did not accept your credential" class="mt-3" />
          <v-btn size="small" variant="tonal" data-testid="up-sign-in" @click="signIn">Sign in</v-btn>
        </template>
        <v-alert v-if="result?.kind === 'unsettled'" type="info" variant="tonal" density="compact" class="mt-3" data-testid="up-unsettled">
          {{ result.reason }} The save may or may not have committed; its outcome is below.
          <div v-if="result.failure" class="text-caption text-mono">{{ result.failure.code }}: {{ result.failure.message }}</div>
          <v-btn size="small" variant="tonal" class="mt-2" :disabled="busy" data-testid="up-resend" @click="send">
            Send again with the same operation ID
          </v-btn>
        </v-alert>
        <RpcFailureAlert v-if="failure" :failure="failure" title="Uploading" class="mt-3" />

        <div v-if="outcome" class="mt-3" :data-testid="`up-outcome-${outcome.state}`">
          <v-alert v-if="outcome.state === 'committed'" type="success" variant="tonal" density="compact">
            <div class="font-weight-medium">Committed as revision {{ outcome.accession.revision }}</div>
            <div class="text-body-2">
              <span class="text-mono">{{ outcome.accession.address?.docId }}</span>, recorded {{ outcome.recordedAt }}.
              <span v-if="replayed">This operation ID was already committed; the recorded outcome was returned.</span>
            </div>
            <v-btn v-if="outcome.accession.address" size="small" variant="tonal" class="mt-2" data-testid="up-open"
                   :to="revisionRoute(outcome.accession.address, outcome.accession.revision)">Open this revision</v-btn>
          </v-alert>
          <v-alert v-else-if="outcome.state === 'rejected'" type="error" variant="tonal" density="compact">
            <div class="font-weight-medium">Rejected: {{ rejectionReason(outcome.rejection.reason) }}</div>
            <div class="text-body-2">{{ outcome.rejection.message }}</div>
          </v-alert>
          <v-alert v-else-if="outcome.state === 'none'" type="info" variant="tonal" density="compact">
            <div class="font-weight-medium">No outcome recorded yet (asked {{ attempts }} times)</div>
            <div class="text-body-2">This does not mean the save failed.</div>
            <v-btn size="small" variant="tonal" class="mt-2" :disabled="busy" data-testid="up-ask-again" @click="follow">Ask again</v-btn>
          </v-alert>
          <v-alert v-else type="warning" variant="tonal" density="compact">
            <div class="font-weight-medium">The outcome lookup is unavailable</div>
            <div class="text-body-2">{{ outcome.failure.message }}</div>
            <v-btn size="small" variant="tonal" class="mt-2" :disabled="busy" @click="follow">Ask again</v-btn>
          </v-alert>
        </div>
      </v-card-text>
    </v-card>

    <OperationOutcomeInspector v-if="form.accountId" :key="form.accountId" :account-id="form.accountId"
                               :initial-operation-id="accountFromQuery ? queryText('operationId') : ''" />
  </div>
</template>

<script setup lang="ts">
import { computed, inject, onMounted, reactive, ref, shallowRef, watch } from 'vue'
import { useRoute } from 'vue-router'
import { create } from '@bufbuild/protobuf'
import OperationOutcomeInspector from '@/components/repository/OperationOutcomeInspector.vue'
import RpcFailureAlert from '@/components/repository/RpcFailureAlert.vue'
import type { NodeAddress } from '@/gen/ai/protomolt/proto/repo/v1/address_pb'
import { ListDrivesRequestSchema } from '@/gen/ai/protomolt/proto/repo/v1/drive_pb'
import { RejectionReasonSchema } from '@/gen/ai/protomolt/proto/repo/v2/errors_pb'
import type { ConsoleSessionStatus } from '@/gen/ai/protomolt/proto/authz/v1/console_session_pb'
import { REPOSITORY_UPLOAD_REQUEST, useRepositoryClients, useRepositorySession, useSignIn } from '@/repository/clients'
import { byteSize, enumLabel, instant } from '@/repository/formatCore'
import type { OutcomeAnswer } from '@/repository/lookupCore'
import { isOwnCancellation, rpcFailure, type RpcFailure } from '@/repository/rpcErrorCore'
import {
  UploadInputError,
  deriveDocIdFromFilename,
  emptyUploadForm,
  fileSha256,
  followOutcome,
  newOperationId,
  planUpload,
  sendUpload,
  type UploadPlan,
  type UploadSendResult,
} from '@/repository/uploadCore'
import { coveringScope, scopeLabel, scopesIn } from '@/repository/sessionWritersCore'
import { useCallScope } from '@/repository/useCallScope'

const clients = useRepositoryClients()
const sessions = useRepositorySession()
const signIn = useSignIn()
const newRequest = inject(REPOSITORY_UPLOAD_REQUEST, () => new XMLHttpRequest())
const route = useRoute()
const scope = useCallScope()
const batchScope = useCallScope()
const drivesScope = useCallScope()

function queryText(name: string): string {
  const value = route.query[name]
  return typeof value === 'string' ? value : ''
}

const form = reactive(emptyUploadForm(queryText('account')))
const accountFromQuery = computed(() => {
  const account = queryText('account')
  return account !== '' && form.accountId === account
})
form.datasourceId = queryText('datasource')
form.docId = queryText('doc')
form.expectedRevision = queryText('expected')

const pathPrefix = ref('')
const lastAutoDocId = ref('')
const dragActive = ref(false)
const file = shallowRef<File | null>(null)

export interface BatchQueueItem {
  file: File
  docId: string
  status: 'READY' | 'HASHING' | 'UPLOADING' | 'SAVED' | 'FAILED' | 'UNSETTLED'
  operationId: string | null
  plan: UploadPlan | null
  message: string
  address: NodeAddress | null
  revision: bigint | null
}

const batchQueue = ref<BatchQueueItem[]>([])
const batchBusy = ref(false)
const driveNames = ref<string[]>([])
const drivesFailure = ref<RpcFailure | null>(null)
const inputError = ref<string | null>(null)
const phase = ref<'idle' | 'hashing' | 'sending' | 'following'>('idle')
const busy = computed(() => phase.value !== 'idle')
const hashed = ref(0)
const plan = shallowRef<UploadPlan | null>(null)
const sent = ref<number | null>(null)
const result = shallowRef<UploadSendResult | null>(null)
const failure = ref<RpcFailure | null>(null)
const outcome = shallowRef<OutcomeAnswer | null>(null)
const attempts = ref(0)
const replayed = ref(false)

const sentPercent = computed(() =>
  plan.value && sent.value !== null && plan.value.sizeBytes > 0 ? (sent.value * 100) / plan.value.sizeBytes : 100,
)

function stageSelectedFiles(selected: File[]) {
  if (busy.value || batchBusy.value) return
  file.value = selected.length > 0 ? selected[0] : null
  if (selected.length > 1) {
    batchQueue.value = selected.map((f) => ({
      file: f,
      docId: deriveDocIdFromFilename(pathPrefix.value, f.name),
      status: 'READY',
      operationId: null,
      plan: null,
      message: '',
      address: null,
      revision: null,
    }))
  } else {
    batchQueue.value = []
    if (selected.length === 1 && pathPrefix.value.trim() !== '') {
      const current = form.docId.trim()
      if (current === '' || current === lastAutoDocId.value) {
        const derived = deriveDocIdFromFilename(pathPrefix.value, selected[0].name)
        form.docId = derived
        lastAutoDocId.value = derived
      }
    }
  }
}

watch(pathPrefix, (nextPrefix, prevPrefix) => {
  if (file.value !== null && batchQueue.value.length <= 1) {
    const current = form.docId.trim()
    const prevDerived = deriveDocIdFromFilename(prevPrefix ?? '', file.value.name)
    if ((current === '' && nextPrefix.trim() !== '') || current === lastAutoDocId.value || current === prevDerived) {
      const next = nextPrefix.trim() === '' && lastAutoDocId.value === '' ? '' : deriveDocIdFromFilename(nextPrefix, file.value.name)
      form.docId = next
      lastAutoDocId.value = next
    }
  }
  for (const item of batchQueue.value) {
    if (item.status === 'READY' && !batchBusy.value) {
      const prevDerived = deriveDocIdFromFilename(prevPrefix ?? '', item.file.name)
      if (item.docId === prevDerived || item.docId.trim() === '') {
        item.docId = deriveDocIdFromFilename(nextPrefix, item.file.name)
      }
    }
  }
})

function fillDocIdFromFilename() {
  if (file.value === null) return
  const derived = deriveDocIdFromFilename(pathPrefix.value, file.value.name)
  form.docId = derived
  lastAutoDocId.value = derived
}

function pickFile(event: Event) {
  const files = (event.target as HTMLInputElement).files
  stageSelectedFiles(files ? Array.from(files) : [])
}

function dropFiles(event: DragEvent) {
  dragActive.value = false
  const dropped = event.dataTransfer?.files
  if (dropped && dropped.length > 0) {
    stageSelectedFiles(Array.from(dropped))
  }
}

async function startBatchUpload() {
  inputError.value = null
  if (busy.value || batchBusy.value || batchQueue.value.length === 0) return
  // Capture every destination before hashing or sending any file. Retried rows
  // keep their original plan, even when the draft form has since changed.
  const pending = batchQueue.value.filter((item) => item.status !== 'SAVED').map((item) => ({
    item,
    form: { ...form, docId: item.docId, expectedRevision: '' },
  }))
  try {
    for (const submitted of pending) {
      if (submitted.item.plan === null) {
        planUpload(submitted.form, submitted.item.file, '', '0'.repeat(64))
      }
    }
  } catch (error) {
    if (error instanceof UploadInputError) {
      inputError.value = error.message
      return
    }
    throw error
  }
  const signal = batchScope.next()
  batchBusy.value = true
  try {
    for (const { item, form: submittedForm } of pending) {
      if (signal.aborted) return
      item.status = 'HASHING'
      item.message = ''
      try {
        if (item.plan === null) {
          const sha256 = await fileSha256(item.file, signal)
          if (signal.aborted) return
          item.plan = planUpload(submittedForm, item.file, newOperationId(), sha256)
          item.operationId = item.plan.operationId
        }
        const itemPlan = item.plan
        item.status = 'UPLOADING'
        const sendResult = await sendUpload(itemPlan, newRequest, () => {}, signal)
        if (signal.aborted) return
        if (sendResult.kind === 'saved') {
          const saved = sendResult.response.answer.case === 'saved' ? sendResult.response.answer.value : undefined
          const accession = saved?.accession
          item.status = accession ? 'SAVED' : 'UNSETTLED'
          item.address = accession?.address ?? null
          item.revision = accession?.revision ?? null
          item.message = accession ? `rev ${accession.revision}` : 'The response has no saved accession; look up the operation before retrying.'
        } else if (sendResult.kind === 'unsettled') {
          item.status = 'UNSETTLED'
          item.message = `${sendResult.reason} The save may have committed; look up the operation or retry with the same operation ID.`
        } else {
          item.status = 'FAILED'
          item.message = `${sendResult.failure.code}: ${sendResult.failure.message}`
        }
      } catch (error) {
        if (signal.aborted) return
        item.status = 'FAILED'
        item.message = error instanceof Error ? error.message : String(error)
      }
    }
  } finally {
    if (!signal.aborted) batchBusy.value = false
  }
}

function rejectionReason(reason: number) {
  return enumLabel(RejectionReasonSchema, reason)
}

function revisionRoute(address: NodeAddress, revision: bigint) {
  return {
    name: 'repository-document',
    query: { account: address.accountId, graph: address.graphId, node: address.graphAddressId, doc: address.docId, revision: revision.toString() },
  }
}

async function start() {
  if (busy.value || batchBusy.value) return
  const submittedForm = { ...form }
  inputError.value = null
  failure.value = null
  result.value = null
  outcome.value = null
  plan.value = null
  sent.value = null
  replayed.value = false
  if (file.value === null) {
    inputError.value = 'File: choose a file to upload'
    return
  }
  const chosen = file.value
  const operationId = newOperationId()
  try {
    // Validate before hashing, so a blank field is named at once.
    planUpload(submittedForm, chosen, operationId, '0'.repeat(64))
  } catch (error) {
    if (error instanceof UploadInputError) {
      inputError.value = error.message
      return
    }
    throw error
  }
  const signal = scope.next()
  phase.value = 'hashing'
  hashed.value = 0
  try {
    const sha256 = await fileSha256(chosen, signal, (n) => (hashed.value = n))
    if (signal.aborted) return
    plan.value = planUpload(submittedForm, chosen, operationId, sha256)
  } catch (error) {
    if (!signal.aborted) {
      phase.value = 'idle'
      failure.value = rpcFailure(error)
    }
    return
  }
  await send()
}

async function send() {
  const current = plan.value
  if (current === null) return
  result.value = null
  failure.value = null
  outcome.value = null
  const signal = scope.next()
  phase.value = 'sending'
  sent.value = 0
  let answer: UploadSendResult
  try {
    answer = await sendUpload(current, newRequest, (n) => (sent.value = n), signal)
  } catch (error) {
    phase.value = 'idle'
    failure.value = rpcFailure(error)
    return
  }
  result.value = answer
  phase.value = 'idle'
  // The screen went away while the send ran: nothing more to show or ask.
  if (signal.aborted && answer.kind !== 'unsettled') return
  if (answer.kind === 'saved') {
    const saved = answer.response.answer.case === 'saved' ? answer.response.answer.value : undefined
    const accession = saved?.accession
    if (saved === undefined || accession === undefined) {
      failure.value = rpcFailure(new Error('The upload route answered 200 without the saved accession.'))
      return
    }
    // The route's own answer is the committed revision; no lookup is needed.
    replayed.value = saved.replayed
    outcome.value = { state: 'committed', accession, recordedAt: instant(accession.committedAt) }
    return
  }
  // An unsettled send may still have committed: ask what was recorded.
  if (answer.kind === 'unsettled') await follow()
}

async function follow() {
  const current = plan.value
  if (current === null || scope.closed) return
  const signal = scope.next()
  phase.value = 'following'
  try {
    outcome.value = await followOutcome(clients.documents, current.accountId, current.operationId, signal, (answer, attempt) => {
      outcome.value = answer
      attempts.value = attempt
    })
  } catch (error) {
    if (!isOwnCancellation(error, signal)) failure.value = rpcFailure(error)
  } finally {
    phase.value = 'idle'
  }
}

function cancel() {
  scope.next()
  phase.value = 'idle'
}

// The signed-in session's writer scopes, shown before the send. A browser
// without a session shows no scopes: the upload runs with whatever credential
// the request carries.
const session = shallowRef<ConsoleSessionStatus | null>(null)
const sessionFailure = ref<string | null>(null)
const availableWriterScopes = computed(() => {
  const s = session.value
  if (s === null || !s.authenticated) return []
  const acct = form.accountId.trim()
  return acct !== '' ? scopesIn(s.intakeWriters, acct) : s.intakeWriters
})

function pickWriterScope(accountId: string, datasourceId: string, connectorId?: string) {
  form.accountId = accountId
  form.datasourceId = datasourceId
  form.connectorId = connectorId ?? ''
}

const writer = computed(() => {
  const s = session.value
  if (s === null || !s.authenticated || form.accountId.trim() === '' || form.datasourceId.trim() === '') return null
  const creating = form.expectedRevision.trim() === '' || form.expectedRevision.trim() === '0'
  const covered = coveringScope(s.intakeWriters, {
    accountId: form.accountId,
    datasourceId: form.datasourceId,
    connectorId: form.connectorId,
    creating,
  })
  return {
    principal: s.principal,
    creating,
    covered: covered === null ? null : scopeLabel(covered),
    scopes: scopesIn(s.intakeWriters, form.accountId).map(scopeLabel),
  }
})

onMounted(async () => {
  try {
    session.value = await sessions.status()
  } catch (error) {
    // Advisory only: the host still decides the upload and reports its own refusal.
    sessionFailure.value = error instanceof Error ? error.message : String(error)
  }
})

onMounted(async () => {
  if (form.accountId === '') return
  const signal = drivesScope.next()
  try {
    const names: string[] = []
    let token = ''
    do {
      const page = await clients.drives.listDrives(
        create(ListDrivesRequestSchema, { accountId: form.accountId, limit: 100, continuationToken: token }),
        { signal },
      )
      names.push(...page.drives.map((d) => d.name))
      token = page.nextContinuationToken
    } while (token !== '')
    driveNames.value = names
  } catch (error) {
    if (!isOwnCancellation(error, signal)) drivesFailure.value = rpcFailure(error)
  }
})
</script>
