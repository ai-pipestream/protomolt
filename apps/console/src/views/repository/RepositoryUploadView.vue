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
        <v-row dense>
          <v-col cols="12" md="4"><v-text-field v-model="form.accountId" label="Account ID" density="compact" data-testid="up-account" /></v-col>
          <v-col cols="12" md="4"><v-text-field v-model="form.datasourceId" label="Datasource ID" density="compact" data-testid="up-datasource" /></v-col>
          <v-col cols="12" md="4">
            <v-combobox v-model="form.drive" :items="driveNames" label="Drive" density="compact" data-testid="up-drive" />
          </v-col>
          <v-col cols="12" md="4">
            <v-text-field v-model="form.docId" label="Document ID (blank: derived from the file's SHA-256)" density="compact"
                          data-testid="up-doc" />
          </v-col>
          <v-col cols="12" md="4">
            <v-text-field v-model="form.expectedRevision" label="Expected revision (blank: a new document)" density="compact"
                          data-testid="up-expected" />
          </v-col>
          <v-col cols="12" md="2"><v-text-field v-model="form.connectorId" label="Connector ID" density="compact" /></v-col>
          <v-col cols="12" md="2"><v-text-field v-model="form.contentType" label="Content type" density="compact" /></v-col>
        </v-row>
        <input ref="fileInput" type="file" data-testid="up-file" @change="pickFile" />
        <div v-if="file" class="text-caption text-mono mt-1">{{ file.name }} · {{ byteSize(file.size) }} · {{ file.type || 'no type' }}</div>
        <RpcFailureAlert v-if="drivesFailure" :failure="drivesFailure" title="Listing drives" class="mt-2" />
        <v-alert v-if="inputError" type="warning" variant="tonal" density="compact" class="mt-2" data-testid="up-input-error">
          {{ inputError }}
        </v-alert>
        <div class="d-flex ga-2 mt-3">
          <v-btn color="primary" size="small" :disabled="busy" data-testid="up-start" @click="start">Upload</v-btn>
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
        <RpcFailureAlert v-if="result?.kind === 'refused'" :failure="result.failure" :title="`The upload route refused the file (HTTP ${result.httpStatus})`" class="mt-3" />
        <v-alert v-if="result?.kind === 'no-answer'" type="info" variant="tonal" density="compact" class="mt-3" data-testid="up-no-answer">
          {{ result.reason }} The save may still commit; its outcome is below.
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
  </div>
</template>

<script setup lang="ts">
import { computed, inject, onMounted, reactive, ref, shallowRef } from 'vue'
import { useRoute } from 'vue-router'
import { create } from '@bufbuild/protobuf'
import RpcFailureAlert from '@/components/repository/RpcFailureAlert.vue'
import type { NodeAddress } from '@/gen/ai/protomolt/proto/repo/v1/address_pb'
import { ListDrivesRequestSchema } from '@/gen/ai/protomolt/proto/repo/v1/drive_pb'
import { RejectionReasonSchema } from '@/gen/ai/protomolt/proto/repo/v2/errors_pb'
import { REPOSITORY_UPLOAD_REQUEST, useRepositoryClients, useSignIn } from '@/repository/clients'
import { byteSize, enumLabel } from '@/repository/formatCore'
import type { OutcomeAnswer } from '@/repository/lookupCore'
import { isOwnCancellation, rpcFailure, type RpcFailure } from '@/repository/rpcErrorCore'
import {
  UploadInputError,
  emptyUploadForm,
  fileSha256,
  followOutcome,
  newOperationId,
  planUpload,
  sendUpload,
  type UploadPlan,
  type UploadSendResult,
} from '@/repository/uploadCore'
import { useCallScope } from '@/repository/useCallScope'

const clients = useRepositoryClients()
const signIn = useSignIn()
const newRequest = inject(REPOSITORY_UPLOAD_REQUEST, () => new XMLHttpRequest())
const route = useRoute()
const scope = useCallScope()
const drivesScope = useCallScope()

function queryText(name: string): string {
  const value = route.query[name]
  return typeof value === 'string' ? value : ''
}

const form = reactive(emptyUploadForm(queryText('account')))
form.datasourceId = queryText('datasource')
form.docId = queryText('doc')
form.expectedRevision = queryText('expected')

const file = shallowRef<File | null>(null)
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

function pickFile(event: Event) {
  const files = (event.target as HTMLInputElement).files
  file.value = files && files.length > 0 ? files[0] : null
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
    planUpload(form, chosen, operationId, '0'.repeat(64))
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
    plan.value = planUpload(form, chosen, operationId, sha256)
  } catch (error) {
    phase.value = 'idle'
    if (!signal.aborted) failure.value = rpcFailure(error)
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
  if (answer.kind === 'unauthenticated') {
    signIn()
    return
  }
  if (answer.kind === 'saved') replayed.value = answer.response.saved?.replayed ?? false
  // A send that saved, or whose answer was lost, has an outcome to look up.
  if (answer.kind === 'saved' || answer.kind === 'no-answer') await follow()
}

async function follow() {
  const current = plan.value
  if (current === null) return
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
