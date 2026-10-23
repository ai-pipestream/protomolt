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
import { computed, inject, onMounted, reactive, ref, shallowRef } from 'vue'
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
    if (signal.aborted) return
    plan.value = planUpload(form, chosen, operationId, sha256)
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
    const saved = answer.response.saved
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
