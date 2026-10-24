<template>
  <div>
    <v-card class="mb-4">
      <v-card-title class="text-subtitle-1 d-flex align-center ga-2">
        <v-btn icon="mdi-arrow-left" size="small" variant="text" :to="{ name: 'repository' }" aria-label="All accounts" />
        <v-icon size="small">mdi-magnify</v-icon>
        Look up
      </v-card-title>
    </v-card>

    <v-row density="compact">
      <v-col cols="12" md="6">
        <v-card class="mb-4">
          <v-card-title class="text-subtitle-2">By call number</v-card-title>
          <v-card-text>
            <v-text-field v-model="call.accountId" label="Account ID" density="compact" data-testid="call-account" />
            <v-text-field v-model="call.graphId" label="Graph ID" density="compact" hint="intake:<account> for intake documents"
                          persistent-hint data-testid="call-graph" />
            <v-text-field v-model="call.graphAddressId" label="Graph address ID" density="compact"
                          hint="The datasource ID for intake documents" persistent-hint class="mt-2" data-testid="call-node" />
            <v-text-field v-model="call.docId" label="Document ID" density="compact" class="mt-2" data-testid="call-doc" />
            <v-text-field v-model="revisionText" label="Revision (blank for current)" density="compact" data-testid="call-revision" />
            <v-alert v-if="callError" type="warning" variant="tonal" density="compact" class="mb-2" data-testid="call-error">
              {{ callError }}
            </v-alert>
            <v-btn color="primary" size="small" data-testid="open-call" @click="openCall">Open document</v-btn>
          </v-card-text>
        </v-card>
      </v-col>

      <v-col cols="12" md="6">
        <v-card class="mb-4">
          <v-card-title class="text-subtitle-2">By operation ID</v-card-title>
          <v-card-text>
            <v-text-field v-model="opAccount" label="Account ID" density="compact" data-testid="op-account" />
            <v-text-field v-model="operationId" label="Operation ID" density="compact" data-testid="op-id" @keydown.enter="lookup" />
            <v-alert v-if="opInputError" type="warning" variant="tonal" density="compact" class="mb-2">{{ opInputError }}</v-alert>
            <v-btn color="primary" size="small" :loading="looking" data-testid="lookup-op" @click="lookup">Look up outcome</v-btn>

            <RpcFailureAlert v-if="opFailure" :failure="opFailure" title="Looking up the operation" class="mt-3" />

            <div v-if="answer" class="mt-3" :data-testid="`outcome-${answer.state}`">
              <v-alert v-if="answer.state === 'committed'" type="success" variant="tonal" density="compact">
                <div class="font-weight-medium">Committed</div>
                <div class="text-body-2">
                  Revision {{ answer.accession.revision }} of
                  <span class="text-mono">{{ answer.accession.address?.docId }}</span>
                  <span v-if="answer.accession.tombstone"> (a tombstone)</span>, recorded {{ answer.recordedAt }}.
                </div>
                <v-btn v-if="answer.accession.address" size="small" variant="tonal" class="mt-2" data-testid="open-committed"
                       :to="documentRoute(answer.accession.address, answer.accession.revision)">Open this revision</v-btn>
              </v-alert>
              <v-alert v-else-if="answer.state === 'rejected'" type="error" variant="tonal" density="compact">
                <div class="font-weight-medium">Committed as a rejection: {{ rejectionReason(answer.rejection.reason) }}</div>
                <div class="text-body-2">{{ answer.rejection.message }}</div>
                <div v-for="(v, i) in answer.rejection.violations" :key="i" class="text-caption text-mono">
                  {{ v.ruleId }} {{ v.fieldPath }} {{ v.message }}
                </div>
                <div class="text-caption mt-1">Recorded {{ answer.recordedAt }}. The save will not commit under this operation ID.</div>
              </v-alert>
              <v-alert v-else-if="answer.state === 'none'" type="info" variant="tonal" density="compact">
                <div class="font-weight-medium">No outcome recorded yet</div>
                <div class="text-body-2">
                  This does not mean the save failed: a request still in flight can commit later. Look again, or retry the
                  save with the same operation ID.
                </div>
              </v-alert>
              <v-alert v-else type="warning" variant="tonal" density="compact">
                <div class="font-weight-medium">The outcome lookup is unavailable</div>
                <div class="text-body-2">
                  The repository could not answer, so nothing is known about this operation. {{ answer.failure.message }}
                </div>
              </v-alert>
            </div>
          </v-card-text>
        </v-card>
      </v-col>
    </v-row>

    <ConceptSchemeBrowser :account-id="call.accountId || opAccount" class="mt-2" />
  </div>
</template>

<script setup lang="ts">
import { reactive, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import ConceptSchemeBrowser from '@/components/repository/ConceptSchemeBrowser.vue'
import RpcFailureAlert from '@/components/repository/RpcFailureAlert.vue'
import type { NodeAddress } from '@/gen/ai/protomolt/proto/repo/v1/address_pb'
import { RejectionReasonSchema } from '@/gen/ai/protomolt/proto/repo/v2/errors_pb'
import { useRepositoryClients } from '@/repository/clients'
import { nodeAddress, parseRevision, type CallNumber } from '@/repository/documentCore'
import { enumLabel } from '@/repository/formatCore'
import { OutcomeInputError, lookupOutcome, type OutcomeAnswer } from '@/repository/lookupCore'
import { isOwnCancellation, rpcFailure, type RpcFailure } from '@/repository/rpcErrorCore'
import { useCallScope } from '@/repository/useCallScope'

const clients = useRepositoryClients()
const route = useRoute()
const router = useRouter()
const scope = useCallScope()
const presetAccount = typeof route.query.account === 'string' ? route.query.account : ''

const call = reactive<CallNumber>({ accountId: presetAccount, graphId: '', graphAddressId: '', docId: '' })
const revisionText = ref('')
const callError = ref<string | null>(null)

const opAccount = ref(presetAccount)
const operationId = ref('')
const looking = ref(false)
const opInputError = ref<string | null>(null)
const opFailure = ref<RpcFailure | null>(null)
const answer = ref<OutcomeAnswer | null>(null)

watch(
  () => route.query.account,
  (next) => {
    const account = typeof next === 'string' ? next : ''
    if (account !== '') {
      call.accountId = account
      opAccount.value = account
    }
  },
)

function documentRoute(address: NodeAddress, revision: bigint | null) {
  const query: Record<string, string> = {
    account: address.accountId,
    graph: address.graphId,
    node: address.graphAddressId,
    doc: address.docId,
  }
  if (revision !== null) query.revision = revision.toString()
  return { name: 'repository-document', query }
}

function openCall() {
  callError.value = null
  try {
    const address = nodeAddress(call)
    void router.push(documentRoute(address, parseRevision(revisionText.value.trim())))
  } catch (error) {
    callError.value = (error as Error).message
  }
}

function rejectionReason(reason: number): string {
  return enumLabel(RejectionReasonSchema, reason)
}

async function lookup() {
  opInputError.value = null
  opFailure.value = null
  answer.value = null
  const signal = scope.next()
  looking.value = true
  try {
    const result = await lookupOutcome(clients.documents, opAccount.value, operationId.value, signal)
    if (signal.aborted) return
    answer.value = result
  } catch (error) {
    if (error instanceof OutcomeInputError) opInputError.value = error.message
    else if (!isOwnCancellation(error, signal)) opFailure.value = rpcFailure(error)
  } finally {
    if (!signal.aborted) looking.value = false
  }
}
</script>
