<template>
  <v-card class="mb-4" data-testid="outcome-inspector">
    <v-card-title class="text-subtitle-2 d-flex align-center ga-2">
      <v-icon size="small">mdi-clipboard-search-outline</v-icon>
      Operation outcome
    </v-card-title>
    <v-card-text>
      <v-text-field v-model="operationId" label="Operation ID" density="compact" data-testid="outcome-operation"
                    @keydown.enter="lookUp" />
      <v-alert v-if="inputError" type="warning" variant="tonal" density="compact" class="mb-2"
               data-testid="outcome-input-error">
        {{ inputError }}
      </v-alert>
      <v-btn color="primary" size="small" :loading="looking" data-testid="outcome-lookup" @click="lookUp">
        Look up outcome
      </v-btn>

      <RpcFailureAlert v-if="failure" :failure="failure" title="Looking up the operation" class="mt-3" />

      <div v-if="view" class="mt-3" :data-testid="`outcome-${view.kind}`">
        <v-alert v-if="view.kind === 'committed'" type="success" variant="tonal" density="compact">
          <div class="font-weight-medium d-flex align-center ga-2">
            Committed
            <v-chip v-if="view.newDocument" size="x-small" data-testid="outcome-new">new document</v-chip>
            <v-chip v-else size="x-small" data-testid="outcome-replaced">replaced revision {{ view.previousRevision }}</v-chip>
            <v-chip v-if="view.tombstone" size="x-small" color="warning">tombstone</v-chip>
          </div>
          <div class="text-body-2">
            Revision {{ view.revision }} of <span class="text-mono">{{ view.docId }}</span>
            <span v-if="view.datasourceId"> (datasource <span class="text-mono">{{ view.datasourceId }}</span>)</span><span v-if="view.recordedAt">, recorded {{ view.recordedAt }}</span>.
          </div>
          <v-btn v-if="view.route" size="small" variant="tonal" class="mt-2" data-testid="outcome-open"
                 :to="view.route">Open this revision</v-btn>
        </v-alert>
        <v-alert v-else-if="view.kind === 'rejected'" type="error" variant="tonal" density="compact">
          <div class="font-weight-medium">Committed as a rejection: {{ view.reason }}</div>
          <div class="text-body-2">{{ view.message }}</div>
          <div v-for="(violation, i) in view.violations" :key="i" class="text-caption text-mono">
            {{ violation.ruleId }} {{ violation.fieldPath }} {{ violation.message }}
          </div>
          <div v-if="view.recordedAt" class="text-caption mt-1">
            Recorded {{ view.recordedAt }}. The save will not commit under this operation ID.
          </div>
        </v-alert>
        <v-alert v-else-if="view.kind === 'none'" type="info" variant="tonal" density="compact">
          {{ view.explanation }}
        </v-alert>
        <v-alert v-else type="warning" variant="tonal" density="compact">
          The lookup could not answer: {{ view.failure.message }}
        </v-alert>
      </div>
    </v-card-text>
  </v-card>
</template>

<script setup lang="ts">
/**
 * Looks up one operation ID in the account and shows its durable outcome: a
 * committed accession (with a link to the revision), a recorded rejection with
 * its violations, or the no-outcome state. The account comes from the hosting
 * view. An initial operation ID (a ?operationId deep link) is looked up on
 * mount, and a later initial ID is looked up when it changes.
 */
import { onMounted, ref, shallowRef, watch } from 'vue'
import RpcFailureAlert from '@/components/repository/RpcFailureAlert.vue'
import { useRepositoryClients } from '@/repository/clients'
import { OutcomeInputError } from '@/repository/lookupCore'
import { inspectOutcome, type OutcomeView } from '@/repository/outcomeCore'
import { isOwnCancellation, rpcFailure, type RpcFailure } from '@/repository/rpcErrorCore'
import { useCallScope } from '@/repository/useCallScope'

const props = defineProps<{ accountId: string; initialOperationId?: string }>()

const clients = useRepositoryClients()
const scope = useCallScope()
const operationId = ref(props.initialOperationId ?? '')
const looking = ref(false)
const inputError = ref<string | null>(null)
const failure = ref<RpcFailure | null>(null)
const view = shallowRef<OutcomeView | null>(null)

async function lookUp() {
  inputError.value = null
  failure.value = null
  view.value = null
  const signal = scope.next()
  looking.value = true
  try {
    view.value = await inspectOutcome(clients.documents, props.accountId, operationId.value, signal)
  } catch (error) {
    if (error instanceof OutcomeInputError) inputError.value = error.message
    else if (!isOwnCancellation(error, signal)) failure.value = rpcFailure(error)
  } finally {
    looking.value = false
  }
}

onMounted(() => {
  if ((props.initialOperationId ?? '').trim() !== '') void lookUp()
})

watch(
  () => props.initialOperationId ?? '',
  (next, previous) => {
    if (next === previous || next.trim() === '') return
    operationId.value = next
    void lookUp()
  },
)
</script>
