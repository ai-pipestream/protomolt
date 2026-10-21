<template>
  <v-dialog :model-value="true" max-width="640" persistent @update:model-value="close">
    <v-card data-testid="delete-dialog">
      <v-card-title class="text-subtitle-1">Delete this document?</v-card-title>
      <v-card-text>
        <p class="text-body-2 mb-2">
          This records a tombstone as the next revision of
          <span class="text-mono" data-testid="delete-call-number">{{ callNumberText }}</span>, replacing revision
          <span class="text-mono" data-testid="delete-revision">{{ revision }}</span>. Earlier revisions and their bytes
          stay; the history keeps every revision.
        </p>
        <div class="text-caption text-medium-emphasis mb-3">
          Operation ID <span class="text-mono" data-testid="delete-operation">{{ operationId }}</span>
        </div>
        <v-textarea v-model="reason" label="Reason (optional)" rows="2" density="compact" :counter="MAX_REASON_LENGTH"
                    data-testid="delete-reason" />
        <v-alert v-if="inputError" type="warning" variant="tonal" density="compact" class="mt-2" data-testid="delete-input-error">
          {{ inputError }}
        </v-alert>
        <v-alert v-if="stale" type="warning" variant="tonal" density="compact" class="mt-2" data-testid="delete-stale">
          {{ stale }}
          <div class="mt-2"><v-btn size="small" variant="tonal" data-testid="delete-reload" @click="emit('reload')">Reload the document</v-btn></div>
        </v-alert>
        <RpcFailureAlert v-if="denied" :failure="denied" title="The repository refused the delete" class="mt-2" />
        <RpcFailureAlert v-if="failure" :failure="failure" title="Deleting" class="mt-2" />
        <div v-if="failure" class="text-caption mt-1">
          The delete may or may not have been recorded. Sending it again reuses the same operation ID, so it cannot
          record twice.
        </div>
      </v-card-text>
      <v-card-actions>
        <v-spacer />
        <v-btn variant="text" :disabled="busy" data-testid="delete-cancel" @click="close">Cancel</v-btn>
        <v-btn color="error" variant="flat" :loading="busy" :disabled="stale !== null || denied !== null"
               data-testid="delete-confirm" @click="confirm">
          {{ failure ? 'Send again' : 'Delete' }}
        </v-btn>
      </v-card-actions>
    </v-card>
  </v-dialog>
</template>

<script setup lang="ts">
import { computed, ref } from 'vue'
import RpcFailureAlert from './RpcFailureAlert.vue'
import type { NodeAddress } from '@/gen/ai/protomolt/proto/repo/v1/address_pb'
import type { Accession } from '@/gen/ai/protomolt/proto/repo/v2/manifest_pb'
import { useRepositoryClients } from '@/repository/clients'
import { DeleteInputError, MAX_REASON_LENGTH, deleteDocument, planDelete, staleText } from '@/repository/deleteCore'
import { rpcFailure, type RpcFailure } from '@/repository/rpcErrorCore'
import { newOperationId } from '@/repository/uploadCore'
import { useCallScope } from '@/repository/useCallScope'

const props = defineProps<{ address: NodeAddress; revision: bigint }>()
const emit = defineEmits<{ deleted: [accession: Accession]; reload: []; close: [] }>()

const clients = useRepositoryClients()
const scope = useCallScope()
// Chosen when the dialog opens, so every send of this delete is one operation.
const operationId = newOperationId()
const reason = ref('')
const busy = ref(false)
const inputError = ref<string | null>(null)
const stale = ref<string | null>(null)
const denied = ref<RpcFailure | null>(null)
const failure = ref<RpcFailure | null>(null)

const callNumberText = computed(
  () => `${props.address.accountId} / ${props.address.graphId} / ${props.address.graphAddressId} / ${props.address.docId}`,
)

function close() {
  if (!busy.value) emit('close')
}

async function confirm() {
  inputError.value = null
  failure.value = null
  let request
  try {
    request = planDelete(props.address, props.revision, operationId, reason.value)
  } catch (error) {
    if (error instanceof DeleteInputError) {
      inputError.value = error.message
      return
    }
    throw error
  }
  const signal = scope.next()
  busy.value = true
  try {
    const answer = await deleteDocument(clients.documents, request, signal)
    if (answer.kind === 'deleted') {
      const accession = answer.response.accession
      if (accession === undefined) throw new Error('DeleteDocument answered without an accession.')
      emit('deleted', accession)
    } else if (answer.kind === 'stale') {
      stale.value = staleText(answer, props.revision)
    } else {
      denied.value = answer.failure
    }
  } catch (error) {
    if (!signal.aborted) failure.value = rpcFailure(error)
  } finally {
    busy.value = false
  }
}
</script>
