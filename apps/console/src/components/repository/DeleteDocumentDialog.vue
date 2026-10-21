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
                    :readonly="planned !== null" data-testid="delete-reason"
                    :hint="planned !== null ? 'Fixed after the first send: a resend must carry the same request.' : undefined"
                    :persistent-hint="planned !== null" />
        <v-alert v-if="inputError" type="warning" variant="tonal" density="compact" class="mt-2" data-testid="delete-input-error">
          {{ inputError }}
        </v-alert>
        <v-alert v-if="stale" type="warning" variant="tonal" density="compact" class="mt-2" data-testid="delete-stale">
          {{ stale }}
          <div class="mt-2"><v-btn size="small" variant="tonal" data-testid="delete-reload" @click="emit('reload')">Reload the document</v-btn></div>
        </v-alert>
        <RpcFailureAlert v-if="refused" :failure="refused" title="The repository refused the delete" class="mt-2" />
        <div v-if="reuseNote" class="text-caption mt-1" data-testid="delete-reuse">{{ reuseNote }}</div>
        <RpcFailureAlert v-if="unsettled" :failure="unsettled" title="Deleting" class="mt-2" />
        <div v-if="unsettled" class="text-caption mt-1" data-testid="delete-unsettled">
          The delete may or may not have been recorded. Sending it again resends the same request under the same
          operation ID, so it cannot record twice.
        </div>
      </v-card-text>
      <v-card-actions>
        <v-spacer />
        <v-btn variant="text" :disabled="busy" data-testid="delete-cancel" @click="close">Cancel</v-btn>
        <v-btn color="error" variant="flat" :loading="busy" :disabled="stale !== null || refused !== null"
               data-testid="delete-confirm" @click="confirm">
          {{ unsettled ? 'Send again' : 'Delete' }}
        </v-btn>
      </v-card-actions>
    </v-card>
  </v-dialog>
</template>

<script setup lang="ts">
import { computed, ref, shallowRef } from 'vue'
import RpcFailureAlert from './RpcFailureAlert.vue'
import type { NodeAddress } from '@/gen/ai/protomolt/proto/repo/v1/address_pb'
import type { DeleteDocumentRequest } from '@/gen/ai/protomolt/proto/repo/v2/document_service_pb'
import type { Accession } from '@/gen/ai/protomolt/proto/repo/v2/manifest_pb'
import { useRepositoryClients } from '@/repository/clients'
import { DeleteInputError, MAX_REASON_LENGTH, deleteDocument, planDelete, staleText } from '@/repository/deleteCore'
import { lookupOutcome } from '@/repository/lookupCore'
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
const refused = ref<RpcFailure | null>(null)
const reuseNote = ref<string | null>(null)
const unsettled = ref<RpcFailure | null>(null)
// The request of the first send; every resend carries exactly this request,
// because the service's retry identity includes the reason.
const planned = shallowRef<DeleteDocumentRequest | null>(null)

const callNumberText = computed(
  () => `${props.address.accountId} / ${props.address.graphId} / ${props.address.graphAddressId} / ${props.address.docId}`,
)

function close() {
  if (!busy.value) emit('close')
}

function delivered(accession: Accession | undefined) {
  if (accession === undefined) throw new Error('DeleteDocument answered without an accession.')
  emit('deleted', accession)
}

async function confirm() {
  inputError.value = null
  unsettled.value = null
  if (planned.value === null) {
    try {
      planned.value = planDelete(props.address, props.revision, operationId, reason.value)
    } catch (error) {
      if (error instanceof DeleteInputError) {
        inputError.value = error.message
        return
      }
      throw error
    }
  }
  const request = planned.value
  const signal = scope.next()
  busy.value = true
  try {
    const answer = await deleteDocument(clients.documents, request, signal)
    if (signal.aborted) return
    switch (answer.kind) {
      case 'deleted':
        delivered(answer.response.accession)
        break
      case 'stale':
        stale.value = staleText(answer, props.revision)
        break
      case 'denied':
      case 'refused':
        refused.value = answer.failure
        break
      case 'reused': {
        // The operation ID already recorded something: show what, rather than guess.
        refused.value = answer.failure
        const recorded = await lookupOutcome(clients.documents, props.address.accountId, operationId, signal)
        if (signal.aborted) return
        if (recorded.state === 'committed' && recorded.accession.tombstone) delivered(recorded.accession)
        else reuseNote.value = `Operation ${operationId} has a recorded outcome that is not this delete (${recorded.state}).`
        break
      }
      case 'unsettled':
        unsettled.value = answer.failure
        break
    }
  } catch (error) {
    if (!signal.aborted) unsettled.value = rpcFailure(error)
  } finally {
    busy.value = false
  }
}
</script>
