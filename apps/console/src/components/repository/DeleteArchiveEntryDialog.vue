<template>
  <v-dialog :model-value="true" max-width="680" persistent>
    <v-card data-testid="delete-archive-dialog">
      <v-card-title>Delete this archive entry?</v-card-title>
      <v-card-text>
        <p>This records a tombstone for <span data-testid="delete-archive-address">{{ target.accountId }} / {{ target.archive }} / {{ target.entryId }}</span>
          only if version <span data-testid="delete-archive-version">{{ observedVersion }}</span> is still current.
          Earlier revisions and their bytes remain. This does not purge stored bytes.</p>
        <p class="text-caption mt-2">Operation ID <span class="text-mono" data-testid="delete-archive-operation">{{ request.operationId }}</span></p>
        <v-alert v-if="receipt" type="success" class="mt-3" data-testid="delete-archive-receipt">
          The archive recorded this deletion. Receipt {{ receipt.operationId }} is COMPLETED.
          <div class="text-caption text-mono">Command SHA-256 {{ receipt.commandSha256 }}</div>
        </v-alert>
        <RpcFailureAlert v-if="failure" :failure="failure" title="Archive deletion" class="mt-3" />
        <v-alert v-if="stale" type="warning" class="mt-3" data-testid="delete-archive-stale">
          This entry changed. Reload it and review the new version before starting another delete.
        </v-alert>
        <p v-if="unsettled" class="mt-2" data-testid="delete-archive-unsettled">
          The outcome is unknown. Check the recorded archive receipt, or resend this exact request with the same operation ID.
          A missing receipt does not prove the request failed. Closing does not cancel a delete already admitted by the server.
        </p>
        <p v-if="lookupNote" class="mt-2" data-testid="delete-archive-lookup">{{ lookupNote }}</p>
      </v-card-text>
      <v-card-actions>
        <v-btn v-if="unsettled" :disabled="busy" data-testid="delete-archive-check" @click="check">Check receipt</v-btn>
        <v-btn v-if="stale" :disabled="busy" data-testid="delete-archive-reload" @click="emit('reload')">Reload entry</v-btn>
        <v-spacer />
        <v-btn :disabled="busy" data-testid="delete-archive-close" @click="finish">{{ receipt ? 'Done' : 'Close' }}</v-btn>
        <v-btn v-if="!receipt" color="error" :loading="busy" :disabled="stale || refused" data-testid="delete-archive-confirm" @click="confirm">
          {{ unsettled ? 'Resend same delete' : 'Delete entry' }}
        </v-btn>
      </v-card-actions>
    </v-card>
  </v-dialog>
</template>

<script setup lang="ts">
import { getCurrentInstance, onBeforeUnmount, ref, shallowRef } from 'vue'
import RpcFailureAlert from './RpcFailureAlert.vue'
import type { EntryAddress } from '@/gen/ai/protomolt/proto/repo/archive/v1/entry_pb'
import type { ArchiveMutationReceipt } from '@/gen/ai/protomolt/proto/repo/archive/v1/archive_mutation_pb'
import { useRepositoryClients } from '@/repository/clients'
import { deleteArchiveEntry, lookupArchiveDelete, planArchiveDelete } from '@/repository/archiveDeleteCore'
import { useArchiveDeleteReceipts } from '@/repository/archiveDeleteReceipts'
import type { RpcFailure } from '@/repository/rpcErrorCore'
import { newOperationId } from '@/repository/uploadCore'
import { useCallScope } from '@/repository/useCallScope'

const props = defineProps<{ address: EntryAddress; version: bigint }>()
const emit = defineEmits<{ deleted: [receipt: ArchiveMutationReceipt]; reload: []; close: [] }>()
const instance = getCurrentInstance()
const clients = useRepositoryClients()
const receiptStore = useArchiveDeleteReceipts()
const scope = useCallScope()
// Snapshot identity and observed version once. Neither lookup nor retry creates a new intent.
const request = planArchiveDelete(props.address, props.version, newOperationId())
const target = request.mutation.case === 'deleteEntry' ? request.mutation.value.address! : props.address
const observedVersion = props.version
const busy = ref(false)
const stale = ref(false)
const refused = ref(false)
const unsettled = ref(false)
const failure = shallowRef<RpcFailure | null>(null)
const receipt = shallowRef<ArchiveMutationReceipt | null>(null)
const lookupNote = ref('')
let activeSignal: AbortSignal | null = null
let emittedDeleted = false

function emitDeletedOnce(arrived: ArchiveMutationReceipt) {
  if (emittedDeleted) return
  emittedDeleted = true
  if (!instance?.isUnmounted) {
    emit('deleted', arrived)
    return
  }
  // Vue's runtime emit is a no-op once isUnmounted is true; invoke any vnode
  // @deleted listener directly when the receipt arrives as unmount settles.
  const rawHandler = instance.vnode.props?.onDeleted ?? instance.vnode.props?.onDeletedOnce
  if (typeof rawHandler === 'function') {
    rawHandler(arrived)
  } else if (Array.isArray(rawHandler)) {
    for (const fn of rawHandler) {
      if (typeof fn === 'function') fn(arrived)
    }
  }
}

function recordArrivedReceipt(arrived: ArchiveMutationReceipt) {
  receipt.value = arrived
  failure.value = null
  stale.value = false
  refused.value = false
  unsettled.value = false
  lookupNote.value = ''
  activeSignal = null
  busy.value = false
  if (!scope.closed) {
    scope.next()
  }
  receiptStore.record(arrived, !scope.closed)
  if (scope.closed) {
    emitDeletedOnce(arrived)
  }
}

onBeforeUnmount(() => {
  if (receipt.value !== null) {
    receiptStore.setDialogOpen(receipt.value.operationId, false)
    emitDeletedOnce(receipt.value)
  }
})

function finish() {
  if (busy.value) return
  if (receipt.value) {
    receiptStore.setDialogOpen(receipt.value.operationId, false)
    emitDeletedOnce(receipt.value)
  } else {
    emit('close')
  }
}

async function confirm() {
  if (busy.value || stale.value || refused.value || receipt.value) return
  const signal = scope.next()
  activeSignal = signal
  busy.value = true
  lookupNote.value = ''
  try {
    const answer = await deleteArchiveEntry(clients.archiveMutations, request, signal)
    if (answer.kind === 'deleted') {
      recordArrivedReceipt(answer.receipt)
      return
    }
    if (signal.aborted) return
    failure.value = answer.failure
    stale.value = answer.kind === 'stale'
    refused.value = answer.kind === 'refused'
    unsettled.value = answer.kind === 'unsettled'
  } finally {
    if (activeSignal === signal) {
      activeSignal = null
      busy.value = false
    }
  }
}

async function check() {
  if (busy.value || !unsettled.value || receipt.value) return
  const signal = scope.next()
  activeSignal = signal
  busy.value = true
  try {
    const result = await lookupArchiveDelete(clients.archiveMutations, request, signal)
    if (result.kind === 'deleted') {
      recordArrivedReceipt(result.receipt)
      return
    }
    if (signal.aborted) return
    if (result.kind === 'unknown') {
      lookupNote.value = 'No receipt is recorded yet; the original request may still commit. Keep this operation ID.'
    } else {
      failure.value = result.failure
      lookupNote.value = 'Receipt lookup failed; this does not settle the original delete.'
    }
  } finally {
    if (activeSignal === signal) {
      activeSignal = null
      busy.value = false
    }
  }
}
</script>
