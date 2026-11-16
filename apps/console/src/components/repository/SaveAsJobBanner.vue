<template>
  <div v-if="visible.length > 0" data-testid="save-as-jobs">
    <VAlert
      v-for="item in visible"
      :key="item.operationId"
      :type="alertType(item)"
      variant="tonal"
      density="compact"
      :closable="canDismiss(item)"
      class="mb-3"
      data-testid="save-as-job-banner"
      :data-job-phase="item.summary?.phase ?? 'unknown'"
      @click:close="jobs.dismiss(item.operationId)"
    >
      <div>
        Save-as operation <span class="text-mono">{{ item.operationId }}</span>:
        <template v-if="item.summary === null && refused(item)">
          refused ({{ item.error?.codeLabel }}): {{ item.error?.message }}
        </template>
        <template v-else-if="item.summary === null">
          {{ item.error ? `outcome not confirmed (${item.error.codeLabel})` : 'awaiting the server response' }}.
        </template>
        <template v-else-if="item.summary.phase === 'succeeded'">
          committed <span class="text-mono">{{ item.summary.destinationRendition?.name }}</span>
          at version v{{ item.summary.destinationVersion.toString() }} on
          <span class="text-mono">{{ item.summary.destinationAddress?.archive }} / {{ item.summary.destinationAddress?.entryId }}</span>.
        </template>
        <template v-else>
          {{ item.summary.stateLabel }}<template v-if="item.summary.failureMessage">: {{ item.summary.failureMessage }}</template>.
        </template>
      </div>
      <div
        v-if="item.summary !== null && item.error !== null"
        class="text-caption"
        data-testid="save-as-job-banner-error"
      >
        The last call for this job failed ({{ item.error.codeLabel }}): {{ item.error.message }}
      </div>
      <div v-if="(item.summary === null && !refused(item)) || item.summary?.phase === 'unresolved'" class="text-caption">
        Keep this operation ID: retrying the same request with it never commits twice.
      </div>
      <VBtn
        v-if="(item.summary === null && !refused(item)) || item.summary?.canReconcile"
        size="small"
        variant="text"
        class="mt-1"
        :loading="item.reconciling"
        data-testid="save-as-job-banner-refresh"
        @click="jobs.refresh(item.operationId)"
      >
        Refresh status
      </VBtn>
      <VBtn
        v-if="canRetry(item)"
        size="small"
        variant="text"
        class="mt-1"
        :loading="item.submitting"
        data-testid="save-as-job-banner-retry"
        @click="jobs.retry(item.operationId)"
      >
        Retry same operation
      </VBtn>
      <VBtn
        v-if="item.summary !== null && item.summary.canCancel"
        size="small"
        variant="text"
        class="mt-1"
        :loading="item.cancelling"
        data-testid="save-as-job-banner-cancel"
        @click="jobs.cancel(item.operationId, 'Cancelled from console Save-as banner')"
      >
        Cancel job
      </VBtn>
    </VAlert>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { VAlert, VBtn } from 'vuetify/components'
import { useSaveAsJobs, type SaveAsJobEntry } from '@/repository/saveAsJobs'

const jobs = useSaveAsJobs()
const visible = computed(() => jobs.items.filter((item) => !item.dialogOpen))

/** The publish was turned down outright: nothing was accepted and nothing is left to learn. */
function refused(item: SaveAsJobEntry): boolean {
  return item.summary === null && !item.submitting && item.error !== null && !item.error.canReconcile
}

/**
 * Dismissing forgets the operation ID and the request. That is safe only once the job has a
 * confirmed end or was refused; a job still queued, running or of unknown outcome may yet
 * commit, and the operator would have no way left to reconcile it.
 */
function canDismiss(item: SaveAsJobEntry): boolean {
  if (item.submitting || item.reconciling || item.cancelling) return false
  if (item.summary !== null) return item.summary.terminal
  return refused(item)
}

/**
 * The retained request can be sent again under its operation ID when the publish outcome is
 * unknown, or when the server says the same operation may be retried.
 */
function canRetry(item: SaveAsJobEntry): boolean {
  if (item.submitting) return false
  if (item.summary === null) return item.error?.kind === 'unsettled'
  return item.summary.canRetrySameOperation
}

function alertType(item: SaveAsJobEntry): 'info' | 'success' | 'warning' | 'error' {
  const phase = item.summary?.phase
  if (phase === undefined) {
    if (refused(item)) return 'error'
    return item.error ? 'warning' : 'info'
  }
  if (phase === 'succeeded') return 'success'
  if (phase === 'queued' || phase === 'running') return 'info'
  if (phase === 'unresolved' || phase === 'cancelled') return 'warning'
  return 'error'
}
</script>
