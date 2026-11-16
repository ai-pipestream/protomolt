<template>
  <div v-if="visible.length > 0" data-testid="save-as-jobs">
    <VAlert
      v-for="item in visible"
      :key="item.operationId"
      :type="alertType(item)"
      variant="tonal"
      density="compact"
      :closable="!busy(item)"
      class="mb-3"
      data-testid="save-as-job-banner"
      :data-job-phase="item.summary?.phase ?? 'unknown'"
      @click:close="jobs.dismiss(item.operationId)"
    >
      <div>
        Save-as operation <span class="text-mono">{{ item.operationId }}</span>:
        <template v-if="item.summary === null">
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
      <div v-if="item.summary === null || item.summary.phase === 'unresolved'" class="text-caption">
        Keep this operation ID: retrying the same request with it never commits twice.
      </div>
      <VBtn
        v-if="item.summary === null || item.summary.canReconcile"
        size="small"
        variant="text"
        class="mt-1"
        :loading="item.reconciling"
        data-testid="save-as-job-banner-refresh"
        @click="jobs.refresh(item.operationId)"
      >
        Refresh status
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

function busy(item: SaveAsJobEntry): boolean {
  return item.submitting || item.cancelling
}

function alertType(item: SaveAsJobEntry): 'info' | 'success' | 'warning' | 'error' {
  const phase = item.summary?.phase
  if (phase === undefined) return item.error ? 'warning' : 'info'
  if (phase === 'succeeded') return 'success'
  if (phase === 'queued' || phase === 'running') return 'info'
  if (phase === 'unresolved' || phase === 'cancelled') return 'warning'
  return 'error'
}
</script>
