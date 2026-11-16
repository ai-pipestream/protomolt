<template>
  <div v-if="entry !== undefined" data-testid="save-as-job-panel">
    <!-- RPC / Conflict / Unsettled alert -->
    <v-alert
      v-if="error !== null"
      :type="error.kind === 'unsettled' ? 'warning' : 'error'"
      variant="tonal"
      :role="error.kind === 'unsettled' ? 'status' : 'alert'"
      class="mt-3"
      data-testid="save-as-rpc-error"
    >
      <div class="font-weight-medium">
        <template v-if="error.kind === 'unsettled'">
          Outcome unresolved ({{ error.codeLabel }}) — rendition commit is not yet confirmed
        </template>
        <template v-else-if="error.kind === 'conflict'">
          Version conflict ({{ error.codeLabel }})
        </template>
        <template v-else>
          Save-as failed ({{ error.codeLabel }})
        </template>
      </div>
      <div class="text-body-2 mt-1">{{ error.message }}</div>
      <div v-if="true" class="text-caption mt-1">
        Operation ID: <code>{{ entry.operationId }}</code>
      </div>
      <div class="d-flex flex-wrap ga-2 mt-2">
        <v-btn
          v-if="error.canReconcile && true"
          size="small"
          variant="tonal"
          :loading="entry.reconciling"
          data-testid="save-as-reconcile-btn"
          @click="emit('refresh')"
        >
          Refresh status
        </v-btn>
        <v-btn
          v-if="error.canReconcile"
          size="small"
          variant="outlined"
          :loading="entry.submitting"
          data-testid="save-as-retry-same-btn"
          @click="emit('retry')"
        >
          Retry same operation
        </v-btn>
        <v-btn
          v-if="error.kind === 'conflict'"
          size="small"
          variant="tonal"
          data-testid="save-as-reload-source-btn"
          @click="emit('reloadSource')"
        >
          Reload source
        </v-btn>
        <v-btn
          v-if="error.kind === 'unauthenticated'"
          size="small"
          variant="tonal"
          @click="emit('signIn')"
        >
          Sign in
        </v-btn>
      </div>
    </v-alert>

    <!-- Sink job status card -->
    <v-alert
      v-if="summary !== null"
      :type="jobAlertType"
      variant="tonal"
      role="status"
      aria-live="polite"
      class="mt-3"
      data-testid="save-as-job-status"
      :data-job-phase="summary.phase"
    >
      <div class="d-flex align-center justify-space-between flex-wrap ga-2">
        <span class="font-weight-medium">Job status: {{ summary.stateLabel }}</span>
        <span class="text-caption">Operation <code>{{ summary.operationId }}</code></span>
      </div>

      <v-progress-linear
        v-if="summary.phase === 'queued' || summary.phase === 'running'"
        indeterminate
        class="my-2"
        aria-label="Sink conversion job in progress"
      />

      <div v-if="summary.phase === 'succeeded'" class="text-body-2 mt-1" data-testid="save-as-job-receipt">
        Committed <code>{{ summary.destinationRendition?.name }}</code>
        ({{ byteSize(summary.bytesCommitted) }})
        at version <strong>v{{ summary.destinationVersion.toString() }}</strong>
        on <code>{{ summary.destinationAddress?.archive }} / {{ summary.destinationAddress?.entryId }}</code>.
      </div>

      <div v-if="summary.phase === 'unresolved'" class="text-body-2 mt-1">
        The server has not yet confirmed whether this rendition committed. Refresh status or retry the same operation before assuming completion.
      </div>

      <div v-if="summary.failureMessage" class="text-body-2 mt-1" data-testid="save-as-job-failure">
        {{ summary.failureMessage }}
        <span v-if="summary.failureReasonCode" class="text-caption">({{ summary.failureReasonCode }})</span>
      </div>

      <div class="d-flex flex-wrap ga-2 mt-2">
        <v-btn
          v-if="summary.canReconcile"
          size="small"
          variant="tonal"
          :loading="entry.reconciling"
          data-testid="save-as-refresh-job-btn"
          @click="emit('refresh')"
        >
          Refresh status
        </v-btn>
        <v-btn
          v-if="summary.canCancel"
          size="small"
          color="warning"
          variant="tonal"
          :loading="entry.cancelling"
          data-testid="save-as-cancel-job-btn"
          @click="emit('cancel')"
        >
          Cancel job
        </v-btn>
        <v-btn
          v-if="summary.canRetrySameOperation"
          size="small"
          variant="outlined"
          :loading="entry.submitting"
          data-testid="save-as-retry-job-btn"
          @click="emit('retry')"
        >
          Retry same operation
        </v-btn>
        <v-btn
          v-if="summary.isConflict"
          size="small"
          variant="tonal"
          data-testid="save-as-conflict-reload-btn"
          @click="emit('reloadSource')"
        >
          Reload source
        </v-btn>
        <v-btn
          v-if="summary.phase === 'succeeded' && destinationEntryLink !== null"
          size="small"
          variant="tonal"
          :to="destinationEntryLink"
          data-testid="save-as-open-entry-btn"
          @click="emit('openEntry')"
        >
          Open destination entry
        </v-btn>
      </div>
    </v-alert>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import type { RouteLocationRaw } from 'vue-router'
import { byteSize } from '@/repository/formatCore'
import type { ClassifiedSinkError, SinkJobSummary } from '@/repository/saveAsJobCore'
import type { SaveAsJobEntry } from '@/repository/saveAsJobs'

/** Shows the active Save-as job's RPC outcome and status card; the job itself lives in the host store. */
const props = defineProps<{ entry: SaveAsJobEntry | undefined }>()

const emit = defineEmits<{
  refresh: []
  retry: []
  cancel: []
  reloadSource: []
  signIn: []
  openEntry: []
}>()

const entry = computed(() => props.entry as SaveAsJobEntry)
const error = computed<ClassifiedSinkError | null>(() => props.entry?.error ?? null)
const summary = computed<SinkJobSummary | null>(() => props.entry?.summary ?? null)

const jobAlertType = computed<'info' | 'success' | 'warning' | 'error'>(() => {
  const j = summary.value
  if (j === null) return 'info'
  if (j.phase === 'succeeded') return 'success'
  if (j.phase === 'queued' || j.phase === 'running') return 'info'
  if (j.phase === 'unresolved' || j.phase === 'cancelled') return 'warning'
  return 'error'
})

const destinationEntryLink = computed<RouteLocationRaw | null>(() => {
  const j = summary.value
  if (j === null || j.phase !== 'succeeded') return null
  const addr = j.destinationAddress
  if (addr === null) return null
  return {
    name: 'repository-archive-entry',
    query: {
      account: addr.accountId,
      archive: addr.archive,
      entry: addr.entryId,
      version: j.destinationVersion > 0n ? j.destinationVersion.toString() : undefined,
    },
  }
})
</script>
