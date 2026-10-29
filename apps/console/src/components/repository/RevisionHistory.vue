<template>
  <v-card class="mb-4" data-testid="history">
    <v-card-text>
      <RpcFailureAlert v-if="failure" :failure="failure" title="Listing revisions" />
      <v-table v-if="rows.length > 0" density="compact">
        <thead>
          <tr>
            <th>Revision</th>
            <th>Committed</th>
            <th>Operation</th>
            <th>Owner</th>
            <th>Validation</th>
            <th>Part changes</th>
            <th />
          </tr>
        </thead>
        <tbody>
          <tr v-for="row in rows" :key="row.revision.toString()" :data-testid="`revision-${row.revision}`">
            <td class="text-mono">
              {{ row.revision }}
              <v-chip v-if="row.tombstone" size="x-small" color="warning" variant="tonal" class="ml-1">tombstone</v-chip>
              <div class="text-caption text-medium-emphasis">after {{ row.previousRevision }}</div>
            </td>
            <td class="text-mono text-caption">{{ row.committedAt ?? '' }}</td>
            <td class="text-mono text-caption">
              <router-link
                v-if="row.operationId"
                :to="{ name: 'repository-lookup', query: { account: props.address.accountId, op: row.operationId } }"
                :data-testid="`revision-op-${row.revision}`"
                class="text-mono text-caption"
              >
                {{ row.operationId }}
              </router-link>
            </td>
            <td class="text-caption">{{ row.owner }}</td>
            <td class="text-caption">{{ row.validation }}</td>
            <td class="text-caption">
              <div v-for="(c, i) in row.changes" :key="i">
                {{ c.kind }} {{ c.part }}
                <span v-if="c.sha256" class="text-mono text-medium-emphasis">{{ c.sha256.slice(0, 12) }}</span>
              </div>
            </td>
            <td>
              <template v-if="row.revision !== shownRevision">
                <v-btn size="x-small" variant="tonal"
                       :data-testid="`open-revision-${row.revision}`" @click="emit('open', row.revision)">Open</v-btn>
                <v-btn v-if="shownRevision !== null" size="x-small" variant="text" class="ml-1"
                       :data-testid="`compare-revision-${row.revision}`" @click="emit('compare', row.revision)">Compare</v-btn>
              </template>
              <span v-else class="text-caption text-medium-emphasis">shown</span>
            </td>
          </tr>
        </tbody>
      </v-table>
      <v-progress-linear v-if="loading" indeterminate class="my-2" />
      <v-btn v-if="nextToken && !loading" size="small" variant="text" class="mt-2" data-testid="history-more" @click="load(false)">
        Older revisions
      </v-btn>
    </v-card-text>
  </v-card>
</template>

<script setup lang="ts">
import { onMounted, ref, shallowRef, watch } from 'vue'
import { create } from '@bufbuild/protobuf'
import RpcFailureAlert from './RpcFailureAlert.vue'
import type { NodeAddress } from '@/gen/ai/protomolt/proto/repo/v1/address_pb'
import { ListRevisionsRequestSchema } from '@/gen/ai/protomolt/proto/repo/v2/document_service_pb'
import { useRepositoryClients } from '@/repository/clients'
import { HISTORY_PAGE_SIZE, revisionRow, type RevisionRow } from '@/repository/historyCore'
import { isOwnCancellation, rpcFailure, type RpcFailure } from '@/repository/rpcErrorCore'
import { useCallScope } from '@/repository/useCallScope'

const props = defineProps<{ address: NodeAddress; shownRevision: bigint | null }>()
const emit = defineEmits<{ open: [revision: bigint]; compare: [revision: bigint] }>()

const clients = useRepositoryClients()
const scope = useCallScope()
const rows = shallowRef<RevisionRow[]>([])
const nextToken = ref('')
const loading = ref(false)
const failure = ref<RpcFailure | null>(null)

async function load(first: boolean) {
  const signal = scope.next()
  loading.value = true
  failure.value = null
  if (first) {
    rows.value = []
    nextToken.value = ''
  }
  try {
    const page = await clients.documents.listRevisions(
      create(ListRevisionsRequestSchema, {
        address: props.address,
        pageSize: HISTORY_PAGE_SIZE,
        pageToken: first ? '' : nextToken.value,
      }),
      { signal },
    )
    if (signal.aborted) return
    const more = page.revisions.map(revisionRow)
    rows.value = first ? more : [...rows.value, ...more]
    nextToken.value = page.nextPageToken
  } catch (error) {
    if (!isOwnCancellation(error, signal)) failure.value = rpcFailure(error)
  } finally {
    if (!signal.aborted) loading.value = false
  }
}

onMounted(() => void load(true))
watch(() => props.address, () => void load(true))
</script>
