<template>
  <v-card class="mb-4" data-testid="files">
    <v-card-text>
      <RpcFailureAlert v-if="failure" :failure="failure" title="Reading metadata and files" />
      <div v-if="blobs.length === 0 && !failure" class="text-body-2 text-medium-emphasis">This revision holds no raw files.</div>
      <v-table v-if="blobs.length > 0" density="compact" class="mb-3">
        <thead><tr><th>File</th><th>MIME type</th><th>Size</th><th /></tr></thead>
        <tbody>
          <tr v-for="blob in blobs" :key="blob.blobId" :data-testid="`blob-${blob.blobId}`">
            <td>
              <div>{{ blob.filename ?? '(no filename recorded)' }}</div>
              <div class="text-caption text-mono text-medium-emphasis">{{ blob.blobId }}</div>
            </td>
            <td class="text-mono text-caption">{{ blob.mimeType ?? '(not recorded)' }}</td>
            <td class="text-mono">{{ byteSize(blob.sizeBytes) }}</td>
            <td>
              <v-btn size="x-small" variant="tonal" :data-testid="`select-blob-${blob.blobId}`" @click="select(blob)">Select</v-btn>
            </td>
          </tr>
        </tbody>
      </v-table>

      <ByteReader
        v-if="selected"
        :key="selected.blobId"
        :title="selected.filename ?? selected.blobId"
        :filename="selected.filename ?? selected.blobId"
        :media-type="selected.mimeType"
        :read="readSelected"
      />
    </v-card-text>
  </v-card>
</template>

<script setup lang="ts">
import { ref } from 'vue'
import ByteReader from './ByteReader.vue'
import RpcFailureAlert from './RpcFailureAlert.vue'
import type { NodeAddress } from '@/gen/ai/protomolt/proto/repo/v1/address_pb'
import { readBlob } from '@/repository/blobReadCore'
import type { ByteRead } from '@/repository/byteReadCore'
import { useRepositoryClients } from '@/repository/clients'
import type { BlobRow } from '@/repository/documentCore'
import { byteSize } from '@/repository/formatCore'
import type { RpcFailure } from '@/repository/rpcErrorCore'

const props = defineProps<{
  address: NodeAddress
  revision: bigint
  blobs: BlobRow[]
  failure: RpcFailure | null
}>()

const clients = useRepositoryClients()
const selected = ref<BlobRow | null>(null)

function select(blob: BlobRow) {
  selected.value = blob
}

const readSelected: ByteRead = async (range, sink, signal, onProgress, stopAfter) => {
  const blob = selected.value
  if (blob === null) throw new Error('No file is selected.')
  const result = await readBlob(
    clients.documents,
    { address: props.address, revision: props.revision, blobId: blob.blobId, range },
    sink,
    signal,
    onProgress,
    stopAfter,
  )
  return { receivedBytes: result.receivedBytes, verification: result.verification, mediaType: result.header.mimeType || (blob.mimeType ?? '') }
}
</script>
