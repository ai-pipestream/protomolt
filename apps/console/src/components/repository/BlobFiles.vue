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

      <template v-if="selected">
        <div class="text-subtitle-2 mb-2">{{ selected.filename ?? selected.blobId }}</div>
        <v-row dense>
          <v-col cols="6" md="3">
            <v-text-field v-model="offsetText" label="Offset (bytes)" density="compact" hide-details data-testid="range-offset" />
          </v-col>
          <v-col cols="6" md="3">
            <v-text-field v-model="lengthText" label="Length (bytes, blank to the end)" density="compact" hide-details
                          data-testid="range-length" />
          </v-col>
          <v-col cols="12" md="6" class="d-flex align-center ga-2">
            <v-btn size="small" variant="tonal" :loading="busy === 'preview'" :disabled="busy !== null" data-testid="preview"
                   @click="preview">Preview</v-btn>
            <v-btn size="small" color="primary" :loading="busy === 'download'" :disabled="busy !== null" data-testid="download"
                   @click="download">Download</v-btn>
            <v-btn v-if="busy !== null" size="small" variant="text" data-testid="cancel" @click="cancel">Cancel</v-btn>
          </v-col>
        </v-row>
        <div class="text-caption text-medium-emphasis mt-1">
          Preview reads the first {{ PREVIEW_BYTES / 1024n }} KiB of the range. Download saves
          {{ diskCapable ? 'straight to the file you choose, one frame at a time' : 'through browser memory, assembled when the read completes' }}.
        </div>
        <v-progress-linear v-if="progress" :model-value="progress.percent" class="mt-2" data-testid="progress" />
        <div v-if="progress" class="text-caption text-mono">{{ progress.received }} of {{ progress.expected }} bytes</div>

        <v-alert v-if="inputError" type="warning" variant="tonal" density="compact" class="mt-2" data-testid="range-error">
          {{ inputError }}
        </v-alert>
        <RpcFailureAlert v-if="readFailure" :failure="readFailure" title="Reading the file" class="mt-2" />
        <v-alert v-if="cancelled" type="info" variant="tonal" density="compact" class="mt-2" data-testid="cancelled">
          Cancelled. Nothing from the read was kept.
        </v-alert>

        <v-alert v-if="outcome" :type="outcome.verification.kind === 'verified' ? 'success' : 'info'" variant="tonal"
                 density="compact" class="mt-2" data-testid="verification">
          <template v-if="outcome.verification.kind === 'verified'">
            {{ outcome.what }}: all {{ outcome.receivedBytes }} bytes hash to the stored SHA-256
            <span class="text-mono">{{ outcome.verification.sha256 }}</span>.
          </template>
          <template v-else>
            {{ outcome.what }}: {{ outcome.receivedBytes }} bytes of a byte range. A range cannot be checked against the
            whole file's SHA-256 <span class="text-mono">{{ outcome.verification.wholeObjectSha256 }}</span>; read the
            whole file to verify it.
          </template>
        </v-alert>

        <template v-if="previewView">
          <pre v-if="previewView.text !== null" class="preview mt-2" data-testid="preview-text">{{ previewView.text }}</pre>
          <pre v-else class="preview text-mono text-caption mt-2" data-testid="preview-hex">{{ previewView.hex.join('\n') }}</pre>
        </template>
      </template>
    </v-card-text>
  </v-card>
</template>

<script setup lang="ts">
import { ref } from 'vue'
import RpcFailureAlert from './RpcFailureAlert.vue'
import type { NodeAddress } from '@/gen/ai/protomolt/proto/repo/v1/address_pb'
import {
  PREVIEW_BYTES,
  collector,
  parseRange,
  readBlob,
  type BlobRange,
  type BlobReadResult,
} from '@/repository/blobReadCore'
import { useRepositoryClients } from '@/repository/clients'
import type { BlobRow } from '@/repository/documentCore'
import { canStreamToDisk, diskSink, memorySink, saveBlob, type DownloadSink } from '@/repository/downloadCore'
import { byteSize, hexDump } from '@/repository/formatCore'
import { isOwnCancellation, rpcFailure, type RpcFailure } from '@/repository/rpcErrorCore'
import { useCallScope } from '@/repository/useCallScope'

const props = defineProps<{
  address: NodeAddress
  revision: bigint
  blobs: BlobRow[]
  failure: RpcFailure | null
}>()

const clients = useRepositoryClients()
const scope = useCallScope()
const diskCapable = canStreamToDisk()

const selected = ref<BlobRow | null>(null)
const offsetText = ref('')
const lengthText = ref('')
const busy = ref<'preview' | 'download' | null>(null)
const inputError = ref<string | null>(null)
const readFailure = ref<RpcFailure | null>(null)
const progress = ref<{ received: string; expected: string; percent: number } | null>(null)
const outcome = ref<(BlobReadResult & { what: string }) | null>(null)
const previewView = ref<{ text: string | null; hex: string[] } | null>(null)
const cancelled = ref(false)

function select(blob: BlobRow) {
  selected.value = blob
  offsetText.value = ''
  lengthText.value = ''
  reset()
}

function reset() {
  cancelled.value = false
  inputError.value = null
  readFailure.value = null
  outcome.value = null
  previewView.value = null
  progress.value = null
}

function range(): BlobRange | null {
  try {
    return parseRange(offsetText.value, lengthText.value)
  } catch (error) {
    inputError.value = (error as Error).message
    return null
  }
}

function track(received: bigint, expected: bigint) {
  progress.value = {
    received: received.toString(),
    expected: expected.toString(),
    percent: expected === 0n ? 100 : Number((received * 100n) / expected),
  }
}

function failed(error: unknown, signal: AbortSignal) {
  if (isOwnCancellation(error, signal)) return
  readFailure.value = rpcFailure(error)
}

function isText(mimeType: string | null): boolean {
  return mimeType !== null && (mimeType.startsWith('text/') || mimeType === 'application/json' || mimeType.endsWith('+xml') || mimeType === 'application/xml')
}

async function preview() {
  const blob = selected.value
  if (!blob) return
  reset()
  const wanted = range()
  if (!wanted) return
  const bounded: BlobRange = {
    offset: wanted.offset,
    length: wanted.length === 0n || wanted.length > PREVIEW_BYTES ? PREVIEW_BYTES : wanted.length,
  }
  const signal = scope.next()
  busy.value = 'preview'
  const out = collector()
  try {
    const result = await readBlob(clients.documents, { address: props.address, revision: props.revision, blobId: blob.blobId, range: bounded }, out.sink, signal, track)
    const bytes = out.bytes()
    outcome.value = { ...result, what: 'Preview' }
    previewView.value = isText(result.header.mimeType || blob.mimeType)
      ? { text: new TextDecoder('utf-8', { fatal: false }).decode(bytes), hex: [] }
      : { text: null, hex: hexDump(bytes, bounded.offset) }
  } catch (error) {
    failed(error, signal)
  } finally {
    busy.value = null
  }
}

async function download() {
  const blob = selected.value
  if (!blob) return
  reset()
  const wanted = range()
  if (!wanted) return
  const filename = blob.filename ?? blob.blobId
  let sink: DownloadSink
  try {
    const target: unknown = globalThis
    sink = canStreamToDisk(target)
      ? await diskSink(target, filename)
      : memorySink(filename, blob.mimeType ?? '', saveBlob)
  } catch (error) {
    // The user closed the save dialog: nothing was read.
    if (error instanceof DOMException && error.name === 'AbortError') return
    throw error
  }
  const signal = scope.next()
  busy.value = 'download'
  try {
    const result = await readBlob(
      clients.documents,
      { address: props.address, revision: props.revision, blobId: blob.blobId, range: wanted },
      (chunk) => sink.write(chunk),
      signal,
      track,
    )
    await sink.finish()
    outcome.value = { ...result, what: 'Download' }
  } catch (error) {
    await sink.discard()
    failed(error, signal)
  } finally {
    busy.value = null
  }
}

function cancel() {
  scope.next()
  cancelled.value = true
}
</script>

<style scoped>
.preview {
  white-space: pre-wrap;
  max-height: 480px;
  overflow: auto;
}
</style>
