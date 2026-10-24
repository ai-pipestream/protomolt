<template>
  <div data-testid="byte-reader">
    <div class="text-subtitle-2 mb-2">{{ title }}</div>
    <v-row density="compact">
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
    <RpcFailureAlert v-if="readFailure" :failure="readFailure" title="Reading the bytes" class="mt-2" />
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
        whole object's SHA-256 <span class="text-mono">{{ outcome.verification.wholeObjectSha256 }}</span>; read the
        whole object to verify it.
      </template>
    </v-alert>

    <template v-if="previewView">
      <pre v-if="previewView.text !== null" class="preview mt-2" data-testid="preview-text">{{ previewView.text }}</pre>
      <pre v-else class="preview text-mono text-caption mt-2" data-testid="preview-hex">{{ previewView.hex.join('\n') }}</pre>
    </template>
  </div>
</template>

<script setup lang="ts">
import { ref } from 'vue'
import RpcFailureAlert from './RpcFailureAlert.vue'
import { PREVIEW_BYTES, collector, parseRange, type BlobRange } from '@/repository/blobReadCore'
import { isTextMediaType, type ByteRead, type ByteReadResult } from '@/repository/byteReadCore'
import { canStreamToDisk, diskSink, memorySink, saveBlob, type DownloadSink } from '@/repository/downloadCore'
import { hexDump } from '@/repository/formatCore'
import { isOwnCancellation, rpcFailure, type RpcFailure } from '@/repository/rpcErrorCore'
import { useCallScope } from '@/repository/useCallScope'

const props = defineProps<{
  title: string
  filename: string
  /** The media type recorded for the bytes; null when none is recorded. */
  mediaType: string | null
  read: ByteRead
}>()

const scope = useCallScope()
const diskCapable = canStreamToDisk()

const offsetText = ref('')
const lengthText = ref('')
const busy = ref<'preview' | 'download' | null>(null)
const inputError = ref<string | null>(null)
const readFailure = ref<RpcFailure | null>(null)
const progress = ref<{ received: string; expected: string; percent: number } | null>(null)
const outcome = ref<(ByteReadResult & { what: string }) | null>(null)
const previewView = ref<{ text: string | null; hex: string[] } | null>(null)
const cancelled = ref(false)

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

async function preview() {
  reset()
  const wanted = range()
  if (!wanted) return
  const signal = scope.next()
  busy.value = 'preview'
  const out = collector()
  try {
    // The range as requested, ending the stream once the preview has its
    // bytes, so an object shorter than the preview is read to its end.
    const result = await props.read(wanted, out.sink, signal, track, PREVIEW_BYTES)
    if (signal.aborted) return
    const bytes = out.bytes()
    outcome.value = { ...result, what: 'Preview' }
    previewView.value = isTextMediaType(result.mediaType || props.mediaType)
      ? { text: new TextDecoder('utf-8', { fatal: false }).decode(bytes), hex: [] }
      : { text: null, hex: hexDump(bytes, wanted.offset) }
  } catch (error) {
    failed(error, signal)
  } finally {
    if (!signal.aborted) busy.value = null
  }
}

async function download() {
  reset()
  const wanted = range()
  if (!wanted) return
  let sink: DownloadSink
  try {
    const target: unknown = globalThis
    sink = canStreamToDisk(target)
      ? await diskSink(target, props.filename)
      : memorySink(props.filename, props.mediaType ?? '', saveBlob)
  } catch (error) {
    // The user closed the save dialog: nothing was read.
    if (error instanceof DOMException && error.name === 'AbortError') return
    readFailure.value = rpcFailure(error)
    return
  }
  const signal = scope.next()
  busy.value = 'download'
  try {
    const result = await props.read(wanted, (chunk) => sink.write(chunk), signal, track)
    if (signal.aborted) {
      await sink.discard()
      return
    }
    await sink.finish()
    outcome.value = { ...result, what: 'Download' }
  } catch (error) {
    await sink.discard()
    failed(error, signal)
  } finally {
    if (!signal.aborted) busy.value = null
  }
}

function cancel() {
  scope.next()
  busy.value = null
  progress.value = null
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
