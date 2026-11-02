<template>
  <section class="mt-4" aria-label="WASM preview" data-testid="wasm-preview">
    <div class="text-subtitle-2">Custom preview</div>
    <p class="text-caption text-medium-emphasis mb-2">
      Choose a local WASM preview module. It runs in this browser with the selected part and its retained schema.
      The result is display text; it does not change the document.
    </p>
    <p v-if="unavailable" class="text-caption" data-testid="preview-unavailable">{{ unavailable }}</p>
    <template v-else>
      <label class="text-caption">Preview module (.wasm)
        <input type="file" accept=".wasm,application/wasm" data-testid="preview-module" @change="selectModule" />
      </label>
      <div v-if="moduleSha" class="text-caption text-mono mt-1">Module SHA-256: {{ moduleSha }}</div>
      <div class="d-flex ga-2 my-2">
        <v-btn size="small" :disabled="!moduleBytes || busy" data-testid="run-preview" @click="run">Run preview</v-btn>
        <v-btn v-if="busy" size="small" variant="text" data-testid="cancel-preview" @click="cancel">Cancel</v-btn>
        <a :href="`${base}examples/type-url-preview.wasm`" download="type-url-preview.wasm" class="text-caption align-self-center">Download example module</a>
      </div>
      <p v-if="busy" role="status">Running preview…</p>
    </template>
    <v-alert v-if="failure" type="error" variant="tonal" density="compact" data-testid="preview-failure">{{ failure }}</v-alert>
    <pre v-if="text !== null" class="preview-text text-body-2" data-testid="preview-text">{{ text === '' ? '(empty preview)' : text }}</pre>
  </section>
</template>

<script setup lang="ts">
import { onBeforeUnmount, ref, shallowRef, watch } from 'vue'
import { create, toBinary } from '@bufbuild/protobuf'
import { AnySchema } from '@bufbuild/protobuf/wkt'
import { Sha256 } from '@/repository/sha256'
import { runPreview } from '@/wasm/preview'
import { PREVIEW_LIMITS } from '@/wasm/previewRuntime'

const props = defineProps<{ typeUrl: string; bytes: Uint8Array; descriptorSet: Uint8Array; unavailable: string | null }>()
const base = import.meta.env.BASE_URL
const moduleBytes = shallowRef<Uint8Array | null>(null)
const moduleSha = ref('')
const busy = ref(false)
const failure = ref<string | null>(null)
const text = ref<string | null>(null)
let controller: AbortController | null = null
let selection = 0

function cancel() {
  controller?.abort()
  controller = null
  busy.value = false
}
onBeforeUnmount(() => { selection++; cancel() })
watch(() => [props.bytes, props.descriptorSet, props.typeUrl, props.unavailable], () => {
  selection++
  cancel()
  text.value = null
  failure.value = null
})

async function selectModule(event: Event) {
  const own = ++selection
  cancel()
  moduleBytes.value = null
  moduleSha.value = ''
  text.value = null
  failure.value = null
  const file = (event.target as HTMLInputElement).files?.[0]
  if (!file) return
  try {
    if (file.size > PREVIEW_LIMITS.moduleBytes) throw new Error('WASM module exceeds the 2 MiB preview budget.')
    const bytes = new Uint8Array(await file.arrayBuffer())
    if (own !== selection) return
    moduleBytes.value = bytes
    moduleSha.value = new Sha256().update(bytes).hex()
  } catch (error) {
    if (own === selection) failure.value = error instanceof Error ? error.message : String(error)
  }
}

async function run() {
  if (!moduleBytes.value || props.unavailable) return
  cancel()
  const own = new AbortController()
  controller = own
  busy.value = true
  failure.value = null
  text.value = null
  try {
    // Check before serializing/copying a potentially large part into the worker.
    if (props.bytes.length + props.descriptorSet.length + new TextEncoder().encode(props.typeUrl).length + 16 > PREVIEW_LIMITS.inputBytes) {
      throw new Error('Input exceeds the 4 MiB preview budget. Download the protobuf to inspect the complete content.')
    }
    const result = await runPreview({
      module: moduleBytes.value,
      payload: toBinary(AnySchema, create(AnySchema, { typeUrl: props.typeUrl, value: props.bytes })),
      descriptorSet: props.descriptorSet,
    }, own.signal)
    if (!own.signal.aborted) text.value = result
  } catch (error) {
    if (!own.signal.aborted) failure.value = error instanceof Error ? error.message : String(error)
  } finally {
    if (controller === own) { busy.value = false; controller = null }
  }
}
</script>

<style scoped>
.preview-text { white-space: pre-wrap; overflow-wrap: anywhere; max-height: 28rem; overflow: auto; }
input { display: block; margin-top: 0.25rem; }
</style>
