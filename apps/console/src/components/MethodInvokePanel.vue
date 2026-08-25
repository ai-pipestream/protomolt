<template>
  <div>
    <div class="d-flex align-center mb-2">
      <span class="text-caption text-medium-emphasis text-mono">{{ method.inputType }}</span>
      <v-spacer />
      <v-btn size="x-small" :variant="mode === 'form' ? 'tonal' : 'outlined'" class="mr-1"
             :disabled="!formable" @click="mode = 'form'">Form</v-btn>
      <v-btn size="x-small" :variant="mode === 'json' ? 'tonal' : 'outlined'"
             @click="toJson">JSON</v-btn>
    </div>

    <template v-if="mode === 'form'">
      <div v-for="field in fields" :key="field.name" class="mb-2">
        <v-switch v-if="isBool(field)" v-model="form[key(field)]" :label="key(field)"
                  density="compact" color="primary" hide-details />
        <v-text-field v-else-if="isScalar(field)" v-model="form[key(field)]"
                      :label="key(field)" :hint="hint(field)" persistent-hint
                      density="compact" class="text-mono" />
        <v-textarea v-else v-model="form[key(field)]" :label="key(field)"
                    :hint="hint(field)" persistent-hint rows="2" auto-grow
                    density="compact" class="text-mono" spellcheck="false" />
      </div>
    </template>
    <v-textarea v-else v-model="rawJson" label="Request (proto3 JSON)" rows="6" auto-grow
                density="compact" class="text-mono" spellcheck="false" />

    <div class="d-flex align-center ga-2 mt-1">
      <v-btn color="primary" size="small" prepend-icon="mdi-play" :loading="calling"
             @click="invoke">
        Call
      </v-btn>
      <span v-if="callError" class="text-error text-caption">{{ callError }}</span>
    </div>

    <template v-if="result">
      <div class="d-flex align-center mt-3 mb-1">
        <v-chip size="x-small" variant="tonal" :color="result.ok ? 'success' : 'error'">
          {{ result.status }}
        </v-chip>
        <span v-if="result.methodType === 'SERVER_STREAMING'"
              class="text-caption text-medium-emphasis ml-2">
          {{ result.responses?.length ?? 0 }} streamed
          message{{ (result.responses?.length ?? 0) === 1 ? '' : 's' }}
        </span>
        <span class="text-caption text-medium-emphasis ml-2 text-mono">
          {{ method.outputType }}
        </span>
      </div>
      <v-alert v-if="!result.ok" type="error" variant="tonal" density="compact">
        {{ result.description || result.status }}
      </v-alert>
      <pre v-for="(response, i) in result.responses ?? []" v-else :key="i"
           class="reply">{{ pretty(response) }}</pre>
    </template>
  </div>
</template>

<script setup lang="ts">
import { computed, reactive, ref } from 'vue'
import { errorMessage } from '../services/api'
import {
  invokeMethod,
  requestSkeleton,
  type ReflectedField,
  type ReflectedMethod,
  type ServiceInvocation,
} from '../services/services'

const props = defineProps<{
  profile: string
  method: ReflectedMethod
}>()

const fields = computed(() => props.method.inputFields ?? [])
// A form is only honest when every field has a widget; otherwise start on JSON.
const formable = computed(() => fields.value.length > 0)
const mode = ref<'form' | 'json'>(formable.value ? 'form' : 'json')

/** Form state, keyed by JSON field name; scalars are edited as text. */
const form = reactive<Record<string, unknown>>(seed())
const rawJson = ref(JSON.stringify(requestSkeleton(props.method), null, 2))

const calling = ref(false)
const callError = ref('')
const result = ref<ServiceInvocation | null>(null)

function seed(): Record<string, unknown> {
  const skeleton = requestSkeleton(props.method)
  const seeded: Record<string, unknown> = {}
  for (const field of props.method.inputFields ?? []) {
    const value = skeleton[key(field)]
    seeded[key(field)] = isBool(field) ? value
        : isScalar(field) ? String(value)
        : JSON.stringify(value)
  }
  return seeded
}

function key(field: ReflectedField): string {
  return field.jsonName || field.name
}

function singular(field: ReflectedField): boolean {
  return field.cardinality !== 'repeated' && field.cardinality !== 'map'
}

function isBool(field: ReflectedField): boolean {
  return singular(field) && field.type === 'bool'
}

function isScalar(field: ReflectedField): boolean {
  return singular(field) && field.type !== 'message' && field.type !== 'group'
}

function hint(field: ReflectedField): string {
  const shape = singular(field) ? field.type : `${field.cardinality} ${field.type}`
  return field.typeName ? `${shape} · ${field.typeName}` : shape
}

/** The request the form states: typed scalars, JSON-parsed composites, blanks omitted. */
function fromForm(): Record<string, unknown> {
  const request: Record<string, unknown> = {}
  for (const field of fields.value) {
    const value = form[key(field)]
    if (isBool(field)) {
      if (value === true) request[key(field)] = true
      continue
    }
    const text = String(value ?? '').trim()
    if (!text) continue
    if (isScalar(field)) {
      request[key(field)] = scalarValue(field, text)
    } else {
      try {
        request[key(field)] = JSON.parse(text)
      } catch {
        throw new Error(`'${key(field)}' is not valid JSON`)
      }
    }
  }
  return request
}

function scalarValue(field: ReflectedField, text: string): unknown {
  switch (field.type) {
    case 'string':
    case 'bytes':
    case 'enum':
      return text
    case 'int64':
    case 'uint64':
    case 'sint64':
    case 'fixed64':
    case 'sfixed64':
      // proto3 JSON accepts 64-bit values as strings, which keeps them exact.
      return text
    default: {
      const numeric = Number(text)
      if (Number.isNaN(numeric)) {
        throw new Error(`'${key(field)}' expects a ${field.type}`)
      }
      return numeric
    }
  }
}

function toJson() {
  if (mode.value === 'form') {
    try {
      rawJson.value = JSON.stringify(fromForm(), null, 2)
    } catch {
      // Keep the previous JSON when the form does not state a request yet.
    }
  }
  mode.value = 'json'
}

async function invoke() {
  calling.value = true
  callError.value = ''
  result.value = null
  try {
    const request = mode.value === 'form' ? fromForm()
        : (JSON.parse(rawJson.value) as Record<string, unknown>)
    result.value = await invokeMethod(props.profile, props.method.fullName, request)
  } catch (e) {
    callError.value = errorMessage(e)
  } finally {
    calling.value = false
  }
}

function pretty(value: unknown): string {
  return JSON.stringify(value, null, 2)
}
</script>

<style scoped>
.reply {
  font-family: var(--mono, ui-monospace, monospace);
  font-size: 0.8rem;
  line-height: 1.5;
  padding: 10px 12px;
  border: 1px solid rgba(var(--v-border-color), var(--v-border-opacity));
  border-radius: 6px;
  overflow-x: auto;
  max-height: 360px;
  margin-bottom: 8px;
}
</style>
