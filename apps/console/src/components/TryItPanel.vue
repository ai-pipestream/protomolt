<template>
  <div>
    <v-alert v-if="error" type="error" variant="tonal" density="compact" class="mb-4">
      <div class="font-weight-medium">Could not load the descriptor set</div>
      <div class="text-caption">{{ error }}</div>
      <template #append>
        <v-btn size="small" variant="text" @click="$emit('reload')">Retry</v-btn>
      </template>
    </v-alert>

    <v-skeleton-loader v-else-if="loading || !model" type="paragraph@2" />

    <template v-else>
      <v-select
        v-model="selectedType"
        :items="model.messageTypeNames"
        label="Message type"
        density="compact"
        variant="outlined"
        prepend-inner-icon="mdi-email-outline"
        class="mb-2"
        style="max-width: 560px"
      />

      <v-empty-state
        v-if="!model.messageTypeNames.length"
        icon="mdi-email-off-outline"
        title="No message types"
        text="This subject's descriptor set defines no messages to compose."
      />

      <v-row v-else-if="formSchema">
        <v-col cols="12" md="6">
          <v-card variant="flat" border>
            <v-card-title class="text-subtitle-2">
              <v-icon size="small" class="mr-1">mdi-form-select</v-icon>
              Compose {{ shortTypeName }}
            </v-card-title>
            <v-card-text>
              <SchemaForm :key="selectedType ?? ''" :schema="formSchema" @data-change="onData" />
            </v-card-text>
            <v-card-actions>
              <v-spacer />
              <v-btn variant="text" size="small" @click="reset">Reset</v-btn>
              <v-btn
                color="primary"
                variant="tonal"
                size="small"
                prepend-icon="mdi-play"
                @click="compose"
              >
                Compose JSON
              </v-btn>
            </v-card-actions>
          </v-card>
        </v-col>

        <v-col cols="12" md="6">
          <v-card variant="flat" border>
            <v-card-title class="text-subtitle-2 d-flex align-center">
              <v-icon size="small" class="mr-1">mdi-code-json</v-icon>
              Composed message
              <v-spacer />
              <v-btn
                v-if="composedJson"
                icon="mdi-content-copy"
                variant="text"
                size="x-small"
                aria-label="Copy JSON"
                @click="copyJson"
              />
            </v-card-title>
            <v-card-text>
              <v-alert
                v-if="composeError"
                type="warning"
                variant="tonal"
                density="compact"
                class="mb-3"
              >
                <div class="font-weight-medium">Not a valid {{ shortTypeName }}</div>
                <div class="text-caption">{{ composeError }}</div>
              </v-alert>
              <template v-if="composedJson">
                <div v-if="composedValue && selectedDesc" class="mb-3 d-flex ga-1">
                  <v-btn
                    size="x-small"
                    :variant="resultView === 'typed' ? 'tonal' : 'outlined'"
                    @click="resultView = 'typed'"
                  >Typed</v-btn>
                  <v-btn
                    size="x-small"
                    :variant="resultView === 'json' ? 'tonal' : 'outlined'"
                    @click="resultView = 'json'"
                  >JSON</v-btn>
                </div>
                <ProtoMessageView
                  v-if="resultView === 'typed' && composedValue && selectedDesc"
                  :desc="selectedDesc"
                  :value="composedValue"
                />
                <pre v-else class="json-output">{{ composedJson }}</pre>
              </template>
              <div v-else class="text-caption text-medium-emphasis">
                Fill the form and press <em>Compose JSON</em> — the draft is validated through the
                real message descriptor (proto3 JSON) before display.
              </div>
            </v-card-text>
          </v-card>
        </v-col>
      </v-row>
    </template>
  </div>
</template>

<script setup lang="ts">
import { computed, ref, shallowRef, watch } from 'vue'
import { fromJson, toJson } from '@bufbuild/protobuf'
import type { JsonValue } from '@bufbuild/protobuf'
import { SchemaForm } from '@/lib/schema-form'
import ProtoMessageView from './ProtoMessageView.vue'
import { descriptorToJsonSchema } from '@/lib/forms/descriptor'
import { toast } from '@/composables/useToast'
import type { DescriptorModel } from '../services/descriptorModel'

const props = defineProps<{
  model: DescriptorModel | null
  loading: boolean
  error: string
}>()

defineEmits<{ reload: [] }>()

const selectedType = ref<string | null>(null)
const draft = ref<Record<string, unknown>>({})
const composedJson = ref('')
const composedValue = shallowRef<JsonValue | null>(null)
const resultView = ref('typed')
const composeError = ref('')

const selectedDesc = computed(() => {
  if (!props.model || !selectedType.value) return null
  return props.model.registry.getMessage(selectedType.value) ?? null
})

const formSchema = computed(() =>
  selectedDesc.value ? descriptorToJsonSchema(selectedDesc.value) : null,
)

const shortTypeName = computed(() => {
  const t = selectedType.value ?? ''
  return t.slice(t.lastIndexOf('.') + 1)
})

watch(
  () => props.model,
  (model) => {
    if (model && !selectedType.value && model.messageTypeNames.length) {
      selectedType.value = model.messageTypeNames[0]
    }
  },
  { immediate: true },
)

watch(selectedType, () => reset())

function onData(data: Record<string, unknown>) {
  draft.value = data
}

function compose() {
  composeError.value = ''
  const desc = selectedDesc.value
  if (!desc || !props.model) return
  try {
    // Round-trip through the real descriptor: proves the composed JSON is a
    // valid proto3-JSON message and normalizes defaults/field names.
    const message = fromJson(desc, draft.value as JsonValue, { registry: props.model.registry })
    const canonical = toJson(desc, message, { registry: props.model.registry })
    composedValue.value = canonical
    composedJson.value = JSON.stringify(canonical, null, 2)
  } catch (e) {
    composeError.value = (e as Error).message ?? String(e)
    composedValue.value = null
    composedJson.value = JSON.stringify(draft.value, null, 2)
  }
}

function reset() {
  draft.value = {}
  composedJson.value = ''
  composedValue.value = null
  composeError.value = ''
}

async function copyJson() {
  try {
    await navigator.clipboard.writeText(composedJson.value)
    toast.success('JSON copied')
  } catch {
    toast.error('Clipboard unavailable')
  }
}
</script>

<style scoped>
.json-output {
  font-family: ui-monospace, 'SF Mono', 'JetBrains Mono', Menlo, Consolas, monospace;
  font-size: 12.5px;
  line-height: 1.5;
  white-space: pre-wrap;
  word-break: break-word;
  margin: 0;
  padding: 12px;
  border-radius: 6px;
  background: rgba(var(--v-theme-on-surface), 0.04);
  border: 1px solid rgba(var(--v-theme-on-surface), 0.1);
}
</style>
