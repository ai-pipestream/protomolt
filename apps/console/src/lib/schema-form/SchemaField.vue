<template>
  <!-- Reference picker (x-pipestream-lookup) -->
  <LookupField
    v-if="kind === 'lookup'"
    :model-value="modelValue"
    :label="label"
    :schema="prop"
    :required="required"
    @update:model-value="$emit('update:modelValue', $event)"
  />

  <!-- Enum -->
  <v-select
    v-else-if="kind === 'enum'"
    :model-value="modelValue ?? prop.default"
    :items="prop.enum"
    :label="label"
    :hint="prop.description"
    :persistent-hint="!!prop.description"
    :clearable="!required"
    density="compact"
    variant="outlined"
    @update:model-value="$emit('update:modelValue', $event ?? undefined)"
  />

  <!-- Boolean -->
  <v-switch
    v-else-if="kind === 'boolean'"
    :model-value="modelValue ?? prop.default ?? false"
    :label="label"
    :hint="prop.description"
    :persistent-hint="!!prop.description"
    color="primary"
    density="compact"
    hide-details="auto"
    class="schema-switch"
    @update:model-value="$emit('update:modelValue', $event === true)"
  />

  <!-- Number / integer slider (opt-in via x-widget: 'slider'; needs min/max) -->
  <div v-else-if="kind === 'number' && prop['x-widget'] === 'slider'" class="schema-slider mb-2">
    <div class="d-flex align-center mb-1">
      <span class="text-caption text-medium-emphasis">{{ label }}</span>
      <v-spacer />
      <span class="text-caption font-weight-medium">{{ modelValue ?? prop.default
        }}<small v-if="prop['x-unit']" class="text-medium-emphasis"> {{ prop['x-unit'] }}</small></span>
    </div>
    <v-slider
      :model-value="(modelValue ?? prop.default) as number"
      :min="prop.minimum ?? 0"
      :max="prop.maximum ?? 100"
      :step="prop['x-step'] ?? (prop.type === 'integer' ? 1 : 0.01)"
      :hint="prop.description"
      :persistent-hint="!!prop.description"
      density="compact"
      color="primary"
      hide-details="auto"
      @update:model-value="onSlider"
    />
  </div>

  <!-- Number / integer -->
  <v-text-field
    v-else-if="kind === 'number'"
    :model-value="modelValue ?? prop.default"
    :label="label"
    :hint="prop.description"
    :persistent-hint="!!prop.description"
    type="number"
    density="compact"
    variant="outlined"
    :rules="required ? [requiredRule] : []"
    @update:model-value="onNumber"
  />

  <!-- String -->
  <v-text-field
    v-else-if="kind === 'string'"
    :model-value="modelValue ?? prop.default"
    :label="label"
    :hint="prop.description"
    :persistent-hint="!!prop.description"
    density="compact"
    variant="outlined"
    :rules="required ? [requiredRule] : []"
    @update:model-value="$emit('update:modelValue', $event === '' ? undefined : $event)"
  />

  <!-- Array of primitives: chips combobox -->
  <v-combobox
    v-else-if="kind === 'primitive-array'"
    :model-value="(modelValue as unknown[]) ?? []"
    :label="label"
    :hint="prop.description"
    :persistent-hint="!!prop.description"
    multiple
    chips
    closable-chips
    density="compact"
    variant="outlined"
    @update:model-value="onPrimitiveArray"
  />

  <!-- Array of objects: repeating rows -->
  <div v-else-if="kind === 'object-array'" class="schema-group pa-3 mb-2">
    <div class="d-flex align-center mb-1">
      <span class="text-subtitle-2">{{ label }}</span>
      <v-spacer />
      <v-btn size="x-small" variant="text" prepend-icon="mdi-plus" @click="addArrayRow">add</v-btn>
    </div>
    <div v-if="prop.description" class="text-caption text-medium-emphasis mb-2">{{ prop.description }}</div>
    <div v-for="(row, i) in arrayRows" :key="i" class="schema-array-row pa-2 mb-1">
      <div class="d-flex">
        <div class="flex-grow-1">
          <SchemaField
            v-for="(childProp, childKey) in prop.items?.properties ?? {}"
            :key="childKey"
            :prop="childProp"
            :label="humanize(String(childKey))"
            :model-value="row?.[childKey]"
            :required="(prop.items?.required ?? []).includes(String(childKey))"
            @update:model-value="(v) => setArrayRowField(i, String(childKey), v)"
          />
        </div>
        <v-btn icon="mdi-close" size="x-small" variant="text" class="ml-1" @click="removeArrayRow(i)" />
      </div>
    </div>
  </div>

  <!-- Nested object with schema: collapsible group -->
  <div v-else-if="kind === 'object'" class="schema-group mb-2">
    <div class="schema-group-header px-3 py-2 d-flex align-center" @click="open = !open">
      <v-icon size="small" class="mr-1">{{ open ? 'mdi-chevron-down' : 'mdi-chevron-right' }}</v-icon>
      <span class="text-subtitle-2">{{ label }}</span>
      <span v-if="prop.description" class="text-caption text-medium-emphasis ml-2 text-truncate">
        {{ prop.description }}
      </span>
    </div>
    <div v-if="open" class="px-3 pb-2">
      <SchemaField
        v-for="(childProp, childKey) in prop.properties"
        :key="childKey"
        :prop="childProp"
        :label="humanize(String(childKey))"
        :model-value="(modelValue as any)?.[childKey]"
        :required="(prop.required ?? []).includes(String(childKey))"
        @update:model-value="(v) => setObjectField(String(childKey), v)"
      />
    </div>
  </div>

  <!-- Schema-less object: free key/value map -->
  <div v-else-if="kind === 'free-map'" class="schema-group pa-3 mb-2">
    <div class="d-flex align-center mb-1">
      <span class="text-subtitle-2">{{ label }}</span>
      <v-spacer />
      <v-btn size="x-small" variant="text" prepend-icon="mdi-plus" @click="addMapRow">add</v-btn>
    </div>
    <div v-if="prop.description" class="text-caption text-medium-emphasis mb-2">{{ prop.description }}</div>
    <div v-for="(row, i) in mapRows" :key="i" class="d-flex ga-2 mb-1">
      <v-text-field :model-value="row.key" label="Key" density="compact" variant="outlined" hide-details @update:model-value="(v: string) => setMapRow(i, v, row.value)" />
      <v-text-field :model-value="row.value" label="Value" density="compact" variant="outlined" hide-details @update:model-value="(v: string) => setMapRow(i, row.key, v)" />
      <v-btn icon="mdi-close" size="x-small" variant="text" class="mt-2" @click="removeMapRow(i)" />
    </div>
  </div>

  <!-- Anything the renderer can't express: honest JSON fallback -->
  <v-textarea
    v-else
    :model-value="jsonText"
    :label="`${label} (JSON)`"
    :hint="prop?.description"
    :persistent-hint="!!prop?.description"
    rows="3"
    density="compact"
    variant="outlined"
    class="font-mono"
    :error-messages="jsonError"
    @update:model-value="onJson"
  />
</template>

<script setup lang="ts">
import { ref, computed, watch } from 'vue'
import LookupField from './LookupField.vue'
import { fieldKind, humanize } from './schemaForm'

const props = defineProps<{
  prop: any
  label: string
  modelValue: unknown
  required?: boolean
}>()

const emit = defineEmits<{ 'update:modelValue': [value: unknown] }>()

const kind = computed(() => fieldKind(props.prop))
const open = ref(false)
const requiredRule = (v: unknown) => (v !== undefined && v !== null && v !== '') || 'required'

function onNumber(v: string) {
  if (v === '' || v === null || v === undefined) {
    emit('update:modelValue', undefined)
    return
  }
  const n = Number(v)
  emit('update:modelValue', Number.isFinite(n) ? (props.prop.type === 'integer' ? Math.round(n) : n) : undefined)
}

function onSlider(v: number | null) {
  if (v === null || v === undefined) {
    emit('update:modelValue', undefined)
    return
  }
  emit('update:modelValue', props.prop.type === 'integer' ? Math.round(v) : v)
}

function onPrimitiveArray(values: unknown[]) {
  const itemType = props.prop.items?.type
  const coerced = (values ?? []).map((v) => {
    if (itemType === 'integer' || itemType === 'number') {
      const n = Number(v)
      return Number.isFinite(n) ? (itemType === 'integer' ? Math.round(n) : n) : v
    }
    return v
  })
  emit('update:modelValue', coerced.length ? coerced : undefined)
}

// --- nested object ---
function setObjectField(key: string, value: unknown) {
  const next = { ...((props.modelValue as any) ?? {}) }
  if (value === undefined) delete next[key]
  else next[key] = value
  emit('update:modelValue', Object.keys(next).length ? next : undefined)
}

// --- array of objects ---
const arrayRows = computed<any[]>(() => (Array.isArray(props.modelValue) ? (props.modelValue as any[]) : []))

function addArrayRow() {
  emit('update:modelValue', [...arrayRows.value, {}])
}

function removeArrayRow(i: number) {
  const next = arrayRows.value.filter((_, idx) => idx !== i)
  emit('update:modelValue', next.length ? next : undefined)
}

function setArrayRowField(i: number, key: string, value: unknown) {
  const next = arrayRows.value.map((row, idx) => (idx === i ? { ...row, [key]: value } : row))
  if (value === undefined) delete next[i]![key]
  emit('update:modelValue', next)
}

// --- free map ---
const mapRows = ref<{ key: string; value: string }[]>([])
watch(
  () => props.modelValue,
  (v) => {
    if (kind.value !== 'free-map') return
    const entries = Object.entries((v as Record<string, unknown>) ?? {}).map(([key, value]) => ({
      key,
      value: String(value),
    }))
    // Keep local rows when they only differ by in-progress (empty-key) edits.
    const committed = mapRows.value.filter((r) => r.key.trim())
    if (JSON.stringify(entries) !== JSON.stringify(committed)) mapRows.value = entries
  },
  { immediate: true },
)

function emitMap() {
  const out: Record<string, string> = {}
  for (const r of mapRows.value) {
    if (r.key.trim()) out[r.key.trim()] = r.value
  }
  emit('update:modelValue', Object.keys(out).length ? out : undefined)
}

function addMapRow() {
  mapRows.value = [...mapRows.value, { key: '', value: '' }]
}

function removeMapRow(i: number) {
  mapRows.value = mapRows.value.filter((_, idx) => idx !== i)
  emitMap()
}

function setMapRow(i: number, key: string, value: string) {
  mapRows.value = mapRows.value.map((r, idx) => (idx === i ? { key, value } : r))
  emitMap()
}

// --- JSON fallback ---
const jsonError = ref('')
const jsonText = computed(() => (props.modelValue === undefined ? '' : JSON.stringify(props.modelValue, null, 2)))

function onJson(text: string) {
  jsonError.value = ''
  if (!text.trim()) {
    emit('update:modelValue', undefined)
    return
  }
  try {
    emit('update:modelValue', JSON.parse(text))
  } catch {
    jsonError.value = 'invalid JSON'
  }
}
</script>

<script lang="ts">
// Recursive self-reference for nested objects / object arrays.
export default { name: 'SchemaField' }
</script>

<style scoped>
.schema-group {
  border: 1px solid rgba(var(--v-theme-on-surface), 0.12);
  border-radius: 6px;
}

.schema-group-header {
  cursor: pointer;
  user-select: none;
}

.schema-group-header:hover {
  background: rgba(var(--v-theme-on-surface), 0.04);
}

.schema-array-row {
  border: 1px dashed rgba(var(--v-theme-on-surface), 0.15);
  border-radius: 6px;
}

.schema-switch {
  margin-top: -4px;
}

.font-mono :deep(textarea) {
  font-family: ui-monospace, monospace;
  font-size: 12px;
}
</style>
