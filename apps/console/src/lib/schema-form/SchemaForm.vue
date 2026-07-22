<template>
  <div class="schema-form">
    <SchemaField
      v-for="key in order"
      :key="key"
      :prop="schema.properties[key]"
      :label="humanize(key)"
      :model-value="data[key]"
      :required="(schema.required ?? []).includes(key)"
      @update:model-value="(v) => setField(key, v)"
    />
  </div>
</template>

<script setup lang="ts">
import { ref, computed, watch } from 'vue'
import SchemaField from './SchemaField.vue'
import { humanize, defaultsFor, propertyOrder } from './schemaForm'

/**
 * Homegrown JSON-Schema form renderer — the JSONForms replacement. Renders
 * the subset our registry schemas use (scalars, enums, lookups, primitive
 * arrays, nested objects, object arrays, free maps) and falls back to a
 * JSON editor per-property for anything else.
 */
const props = defineProps<{
  schema: any
  initialData?: any
  /** Optional module-shipped uischema; honored for property ORDER. */
  uischema?: any
}>()

const emit = defineEmits<{ 'data-change': [data: any] }>()

const data = ref<any>({})

// Initialize ONLY when the schema changes — consumers echo data-change back
// into initialData (e.g. the node config drawer), and re-merging defaults on
// every echo would resurrect fields the user just cleared.
watch(
  () => props.schema,
  () => {
    data.value = { ...defaultsFor(props.schema), ...(props.initialData ?? {}) }
  },
  { immediate: true },
)

const order = computed(() => propertyOrder(props.schema, props.uischema))

function setField(key: string, value: unknown) {
  const next = { ...data.value }
  if (value === undefined) delete next[key]
  else next[key] = value
  data.value = next
  emit('data-change', next)
}
</script>
