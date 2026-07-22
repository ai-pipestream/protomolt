<template>
  <v-select
    v-if="hasResolver"
    :model-value="modelValue"
    :items="items"
    :label="label"
    :hint="description"
    :persistent-hint="!!description"
    :multiple="multiple"
    :chips="multiple"
    :closable-chips="multiple"
    :loading="loading"
    :clearable="!required"
    density="compact"
    variant="outlined"
    @update:model-value="$emit('update:modelValue', $event ?? undefined)"
  />
  <!-- No resolver registered for this kind: free-form entry that still
       honours single/multi shape -->
  <v-combobox
    v-else
    :model-value="modelValue"
    :label="label"
    :hint="description"
    :persistent-hint="!!description"
    :multiple="multiple"
    :chips="multiple"
    :closable-chips="multiple"
    density="compact"
    variant="outlined"
    @update:model-value="$emit('update:modelValue', $event ?? undefined)"
  />
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { hasLookupResolver, resolveLookup, type LookupOption } from './lookups'

const props = defineProps<{
  modelValue: unknown
  label: string
  /** The property schema carrying x-pipestream-lookup. */
  schema: any
  required?: boolean
}>()

defineEmits<{ 'update:modelValue': [value: unknown] }>()

const kind = computed(() => props.schema?.['x-pipestream-lookup'] as string)
const multiple = computed(() => props.schema?.type === 'array')
const description = computed(() => props.schema?.description as string | undefined)
const hasResolver = computed(() => hasLookupResolver(kind.value))

const items = ref<LookupOption[]>([])
const loading = ref(false)

onMounted(async () => {
  if (!hasResolver.value) return
  loading.value = true
  try {
    items.value = await resolveLookup(kind.value)
  } finally {
    loading.value = false
  }
})
</script>
