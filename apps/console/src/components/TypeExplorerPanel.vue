<template>
  <div>
    <v-alert v-if="error" type="error" variant="tonal" density="compact" class="mb-4">
      <div class="font-weight-medium">Could not load the descriptor set</div>
      <div class="text-caption">{{ error }}</div>
      <template #append>
        <v-btn size="small" variant="text" @click="$emit('reload')">Retry</v-btn>
      </template>
    </v-alert>

    <v-skeleton-loader v-else-if="loading || !model" type="paragraph@3" />

    <template v-else>
      <div class="text-caption text-medium-emphasis mb-3">
        Latest version and its transitive references, compiled server-side to a
        <code>FileDescriptorSet</code> ({{ model.files.length }}
        file{{ model.files.length === 1 ? '' : 's' }}).
      </div>

      <v-expansion-panels v-model="openFiles" multiple variant="accordion">
        <v-expansion-panel v-for="file in model.files" :key="file.name" :value="file.name">
          <v-expansion-panel-title>
            <v-icon size="small" class="mr-2 text-medium-emphasis">mdi-file-outline</v-icon>
            <span class="font-mono">{{ file.name }}</span>
            <v-chip v-if="file.package" size="x-small" variant="tonal" class="ml-3">
              package {{ file.package }}
            </v-chip>
          </v-expansion-panel-title>
          <v-expansion-panel-text>
            <TypeExplorerNode
              v-for="type in file.types"
              :key="type.typeName"
              :node="type"
            />
            <div v-if="!file.types.length" class="text-caption text-medium-emphasis">
              No types in this file.
            </div>
          </v-expansion-panel-text>
        </v-expansion-panel>
      </v-expansion-panels>
    </template>
  </div>
</template>

<script setup lang="ts">
import { ref, watch } from 'vue'
import TypeExplorerNode from './TypeExplorerNode.vue'
import type { DescriptorModel } from '../services/descriptorModel'

const props = defineProps<{
  model: DescriptorModel | null
  loading: boolean
  error: string
}>()

defineEmits<{ reload: [] }>()

const openFiles = ref<string[]>([])

// Open every file by default once the model lands.
watch(
  () => props.model,
  (model) => {
    if (model) openFiles.value = model.files.map((f) => f.name)
  },
  { immediate: true },
)
</script>

<style scoped>
.font-mono {
  font-family: ui-monospace, 'SF Mono', 'JetBrains Mono', Menlo, Consolas, monospace;
  font-size: 13px;
}
</style>
