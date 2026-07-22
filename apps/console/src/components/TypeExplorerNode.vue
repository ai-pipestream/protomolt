<template>
  <div class="type-node mb-3">
    <div class="type-header d-flex align-center px-3 py-2" @click="open = !open">
      <v-icon size="small" class="mr-1">{{ open ? 'mdi-chevron-down' : 'mdi-chevron-right' }}</v-icon>
      <v-chip size="x-small" :color="kindColor" variant="tonal" class="mr-2 text-uppercase">
        {{ node.kind }}
      </v-chip>
      <span class="font-mono font-weight-medium">{{ node.name }}</span>
      <span class="font-mono text-caption text-medium-emphasis ml-2 text-truncate">
        {{ node.typeName }}
      </span>
    </div>

    <div v-if="open" class="px-3 pb-2 pt-1">
      <!-- Message fields -->
      <v-table v-if="node.kind === 'message'" density="compact" class="type-table">
        <thead>
          <tr>
            <th style="width: 1%">#</th>
            <th>Field</th>
            <th>Type</th>
            <th>Label</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="field in node.fields" :key="field.number">
            <td class="text-medium-emphasis">{{ field.number }}</td>
            <td class="font-mono">
              {{ field.name }}
              <v-chip v-if="field.oneof" size="x-small" variant="outlined" class="ml-1">
                oneof {{ field.oneof }}
              </v-chip>
            </td>
            <td class="font-mono" :title="field.refTypeName">{{ field.type }}</td>
            <td class="text-medium-emphasis">{{ field.label }}</td>
          </tr>
          <tr v-if="!node.fields?.length">
            <td colspan="4" class="text-caption text-medium-emphasis">No fields.</td>
          </tr>
        </tbody>
      </v-table>

      <!-- Enum values -->
      <v-table v-else-if="node.kind === 'enum'" density="compact" class="type-table">
        <thead>
          <tr>
            <th style="width: 1%">#</th>
            <th>Value</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="value in node.values" :key="value.name">
            <td class="text-medium-emphasis">{{ value.number }}</td>
            <td class="font-mono">{{ value.name }}</td>
          </tr>
        </tbody>
      </v-table>

      <!-- Service methods -->
      <v-table v-else-if="node.kind === 'service'" density="compact" class="type-table">
        <thead>
          <tr>
            <th>Method</th>
            <th>Kind</th>
            <th>Input</th>
            <th>Output</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="method in node.methods" :key="method.name">
            <td class="font-mono">{{ method.name }}</td>
            <td class="text-medium-emphasis">{{ method.kind }}</td>
            <td class="font-mono text-caption">{{ method.inputType }}</td>
            <td class="font-mono text-caption">{{ method.outputType }}</td>
          </tr>
        </tbody>
      </v-table>

      <!-- Nested messages / enums -->
      <div v-if="node.children?.length" class="ml-4 mt-2">
        <TypeExplorerNode v-for="child in node.children" :key="child.typeName" :node="child" />
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, ref } from 'vue'
import type { TypeNode } from '../services/descriptorModel'

const props = defineProps<{ node: TypeNode }>()

const open = ref(true)

const kindColor = computed(() =>
  props.node.kind === 'message' ? 'primary' : props.node.kind === 'enum' ? 'secondary' : 'success',
)
</script>

<script lang="ts">
// Recursive self-reference for nested message/enum types.
export default { name: 'TypeExplorerNode' }
</script>

<style scoped>
.type-node {
  border: 1px solid rgba(var(--v-theme-on-surface), 0.12);
  border-radius: 6px;
}

.type-header {
  cursor: pointer;
  user-select: none;
}

.type-header:hover {
  background: rgba(var(--v-theme-on-surface), 0.04);
}

.type-table {
  background: transparent;
}

.font-mono {
  font-family: ui-monospace, 'SF Mono', 'JetBrains Mono', Menlo, Consolas, monospace;
  font-size: 12.5px;
}
</style>
