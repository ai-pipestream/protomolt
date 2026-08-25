<template>
  <ul class="checks">
    <li v-for="check in checks" :key="check.id" class="text-body-2">
      <v-icon size="small" :color="iconColor(check)" :icon="icon(check)"
              :aria-label="check.status" />
      <span class="text-mono id">{{ check.id }}</span>
      <span class="text-medium-emphasis">{{ check.detail }}</span>
    </li>
  </ul>
</template>

<script setup lang="ts">
import { passed, skipped, type WorkRecordCheck } from '../services/receipts'

defineProps<{ checks: WorkRecordCheck[] }>()

function icon(check: WorkRecordCheck): string {
  return passed(check) ? 'mdi-check-circle-outline'
      : skipped(check) ? 'mdi-minus-circle-outline'
      : 'mdi-close-circle-outline'
}

function iconColor(check: WorkRecordCheck): string | undefined {
  return passed(check) ? 'success' : skipped(check) ? undefined : 'error'
}
</script>

<style scoped>
.checks {
  list-style: none;
  padding: 0;
  margin: 0;
  display: grid;
  gap: 4px;
}
.checks li {
  display: flex;
  align-items: baseline;
  gap: 8px;
}
.checks .id {
  white-space: nowrap;
}
</style>
