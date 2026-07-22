<template>
  <div
    style="position: fixed; bottom: 16px; right: 16px; z-index: 4000; display: flex;
           flex-direction: column; gap: 8px; align-items: flex-end"
    aria-live="polite"
  >
    <v-alert
      v-for="entry in toastQueue"
      :key="entry.id"
      :type="entry.kind"
      density="compact"
      variant="elevated"
      rounded="lg"
      max-width="420"
      closable
      @click:close="dismiss(entry.id)"
    >
      {{ entry.text }}
    </v-alert>
  </div>
</template>

<script setup lang="ts">
import { toastQueue } from '../composables/useToast'

function dismiss(id: number) {
  const at = toastQueue.findIndex((t) => t.id === id)
  if (at >= 0) {
    toastQueue.splice(at, 1)
  }
}
</script>
