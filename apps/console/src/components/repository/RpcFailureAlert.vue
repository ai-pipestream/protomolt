<template>
  <v-alert type="error" variant="tonal" density="compact" class="mb-3" data-testid="rpc-failure">
    <div class="d-flex align-center flex-wrap ga-2">
      <span v-if="title" class="font-weight-medium">{{ title }}</span>
      <v-chip size="x-small" variant="flat" color="error" class="text-mono">{{ failure.code }}</v-chip>
    </div>
    <div v-if="failure.message" class="text-body-2 mt-1">{{ failure.message }}</div>
  </v-alert>
</template>

<script setup lang="ts">
import type { RpcFailure } from '@/repository/rpcErrorCore'

defineProps<{
  failure: RpcFailure
  /** What was being done, for example "Listing documents". */
  title?: string
}>()
</script>
