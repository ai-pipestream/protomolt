<template>
  <div v-if="claims.length" class="mt-3">
    <div class="text-caption font-weight-medium mb-1">What this record does not claim</div>
    <p class="text-caption text-medium-emphasis mb-1">
      A receipt is honest about its limits: these are stated by the verifier, not
      implied by silence.
    </p>
    <ul class="claims text-caption text-medium-emphasis">
      <li v-for="claim in claims" :key="claim">{{ claim }}</li>
    </ul>
  </div>
</template>

<script setup lang="ts">
defineProps<{ claims: string[] }>()
</script>

<style scoped>
.claims {
  padding-left: 18px;
  margin: 0;
  display: grid;
  gap: 2px;
}
</style>
