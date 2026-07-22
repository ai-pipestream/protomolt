<template>
  <v-card class="mb-4" variant="flat" border>
    <v-card-text class="d-flex align-center flex-wrap ga-3 py-3">
      <div class="d-flex align-center">
        <v-icon class="mr-2" color="primary">mdi-source-branch</v-icon>
        <div>
          <div class="text-subtitle-1 font-weight-medium">ProtoMolt Schema Registry</div>
          <div class="text-caption text-medium-emphasis">
            Protobuf schemas, versioned and compatibility-gated
          </div>
        </div>
      </div>
      <v-spacer />

      <!-- Health -->
      <v-tooltip location="bottom" :text="healthTooltip">
        <template #activator="{ props: activator }">
          <v-chip
            v-bind="activator"
            :color="health === 'up' ? 'success' : health === 'down' ? 'error' : undefined"
            variant="tonal"
            size="small"
          >
            <v-icon start size="x-small">
              {{ health === 'up' ? 'mdi-check-circle' : health === 'down' ? 'mdi-alert-circle' : 'mdi-dots-horizontal' }}
            </v-icon>
            {{ health === 'up' ? 'Registry up' : health === 'down' ? 'Registry unreachable' : 'Checking…' }}
          </v-chip>
        </template>
      </v-tooltip>

      <!-- Global compatibility mode + inline editor -->
      <template v-if="globalMode">
        <span class="text-caption text-medium-emphasis">Global mode</span>
        <CompatibilityBadge :mode="globalMode" @changed="onGlobalChanged" />
      </template>

      <v-btn
        prepend-icon="mdi-link-variant"
        variant="tonal"
        size="small"
        class="mr-2"
        :to="{ name: 'schema-registry-chains' }"
      >Chains</v-btn>

      <v-btn
        prepend-icon="mdi-set-merge"
        variant="tonal"
        size="small"
        class="mr-2"
        :to="{ name: 'schema-registry-merge' }"
      >Merge schemas</v-btn>

      <v-btn
        prepend-icon="mdi-connection"
        variant="tonal"
        size="small"
        :to="{ name: 'schema-registry-connect' }"
      >Connect a service</v-btn>

      <v-btn
        icon="mdi-refresh"
        variant="text"
        size="small"
        :loading="refreshing"
        aria-label="Refresh registry status"
        @click="refresh"
      />
    </v-card-text>
  </v-card>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'
import CompatibilityBadge from './CompatibilityBadge.vue'
import { errorMessage, registryApi, type CompatibilityMode } from '../services/api'

const emit = defineEmits<{ 'global-mode': [mode: CompatibilityMode] }>()

const health = ref<'unknown' | 'up' | 'down'>('unknown')
const healthTooltip = ref('Checking registry health…')
const globalMode = ref<CompatibilityMode | null>(null)
const refreshing = ref(false)

async function refresh() {
  refreshing.value = true
  try {
    const up = await registryApi.health()
    health.value = up ? 'up' : 'down'
    healthTooltip.value = up
      ? 'GET /health answered {"status":"UP"}'
      : 'The registry did not answer its health probe'
    if (up) {
      try {
        globalMode.value = await registryApi.globalConfig()
        emit('global-mode', globalMode.value)
      } catch (e) {
        healthTooltip.value = `Config read failed: ${errorMessage(e)}`
      }
    }
  } finally {
    refreshing.value = false
  }
}

function onGlobalChanged(mode: CompatibilityMode) {
  globalMode.value = mode
  emit('global-mode', mode)
}

onMounted(refresh)
</script>
