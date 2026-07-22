<template>
  <v-app>
    <v-app-bar flat border density="comfortable">
      <template #prepend>
        <router-link to="/" style="display: flex; align-items: center; text-decoration: none">
          <img src="/protomolt-logo.svg" alt="" width="30" height="30" class="ml-2 mr-3" />
          <span class="molt-wordmark text-h6 text-on-surface">
            Proto<span class="molt-accent">Molt</span>
            <span class="text-medium-emphasis font-weight-regular ml-1">console</span>
          </span>
        </router-link>
      </template>
      <v-spacer />
      <v-chip
        class="text-mono mr-2"
        size="small"
        variant="tonal"
        :color="registryUp === false ? 'error' : registryUp ? 'success' : undefined"
        :prepend-icon="registryUp === false ? 'mdi-lan-disconnect' : 'mdi-lan-connect'"
      >
        {{ registryLabel }}
      </v-chip>
      <v-btn
        :icon="isDark ? 'mdi-weather-sunny' : 'mdi-weather-night'"
        variant="text"
        :aria-label="isDark ? 'Switch to light theme' : 'Switch to dark theme'"
        @click="toggleTheme"
      />
    </v-app-bar>

    <v-main>
      <v-container fluid style="max-width: 1400px">
        <router-view />
      </v-container>
    </v-main>

    <ToastHost />
  </v-app>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { useTheme } from 'vuetify'
import ToastHost from './components/ToastHost.vue'
import { registryApi } from './services/api'

const theme = useTheme()
const isDark = computed(() => theme.global.current.value.dark)

function toggleTheme() {
  const next = isDark.value ? 'moltLight' : 'moltDark'
  theme.global.name.value = next
  localStorage.setItem('protomolt-theme', next === 'moltLight' ? 'light' : 'dark')
}

// A single startup probe labels the connection chip; the views surface
// request-level failures themselves.
const registryUp = ref<boolean | null>(null)
const registryLabel = computed(() =>
  registryUp.value === false ? 'registry unreachable' : '/api/protomolt')

onMounted(async () => {
  try {
    await registryApi.listSubjects()
    registryUp.value = true
  } catch {
    registryUp.value = false
  }
})
</script>
