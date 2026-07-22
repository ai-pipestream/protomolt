<template>
  <v-menu v-model="menuOpen" :close-on-content-click="false" location="bottom">
    <template #activator="{ props: activator }">
      <v-chip
        v-bind="activator"
        :color="chipColor"
        variant="tonal"
        size="small"
        class="compat-badge"
        :aria-label="`Compatibility mode ${mode}${inherited ? ' (inherited from global config)' : ''} — click to change`"
      >
        <v-icon start size="x-small">mdi-shield-check</v-icon>
        {{ mode }}
        <span v-if="inherited" class="ml-1 text-medium-emphasis">(global)</span>
        <v-icon end size="x-small">mdi-pencil</v-icon>
      </v-chip>
    </template>
    <v-card min-width="300">
      <v-card-text>
        <div class="text-subtitle-2 mb-1">
          {{ subject ? 'Subject compatibility' : 'Global compatibility' }}
        </div>
        <div v-if="subject" class="text-caption text-medium-emphasis mb-3">
          Overrides the global mode for <code>{{ subject }}</code>.
        </div>
        <div v-else class="text-caption text-medium-emphasis mb-3">
          Default mode for subjects without their own override.
        </div>
        <v-select
          v-model="selected"
          :items="modes"
          label="Mode"
          density="compact"
          variant="outlined"
          hide-details
          autofocus
        />
        <v-alert v-if="saveError" type="error" density="compact" variant="tonal" class="mt-3">
          {{ saveError }}
        </v-alert>
      </v-card-text>
      <v-card-actions>
        <v-spacer />
        <v-btn size="small" variant="text" @click="menuOpen = false">Cancel</v-btn>
        <v-btn
          size="small"
          color="primary"
          variant="tonal"
          :loading="saving"
          :disabled="selected === mode && !inherited"
          @click="save"
        >
          Save
        </v-btn>
      </v-card-actions>
    </v-card>
  </v-menu>
</template>

<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { toast } from '@/composables/useToast'
import {
  COMPATIBILITY_MODES,
  errorMessage,
  registryApi,
  type CompatibilityMode,
} from '../services/api'

const props = defineProps<{
  /** Effective mode being displayed. */
  mode: CompatibilityMode
  /** Subject to configure; omit for the global config. */
  subject?: string
  /** True when the subject has no override and inherits the global mode. */
  inherited?: boolean
}>()

const emit = defineEmits<{ changed: [mode: CompatibilityMode] }>()

const modes = [...COMPATIBILITY_MODES]
const menuOpen = ref(false)
const selected = ref<CompatibilityMode>(props.mode)
const saving = ref(false)
const saveError = ref('')

watch(menuOpen, (open) => {
  if (open) {
    selected.value = props.mode
    saveError.value = ''
  }
})

const chipColor = computed(() => (props.subject && !props.inherited ? 'primary' : 'secondary'))

async function save() {
  saving.value = true
  saveError.value = ''
  try {
    const applied = props.subject
      ? await registryApi.setSubjectConfig(props.subject, selected.value)
      : await registryApi.setGlobalConfig(selected.value)
    toast.success(
      props.subject
        ? `Compatibility for ${props.subject} set to ${applied}`
        : `Global compatibility set to ${applied}`,
    )
    menuOpen.value = false
    emit('changed', applied)
  } catch (e) {
    saveError.value = errorMessage(e)
  } finally {
    saving.value = false
  }
}
</script>

<style scoped>
.compat-badge {
  cursor: pointer;
}
</style>
