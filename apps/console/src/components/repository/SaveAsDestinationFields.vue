<template>
  <div class="mb-3" data-testid="save-as-destination">
    <div class="text-subtitle-2 mb-2">Archive destination</div>
    <v-row density="compact">
      <v-col cols="12" sm="4">
        <v-combobox
          v-if="archives.length > 0"
          v-model="form.archive"
          :items="archives"
          label="Destination archive"
          density="compact"
          variant="outlined"
          hide-details="auto"
          :disabled="disabled"
          data-testid="save-as-archive-input"
          @update:model-value="emit('edited')"
        />
        <v-text-field
          v-else
          v-model="form.archive"
          label="Destination archive"
          density="compact"
          variant="outlined"
          hide-details="auto"
          :disabled="disabled"
          data-testid="save-as-archive-input"
          @update:model-value="emit('edited')"
        />
      </v-col>
      <v-col cols="12" sm="4">
        <v-text-field
          v-model="form.entryId"
          label="Destination entry ID"
          density="compact"
          variant="outlined"
          hide-details="auto"
          :disabled="disabled"
          data-testid="save-as-entry-input"
          @update:model-value="emit('edited')"
        />
      </v-col>
      <v-col cols="12" sm="4">
        <v-text-field
          v-model="form.renditionName"
          label="Rendition slot name"
          density="compact"
          variant="outlined"
          hide-details="auto"
          :disabled="disabled"
          data-testid="save-as-rendition-input"
          @update:model-value="emit('renditionNameEdited')"
        />
      </v-col>
      <v-col cols="12" sm="6">
        <v-select
          v-model="form.preconditionMode"
          :items="preconditionModeItems"
          item-title="title"
          item-value="value"
          label="Destination version precondition"
          density="compact"
          variant="outlined"
          hide-details="auto"
          :disabled="disabled"
          data-testid="save-as-precondition-select"
          @update:model-value="emit('edited')"
        />
      </v-col>
      <v-col v-if="form.preconditionMode === 'expected-version'" cols="12" sm="6">
        <v-text-field
          v-model="form.expectedDestinationVersionText"
          label="Expected destination version"
          density="compact"
          variant="outlined"
          hide-details="auto"
          inputmode="numeric"
          :disabled="disabled"
          data-testid="save-as-expected-version-input"
          @update:model-value="emit('edited')"
        />
      </v-col>
    </v-row>
  </div>
</template>

<script setup lang="ts">
import type { SaveAsDestinationInput } from '@/repository/saveAsCore'

/** Archive destination and write precondition fields of the Save-as draft. The dialog owns the form. */
defineProps<{ form: SaveAsDestinationInput; disabled: boolean; archives: readonly string[] }>()
const emit = defineEmits<{ edited: []; renditionNameEdited: [] }>()

const preconditionModeItems = [
  { title: 'Match expected destination version', value: 'expected-version' as const },
  { title: 'Create new archive entry only', value: 'create-only' as const },
  { title: 'Any current destination version', value: 'unconditional' as const },
]
</script>
