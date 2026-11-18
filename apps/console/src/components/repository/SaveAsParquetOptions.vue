<template>
  <div class="mb-4" data-testid="save-as-parquet-options">
    <div class="text-subtitle-2 mb-2">Parquet projection &amp; masking options</div>
    <v-row density="compact">
      <v-col cols="12" sm="6">
        <v-text-field
          v-model="form.columnsText"
          label="Projected columns (optional)"
          :hint="PARQUET_COLUMNS_HINT"
          persistent-hint
          density="compact"
          variant="outlined"
          :disabled="disabled"
          data-testid="parquet-columns-input"
          @update:model-value="emit('edited')"
        />
      </v-col>
      <v-col cols="12" sm="6">
        <v-text-field
          v-model="form.maxOutputBytesText"
          label="Max output bytes (optional)"
          hint="Whole bytes; leave blank for provider ceiling"
          persistent-hint
          density="compact"
          variant="outlined"
          inputmode="numeric"
          :disabled="disabled"
          data-testid="parquet-max-bytes-input"
          @update:model-value="emit('edited')"
        />
      </v-col>
      <v-col cols="12" sm="6">
        <v-text-field
          v-model="form.maskedSensitivityClassesText"
          label="Masked sensitivity classes (optional)"
          :hint="PARQUET_CLASSES_HINT"
          persistent-hint
          density="compact"
          variant="outlined"
          :disabled="disabled"
          data-testid="parquet-sensitivity-input"
          @update:model-value="emit('edited')"
        />
      </v-col>
      <v-col cols="12" sm="6">
        <v-select
          v-model="form.maskStrategy"
          :items="maskStrategyItems"
          item-title="title"
          item-value="value"
          label="Mask strategy"
          density="compact"
          variant="outlined"
          hide-details="auto"
          :disabled="disabled"
          data-testid="parquet-mask-strategy-select"
          @update:model-value="emit('edited')"
        />
      </v-col>
      <v-col cols="12" sm="6">
        <v-text-field
          v-model="form.mappingId"
          label="Mapping ID (optional)"
          density="compact"
          variant="outlined"
          hide-details="auto"
          :disabled="disabled"
          data-testid="parquet-mapping-id-input"
          @update:model-value="emit('edited')"
        />
      </v-col>
      <v-col cols="12" sm="6">
        <v-text-field
          v-model="form.policyId"
          label="Policy ID (optional)"
          density="compact"
          variant="outlined"
          hide-details="auto"
          :disabled="disabled"
          data-testid="parquet-policy-id-input"
          @update:model-value="emit('edited')"
        />
      </v-col>
    </v-row>
  </div>
</template>

<script setup lang="ts">
import {
  PARQUET_CLASSES_HINT,
  PARQUET_COLUMNS_HINT,
  PARQUET_MASK_STRATEGY_CHOICES,
  type ParquetOptionsFormInput,
} from '@/repository/saveAsCore'

/**
 * Parquet projection and masking fields of the Save-as draft. The dialog owns the form; any edit
 * emits `edited` so the dialog can drop a settled attempt.
 */
defineProps<{ form: ParquetOptionsFormInput; disabled: boolean }>()
const emit = defineEmits<{ edited: [] }>()

const maskStrategyItems = PARQUET_MASK_STRATEGY_CHOICES
</script>
