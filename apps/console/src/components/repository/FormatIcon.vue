<template>
  <span
    v-if="compact"
    class="format-icon format-icon--compact"
    role="img"
    :aria-label="descriptor.accessibleLabel"
    :title="descriptor.accessibleLabel"
    data-testid="format-icon"
    :data-format-family="descriptor.family"
    :data-hint-source="descriptor.hintSource"
  >
    <v-icon :icon="descriptor.icon" size="small" aria-hidden="true" class="format-icon__glyph" />
  </span>
  <span
    v-else
    class="format-icon"
    data-testid="format-icon"
    :data-format-family="descriptor.family"
    :data-hint-source="descriptor.hintSource"
  >
    <v-icon :icon="descriptor.icon" size="small" aria-hidden="true" class="format-icon__glyph" />
    <span class="format-icon__text">
      <span class="format-icon__label">{{ descriptor.label }}</span>
      <span
        v-if="descriptor.hintSource === 'extension-hint'"
        class="format-icon__hint"
        data-testid="format-extension-hint"
      >(filename hint)</span>
      <code
        v-if="showMediaType && descriptor.sanitizedMediaType !== ''"
        class="format-icon__mime"
      >{{ descriptor.sanitizedMediaType }}</code>
    </span>
    <v-chip
      v-if="authoritativeProtobuf"
      size="x-small"
      variant="tonal"
      color="primary"
      label
      data-testid="format-badge-protobuf"
    >
      Authoritative
    </v-chip>
    <v-chip
      v-else-if="derived"
      size="x-small"
      variant="tonal"
      color="secondary"
      label
      data-testid="format-badge-derived"
    >
      Derived
    </v-chip>
    <v-chip
      v-if="stale"
      size="x-small"
      variant="tonal"
      color="warning"
      label
      :title="staleReason || undefined"
      data-testid="format-badge-stale"
    >
      Stale
    </v-chip>
    <v-chip
      v-if="external"
      size="x-small"
      variant="outlined"
      label
      data-testid="format-badge-external"
    >
      External
    </v-chip>
  </span>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { classifyFormat } from '@/repository/formatIcons'

const props = withDefaults(
  defineProps<{
    mediaType?: string | null
    filename?: string | null
    authoritativeProtobuf?: boolean
    derived?: boolean
    stale?: boolean
    staleReason?: string | null
    external?: boolean
    showMediaType?: boolean
    compact?: boolean
  }>(),
  {
    mediaType: '',
    filename: '',
    authoritativeProtobuf: false,
    derived: false,
    stale: false,
    staleReason: null,
    external: false,
    showMediaType: false,
    compact: false,
  },
)

const descriptor = computed(() =>
  classifyFormat(props.mediaType, {
    filename: props.filename,
    authoritativeProtobuf: props.authoritativeProtobuf,
    derived: props.derived,
    stale: props.stale,
    external: props.external,
  }),
)
</script>

<style scoped>
.format-icon {
  display: inline-flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 6px;
  vertical-align: middle;
}

.format-icon--compact {
  gap: 0;
}

.format-icon__glyph {
  /* Maintain >= 3:1 non-text contrast against surface backgrounds. */
  color: rgb(var(--v-theme-on-surface));
  opacity: 0.88;
  flex: none;
}

.format-icon__text {
  display: inline-flex;
  align-items: baseline;
  flex-wrap: wrap;
  gap: 6px;
}

.format-icon__label {
  font-weight: 500;
  color: rgb(var(--v-theme-on-surface));
}

.format-icon__hint {
  font-size: 0.78rem;
  color: rgb(var(--v-theme-on-surface));
  opacity: 0.78;
}

.format-icon__mime {
  font-size: 0.78rem;
  color: rgb(var(--v-theme-on-surface));
  opacity: 0.82;
}
</style>
