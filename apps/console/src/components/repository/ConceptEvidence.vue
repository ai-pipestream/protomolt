<template>
  <div class="text-caption" data-testid="concept-evidence">
    <div>
      Matched against {{ evidence.expandedConcepts }} concept{{ evidence.expandedConcepts === 1 ? '' : 's' }} from
      <span v-for="(s, i) in evidence.schemes" :key="s.schemeId" :data-testid="`evidence-scheme-${s.schemeId}`">
        <code>{{ s.schemeId }}</code> version <code>{{ s.version }}</code><span v-if="i < evidence.schemes.length - 1">, </span>
      </span>
    </div>
    <div v-if="evidence.unresolved.length > 0" class="mt-1" data-testid="evidence-unresolved">
      Mappings the expansion could not follow:
      <div v-for="(u, i) in evidence.unresolved" :key="i">
        <code>{{ u.from }}</code> {{ u.relation }} <code>{{ u.target }}</code>: {{ u.reason }}
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import type { EvidenceView } from '@/repository/linksCore'

defineProps<{ evidence: EvidenceView }>()
</script>
