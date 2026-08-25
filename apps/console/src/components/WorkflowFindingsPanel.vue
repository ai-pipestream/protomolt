<template>
  <div>
    <v-alert type="warning" variant="tonal" density="compact" class="mb-2">
      {{ headline }}
    </v-alert>
    <div v-for="group in groups" :key="group.step" class="mb-2">
      <div class="text-caption font-weight-medium mb-1">
        <v-icon size="x-small" class="mr-1">
          {{ group.step ? 'mdi-debug-step-into' : 'mdi-sitemap-outline' }}
        </v-icon>
        {{ group.step ? `Step ${group.step}` : 'The workflow itself' }}
      </div>
      <div v-for="(finding, i) in group.findings" :key="i" class="finding text-caption mb-1">
        <v-chip size="x-small" variant="tonal" color="warning" class="mr-1">
          {{ kindLabel(finding.kind) }}
        </v-chip>
        <span>{{ finding.error }}</span>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import type { WorkflowFinding } from '../services/workflows'

const props = defineProps<{
  findings: WorkflowFinding[]
  /** What produced the findings: shapes the headline sentence. */
  source: 'check' | 'save'
}>()

const headline = computed(() => {
  const n = props.findings.length
  const problems = n === 1 ? 'one problem' : `${n} problems`
  return props.source === 'save'
    ? `Not stored: the same check that guards every save found ${problems}.`
    : `The workflow does not verify yet — ${problems} to resolve before it can run.`
})

/** Workflow-level findings first, then step groups in first-appearance order. */
const groups = computed(() => {
  const byStep = new Map<string, WorkflowFinding[]>()
  for (const finding of props.findings) {
    const list = byStep.get(finding.step) ?? []
    list.push(finding)
    byStep.set(finding.step, list)
  }
  return [...byStep.entries()]
    .sort(([a], [b]) => (a === '' ? -1 : b === '' ? 1 : 0))
    .map(([step, findings]) => ({ step, findings }))
})

/** The verifier's kind slugs, said the way a person reads the definition. */
const KIND_LABELS: Record<string, string> = {
  method: 'step method',
  when: 'run condition',
  rule: 'mapping rule',
  celRule: 'CEL rule',
  output: 'output mapping',
  workflow: 'workflow shape',
  contract: 'declared contract',
}

function kindLabel(kind: string): string {
  return KIND_LABELS[kind] ?? kind
}
</script>

<style scoped>
.finding {
  display: flex;
  align-items: baseline;
  gap: 2px;
}
</style>
