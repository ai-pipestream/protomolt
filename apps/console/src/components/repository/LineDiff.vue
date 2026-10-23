<template>
  <div>
    <div v-if="diff.stats.added === 0 && diff.stats.removed === 0" class="text-body-2 text-medium-emphasis" :data-testid="`${testid}-same`">
      {{ same }}
    </div>
    <template v-else>
      <div class="d-flex ga-2 mb-1">
        <v-chip size="x-small" color="success" variant="tonal" :data-testid="`${testid}-added`">+{{ diff.stats.added }}</v-chip>
        <v-chip size="x-small" color="error" variant="tonal" :data-testid="`${testid}-removed`">−{{ diff.stats.removed }}</v-chip>
      </div>
      <div class="diff-view" :data-testid="testid">
        <table class="diff-table">
          <tbody>
            <tr v-for="(line, i) in diff.lines" :key="i" :class="`diff-${line.op}`">
              <td class="diff-gutter">{{ line.left ?? '' }}</td>
              <td class="diff-gutter">{{ line.right ?? '' }}</td>
              <td class="diff-sign">{{ line.op === 'add' ? '+' : line.op === 'del' ? '−' : '' }}</td>
              <td class="diff-text">{{ line.text || ' ' }}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </template>
  </div>
</template>

<script setup lang="ts">
import type { SectionDiff } from '@/repository/revisionDiffCore'

defineProps<{
  diff: SectionDiff
  /** What to say when the two sides are identical. */
  same: string
  testid: string
}>()
</script>

<style scoped>
.diff-view {
  overflow-x: auto;
  max-height: 480px;
  overflow-y: auto;
  border: 1px solid rgba(var(--v-theme-on-surface), 0.12);
  border-radius: 6px;
}
.diff-table {
  border-collapse: collapse;
  width: 100%;
  font-family: ui-monospace, 'SF Mono', 'JetBrains Mono', Menlo, Consolas, monospace;
  font-size: 12.5px;
  line-height: 1.55;
}
.diff-gutter {
  user-select: none;
  text-align: right;
  padding: 0 8px;
  width: 1%;
  min-width: 34px;
  color: rgba(var(--v-theme-on-surface), 0.35);
}
.diff-sign {
  user-select: none;
  width: 1%;
  padding: 0 4px;
  text-align: center;
  font-weight: 700;
}
.diff-text {
  white-space: pre;
  padding: 0 16px 0 6px;
  width: 99%;
}
.diff-add {
  background: rgba(var(--v-theme-success), 0.12);
}
.diff-add .diff-sign {
  color: rgb(var(--v-theme-success));
}
.diff-del {
  background: rgba(var(--v-theme-error), 0.12);
}
.diff-del .diff-sign {
  color: rgb(var(--v-theme-error));
}
</style>
