<template>
  <div class="proto-source">
    <table class="proto-source-table">
      <tbody>
        <tr v-for="(line, i) in lines" :key="i">
          <td class="proto-gutter">{{ i + 1 }}</td>
          <!-- eslint-disable-next-line vue/no-v-html — highlightProtoLines escapes all source text -->
          <td class="proto-line"><span v-html="line || '&nbsp;'" /></td>
        </tr>
      </tbody>
    </table>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { highlightProtoLines } from '../services/protoHighlight'

const props = defineProps<{ code: string }>()

const lines = computed(() => highlightProtoLines(props.code))
</script>

<style scoped>
.proto-source {
  overflow-x: auto;
  border: 1px solid rgba(var(--v-theme-on-surface), 0.12);
  border-radius: 6px;
  background: rgba(var(--v-theme-on-surface), 0.03);
}

.proto-source-table {
  border-collapse: collapse;
  width: 100%;
  font-family: ui-monospace, 'SF Mono', 'JetBrains Mono', Menlo, Consolas, monospace;
  font-size: 12.5px;
  line-height: 1.55;
}

.proto-gutter {
  user-select: none;
  text-align: right;
  padding: 0 10px 0 12px;
  width: 1%;
  color: rgba(var(--v-theme-on-surface), 0.35);
  border-right: 1px solid rgba(var(--v-theme-on-surface), 0.08);
}

.proto-line {
  padding: 0 16px 0 12px;
  white-space: pre;
}

:deep(.pshl-keyword) { color: rgb(var(--v-theme-primary)); font-weight: 600; }
:deep(.pshl-type) { color: #2aa198; }
:deep(.pshl-string) { color: #b58900; }
:deep(.pshl-number) { color: #d33682; }
:deep(.pshl-comment) { color: rgba(var(--v-theme-on-surface), 0.45); font-style: italic; }
</style>
