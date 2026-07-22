<template>
  <div class="pmn" :style="{ '--pmn-depth': depth }">
    <div class="pmn-row" :class="{ 'pmn-toggle': hasChildren }" @click="hasChildren && (open = !open)">
      <span v-if="hasChildren" class="pmn-chevron" :class="{ open }">▸</span>
      <span v-else class="pmn-chevron pmn-chevron-spacer" />
      <span v-if="node.label" class="pmn-label">{{ node.label }}</span>
      <span v-if="node.oneof" class="pmn-oneof">oneof {{ node.oneof }}</span>
      <span v-if="displayText" class="pmn-value" :class="`pmn-${node.kind}`">{{ displayText }}</span>
      <span v-if="node.detail" class="pmn-detail">{{ node.detail }}</span>
      <span v-if="node.typeLabel" class="pmn-type">{{ node.typeLabel }}</span>
    </div>
    <div v-if="hasChildren && open" class="pmn-children">
      <ProtoMessageNode v-for="(child, i) in node.children" :key="i" :node="child" :depth="depth + 1" />
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, ref } from 'vue'
import type { ViewNode } from '@/lib/message-view/messageTree'

const props = defineProps<{
  node: ViewNode
  depth: number
}>()

const open = ref(props.depth < 2)
const hasChildren = computed(
  () => (props.node.children?.length ?? 0) > 0,
)
const displayText = computed(() => {
  if (props.node.kind === 'text' && props.node.display !== undefined) {
    return `"${props.node.display}"`
  }
  return props.node.display
})
</script>

<style scoped>
.pmn-row {
  display: flex;
  align-items: baseline;
  gap: 0.55em;
  padding-left: calc(var(--pmn-depth) * 0);
  border-radius: 4px;
}
.pmn-toggle { cursor: pointer; }
.pmn-toggle:hover { background: rgba(var(--v-theme-on-surface), 0.05); }
.pmn-chevron {
  flex: none;
  width: 1em;
  color: rgb(var(--v-theme-on-surface-variant));
  transition: transform 0.12s ease;
  font-size: 0.8em;
}
.pmn-chevron.open { transform: rotate(90deg); }
.pmn-chevron-spacer { visibility: hidden; }
.pmn-label { color: rgb(var(--v-theme-on-surface)); font-weight: 550; }
.pmn-label::after { content: ':'; color: rgb(var(--v-theme-on-surface-variant)); }
.pmn-oneof {
  font-size: 0.72em;
  color: rgb(var(--v-theme-on-surface-variant));
  border: 1px solid rgba(var(--v-theme-on-surface), 0.18);
  border-radius: 9px;
  padding: 0 0.5em;
}
.pmn-value { overflow-wrap: anywhere; }
.pmn-text { color: rgb(var(--v-theme-primary)); }
.pmn-number, .pmn-int64 { color: rgb(var(--v-theme-secondary, 120, 160, 220)); }
.pmn-bool { font-weight: 650; }
.pmn-enum {
  background: rgba(var(--v-theme-primary), 0.14);
  border-radius: 4px;
  padding: 0 0.45em;
  font-weight: 550;
}
.pmn-bytes, .pmn-wkt { color: rgb(var(--v-theme-on-surface)); }
.pmn-json { color: rgb(var(--v-theme-on-surface-variant)); }
.pmn-message { font-style: italic; color: rgb(var(--v-theme-on-surface-variant)); }
.pmn-detail {
  font-size: 0.82em;
  color: rgb(var(--v-theme-on-surface-variant));
}
.pmn-type {
  margin-left: auto;
  flex: none;
  font-size: 0.74em;
  color: rgb(var(--v-theme-on-surface-variant));
  opacity: 0.85;
}
.pmn-children {
  margin-left: 0.5em;
  padding-left: 0.9em;
  border-left: 1px solid rgba(var(--v-theme-on-surface), 0.12);
}
</style>
