<template>
  <div class="pmv">
    <ProtoMessageNode v-for="(node, i) in tree" :key="i" :node="node" :depth="0" />
    <div v-if="tree.length === 0" class="pmv-empty">(empty message)</div>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import type { DescMessage, JsonValue } from '@bufbuild/protobuf'
import { buildMessageTree } from '@/lib/message-view/messageTree'
import ProtoMessageNode from './ProtoMessageNode.vue'

const props = defineProps<{
  desc: DescMessage
  value: JsonValue
}>()

const tree = computed(() => buildMessageTree(props.desc, props.value))
</script>

<style scoped>
.pmv {
  font-family: var(--pm-font-mono, 'JetBrains Mono Variable', monospace);
  font-size: 0.82rem;
  line-height: 1.7;
}
.pmv-empty {
  color: rgb(var(--v-theme-on-surface-variant, 128, 128, 128));
  font-style: italic;
}
</style>
