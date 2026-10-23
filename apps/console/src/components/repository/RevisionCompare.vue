<template>
  <div data-testid="compare">
    <v-card class="mb-4">
      <v-card-text class="d-flex align-center flex-wrap ga-3">
        <v-text-field v-model="baseInput" label="Compare with revision" density="compact" hide-details style="max-width: 200px"
                      data-testid="compare-input" @keydown.enter="applyBase" />
        <v-btn size="small" variant="tonal" data-testid="compare-apply" @click="applyBase">Compare</v-btn>
        <v-btn size="small" variant="text" prepend-icon="mdi-swap-horizontal" data-testid="compare-swap" @click="emit('swap')">Swap</v-btn>
        <v-btn size="small" variant="text" data-testid="compare-close" @click="emit('close')">Close comparison</v-btn>
        <v-spacer />
        <span v-if="sides" class="text-caption text-mono" data-testid="compare-heading">
          revision {{ base }} ({{ facts(sides.base) }}) → revision {{ target }} ({{ facts(sides.target) }})
        </span>
      </v-card-text>
      <v-card-text v-if="inputError" class="pt-0">
        <v-alert type="warning" variant="tonal" density="compact" data-testid="compare-input-error">{{ inputError }}</v-alert>
      </v-card-text>
    </v-card>

    <RpcFailureAlert v-if="failure" :failure="failure" title="Reading the revisions to compare" />
    <v-progress-linear v-if="loading" indeterminate class="mb-2" />

    <template v-if="sides">
      <v-card class="mb-4">
        <v-card-title class="text-subtitle-2">Parts</v-card-title>
        <v-card-text>
          <v-table density="compact" data-testid="compare-parts">
            <thead>
              <tr><th>Part</th><th>Change</th><th>Revision {{ base }}</th><th>Revision {{ target }}</th></tr>
            </thead>
            <tbody>
              <tr v-for="row in parts" :key="row.label" :data-testid="`compare-part-${row.label}`">
                <td>{{ row.label }}</td>
                <td>
                  <v-chip size="x-small" variant="tonal" :color="changeColor(row.change)">{{ row.change }}</v-chip>
                  <div v-if="row.differs.length > 0" class="text-caption text-medium-emphasis">{{ row.differs.join(', ') }}</div>
                </td>
                <td class="text-caption"><PartCell :row="row.base" :differs="row.differs" /></td>
                <td class="text-caption"><PartCell :row="row.target" :differs="row.differs" /></td>
              </tr>
            </tbody>
          </v-table>
        </v-card-text>
      </v-card>

      <v-card v-for="section in sections" :key="section.key" class="mb-4">
        <v-card-title class="text-subtitle-2">{{ section.title }}</v-card-title>
        <v-card-text>
          <RpcFailureAlert v-for="(f, i) in section.failures" :key="i" :failure="f.failure" :title="f.title" />
          <div v-for="(note, i) in section.notes" :key="`n${i}`" class="text-caption text-medium-emphasis mb-1">{{ note }}</div>
          <LineDiff v-if="section.failures.length === 0" :diff="section.diff" :same="section.same" :testid="`compare-${section.key}`" />
        </v-card-text>
      </v-card>
    </template>
  </div>
</template>

<script setup lang="ts">
import { computed, defineComponent, h, onMounted, ref, shallowRef, watch, type PropType } from 'vue'
import LineDiff from './LineDiff.vue'
import RpcFailureAlert from './RpcFailureAlert.vue'
import type { NodeAddress } from '@/gen/ai/protomolt/proto/repo/v1/address_pb'
import { useRepositoryClients } from '@/repository/clients'
import { parseRevision, type PartRow } from '@/repository/documentCore'
import { byteSize } from '@/repository/formatCore'
import {
  fragmentLines,
  partDiff,
  readSide,
  revisionFacts,
  searchMetadataLines,
  sectionDiff,
  type FragmentSide,
  type PartChange,
  type PartField,
  type RevisionSide,
  type SectionDiff,
} from '@/repository/revisionDiffCore'
import { isOwnCancellation, rpcFailure, type RpcFailure } from '@/repository/rpcErrorCore'
import { useCallScope } from '@/repository/useCallScope'

const props = defineProps<{ address: NodeAddress; base: bigint; target: bigint }>()
const emit = defineEmits<{ base: [revision: bigint]; swap: []; close: [] }>()

const clients = useRepositoryClients()
const scope = useCallScope()
const sides = shallowRef<{ base: RevisionSide; target: RevisionSide } | null>(null)
const loading = ref(false)
const failure = ref<RpcFailure | null>(null)
const baseInput = ref(props.base.toString())
const inputError = ref<string | null>(null)

/** One manifest entry's columns, with the columns that changed marked. */
const PartCell = defineComponent({
  props: {
    row: { type: Object as PropType<PartRow | null>, default: null },
    differs: { type: Array as PropType<PartField[]>, required: true },
  },
  setup(cell) {
    return () => {
      const row = cell.row
      if (row === null) return h('span', { class: 'text-medium-emphasis' }, 'absent')
      const mark = (f: PartField, text: string) => h('div', { class: cell.differs.includes(f) ? 'font-weight-bold' : '' }, text)
      return h('div', [
        mark('state', `${row.state}${row.deletedReason ? `: ${row.deletedReason}` : ''}`),
        mark('sizeBytes', byteSize(row.sizeBytes)),
        mark('sha256', row.sha256 ? `sha256 ${row.sha256.slice(0, 16)}` : 'no digest'),
        mark('writtenInRevision', `written in ${row.writtenInRevision}`),
        mark('owner', row.owner),
        ...(row.opaque ? [mark('opaque', 'opaque')] : []),
      ])
    }
  },
})

function facts(side: RevisionSide): string {
  const f = revisionFacts(side.manifest)
  return `${f.tombstone ? 'tombstone, ' : ''}committed ${f.committedAt ?? 'at an unrecorded time'}`
}

function changeColor(change: PartChange): string {
  return change === 'added' ? 'success' : change === 'removed' ? 'error' : change === 'changed' ? 'warning' : 'default'
}

const parts = computed(() => (sides.value ? partDiff(sides.value.base.manifest, sides.value.target.manifest) : []))

interface Section {
  key: string
  title: string
  same: string
  diff: SectionDiff
  notes: string[]
  failures: { title: string; failure: RpcFailure }[]
}

function fragmentNotes(name: string, revision: bigint, side: FragmentSide): string[] {
  if (side.kind === 'absent') return [side.reason]
  if (side.kind === 'lines' && side.decodedAs !== null) return [`${name} at revision ${revision} decoded as ${side.decodedAs}.`]
  return []
}

function fragmentFailures(name: string, revision: bigint, side: FragmentSide) {
  return side.kind === 'failed' ? [{ title: `Reading ${name} at revision ${revision}`, failure: side.failure }] : []
}

const sections = computed<Section[]>(() => {
  const s = sides.value
  if (s === null) return []
  const fragment = (
    id: string,
    key: 'core' | 'parsed',
    name: string,
    pick: Parameters<typeof fragmentLines>[1],
    title: string,
    same: string,
  ): Section => ({
    key: id,
    title,
    same,
    diff: sectionDiff(fragmentLines(s.base[key], pick), fragmentLines(s.target[key], pick)),
    notes: [...fragmentNotes(name, props.base, s.base[key]), ...fragmentNotes(name, props.target, s.target[key])],
    failures: [...fragmentFailures(name, props.base, s.base[key]), ...fragmentFailures(name, props.target, s.target[key])],
  })
  const linkFailures = [
    ...(s.base.links.kind === 'failed' ? [{ title: `Listing links at revision ${props.base}`, failure: s.base.links.failure }] : []),
    ...(s.target.links.kind === 'failed' ? [{ title: `Listing links at revision ${props.target}`, failure: s.target.links.failure }] : []),
  ]
  return [
    fragment('metadata', 'core', 'CORE', (f) => searchMetadataLines(f.lines), 'Search metadata', 'The search metadata is the same.'),
    fragment('concepts', 'core', 'CORE', (f) => f.concepts, 'Concept tags', 'The bound concept tags are the same.'),
    {
      key: 'links',
      title: 'Outbound links',
      same: 'The links are the same.',
      diff: sectionDiff(
        s.base.links.kind === 'lines' ? s.base.links.lines : [],
        s.target.links.kind === 'lines' ? s.target.links.lines : [],
      ),
      notes: [],
      failures: linkFailures,
    },
    fragment('core', 'core', 'CORE', (f) => f.lines, 'CORE', 'The decoded CORE fragments are the same.'),
    fragment('parsed', 'parsed', 'PARSED', (f) => f.lines, 'PARSED', 'The decoded PARSED fragments are the same.'),
  ]
})

async function load() {
  const signal = scope.next()
  loading.value = true
  failure.value = null
  sides.value = null
  baseInput.value = props.base.toString()
  try {
    const [base, target] = await Promise.all([
      readSide(clients.documents, props.address, props.base, signal),
      readSide(clients.documents, props.address, props.target, signal),
    ])
    if (!signal.aborted) sides.value = { base, target }
  } catch (error) {
    if (!isOwnCancellation(error, signal) && !signal.aborted) failure.value = rpcFailure(error)
  } finally {
    if (!signal.aborted) loading.value = false
  }
}

function applyBase() {
  inputError.value = null
  let revision: bigint | null
  try {
    revision = parseRevision(baseInput.value.trim())
  } catch (error) {
    inputError.value = (error as Error).message
    return
  }
  if (revision === null) {
    inputError.value = 'Name the revision to compare with.'
    return
  }
  if (revision === props.target) {
    inputError.value = `Revision ${revision} is the revision shown; pick another one.`
    return
  }
  emit('base', revision)
}

onMounted(() => void load())
watch(() => [props.address, props.base, props.target], () => void load())
</script>
