<template>
  <div>
    <div class="text-caption text-medium-emphasis mb-3">
      Edit a candidate schema for <code>{{ subject }}</code> and check it against the registry.
      The check uses the content-identity lookup (the server has no dry-run endpoint) — compatibility
      under the effective mode
      <CompatibilityBadge
        v-if="effectiveMode"
        :mode="effectiveMode"
        :subject="subject"
        :inherited="modeInherited"
        @changed="$emit('mode-changed', $event)"
      />
      is enforced when you actually register.
    </div>

    <v-row>
      <v-col cols="12" lg="7">
        <v-textarea
          v-model="candidate"
          label="Candidate schema (.proto)"
          variant="outlined"
          rows="16"
          auto-grow
          class="proto-editor"
          spellcheck="false"
          @update:model-value="invalidate"
        />

        <!-- References editor -->
        <v-card variant="flat" border class="mb-4">
          <v-card-title class="text-subtitle-2 d-flex align-center">
            <v-icon size="small" class="mr-1">mdi-link-variant</v-icon>
            References
            <v-spacer />
            <v-btn size="x-small" variant="text" prepend-icon="mdi-plus" @click="addReference">
              add
            </v-btn>
          </v-card-title>
          <v-card-text v-if="references.length" class="pt-0">
            <div v-for="(ref, i) in references" :key="i" class="d-flex ga-2 mb-2 align-center">
              <v-text-field
                v-model="ref.name"
                label="Import name"
                density="compact"
                variant="outlined"
                hide-details
                @update:model-value="invalidate"
              />
              <v-text-field
                v-model="ref.subject"
                label="Subject"
                density="compact"
                variant="outlined"
                hide-details
                @update:model-value="invalidate"
              />
              <v-text-field
                v-model.number="ref.version"
                label="Version"
                type="number"
                density="compact"
                variant="outlined"
                hide-details
                style="max-width: 110px"
                @update:model-value="invalidate"
              />
              <v-btn
                icon="mdi-close"
                size="x-small"
                variant="text"
                aria-label="Remove reference"
                @click="removeReference(i)"
              />
            </div>
          </v-card-text>
          <v-card-text v-else class="pt-0 text-caption text-medium-emphasis">
            No references — add one per <code>import</code> of another registered subject.
          </v-card-text>
        </v-card>

        <div class="d-flex ga-2">
          <v-btn
            color="primary"
            variant="tonal"
            prepend-icon="mdi-magnify-scan"
            :loading="checking"
            :disabled="!candidate.trim()"
            @click="check"
          >
            Check
          </v-btn>
          <v-btn variant="text" :disabled="checking || registering" @click="resetToLatest">
            Reset to latest
          </v-btn>
        </div>
      </v-col>

      <v-col cols="12" lg="5">
        <!-- Outcome -->
        <v-alert v-if="checkError" type="error" variant="tonal" density="compact" class="mb-4">
          {{ checkError }}
        </v-alert>

        <template v-if="outcome">
          <v-alert
            v-if="outcome.kind === 'unchanged'"
            type="success"
            variant="tonal"
            class="mb-4"
            icon="mdi-equal"
          >
            <div class="font-weight-medium">Unchanged</div>
            This exact schema is already registered as
            <strong>v{{ outcome.version }}</strong> (id {{ outcome.id }}). Registering again would
            be a no-op.
          </v-alert>

          <v-alert
            v-else-if="outcome.kind === 'invalid'"
            type="error"
            variant="tonal"
            class="mb-4"
            icon="mdi-file-alert-outline"
          >
            <div class="font-weight-medium">Invalid schema</div>
            <div class="text-caption">{{ outcome.message }}</div>
          </v-alert>

          <v-alert
            v-else
            type="warning"
            variant="tonal"
            class="mb-4"
            icon="mdi-alert-decagram-outline"
          >
            <div class="font-weight-medium">
              {{ outcome.kind === 'new-subject' ? 'New subject' : 'This WILL register a new version' }}
            </div>
            <div class="text-caption mb-2">
              {{
                outcome.kind === 'new-subject'
                  ? `“${subject}” does not exist yet — registering creates it at version 1.`
                  : 'The candidate differs from every stored version. Registering writes it to the registry permanently, after the server checks it under the effective compatibility mode.'
              }}
            </div>
            <v-checkbox
              v-model="confirmed"
              density="compact"
              hide-details
              :label="`I understand this registers a new version of ${subject}`"
            />
            <v-btn
              color="warning"
              variant="flat"
              size="small"
              class="mt-2"
              prepend-icon="mdi-upload"
              :disabled="!confirmed"
              :loading="registering"
              @click="doRegister"
            >
              Register now
            </v-btn>
          </v-alert>
        </template>

        <!-- Register result -->
        <v-alert
          v-if="registered"
          type="success"
          variant="tonal"
          class="mb-4"
          icon="mdi-check-decagram"
        >
          Registered — global id <strong>{{ registered.id }}</strong>.
        </v-alert>

        <!-- 409 violations, parsed -->
        <v-card v-if="violations.length" variant="flat" border class="mb-4">
          <v-card-title class="text-subtitle-2 text-error">
            <v-icon size="small" class="mr-1" color="error">mdi-shield-alert</v-icon>
            Incompatible — {{ violations.length }} violation{{ violations.length === 1 ? '' : 's' }}
          </v-card-title>
          <v-table density="compact">
            <thead>
              <tr>
                <th style="width: 1%">Vs</th>
                <th>Rule</th>
                <th>Path</th>
                <th>Detail</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="(violation, i) in violations" :key="i">
                <td class="text-medium-emphasis">{{ violation.version ?? '—' }}</td>
                <td>
                  <v-chip v-if="violation.rule" size="x-small" color="error" variant="tonal" class="font-mono">
                    {{ violation.rule }}
                  </v-chip>
                  <span v-else class="text-medium-emphasis">—</span>
                </td>
                <td class="font-mono text-caption">{{ violation.path || '—' }}</td>
                <td class="text-caption">{{ violation.detail }}</td>
              </tr>
            </tbody>
          </v-table>
        </v-card>

        <v-card v-if="!outcome && !violations.length && !checkError" variant="flat" border>
          <v-card-text class="text-caption text-medium-emphasis">
            <div class="mb-2 d-flex align-center">
              <v-icon size="small" class="mr-2">mdi-information-outline</v-icon>
              How the check works
            </div>
            <ol class="ml-4">
              <li><strong>Check</strong> asks the registry if this exact content is already stored.</li>
              <li>If it is new, you get an explicit confirmation step — nothing registers silently.</li>
              <li>
                <strong>Register</strong> runs the server's compatibility gate; a rejection is shown
                as rule / path / detail rows.
              </li>
            </ol>
          </v-card-text>
        </v-card>
      </v-col>
    </v-row>
  </div>
</template>

<script setup lang="ts">
import { ref, watch } from 'vue'
import CompatibilityBadge from './CompatibilityBadge.vue'
import { toast } from '@/composables/useToast'
import {
  errorMessage,
  registryApi,
  type CompatibilityMode,
  type CompatViolation,
  type SchemaReference,
} from '../services/api'
import { checkCandidate, registerCandidate, type CheckOutcome } from '../services/compatCheck'

const props = defineProps<{
  subject: string
  /** Latest schema text, used to seed the editor. */
  latestSchema: string
  /** Latest version's references, used to seed the references editor. */
  latestReferences: SchemaReference[]
  effectiveMode: CompatibilityMode | null
  modeInherited: boolean
}>()

const emit = defineEmits<{
  'mode-changed': [mode: CompatibilityMode]
  registered: []
}>()

const candidate = ref('')
const references = ref<SchemaReference[]>([])
const checking = ref(false)
const registering = ref(false)
const confirmed = ref(false)
const outcome = ref<CheckOutcome | null>(null)
const checkError = ref('')
const violations = ref<CompatViolation[]>([])
const registered = ref<{ id: number } | null>(null)

watch(
  () => [props.subject, props.latestSchema] as const,
  () => resetToLatest(),
  { immediate: true },
)

function resetToLatest() {
  candidate.value = props.latestSchema
  references.value = props.latestReferences.map((r) => ({ ...r }))
  invalidate()
}

/** Any edit voids the previous check — no stale "will register" banners. */
function invalidate() {
  outcome.value = null
  checkError.value = ''
  violations.value = []
  registered.value = null
  confirmed.value = false
}

function addReference() {
  references.value = [...references.value, { name: '', subject: '', version: 1 }]
  invalidate()
}

function removeReference(i: number) {
  references.value = references.value.filter((_, idx) => idx !== i)
  invalidate()
}

function cleanReferences(): SchemaReference[] {
  return references.value
    .filter((r) => r.name.trim() || r.subject.trim())
    .map((r) => ({ name: r.name.trim(), subject: r.subject.trim(), version: Number(r.version) || 1 }))
}

async function check() {
  checking.value = true
  invalidate()
  try {
    outcome.value = await checkCandidate(
      registryApi,
      props.subject,
      candidate.value,
      cleanReferences(),
    )
  } catch (e) {
    checkError.value = errorMessage(e)
  } finally {
    checking.value = false
  }
}

async function doRegister() {
  registering.value = true
  checkError.value = ''
  violations.value = []
  try {
    const result = await registerCandidate(
      registryApi,
      props.subject,
      candidate.value,
      cleanReferences(),
    )
    if (result.kind === 'registered') {
      registered.value = { id: result.id }
      outcome.value = null
      toast.success(`Registered new version of ${props.subject} (id ${result.id})`)
      emit('registered')
    } else if (result.kind === 'incompatible') {
      violations.value = result.violations
      outcome.value = null
    } else {
      outcome.value = { kind: 'invalid', message: result.message }
    }
  } catch (e) {
    checkError.value = errorMessage(e)
  } finally {
    registering.value = false
  }
}
</script>

<style scoped>
.proto-editor :deep(textarea) {
  font-family: ui-monospace, 'SF Mono', 'JetBrains Mono', Menlo, Consolas, monospace;
  font-size: 12.5px;
  line-height: 1.55;
}

.font-mono {
  font-family: ui-monospace, 'SF Mono', 'JetBrains Mono', Menlo, Consolas, monospace;
}
</style>
