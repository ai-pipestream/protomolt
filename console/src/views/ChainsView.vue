<template>
  <v-container fluid class="pa-6">
    <v-card variant="flat" border class="mb-4">
      <v-card-title class="text-subtitle-1 d-flex align-center">
        <v-btn icon="mdi-arrow-left" variant="text" size="small" class="mr-2"
               :to="{ name: 'schema-registry-subjects' }" aria-label="Back to subjects" />
        <v-icon size="small" class="mr-2">mdi-link-variant</v-icon>
        Chains
        <v-spacer />
        <span class="text-caption text-medium-emphasis">
          compose services into one type-checked call
        </span>
      </v-card-title>
      <v-card-text class="text-body-2 text-medium-emphasis pt-0">
        A chain runs serial gRPC calls where each step's request is mapped from the chain
        input and every prior step's response. Definitions are verified before anything
        runs and version in the registry — saving here is gated by the same check.
      </v-card-text>
    </v-card>

    <v-row dense>
      <!-- Stored chains -->
      <v-col cols="12" md="3">
        <v-card variant="flat" border>
          <v-card-title class="text-subtitle-2 d-flex align-center">
            Stored
            <v-spacer />
            <v-btn icon="mdi-refresh" variant="text" size="x-small" aria-label="Reload chains"
                   @click="load" />
          </v-card-title>
          <v-list density="compact" nav>
            <v-list-item v-for="name in names" :key="name" :active="name === selected"
                         @click="select(name)">
              <v-list-item-title class="text-body-2" style="font-family: var(--mono, monospace)">
                {{ name }}
              </v-list-item-title>
            </v-list-item>
            <v-list-item v-if="!names.length">
              <v-list-item-title class="text-caption text-medium-emphasis">
                Nothing stored yet — edit and save one.
              </v-list-item-title>
            </v-list-item>
          </v-list>
        </v-card>
      </v-col>

      <!-- Definition -->
      <v-col cols="12" md="5">
        <v-card variant="flat" border>
          <v-card-title class="text-subtitle-2 d-flex align-center">
            Definition
            <v-chip v-if="summary" size="x-small" variant="tonal" class="ml-2">{{ summary }}</v-chip>
            <v-spacer />
            <v-btn size="small" variant="tonal" class="mr-2" :loading="checking" @click="check">
              Check
            </v-btn>
            <v-btn size="small" color="primary" :loading="saving" :disabled="!saveName"
                   @click="save">
              Save
            </v-btn>
          </v-card-title>
          <v-card-text>
            <v-text-field
              v-model="saveName"
              label="Chain name"
              density="compact"
              variant="outlined"
              class="mb-2"
            />
            <v-textarea
              v-model="editor"
              variant="outlined"
              rows="18"
              auto-grow
              spellcheck="false"
              class="mono-editor"
              @update:model-value="dirty = true"
            />
            <v-alert v-if="checkResult && checkResult.ok" type="success" variant="tonal"
                     density="compact">
              The chain verifies: every method resolves, every rule and gate type-checks.
            </v-alert>
            <template v-if="findings.length">
              <v-alert type="warning" variant="tonal" density="compact" class="mb-2">
                {{ findings.length }} finding{{ findings.length === 1 ? '' : 's' }}
              </v-alert>
              <div v-for="(finding, i) in findings" :key="i" class="text-caption mb-1">
                <v-chip size="x-small" variant="tonal" color="warning" class="mr-1">
                  {{ finding.step || 'chain' }} · {{ finding.kind }}
                </v-chip>
                <code>{{ finding.error }}</code>
              </div>
            </template>
            <v-alert v-if="error" type="error" variant="tonal" density="compact">
              {{ error }}
            </v-alert>
          </v-card-text>
        </v-card>
      </v-col>

      <!-- Run -->
      <v-col cols="12" md="4">
        <v-card variant="flat" border>
          <v-card-title class="text-subtitle-2 d-flex align-center">
            Run
            <v-chip v-if="inputType" size="x-small" variant="tonal" class="ml-2">
              {{ inputType }}
            </v-chip>
            <v-spacer />
            <v-btn size="small" color="primary" prepend-icon="mdi-play"
                   :loading="running" :disabled="!selected || dirty" @click="run">
              Run
            </v-btn>
          </v-card-title>
          <v-card-text>
            <p v-if="dirty" class="text-caption text-medium-emphasis mb-2">
              Save the edited definition first — runs execute the stored chain.
            </p>
            <v-textarea
              v-model="runInput"
              label="Chain input (proto3 JSON)"
              variant="outlined"
              rows="6"
              auto-grow
              spellcheck="false"
              class="mono-editor"
            />
            <template v-if="runResult">
              <div class="d-flex align-center flex-wrap ga-1 mb-2">
                <v-chip v-for="step in runResult.steps ?? []" :key="step.name" size="small"
                        variant="tonal" :color="step.skipped ? undefined : 'success'"
                        :prepend-icon="step.skipped ? 'mdi-debug-step-over' : 'mdi-check'">
                  {{ step.name }}
                </v-chip>
              </div>
              <v-alert v-if="!runResult.ok" type="error" variant="tonal" density="compact">
                <span v-if="runResult.failedStep"><code>{{ runResult.failedStep }}</code>: </span>
                {{ runResult.error }}
              </v-alert>
              <template v-else>
                <div class="text-caption text-medium-emphasis mb-1">
                  {{ runResult.outputType }}
                </div>
                <pre class="output">{{ pretty(runResult.output) }}</pre>
              </template>
            </template>
          </v-card-text>
        </v-card>
      </v-col>
    </v-row>
  </v-container>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { errorMessage } from '../services/api'
import {
  chainSummary,
  checkChain,
  getChain,
  listChains,
  parseDefinition,
  putChain,
  runChain,
  type ChainCheck,
  type ChainFinding,
  type ChainRun,
} from '../services/chains'
import { toast } from '../composables/useToast'

const names = ref<string[]>([])
const selected = ref<string | null>(null)
const saveName = ref('')
const editor = ref('')
const dirty = ref(false)
const runInput = ref('{}')
const error = ref('')
const checking = ref(false)
const saving = ref(false)
const running = ref(false)
const checkResult = ref<ChainCheck | null>(null)
const findings = ref<ChainFinding[]>([])
const runResult = ref<ChainRun | null>(null)

const parsed = computed<Record<string, unknown> | null>(() => {
  try {
    return parseDefinition(editor.value)
  } catch {
    return null
  }
})
const summary = computed(() => (parsed.value ? chainSummary(parsed.value) : ''))
const inputType = computed(() =>
  parsed.value && typeof parsed.value.inputType === 'string' ? parsed.value.inputType : '')

onMounted(load)

async function load() {
  try {
    names.value = await listChains()
    error.value = ''
    if (!selected.value && names.value.length) {
      await select(names.value[0])
    }
  } catch (e) {
    error.value = errorMessage(e)
  }
}

async function select(name: string) {
  try {
    const definition = await getChain(name)
    selected.value = name
    saveName.value = name
    editor.value = JSON.stringify(definition, null, 2)
    dirty.value = false
    checkResult.value = null
    findings.value = []
    runResult.value = null
    error.value = ''
  } catch (e) {
    error.value = errorMessage(e)
  }
}

async function check() {
  checking.value = true
  checkResult.value = null
  findings.value = []
  try {
    const definition = parseDefinition(editor.value)
    const result = await checkChain(definition)
    checkResult.value = result
    findings.value = result.findings ?? []
    error.value = ''
  } catch (e) {
    error.value = errorMessage(e)
  } finally {
    checking.value = false
  }
}

async function save() {
  saving.value = true
  findings.value = []
  try {
    const definition = parseDefinition(editor.value)
    await putChain(saveName.value, definition)
    toast.success(`Stored ${saveName.value}`)
    dirty.value = false
    selected.value = saveName.value
    error.value = ''
    await load()
  } catch (e) {
    const gate = e as Error & { findings?: ChainFinding[] }
    findings.value = gate.findings ?? []
    error.value = errorMessage(e)
  } finally {
    saving.value = false
  }
}

async function run() {
  if (!selected.value) return
  running.value = true
  runResult.value = null
  try {
    const input = JSON.parse(runInput.value)
    runResult.value = await runChain(selected.value, input)
    error.value = ''
  } catch (e) {
    error.value = errorMessage(e)
  } finally {
    running.value = false
  }
}

function pretty(value: unknown): string {
  return JSON.stringify(value, null, 2)
}
</script>

<style scoped>
.mono-editor :deep(textarea) {
  font-family: var(--mono, ui-monospace, monospace);
  font-size: 0.82rem;
  line-height: 1.5;
}
.output {
  font-family: var(--mono, ui-monospace, monospace);
  font-size: 0.8rem;
  line-height: 1.5;
  padding: 10px 12px;
  border: 1px solid rgba(var(--v-border-color), var(--v-border-opacity));
  border-radius: 6px;
  overflow-x: auto;
  max-height: 420px;
}
</style>
