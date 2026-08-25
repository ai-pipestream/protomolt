<template>
  <div>
    <v-card variant="flat" border class="mb-4">
      <v-card-title class="text-subtitle-1 d-flex align-center">
        <v-icon size="small" class="mr-2">mdi-file-certificate-outline</v-icon>
        Receipts
      </v-card-title>
      <v-card-text class="text-body-2 text-medium-emphasis pt-0">
        A work record is a signed receipt for a run: what ran, over what, in what order.
        Verification checks that its signatures and digests hold; evaluation replays it
        against a workflow to show the workflow was actually followed.
      </v-card-text>
    </v-card>

    <v-row dense>
      <v-col cols="12" md="4">
        <v-card variant="flat" border class="mb-4">
          <v-card-title class="text-subtitle-2">The record</v-card-title>
          <v-card-text>
            <v-text-field v-model="runId" label="Run id" density="compact" class="mb-1"
                          hint="A recorded run's identity — export projects its receipt"
                          persistent-hint @keydown.enter="exportFromRun" />
            <v-btn size="small" variant="tonal" class="mb-4" :loading="exporting"
                   :disabled="!runId" @click="exportFromRun">
              Export from the run
            </v-btn>
            <v-textarea v-model="record" label="Or paste a record (base64)" rows="5"
                        density="compact" class="text-mono" spellcheck="false" />
            <v-btn color="primary" size="small" class="mb-4" :loading="verifying"
                   :disabled="!record.trim()" @click="verify">
              Verify
            </v-btn>
            <v-select v-model="workflowName" :items="workflowNames"
                      label="Workflow to evaluate against" density="compact" class="mb-1"
                      hint="Evaluation replays the record against a stored workflow's contract"
                      persistent-hint />
            <v-btn size="small" variant="tonal" :loading="evaluating"
                   :disabled="!record.trim() || !workflowName" @click="evaluate">
              Evaluate
            </v-btn>
            <v-alert v-if="error" type="error" variant="tonal" density="compact" class="mt-2">
              {{ error }}
            </v-alert>
          </v-card-text>
        </v-card>
      </v-col>

      <v-col cols="12" md="8">
        <template v-if="verification">
          <v-card variant="flat" border class="mb-4">
            <v-card-title class="text-subtitle-2 d-flex align-center">
              Verification
              <v-chip size="small" variant="tonal" class="ml-2"
                      :color="verification.verified ? 'success' : 'error'"
                      :prepend-icon="verification.verified ? 'mdi-check-decagram'
                          : 'mdi-alert-decagram'">
                {{ verification.verified ? 'the record holds' : 'the record does not hold' }}
              </v-chip>
              <v-spacer />
              <span v-if="verification.manifestDigest"
                    class="text-caption text-medium-emphasis text-mono">
                {{ shortDigest(verification.manifestDigest) }}
              </span>
            </v-card-title>
            <v-card-text>
              <CheckList :checks="verification.checks" />
              <NonClaims :claims="verification.nonClaims" />
            </v-card-text>
          </v-card>
        </template>

        <template v-if="evaluation">
          <v-card variant="flat" border class="mb-4">
            <v-card-title class="text-subtitle-2 d-flex align-center">
              Evaluation
              <v-chip size="small" variant="tonal" class="ml-2"
                      :color="evaluation.accepted ? 'success' : 'error'"
                      :prepend-icon="evaluation.accepted ? 'mdi-check-decagram'
                          : 'mdi-alert-decagram'">
                {{ evaluation.accepted ? 'accepted' : 'not accepted' }}
              </v-chip>
              <v-chip v-if="evaluation.policyId" size="x-small" variant="tonal"
                      class="ml-2 text-mono">
                policy {{ evaluation.policyId }}
              </v-chip>
            </v-card-title>
            <v-card-text>
              <CheckList :checks="evaluation.checks" />
              <template v-if="evaluation.replaySteps.length">
                <div class="text-caption font-weight-medium mt-3 mb-1">
                  The recorded steps, replayed
                </div>
                <div class="d-flex align-center flex-wrap ga-1 mb-2">
                  <v-chip v-for="step in evaluation.replaySteps" :key="step.stepName"
                          size="small" variant="tonal"
                          :color="step.ok ? 'success' : 'error'"
                          :prepend-icon="step.ok ? 'mdi-check' : 'mdi-close'">
                    {{ step.stepName }}
                  </v-chip>
                </div>
                <p v-for="step in failedReplays" :key="step.stepName"
                   class="text-caption text-error mb-1">
                  <code>{{ step.stepName }}</code>: {{ step.detail }}
                </p>
              </template>
              <NonClaims :claims="evaluation.nonClaims" />
            </v-card-text>
          </v-card>
        </template>

        <v-card v-if="!verification && !evaluation" variant="flat" border>
          <v-card-text class="text-body-2 text-medium-emphasis">
            <p class="mb-2">
              Verification renders each signature and digest check with what it proved —
              and, just as deliberately, what the record never claimed.
            </p>
            <p class="mb-0">
              Records verify anywhere: the zero-dependency verifier ships separately,
              so a counterpart does not need ProtoMolt to check a receipt you hand them.
            </p>
          </v-card-text>
        </v-card>
      </v-col>
    </v-row>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref, shallowRef } from 'vue'
import CheckList from '../components/WorkRecordCheckList.vue'
import NonClaims from '../components/WorkRecordNonClaims.vue'
import { errorMessage } from '../services/api'
import {
  evaluateRecord,
  exportRecord,
  verifyRecord,
  type Evaluation,
  type Verification,
} from '../services/receipts'
import { compileWorkflow, getWorkflow, listWorkflows } from '../services/workflows'
import { toast } from '../composables/useToast'

const runId = ref('')
const record = ref('')
const workflowNames = ref<string[]>([])
const workflowName = ref<string | null>(null)
const exporting = ref(false)
const verifying = ref(false)
const evaluating = ref(false)
const error = ref('')
const verification = shallowRef<Verification | null>(null)
const evaluation = shallowRef<Evaluation | null>(null)

const failedReplays = computed(() =>
  (evaluation.value?.replaySteps ?? []).filter((s) => !s.ok && s.detail))

onMounted(async () => {
  try {
    workflowNames.value = await listWorkflows()
    workflowName.value = workflowNames.value[0] ?? null
  } catch {
    // No registry, no stored workflows: verification still works on its own.
  }
})

async function exportFromRun() {
  exporting.value = true
  error.value = ''
  try {
    const exported = await exportRecord(runId.value)
    record.value = exported.recordBase64
    toast.success(exported.recordId ? `Exported ${exported.recordId}` : 'Exported')
  } catch (e) {
    error.value = errorMessage(e)
  } finally {
    exporting.value = false
  }
}

async function verify() {
  verifying.value = true
  error.value = ''
  evaluation.value = null
  try {
    verification.value = await verifyRecord(record.value.trim())
  } catch (e) {
    verification.value = null
    error.value = errorMessage(e)
  } finally {
    verifying.value = false
  }
}

async function evaluate() {
  if (!workflowName.value) return
  evaluating.value = true
  error.value = ''
  verification.value = null
  try {
    // The verb takes the compiled workflow and its schema: evaluation is a
    // claim about a specific contract, stated here by a stored definition.
    const definition = await getWorkflow(workflowName.value)
    const compiled = await compileWorkflow(definition)
    const schema = definition.schema as Record<string, unknown> | undefined
    if (!schema) {
      throw new Error(`workflow '${workflowName.value}' has no schema to evaluate against`)
    }
    evaluation.value = await evaluateRecord(record.value.trim(), compiled, schema)
  } catch (e) {
    evaluation.value = null
    error.value = errorMessage(e)
  } finally {
    evaluating.value = false
  }
}

function shortDigest(digest: string): string {
  return digest.length > 20 ? `${digest.slice(0, 20)}…` : digest
}
</script>
