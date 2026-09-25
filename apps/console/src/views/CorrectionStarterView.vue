<template>
  <div class="py-4 mx-auto" style="max-width: 1040px">
    <h1 class="text-h4 mb-2">Run your first correction</h1>
    <p class="text-medium-emphasis mb-6">Source data → typed projection → validated result → judgment → signed receipts.</p>
    <v-progress-linear v-if="loading" indeterminate />
    <v-card v-else-if="!authenticated" class="pa-6" max-width="540">
      <h2 class="text-h6 mb-3">Connect to your coordinator</h2>
      <p class="mb-4">Use the console token from your starter installation.</p>
      <v-form @submit.prevent="login">
        <v-text-field v-model="token" type="password" label="Console token" autocomplete="current-password" />
        <v-btn type="submit" color="primary" :loading="busy">Connect</v-btn>
      </v-form>
    </v-card>
    <template v-else>
      <v-alert v-if="available" type="info" variant="tonal" class="mb-5">
        The standard Compose starter uses a deterministic Ada Lovelace fixture. It needs no model account.
        A separately configured live provider is identified in the recorded judgment.
      </v-alert>
      <v-card v-if="available" class="pa-6 mb-5">
        <h2 class="text-h6 mb-4">1. Supply the source</h2>
        <v-form @submit.prevent="run">
          <v-text-field v-model="runId" label="Run ID" hint="Single-use lowercase identifier. Keep it to retrieve this run after restart." persistent-hint class="mb-3" />
          <v-text-field v-model="recordId" label="Source record ID" />
          <v-textarea v-model="contactText" label="Contact text" rows="2" />
          <v-textarea v-model="privateNotes" label="Internal notes (excluded from model evidence)" rows="2" />
          <v-btn type="submit" color="primary" :loading="busy" :disabled="busy">Run correction</v-btn>
          <v-btn class="ml-3" variant="text" :disabled="busy" @click="runId = newRunId()">New run ID</v-btn>
        </v-form>
      </v-card>
      <v-card v-if="available" class="pa-6 mb-5">
        <h2 class="text-h6 mb-3">2. Inspect the recorded outcome</h2>
        <div class="d-flex ga-3 align-center mb-3">
          <v-text-field v-model="lookupId" label="Saved run ID" hide-details />
          <v-btn :disabled="busy || !lookupId.trim()" @click="retrieve">Retrieve</v-btn>
        </div>
        <template v-if="outcome">
          <v-chip class="mb-3" :color="accepted ? 'success' : 'warning'">{{ disposition }}</v-chip>
          <p class="mb-2">{{ reason }}</p>
          <p class="text-medium-emphasis mb-4">The service validated and verified the stored records. Receipt signatures bind the evidence and policy; they do not prove the judgment is true.</p>
          <v-btn variant="outlined" @click="download">Download outcome and receipts</v-btn>
          <details class="mt-4"><summary>Evidence and protocol response</summary><pre class="receipt-json">{{ JSON.stringify(outcome, null, 2) }}</pre></details>
        </template>
        <p v-else class="text-medium-emphasis">Run the fixture or retrieve a saved run. Completed outcomes remain available after a restart.</p>
      </v-card>
      <v-card v-if="available" class="pa-6 mb-5">
        <h2 class="text-h6 mb-3">3. Connect a client</h2>
        <p>Use the same <code>correction</code> service profile from gRPC, MCP, or ACP.</p>
        <ul class="ml-5 my-3">
          <li>gRPC: the published coordinator port (default <code>localhost:9090</code>), <code>ProtoMoltService.ServiceInvoke</code>.</li>
          <li>MCP: <code>{{ mcpUrl }}</code>, streamable HTTP, tool <code>service-invoke</code>.</li>
          <li>ACP: launch <code>docker compose run --rm -T acp</code> from the starter directory. ACP uses stdio.</li>
        </ul>
        <p class="text-medium-emphasis">Protocol clients use the separate API credential. The browser session cannot invoke arbitrary services.</p>
        <details class="mt-3"><summary>Retrieve this run through a protocol client</summary><pre class="receipt-json">{{ invocation }}</pre></details>
        <p class="mt-4">For independent verification, export public evidence and trust using the starter's verification instructions. The JSON download alone is not the full offline evidence bundle.</p>
      </v-card>
      <v-btn variant="text" @click="logout">Sign out</v-btn>
    </template>
    <v-alert v-if="error" type="error" variant="tonal" class="mt-4">{{ error }}</v-alert>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { TaskApi } from '../services/tasks'

const session = new TaskApi()
const loading = ref(true)
const busy = ref(false)
const authenticated = ref(false)
const available = ref(false)
const token = ref('')
const error = ref('')
const newRunId = () => `contact-${crypto.randomUUID()}`
const runId = ref(newRunId())
const lookupId = ref('')
const recordId = ref('contact-1')
const contactText = ref('Ada Lovelace; ada [at] example.org')
const privateNotes = ref('Private source note; excluded from provider evidence.')
type Outcome = { outcome?: { runId?: string; assessment?: { disposition?: string; reason?: string } } }
const outcome = ref<Outcome | null>(null)
const disposition = computed(() => outcome.value?.outcome?.assessment?.disposition?.replace('ASSESSMENT_DISPOSITION_', '') ?? 'RECORDED')
const accepted = computed(() => disposition.value === 'ACCEPTED')
const reason = computed(() => outcome.value?.outcome?.assessment?.reason ?? '')
const mcpUrl = `${window.location.origin}/mcp`
const invocation = computed(() => JSON.stringify({ name: 'correction', endpoint: 'default', method: 'ai.protomolt.proto.correction.v1.CorrectionService/GetCorrection', request: { runId: lookupId.value || runId.value } }, null, 2))

async function request(path: string, body?: object): Promise<Outcome> {
  const response = await fetch(`/api/correction${path}`, {
    method: body ? 'POST' : 'GET', credentials: 'same-origin',
    ...(body ? { headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) } : {}),
  })
  const result = await response.json().catch(() => ({}))
  if (!response.ok) {
    if (response.status === 401) authenticated.value = false
    const code = String(result.error ?? response.status)
    if (code === 'ALREADY_EXISTS') throw new Error('This run ID has already been used. Retrieve the completed run or choose a new ID; do not retry it automatically.')
    if (code === 'NOT_FOUND') throw new Error('No completed outcome exists for this ID. Interrupted runs cannot resume in this starter.')
    if (code === 'invalid-input' || code === 'INVALID_ARGUMENT') throw new Error('The request does not satisfy the correction contract. Check the run ID and source fields.')
    throw new Error(`Correction service: ${code}`)
  }
  return result as Outcome
}

async function checkAvailable() {
  await request('')
  available.value = true
}
async function perform(work: () => Promise<void>) {
  busy.value = true; error.value = ''
  try { await work() } catch (failure) { error.value = failure instanceof Error ? failure.message : String(failure) }
  finally { busy.value = false }
}
async function login() {
  await perform(async () => { authenticated.value = (await session.login(token.value)).authenticated; token.value = ''; await checkAvailable() })
}
async function logout() { await session.logout(); authenticated.value = false; outcome.value = null }
async function run() {
  await perform(async () => {
    outcome.value = null
    outcome.value = await request('/runs', { workflowName: 'correct-contact', runId: runId.value, source: { recordId: recordId.value, contactText: contactText.value, internalNotes: privateNotes.value } })
    lookupId.value = runId.value
  })
}
async function retrieve() { await perform(async () => { outcome.value = null; outcome.value = await request(`/runs/${encodeURIComponent(lookupId.value.trim())}`) }) }
function download() {
  const url = URL.createObjectURL(new Blob([JSON.stringify(outcome.value, null, 2)], { type: 'application/json' }))
  const link = document.createElement('a'); link.href = url; link.download = `${outcome.value?.outcome?.runId ?? 'correction'}-outcome.json`; link.click(); URL.revokeObjectURL(url)
}
onMounted(async () => {
  await perform(async () => { authenticated.value = (await session.sessionStatus()).authenticated; if (authenticated.value) await checkAvailable() })
  loading.value = false
})
</script>

<style scoped>
.receipt-json { white-space: pre-wrap; overflow-wrap: anywhere; max-height: 32rem; overflow: auto; padding: 1rem; font-size: .8rem; }
summary { cursor: pointer; }
</style>
