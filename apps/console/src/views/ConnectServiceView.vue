<template>
  <div>
    <h2 class="text-h6 mb-1">Connect a gRPC service</h2>
    <p class="text-body-2 text-medium-emphasis mb-4">
      Point ProtoMolt at a service and leave with working directions: agents get it over MCP,
      pipelines get it as a Kafka Connect sink. Reflection needs only the address; services
      that publish their contract instead can bring it from git or pasted sources.
    </p>

    <v-card variant="outlined" class="mb-4">
      <v-card-text>
        <v-btn-group density="compact" divided class="mb-4">
          <v-btn
            v-for="mode in modes"
            :key="mode.value"
            size="small"
            :variant="source === mode.value ? 'tonal' : 'text'"
            @click="source = mode.value"
          >{{ mode.label }}</v-btn>
        </v-btn-group>

        <v-row dense>
          <v-col cols="12" md="5">
            <v-text-field
              v-model="target"
              label="gRPC target"
              placeholder="inference-host:9000"
              density="compact"
              hide-details
            />
          </v-col>
          <v-col cols="12" md="3" class="d-flex align-center">
            <v-switch
              v-model="tls"
              label="TLS"
              density="compact"
              color="primary"
              hide-details
            />
          </v-col>
        </v-row>

        <template v-if="source === 'git'">
          <v-row dense class="mt-2">
            <v-col cols="12" md="6">
              <v-text-field v-model="gitRepo" label="Git repository URL" density="compact" hide-details />
            </v-col>
            <v-col cols="6" md="2">
              <v-text-field v-model="gitRef" label="Ref" placeholder="main" density="compact" hide-details />
            </v-col>
            <v-col cols="6" md="4">
              <v-text-field
                v-model="gitSubdir"
                label="Subdir"
                placeholder="proto ('.' for root)"
                density="compact"
                hide-details
              />
            </v-col>
          </v-row>
        </template>

        <template v-if="source === 'paste'">
          <v-row dense class="mt-2">
            <v-col cols="12" md="4">
              <v-text-field
                v-model="pastePath"
                label="Import path"
                placeholder="acme/v1/service.proto"
                density="compact"
                hide-details
              />
            </v-col>
          </v-row>
          <v-textarea
            v-model="pasteText"
            label=".proto source"
            rows="8"
            class="mt-2 font-mono"
            density="compact"
            hide-details
          />
        </template>

        <div class="d-flex align-center ga-3 mt-4">
          <v-btn color="primary" :loading="busy" @click="connect">
            {{ source === 'reflect' ? 'Reflect' : source === 'git' ? 'Gather from git' : 'Compile' }}
          </v-btn>
          <span v-if="error" class="text-error text-body-2">{{ error }}</span>
        </div>
      </v-card-text>
    </v-card>

    <template v-if="schema">
      <v-alert type="success" variant="tonal" density="compact" class="mb-4">
        Schema in hand: {{ schema.services.length }} service(s), {{ schema.fileCount }} file(s)
        <template v-if="schema.services.length"> — {{ schema.services.join(', ') }}</template>
      </v-alert>

      <v-row dense>
        <v-col cols="12" md="6">
          <v-card variant="outlined" class="mb-4">
            <v-card-title class="text-subtitle-2 d-flex align-center">
              Make it agent-operable (MCP)
              <v-spacer />
              <v-btn icon="mdi-content-copy" variant="text" size="x-small" @click="copy(mcpCommand)" />
            </v-card-title>
            <v-card-text>
              <v-text-field
                v-model="serveUrl"
                label="protomolt-serve URL agents will use"
                density="compact"
                class="mb-2"
                hide-details
              />
              <pre class="snippet">{{ mcpCommand }}</pre>
              <div class="text-caption text-medium-emphasis mt-3 mb-1 d-flex align-center">
                Hand your agent this prompt
                <v-spacer />
                <v-btn icon="mdi-content-copy" variant="text" size="x-small" @click="copy(agentPrompt)" />
              </div>
              <pre class="snippet">{{ agentPrompt }}</pre>
            </v-card-text>
          </v-card>
        </v-col>

        <v-col cols="12" md="6">
          <v-card variant="outlined" class="mb-4">
            <v-card-title class="text-subtitle-2 d-flex align-center">
              Make it a Kafka Connect sink
              <v-spacer />
              <v-btn icon="mdi-content-copy" variant="text" size="x-small" @click="copy(sinkCurl)" />
            </v-card-title>
            <v-card-text>
              <v-row dense class="mb-1">
                <v-col cols="12" md="7">
                  <v-select
                    v-model="sinkMethod"
                    :items="methodChoices"
                    label="Method (unary or client-streaming)"
                    density="compact"
                    hide-details
                  />
                </v-col>
                <v-col cols="6" md="5">
                  <v-text-field v-model="sinkTopics" label="Topics" density="compact" hide-details />
                </v-col>
              </v-row>
              <v-row dense class="mb-2">
                <v-col cols="6" md="5">
                  <v-select
                    v-model="sinkFormat"
                    :items="['protobuf', 'confluent', 'json']"
                    label="Record value format"
                    density="compact"
                    hide-details
                  />
                </v-col>
              </v-row>
              <pre v-if="sinkMethod" class="snippet">{{ sinkCurl }}</pre>
              <div v-else class="text-caption text-medium-emphasis">
                Pick a method to generate the connector config.
              </div>
            </v-card-text>
          </v-card>
        </v-col>
      </v-row>
    </template>
  </div>
</template>

<script setup lang="ts">
import { computed, ref, shallowRef } from 'vue'
import { fromBinary } from '@bufbuild/protobuf'
import { FileDescriptorSetSchema } from '@bufbuild/protobuf/wkt'
import { registryFromDescriptorSet } from '@/services/descriptorModel'
import {
  buildAgentPrompt,
  buildMcpCommand,
  buildSinkCurl,
} from '@/lib/connect/directions'
import { toast } from '@/composables/useToast'

type SourceMode = 'reflect' | 'git' | 'paste'

const modes: { value: SourceMode; label: string }[] = [
  { value: 'reflect', label: 'Reflection' },
  { value: 'git', label: 'Git repository' },
  { value: 'paste', label: 'Paste .proto' },
]

const source = ref<SourceMode>('reflect')
const target = ref('')
const tls = ref(false)
const gitRepo = ref('')
const gitRef = ref('')
const gitSubdir = ref('')
const pastePath = ref('service.proto')
const pasteText = ref('')

const busy = ref(false)
const error = ref('')
const serveUrl = ref(`${window.location.protocol}//${window.location.hostname}:8080`)

interface AcquiredSchema {
  descriptorSetBase64: string
  services: string[]
  methods: string[]
  fileCount: number
}
const schema = shallowRef<AcquiredSchema | null>(null)

const sinkMethod = ref<string | null>(null)
const sinkTopics = ref('events')
const sinkFormat = ref<'protobuf' | 'confluent' | 'json'>('protobuf')

const methodChoices = computed(() => schema.value?.methods ?? [])

async function verb(name: string, body: unknown): Promise<Record<string, unknown>> {
  const response = await fetch(`/api/serve/grpc-json/ProtoMoltService/${name}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  })
  const json = (await response.json()) as Record<string, unknown>
  if (!response.ok) {
    throw new Error(String(json.error ?? response.statusText))
  }
  return json
}

async function connect() {
  busy.value = true
  error.value = ''
  schema.value = null
  sinkMethod.value = null
  try {
    let descriptorSetBase64: string
    if (source.value === 'reflect') {
      if (!target.value) throw new Error('Enter the gRPC target to reflect.')
      const result = await verb('Reflect', { target: target.value, tls: tls.value })
      if (!result.ok) {
        throw new Error(
          `${result.error ?? 'Reflection failed'} — if the server does not enable ` +
            `reflection, bring the schema from git or paste it.`,
        )
      }
      descriptorSetBase64 = String(result.descriptorSetBase64)
    } else if (source.value === 'git') {
      if (!gitRepo.value) throw new Error('Enter the git repository URL.')
      const body: Record<string, unknown> = { repo: gitRepo.value }
      if (gitRef.value) body.ref = gitRef.value
      if (gitSubdir.value) body.subdir = gitSubdir.value
      const result = await verb('GatherGit', body)
      if (!result.ok) throw new Error(String(result.error ?? 'Gather failed'))
      descriptorSetBase64 = String(result.descriptorSetBase64)
    } else {
      if (!pasteText.value.trim()) throw new Error('Paste the .proto source.')
      const result = await verb('Compile', {
        sources: { [pastePath.value || 'service.proto']: pasteText.value },
      })
      if (!result.ok) {
        throw new Error(String((result.errors as string[] | undefined)?.[0] ?? 'Compile failed'))
      }
      descriptorSetBase64 = String(result.descriptorSetBase64)
    }
    schema.value = inspect(descriptorSetBase64)
    if (schema.value.methods.length === 1) sinkMethod.value = schema.value.methods[0]
  } catch (e) {
    error.value = (e as Error).message
  } finally {
    busy.value = false
  }
}

/** Client-side reflection over the acquired descriptor set: services + callable methods. */
function inspect(descriptorSetBase64: string): AcquiredSchema {
  const bytes = Uint8Array.from(atob(descriptorSetBase64), (c) => c.charCodeAt(0))
  const registry = registryFromDescriptorSet(bytes)
  const set = fromBinary(FileDescriptorSetSchema, bytes)
  const services: string[] = []
  const methods: string[] = []
  for (const file of set.file) {
    if (file.name?.startsWith('google/protobuf/')) continue
    for (const service of file.service) {
      const full = file.package ? `${file.package}.${service.name}` : String(service.name)
      if (full === 'grpc.reflection.v1.ServerReflection') continue
      services.push(full)
      const desc = registry.getService(full)
      for (const method of desc?.methods ?? []) {
        // The sink supports unary and client-streaming shapes.
        if (method.methodKind === 'unary' || method.methodKind === 'client_streaming') {
          methods.push(`${full}/${method.name}`)
        }
      }
    }
  }
  return { descriptorSetBase64, services, methods, fileCount: set.file.length }
}

const directionsInput = computed(() => ({
  serveUrl: serveUrl.value,
  target: target.value || '(your service host:port)',
  services: schema.value?.services ?? [],
}))

const mcpCommand = computed(() => buildMcpCommand(directionsInput.value))
const agentPrompt = computed(() => buildAgentPrompt(directionsInput.value))

const sinkCurl = computed(() => {
  if (!schema.value || !sinkMethod.value) return ''
  return buildSinkCurl({
    name: sinkMethod.value.split('/')[1]?.toLowerCase() + '-sink',
    topics: sinkTopics.value,
    target: target.value || 'your-service:9090',
    method: sinkMethod.value,
    descriptorSetBase64: schema.value.descriptorSetBase64,
    valueFormat: sinkFormat.value,
  })
})

async function copy(text: string) {
  await navigator.clipboard.writeText(text)
  toast.success('Copied')
}
</script>

<style scoped>
.snippet {
  font-family: 'JetBrains Mono Variable', monospace;
  font-size: 0.76rem;
  line-height: 1.55;
  background: rgba(var(--v-theme-on-surface), 0.05);
  border: 1px solid rgba(var(--v-theme-on-surface), 0.1);
  border-radius: 6px;
  padding: 10px 12px;
  white-space: pre-wrap;
  overflow-wrap: anywhere;
  max-height: 260px;
  overflow-y: auto;
}
.font-mono :deep(textarea) {
  font-family: 'JetBrains Mono Variable', monospace;
  font-size: 0.8rem;
}
</style>
