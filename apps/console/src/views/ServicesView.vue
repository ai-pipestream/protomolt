<template>
  <div>
    <v-card variant="flat" border class="mb-4">
      <v-card-title class="text-subtitle-1 d-flex align-center">
        <v-icon size="small" class="mr-2">mdi-connection</v-icon>
        Services
        <v-spacer />
        <v-btn size="small" variant="tonal" prepend-icon="mdi-compass-outline"
               :to="{ name: 'services-connect' }">
          Connection directions
        </v-btn>
      </v-card-title>
      <v-card-text class="text-body-2 text-medium-emphasis pt-0">
        Register a gRPC service and every one of its methods becomes a live verb:
        callable here, from agents over MCP, and by name from workflows. Reflection
        reads the contract straight from the running service.
      </v-card-text>
    </v-card>

    <v-row dense>
      <!-- Registered profiles + registration -->
      <v-col cols="12" md="4">
        <v-card variant="flat" border class="mb-4">
          <v-card-title class="text-subtitle-2 d-flex align-center">
            Registered
            <v-spacer />
            <v-btn icon="mdi-refresh" variant="text" size="x-small"
                   aria-label="Reload registered services" @click="load" />
          </v-card-title>
          <v-list density="compact" nav>
            <v-list-item v-for="service in services" :key="service.name"
                         :active="service.name === selected" @click="select(service.name)">
              <v-list-item-title class="text-body-2 text-mono">
                {{ service.name }}
              </v-list-item-title>
              <v-list-item-subtitle v-if="service.endpoints?.length" class="text-caption">
                {{ service.endpoints.join(', ') }}
              </v-list-item-subtitle>
            </v-list-item>
            <v-list-item v-if="!services.length && loaded">
              <v-list-item-title class="text-caption text-medium-emphasis">
                Nothing registered yet — point ProtoMolt at a service below.
              </v-list-item-title>
            </v-list-item>
          </v-list>
        </v-card>

        <v-card variant="flat" border>
          <v-card-title class="text-subtitle-2">Register a service</v-card-title>
          <v-card-text>
            <v-text-field v-model="registerName" label="Profile name"
                          placeholder="billing" density="compact" class="mb-2" hide-details />
            <v-text-field v-model="registerTarget" label="gRPC target"
                          placeholder="billing-host:9000" density="compact" class="mb-2"
                          hide-details />
            <v-switch v-model="registerTls" label="TLS" density="compact" color="primary"
                      hide-details class="mb-2" />
            <v-btn color="primary" size="small" :loading="registering" @click="register">
              Reflect and register
            </v-btn>
            <v-alert v-if="registerError" type="error" variant="tonal" density="compact"
                     class="mt-2">
              {{ registerError }}
            </v-alert>
          </v-card-text>
        </v-card>
      </v-col>

      <!-- Methods of the selected profile -->
      <v-col cols="12" md="8">
        <v-alert v-if="error" type="error" variant="tonal" density="compact" class="mb-2">
          {{ error }}
        </v-alert>
        <template v-if="selected && inspection">
          <v-card v-for="service in shownServices" :key="service.name"
                  variant="flat" border class="mb-4">
            <v-card-title class="text-subtitle-2 text-mono">{{ service.name }}</v-card-title>
            <v-expansion-panels variant="accordion" flat>
              <v-expansion-panel v-for="method in service.methods ?? []" :key="method.fullName">
                <v-expansion-panel-title>
                  <div class="d-flex align-center flex-wrap ga-2" style="width: 100%">
                    <span class="text-body-2 text-mono">{{ method.name }}</span>
                    <v-chip v-if="callable(method)" size="x-small" variant="tonal"
                            color="primary" class="text-mono">
                      {{ verbNameFor(method) }}
                    </v-chip>
                    <v-chip v-else size="x-small" variant="tonal">
                      client-streaming — no verb
                    </v-chip>
                    <v-spacer />
                    <span class="text-caption text-medium-emphasis text-mono">
                      {{ short(method.inputType) }} → {{ short(method.outputType) }}
                    </span>
                  </div>
                </v-expansion-panel-title>
                <v-expansion-panel-text>
                  <MethodInvokePanel v-if="callable(method)" :profile="selected"
                                     :method="method" />
                  <p v-else class="text-caption text-medium-emphasis">
                    A client-streaming method needs a message stream, which one request
                    cannot carry — call it from a gRPC client with the generated stubs.
                  </p>
                </v-expansion-panel-text>
              </v-expansion-panel>
            </v-expansion-panels>
          </v-card>
        </template>
        <v-card v-else-if="loaded && !services.length" variant="flat" border>
          <v-card-text class="text-body-2 text-medium-emphasis">
            <p class="mb-2">
              A registered service shows up here with every method it declares, a form
              to call each one, and the verb name agents and workflows reach it by.
            </p>
            <p class="mb-0">
              Try it with any reflection-enabled gRPC service — ProtoMolt's own
              serve process works: register <code>localhost</code> with its gRPC port.
            </p>
          </v-card-text>
        </v-card>
      </v-col>
    </v-row>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref, shallowRef } from 'vue'
import { useRoute } from 'vue-router'
import MethodInvokePanel from '../components/MethodInvokePanel.vue'
import {
  inspectService,
  isReflectionService,
  listServices,
  registerService,
  verbName,
  type ReflectedMethod,
  type ServiceInspection,
  type ServiceSummary,
} from '../services/services'
import { errorMessage } from '../services/api'
import { toast } from '../composables/useToast'

const route = useRoute()
const services = ref<ServiceSummary[]>([])
const selected = ref<string | null>(null)
const inspection = shallowRef<ServiceInspection | null>(null)
const shownServices = computed(() =>
  (inspection.value?.services ?? []).filter((s) => !isReflectionService(s.name)))
const loaded = ref(false)
const error = ref('')

const registerName = ref('')
const registerTarget = ref('')
const registerTls = ref(false)
const registering = ref(false)
const registerError = ref('')

onMounted(async () => {
  await load()
  // Deep link: /services?profile=<name> selects a profile; ?target= prefills registration.
  if (typeof route.query.profile === 'string') {
    await select(route.query.profile)
  }
  if (typeof route.query.target === 'string') {
    registerTarget.value = route.query.target
  }
})

async function load() {
  try {
    services.value = await listServices()
    error.value = ''
  } catch (e) {
    error.value = errorMessage(e)
  } finally {
    loaded.value = true
  }
}

async function select(name: string) {
  selected.value = name
  inspection.value = null
  try {
    const inspected = await inspectService(name)
    // A slower inspection must not land under a newer selection.
    if (selected.value !== name) return
    inspection.value = inspected
    error.value = ''
  } catch (e) {
    if (selected.value !== name) return
    error.value = errorMessage(e)
  }
}

async function register() {
  registering.value = true
  registerError.value = ''
  try {
    const result = await registerService(registerName.value, registerTarget.value,
        registerTls.value, '')
    if (!result.ok) {
      throw new Error(result.error || 'Registration failed')
    }
    toast.success(`Registered ${registerName.value}`)
    await load()
    await select(registerName.value)
  } catch (e) {
    registerError.value = errorMessage(e)
  } finally {
    registering.value = false
  }
}

function callable(method: ReflectedMethod): boolean {
  return !method.clientStreaming
}

function verbNameFor(method: ReflectedMethod): string {
  return verbName(selected.value ?? '', method, inspection.value?.services ?? [])
}

function short(typeName: string): string {
  return typeName.slice(typeName.lastIndexOf('.') + 1)
}
</script>
