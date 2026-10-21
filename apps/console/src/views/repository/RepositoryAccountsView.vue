<template>
  <div>
    <v-card class="mb-4">
      <v-card-title class="text-subtitle-1 d-flex align-center">
        <v-icon size="small" class="mr-2">mdi-archive-outline</v-icon>
        Repository
        <v-spacer />
        <v-btn size="small" variant="tonal" prepend-icon="mdi-magnify" :to="{ name: 'repository-lookup' }">
          Look up a document or operation
        </v-btn>
      </v-card-title>
      <v-card-text class="text-body-2 text-medium-emphasis pt-0">
        Pick an account to see its drives and documents. You see only the accounts your session is bound to.
      </v-card-text>
    </v-card>

    <v-row dense>
      <v-col cols="12" md="7">
        <v-card class="mb-4">
          <v-card-title class="text-subtitle-2">Your accounts</v-card-title>
          <v-card-text>
            <v-progress-linear v-if="loading" indeterminate class="mb-2" />
            <RpcFailureAlert v-if="failure" :failure="failure" title="Reading the session" />
            <template v-if="session?.authenticated">
              <div class="text-caption text-medium-emphasis mb-1" data-testid="session-principal">
                Signed in as <span class="text-mono">{{ session.principal }}</span>
              </div>
              <v-list v-if="session.accountIds.length > 0" density="compact" data-testid="account-list">
                <v-list-item
                  v-for="accountId in session.accountIds"
                  :key="accountId"
                  :to="accountRoute(accountId)"
                  :title="accountId"
                  prepend-icon="mdi-domain"
                />
              </v-list>
              <div v-else class="text-body-2 text-medium-emphasis" data-testid="no-accounts">
                Your session is bound to no accounts.
              </div>
            </template>
            <div v-else-if="session" data-testid="signed-out">
              <div class="text-body-2 mb-2">You are not signed in.</div>
              <v-btn size="small" color="primary" data-testid="sign-in" @click="signIn">Sign in</v-btn>
            </div>
          </v-card-text>
        </v-card>
      </v-col>
      <v-col cols="12" md="5">
        <v-card class="mb-4">
          <v-card-title class="text-subtitle-2">Open an account by ID</v-card-title>
          <v-card-text>
            <v-text-field
              v-model="typedAccount"
              label="Account ID"
              density="compact"
              data-testid="account-id-input"
              @keydown.enter="openTyped"
            />
            <v-btn color="primary" size="small" :disabled="typedAccount.trim() === ''" data-testid="open-account" @click="openTyped">
              Open
            </v-btn>
          </v-card-text>
        </v-card>
      </v-col>
    </v-row>
  </div>
</template>

<script setup lang="ts">
import { onMounted, ref, shallowRef } from 'vue'
import { useRouter } from 'vue-router'
import RpcFailureAlert from '@/components/repository/RpcFailureAlert.vue'
import type { ConsoleSessionStatus } from '@/gen/ai/protomolt/proto/authz/v1/console_session_pb'
import { useRepositorySession, useSignIn } from '@/repository/clients'
import { rpcFailure, type RpcFailure } from '@/repository/rpcErrorCore'
import { SessionApiError } from '@/services/session'

const router = useRouter()
const sessions = useRepositorySession()
const signIn = useSignIn()
const session = shallowRef<ConsoleSessionStatus | null>(null)
const loading = ref(false)
const failure = ref<RpcFailure | null>(null)
const typedAccount = ref('')

function accountRoute(accountId: string) {
  return { name: 'repository-account', params: { accountId } }
}

function openTyped() {
  const id = typedAccount.value.trim()
  if (id !== '') void router.push(accountRoute(id))
}

// The accounts come from the session itself: the repository host does not
// serve AccountService, and the session names exactly the accounts its
// principal is bound to.
onMounted(async () => {
  loading.value = true
  try {
    session.value = await sessions.status()
  } catch (error) {
    failure.value =
      error instanceof SessionApiError
        ? { code: `HTTP ${error.status}`, codeNumber: null, message: error.message }
        : rpcFailure(error)
  } finally {
    loading.value = false
  }
})
</script>
