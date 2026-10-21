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
          <v-card-title class="text-subtitle-2">Accounts</v-card-title>
          <v-card-text>
            <v-progress-linear v-if="loading" indeterminate class="mb-2" />
            <RpcFailureAlert v-if="failure" :failure="failure" title="Listing accounts" />
            <v-list v-if="accounts.length > 0" density="compact" data-testid="account-list">
              <v-list-item
                v-for="account in accounts"
                :key="account.accountId"
                :to="accountRoute(account.accountId)"
                :title="account.displayName || account.accountId"
                :subtitle="`${account.accountId} · ${statusLabel(account.status)}`"
                prepend-icon="mdi-domain"
              />
            </v-list>
            <div v-else-if="!loading && !failure" class="text-body-2 text-medium-emphasis">
              No accounts are visible to this session.
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
import { onMounted, ref } from 'vue'
import { useRouter } from 'vue-router'
import { create } from '@bufbuild/protobuf'
import RpcFailureAlert from '@/components/repository/RpcFailureAlert.vue'
import { AccountStatusSchema, type Account } from '@/gen/ai/protomolt/proto/account/v1/account_pb'
import { ListAccountsRequestSchema } from '@/gen/ai/protomolt/proto/account/v1/account_service_pb'
import { useRepositoryClients } from '@/repository/clients'
import { enumLabel } from '@/repository/formatCore'
import { isOwnCancellation, rpcFailure, type RpcFailure } from '@/repository/rpcErrorCore'
import { useCallScope } from '@/repository/useCallScope'

const clients = useRepositoryClients()
const router = useRouter()
const scope = useCallScope()
const accounts = ref<Account[]>([])
const loading = ref(false)
const failure = ref<RpcFailure | null>(null)
const typedAccount = ref('')

function accountRoute(accountId: string) {
  return { name: 'repository-account', params: { accountId } }
}

function statusLabel(status: number): string {
  return enumLabel(AccountStatusSchema, status)
}

function openTyped() {
  const id = typedAccount.value.trim()
  if (id !== '') void router.push(accountRoute(id))
}

onMounted(async () => {
  const signal = scope.next()
  loading.value = true
  try {
    const all: Account[] = []
    let token = ''
    do {
      const page = await clients.accounts.listAccounts(
        create(ListAccountsRequestSchema, { limit: 200, continuationToken: token }),
        { signal },
      )
      all.push(...page.accounts)
      token = page.nextContinuationToken
    } while (token !== '')
    accounts.value = all
  } catch (error) {
    if (!isOwnCancellation(error, signal)) failure.value = rpcFailure(error)
  } finally {
    loading.value = false
  }
})
</script>
