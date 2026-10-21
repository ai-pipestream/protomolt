<template>
  <div class="py-4">
    <v-card class="mx-auto pa-8" max-width="560" rounded="lg">
      <v-icon icon="mdi-shield-account-outline" color="primary" size="44" />
      <h1 class="text-h4 mt-4">Sign in</h1>

      <v-alert v-if="failure" type="error" variant="tonal" class="mt-4" data-test="failure">
        {{ failure }}
      </v-alert>
      <v-alert v-else-if="hostError" type="warning" variant="tonal" class="mt-4" data-test="host-error">
        {{ hostError }}
      </v-alert>

      <template v-if="checking">
        <v-progress-linear indeterminate color="primary" class="mt-6" />
        <div class="text-medium-emphasis mt-4">Checking your session…</div>
      </template>

      <template v-else-if="session?.authenticated">
        <p class="text-medium-emphasis mt-2" data-test="signed-in">
          You are signed in as <strong>{{ session.principal }}</strong
          ><span v-if="session.accountIds.length"> with access to {{ accountSummary }}</span
          ><span v-else>, bound to no account yet</span>.
        </p>
        <div class="d-flex flex-wrap ga-3 mt-6">
          <v-btn color="primary" prepend-icon="mdi-arrow-right" :to="continueRoute" data-test="continue">Continue</v-btn>
          <v-btn variant="outlined" prepend-icon="mdi-logout" :loading="signingOut" data-test="sign-out" @click="signOut">
            Sign out
          </v-btn>
        </div>
      </template>

      <template v-else>
        <p class="text-medium-emphasis mt-2 mb-6">
          The repository console signs you in through your organisation's identity provider.
          No password is entered here, and the browser never holds a token.
        </p>
        <v-btn color="primary" block :href="loginHref" prepend-icon="mdi-login" data-test="sign-in">
          Sign in with your identity provider
        </v-btn>
      </template>
    </v-card>
  </div>
</template>

<script setup lang="ts">
// The console's sign-in page. Login is a full page navigation through the
// host's /console/session/login route, which sends the browser to the
// provider and brings it back to the return path; this page only shows the
// state, the reason a login failed, and the way in or out.
import { computed, onMounted, ref } from 'vue'
import { useRoute } from 'vue-router'
import type { ConsoleSessionStatus } from '@/gen/ai/protomolt/proto/authz/v1/console_session_pb'
import { SessionApi, loginLocation, returnRoute, signInFailureMessage } from '@/services/session'

const props = defineProps<{ api?: SessionApi }>()
const api = props.api ?? new SessionApi()
const route = useRoute()

const returnTo = computed(() => {
  const value = route.query.return
  return typeof value === 'string' ? value : null
})
const failure = computed(() => {
  const value = route.query.error
  return signInFailureMessage(typeof value === 'string' ? value : null)
})
const loginHref = computed(() => loginLocation(returnTo.value ?? '/console/'))
const continueRoute = computed(() => returnRoute(returnTo.value))

const checking = ref(true)
const signingOut = ref(false)
const hostError = ref<string | null>(null)
const session = ref<ConsoleSessionStatus | null>(null)

const accountSummary = computed(() => {
  const ids = session.value?.accountIds ?? []
  return ids.length === 1 ? `account ${ids[0]}` : `${ids.length} accounts (${ids.join(', ')})`
})

async function refresh() {
  checking.value = true
  hostError.value = null
  try {
    session.value = await api.status()
  } catch (error) {
    session.value = null
    hostError.value = error instanceof Error ? error.message : String(error)
  } finally {
    checking.value = false
  }
}

async function signOut() {
  signingOut.value = true
  try {
    await api.logout()
    await refresh()
  } catch (error) {
    hostError.value = error instanceof Error ? error.message : String(error)
  } finally {
    signingOut.value = false
  }
}

onMounted(refresh)
</script>
