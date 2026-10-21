<template>
  <router-view />
</template>

<script setup lang="ts">
// The parent of every /repository route. It provides the repository clients
// over the browser transport, so the generated stubs and Connect load with the
// repository screens instead of with every console page. A gateway
// UNAUTHENTICATED (no session, or an expired one) starts sign-in and comes back
// to the page that was open.
import { provide } from 'vue'
import { REPOSITORY_CLIENTS, REPOSITORY_SIGN_IN, repositoryClients } from '@/repository/clients'
import { repositoryTransport, signInLocation } from '@/repository/transport'

const signIn = () => window.location.assign(signInLocation(window.location.pathname + window.location.search))
provide(REPOSITORY_SIGN_IN, signIn)
provide(REPOSITORY_CLIENTS, repositoryClients(repositoryTransport(signIn)))
</script>
