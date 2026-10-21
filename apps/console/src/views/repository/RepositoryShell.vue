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
import { REPOSITORY_CLIENTS, repositoryClients } from '@/repository/clients'
import { repositoryTransport, signInLocation } from '@/repository/transport'

provide(
  REPOSITORY_CLIENTS,
  repositoryClients(
    repositoryTransport(() => window.location.assign(signInLocation(window.location.pathname + window.location.search))),
  ),
)
</script>
