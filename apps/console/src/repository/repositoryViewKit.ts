/**
 * Mounting plumbing for the repository screens' component tests (jsdom): the
 * repository routes by name, the generated clients over an in-memory router,
 * and Vuetify. Each test supplies the service handlers it needs.
 */
import { mount, flushPromises } from '@vue/test-utils'
import type { ConnectRouter } from '@connectrpc/connect'
import { createMemoryHistory, createRouter, type RouteLocationRaw, type Router } from 'vue-router'
import type { Component } from 'vue'
import { installDomStubs, vuetifyForTests } from '@/componentTestKit'
import { REPOSITORY_CLIENTS, REPOSITORY_SIGN_IN } from './clients'
import { routerClients } from './repositoryTestKit'

const stub = { template: '<div />' }

export function repositoryRouter(): Router {
  return createRouter({
    history: createMemoryHistory(),
    routes: [
      { path: '/repository', name: 'repository', component: stub },
      { path: '/repository/accounts/:accountId', name: 'repository-account', component: stub },
      { path: '/repository/document', name: 'repository-document', component: stub },
      { path: '/repository/lookup', name: 'repository-lookup', component: stub },
      { path: '/repository/upload', name: 'repository-upload', component: stub },
      { path: '/repository/archive', name: 'repository-archive', component: stub },
      { path: '/repository/archive-entry', name: 'repository-archive-entry', component: stub },
    ],
  })
}

/** Mounts `view` at `location` with clients answered by `routes`. */
export async function mountRepositoryView(
  view: Component,
  location: RouteLocationRaw,
  routes: (router: ConnectRouter) => void,
  provide: Record<symbol, unknown> = {},
) {
  installDomStubs()
  const router = repositoryRouter()
  await router.push(location)
  await router.isReady()
  const wrapper = mount(view, {
    global: {
      plugins: [vuetifyForTests(), router],
      provide: {
        [REPOSITORY_CLIENTS as symbol]: routerClients(routes),
        [REPOSITORY_SIGN_IN as symbol]: () => {
          throw new Error('This test did not expect a sign-in.')
        },
        ...provide,
      },
    },
    attachTo: document.body,
  })
  await settle()
  return { wrapper, router }
}

/** Lets in-memory calls and the renders they cause finish. */
export async function settle(): Promise<void> {
  for (let i = 0; i < 5; i++) {
    await flushPromises()
    await new Promise((resolve) => setTimeout(resolve, 0))
  }
}
