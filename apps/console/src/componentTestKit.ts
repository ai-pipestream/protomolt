/**
 * Shared plumbing for the schema-registry component tests (jsdom): DOM API
 * stubs Vuetify expects, a Vuetify instance with the full component set (the
 * app registers all of vuetify/components globally in main.ts, so tests
 * mirror that), and a memory-history router for <router-link>.
 */
import { createMemoryHistory, createRouter, type Router } from 'vue-router'
import { createVuetify } from 'vuetify'
import * as components from 'vuetify/components'
import * as directives from 'vuetify/directives'

export function installDomStubs(): void {
  if (!('ResizeObserver' in globalThis)) {
    class ResizeObserverStub {
      observe(): void {}
      unobserve(): void {}
      disconnect(): void {}
    }
    ;(globalThis as Record<string, unknown>).ResizeObserver = ResizeObserverStub
  }
  if (typeof window !== 'undefined' && !window.matchMedia) {
    window.matchMedia = (query: string) =>
      ({
        matches: false,
        media: query,
        onchange: null,
        addListener: () => {},
        removeListener: () => {},
        addEventListener: () => {},
        removeEventListener: () => {},
        dispatchEvent: () => false,
      }) as MediaQueryList
  }
  if (typeof window !== 'undefined' && !window.visualViewport) {
    Object.defineProperty(window, 'visualViewport', {
      value: new EventTarget(),
      configurable: true,
    })
  }
}

export function vuetifyForTests() {
  return createVuetify({ components, directives })
}

export function routerForTests(): Router {
  return createRouter({
    history: createMemoryHistory(),
    routes: [
      // Named routes components link to; targets are stubs.
      {
        path: '/schema-registry/connect',
        name: 'schema-registry-connect',
        component: { template: '<div />' },
      },
      {
        path: '/schema-registry/merge',
        name: 'schema-registry-merge',
        component: { template: '<div />' },
      },
      {
        path: '/schema-registry/chains',
        name: 'schema-registry-chains',
        component: { template: '<div />' },
      },
      { path: '/:pathMatch(.*)*', component: { template: '<div />' } },
    ],
  })
}
