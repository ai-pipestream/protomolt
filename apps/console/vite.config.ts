import { fileURLToPath, URL } from 'node:url'
import vue from '@vitejs/plugin-vue'
import { defineConfig } from 'vitest/config'

// One config drives vite and vitest. The dev server proxies the console's
// same-origin API base (/api/protomolt) to a running registry - by default
// protomolt-serve's Confluent-protocol port. Point PROTOMOLT_REGISTRY_URL at
// any other Confluent-compatible registry.
const registry = process.env.PROTOMOLT_REGISTRY_URL ?? 'http://localhost:8081'
// protomolt-serve's REST/verbs port, for reflect/compile/gather calls.
const serve = process.env.PROTOMOLT_SERVE_URL ?? 'http://localhost:8080'

export default defineConfig({
  // Served at /console by protomolt-serve (and any reverse proxy that mirrors it).
  base: '/console/',
  plugins: [vue()],
  resolve: {
    alias: {
      '@': fileURLToPath(new URL('./src', import.meta.url)),
    },
  },
  server: {
    proxy: {
      '/api/protomolt': {
        target: registry,
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/api\/protomolt/, ''),
      },
      '/api/serve': {
        target: serve,
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/api\/serve/, ''),
      },
    },
  },
  test: {
    // Pure-logic tests run in node; component tests opt into jsdom per-file
    // with a `// @vitest-environment jsdom` docblock.
    environment: 'node',
    include: ['src/**/*.test.ts'],
    server: {
      deps: {
        inline: ['vuetify'],
      },
    },
  },
})
