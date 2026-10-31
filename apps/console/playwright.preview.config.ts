import { defineConfig } from '@playwright/test'

// Exercises actual browser workers/WASM; no repository server or credentials needed.
export default defineConfig({
  testDir: './browser',
  workers: 1,
  forbidOnly: true,
  retries: 0,
  timeout: 20_000,
  use: { baseURL: 'http://127.0.0.1:5198', headless: true },
  webServer: {
    command: 'npm run dev -- --host 127.0.0.1 --port 5198 --strictPort',
    url: 'http://127.0.0.1:5198/console/',
    reuseExistingServer: false,
  },
})
