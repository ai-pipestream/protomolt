import { defineConfig } from '@playwright/test'
import { EVIDENCE_UPSTREAM_PORT } from './evidence/evidencePort'

// Opt-in evidence run for console fixes: drives the real views in Chromium and writes screenshots
// to evidence-screens/. It is not part of the default PR checks (`npm run test:evidence-browser`).
// The dev server proxies the repository routes to the fake upstream that evidence specs start on
// EVIDENCE_UPSTREAM_PORT, so a spec can prove a request crosses the development proxy.
export default defineConfig({
  testDir: './evidence',
  workers: 1,
  forbidOnly: true,
  retries: 0,
  timeout: 30_000,
  use: { baseURL: 'http://127.0.0.1:5298', headless: true },
  webServer: {
    command: 'npm run dev -- --host 127.0.0.1 --port 5298 --strictPort',
    url: 'http://127.0.0.1:5298/console/',
    reuseExistingServer: false,
    env: { PROTOMOLT_REPOSITORY_URL: `http://127.0.0.1:${EVIDENCE_UPSTREAM_PORT}` },
  },
})
