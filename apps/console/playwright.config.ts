import { defineConfig, devices } from '@playwright/test'

// The browser leg: Chromium drives the built console served by a real repository host.
// The host is booted by ConsoleBrowserIT (repo/fixtures, PROTOMOLT_CONSOLE_BROWSER_IT=1),
// which passes its address and the Keycloak user in the environment.
const baseURL = process.env.CONSOLE_BASE_URL
if (baseURL === undefined || baseURL === '') {
  throw new Error('CONSOLE_BASE_URL is not set: run the browser leg through ConsoleBrowserIT')
}

export default defineConfig({
  testDir: './e2e',
  // The specs share one seeded host and some of them write to it, so they run one at a time.
  fullyParallel: false,
  workers: 1,
  forbidOnly: true,
  retries: 0,
  reporter: [['list'], ['html', { open: 'never' }]],
  timeout: 60_000,
  use: {
    baseURL,
    trace: 'retain-on-failure',
  },
  projects: [
    { name: 'sign-in', testMatch: /signIn\.setup\.ts/ },
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'], storageState: 'e2e/.auth/session.json' },
      dependencies: ['sign-in'],
    },
  ],
})
