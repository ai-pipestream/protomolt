import { createHash } from 'node:crypto'
import { readFile } from 'node:fs/promises'
import { type Download, type Page } from '@playwright/test'

/** Where the sign-in setup leaves the session cookie for the specs. */
export const SESSION_STATE = 'e2e/.auth/session.json'

function required(name: string): string {
  const value = process.env[name]
  if (value === undefined || value === '') throw new Error(`${name} is not set: run the browser leg through ConsoleBrowserIT`)
  return value
}

/**
 * Signs in the way a person does: the host's login route sends the browser to the
 * provider, the provider's form takes the password, and the callback sets the session
 * cookie and returns to {@code returnTo}.
 */
export async function signInThroughProvider(page: Page, returnTo: string): Promise<void> {
  await page.goto(`/console/session/login?return=${encodeURIComponent(returnTo)}`)
  await page.locator('#username').fill(required('CONSOLE_USER'))
  await page.locator('#password').fill(required('CONSOLE_PASSWORD'))
  await page.locator('#kc-login').click()
  await page.waitForURL((url) => url.pathname === returnTo)
}

/**
 * Takes the File System Access picker away, so a download goes through browser memory and
 * reaches the page's download event, which the specs can read back.
 */
export async function downloadsThroughMemory(page: Page): Promise<void> {
  await page.addInitScript(() => {
    Object.defineProperty(window, 'showSaveFilePicker', { value: undefined, configurable: true })
  })
}

/** The document IDs of the listed rows, in table order. */
export async function listedDocuments(page: Page): Promise<string[]> {
  const ids = await page.locator('[data-testid^="doc-row-"]').evaluateAll((rows) =>
    rows.map((row) => row.getAttribute('data-testid') ?? ''),
  )
  return ids.map((id) => id.slice('doc-row-'.length))
}

/** Picks an option of a Vuetify select by its visible title. */
export async function choose(page: Page, testId: string, option: string): Promise<void> {
  await page.getByTestId(testId).click()
  await page.getByRole('option', { name: option, exact: true }).click()
}

/** The bytes and SHA-256 of a finished download. */
export async function downloaded(download: Download): Promise<{ bytes: Buffer; sha256: string }> {
  const path = await download.path()
  const bytes = await readFile(path)
  return { bytes, sha256: createHash('sha256').update(bytes).digest('hex') }
}

/**
 * Sends the operator token on the page's write calls. A signed-in session is refused on
 * writes until it carries a writer credential (the #529 session upload follow-up), so the
 * write specs act as the operator for now; the host asks the bearer credential first.
 */
export async function writeAsOperator(page: Page): Promise<void> {
  const token = required('CONSOLE_OPERATOR_TOKEN')
  for (const path of ['**/connect/**', '**/v1/documents:upload*']) {
    await page.route(path, (route) =>
      route.continue({ headers: { ...route.request().headers(), authorization: `Bearer ${token}` } }),
    )
  }
}
