import { expect, test as setup } from '@playwright/test'
import { SESSION_STATE, signInThroughProvider } from './consoleKit'

// Signs in once through the provider's own login page; every spec reuses the session cookie.
setup('sign in through the provider', async ({ page }) => {
  await signInThroughProvider(page, '/console/repository')
  await expect(page).toHaveURL(/\/console\/repository$/)
  await page.context().storageState({ path: SESSION_STATE })
})
