import { createHash, randomBytes } from 'node:crypto'
import { expect, test } from '@playwright/test'
import { downloaded, downloadsThroughMemory, writeAsOperator } from './consoleKit'

// Six MiB and change, so the upload streams many frames and the progress moves.
const SIZE = 6 * 1024 * 1024 + 31

test('a multi-MB upload shows its operation ID first, commits, and opens as the new revision', async ({ page }) => {
  test.slow()
  await writeAsOperator(page)
  await downloadsThroughMemory(page)
  const bytes = randomBytes(SIZE)
  const sha256 = createHash('sha256').update(bytes).digest('hex')

  await page.goto('/console/repository/accounts/corpus-gamma')
  await page.getByTestId('account-upload').click()
  await page.getByTestId('up-account').locator('input').fill('corpus-gamma')
  await page.getByTestId('up-datasource').locator('input').fill('ds-crawler')
  await page.getByRole('combobox', { name: 'Drive' }).fill('intake')
  await page.getByTestId('up-doc').locator('input').fill('browser-upload')
  await page.getByTestId('up-file').setInputFiles({ name: 'browser-upload.bin', mimeType: 'application/octet-stream', buffer: bytes })
  await page.getByTestId('up-start').click()

  const operation = page.getByTestId('up-operation')
  await expect(operation).toHaveText(/^[0-9a-f-]{36}$/)
  await expect(page.getByTestId('up-progress')).toContainText(sha256)
  const committed = page.getByTestId('up-outcome-committed')
  await expect(committed).toBeVisible({ timeout: 60_000 })
  await expect(page.getByTestId('up-sent')).toHaveText(`${SIZE} of ${SIZE} bytes sent`)
  const operationId = await operation.innerText()

  await page.getByTestId('up-open').click()
  await expect(page.getByTestId('doc-id')).toHaveText('browser-upload')
  await expect(page.getByTestId('revision-chip')).toContainText('revision 1')
  await page.getByTestId('tab-history').click()
  await expect(page.getByTestId('history').getByTestId('revision-1')).toContainText(operationId)

  await page.getByTestId('tab-files').click()
  await page.locator('[data-testid^="select-blob-"]').first().click()
  const reader = page.getByTestId('byte-reader')
  const download = page.waitForEvent('download')
  await reader.getByTestId('download').click()
  const file = await downloaded(await download)
  expect(file.sha256).toBe(sha256)
  await expect(reader.getByTestId('verification')).toContainText(`all ${SIZE} bytes hash to the stored SHA-256 ${sha256}`)
})
