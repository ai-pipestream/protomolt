import { expect, test } from '@playwright/test'
import { downloaded, downloadsThroughMemory } from './consoleKit'

// corpus-alpha's docs archive: a retained report with two versions (version 2 re-put the
// original and the markdown, the thumbnail carried forward) and a scanned contract with pages.
test('an archive shows its stats and entries, and an entry its renditions and versions', async ({ page }) => {
  await page.goto('/console/repository/accounts/corpus-alpha')
  await page.getByTestId('archive-list').getByText('docs', { exact: true }).click()
  await expect(page.getByTestId('archive-name')).toHaveText('docs')
  const stats = page.getByTestId('archive-stats')
  await expect(stats).toContainText(/Entries\s*2/)
  await expect(stats).toContainText(/Versions\s*3/)
  const entries = page.getByTestId('entry-table')
  await expect(entries.getByTestId('entry-reports/annual-2025')).toContainText('Annual report 2025')
  await expect(entries.getByTestId('entry-scans/contract-0042')).toContainText('Contract 42')

  await entries.getByTestId('entry-reports/annual-2025').click()
  await expect(page.getByTestId('entry-id')).toHaveText('reports/annual-2025')
  const renditions = page.getByTestId('rendition-table')
  for (const name of ['original', 'markdown', 'thumbnail']) {
    await expect(renditions.getByTestId(`rendition-${name}`)).toBeVisible()
  }
  const versions = page.getByTestId('versions')
  await expect(versions.getByTestId('version-2')).toContainText('shown')
  await versions.getByTestId('open-version-1').click()
  await expect(page.getByTestId('version-chip')).toContainText('1')
  await expect(versions.getByTestId('version-1')).toContainText('shown')
})

test('a rendition downloads streamed and hashes to the SHA-256 the entry records', async ({ page }) => {
  await downloadsThroughMemory(page)
  await page.goto('/console/repository/accounts/corpus-alpha')
  await page.getByTestId('archive-list').getByText('docs', { exact: true }).click()
  await page.getByTestId('entry-reports/annual-2025').click()
  const original = page.getByTestId('rendition-original')
  const recorded = (await original.locator('td').nth(4).innerText()).trim()
  expect(recorded).toMatch(/^[0-9a-f]{64}$/)

  await original.getByTestId('select-rendition-original').click()
  const reader = page.getByTestId('byte-reader')
  const download = page.waitForEvent('download')
  await reader.getByTestId('download').click()
  const file = await downloaded(await download)
  expect(file.sha256).toBe(recorded)
  await expect(reader.getByTestId('verification')).toContainText(`all ${file.bytes.length} bytes hash to the stored SHA-256 ${recorded}`)
})
