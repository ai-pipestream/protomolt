import { expect, test } from '@playwright/test'
import { downloaded, downloadsThroughMemory } from './consoleKit'

const SHA256 = /[0-9a-f]{64}/

function documentRoute(node: string, doc: string): string {
  return `/console/repository/document?account=corpus-alpha&graph=intake:corpus-alpha&node=${node}&doc=${doc}`
}

test('the manifest lists every part and a part decodes with its retained descriptor', async ({ page }) => {
  await page.goto('/console/repository/accounts/corpus-alpha')
  await page.getByTestId('doc-row-annual-report-2025').click()
  await expect(page.getByTestId('doc-id')).toHaveText('annual-report-2025')
  await expect(page.getByTestId('revision-chip')).toHaveText('revision 3 (current)')

  const parts = page.getByTestId('parts-table')
  for (const part of ['CORE', 'BLOBS', 'PARSED']) {
    await expect(parts.getByRole('row').filter({ hasText: part }).getByText('PRESENT')).toBeVisible()
  }
  await page.getByTestId('read-part-0').click()
  const view = page.getByTestId('part-view')
  await expect(view.getByTestId('decoded-with')).toContainText('type.googleapis.com/ai.protomolt.proto.repo.v1.Document')
  await expect(view.getByTestId('decoded-with')).toContainText(SHA256)
  // The decoded tree carries the current revision's title, not the first one.
  await expect(view).toContainText('Annual report, final')
})

test('revision history shows each revision with its part changes and opens an earlier one', async ({ page }) => {
  await page.goto(documentRoute('ds-crawler', 'annual-report-2025'))
  await page.getByTestId('tab-history').click()
  const history = page.getByTestId('history')
  await expect(history.getByTestId('revision-3')).toContainText('REPLACED CORE')
  await expect(history.getByTestId('revision-2')).toContainText('REPLACED PARSED')
  for (const added of ['ADDED CORE', 'ADDED BLOBS', 'ADDED PARSED']) {
    await expect(history.getByTestId('revision-1')).toContainText(added)
  }

  await history.getByTestId('open-revision-1').click()
  await expect(page).toHaveURL(/[?&]revision=1(&|$)/)
  await expect(page.getByTestId('revision-chip')).toHaveText('revision 1')
  await expect(page.getByTestId('open-current')).toBeVisible()
  // At revision 1 every part was written in revision 1.
  await page.getByRole('tab', { name: 'Parts' }).click()
  const written = await page.getByTestId('parts-table').locator('tbody tr td:nth-child(5)').allInnerTexts()
  expect(written).toEqual(['1', '1', '1'])
})

test('the large file downloads whole with its SHA-256 verified, and a byte range matches the same bytes', async ({ page }) => {
  test.slow()
  await downloadsThroughMemory(page)
  await page.goto(documentRoute('ds-crawler', 'service-manual'))
  await page.getByTestId('tab-files').click()
  await page.locator('[data-testid^="select-blob-"]').first().click()
  const reader = page.getByTestId('byte-reader')
  await expect(reader).toContainText('through browser memory')

  const whole = page.waitForEvent('download')
  await reader.getByTestId('download').click()
  const file = await downloaded(await whole)
  // The corpus's large blob: three MiB and 17 bytes, streamed in many frames.
  expect(file.bytes.length).toBe(3 * 1024 * 1024 + 17)
  const verified = reader.getByTestId('verification')
  await expect(verified).toContainText(`all ${file.bytes.length} bytes hash to the stored SHA-256`)
  await expect(verified).toContainText(file.sha256)

  await reader.getByTestId('range-offset').locator('input').fill('1048000')
  await reader.getByTestId('range-length').locator('input').fill('70000')
  const ranged = page.waitForEvent('download')
  await reader.getByTestId('download').click()
  const range = await downloaded(await ranged)
  expect(range.bytes.length).toBe(70000)
  expect(range.bytes.equals(file.bytes.subarray(1048000, 1048000 + 70000))).toBe(true)
  // A range is not the whole object, so it cannot match the stored digest; the screen says so.
  await expect(reader.getByTestId('verification')).not.toContainText('hash to the stored SHA-256')
  await expect(reader.getByTestId('verification')).toContainText(file.sha256)
})
