import { expect, test } from '@playwright/test'
import { choose, listedDocuments } from './consoleKit'

// corpus-alpha as the analyst sees it: every live document the analyst may read. The restricted
// ledger (auditor only) never lists, and the withdrawn photo lists only with tombstones shown.
const LIVE = [
  'annual-report-2025',
  'charter',
  'product-sheet',
  'service-manual',
  'anthologie',
  'budget-q1',
  'minutes-2025-02',
]

test.beforeEach(async ({ page }) => {
  await page.goto('/console/repository')
  await page.getByTestId('account-list').getByText('corpus-alpha', { exact: true }).click()
  await expect(page).toHaveURL(/\/console\/repository\/accounts\/corpus-alpha$/)
  await expect(page.getByTestId('document-table')).toBeVisible()
})

test('the picker lists exactly the accounts the session is bound to', async ({ page }) => {
  await page.goto('/console/repository')
  await expect(page.getByTestId('session-principal')).toContainText('analyst')
  const items = page.getByTestId('account-list').locator('.v-list-item-title')
  await expect(items).toHaveText(['corpus-alpha', 'corpus-beta', 'corpus-gamma'])
})

test('an account opens on its drives and its readable documents by call number', async ({ page }) => {
  const drives = page.getByTestId('drive-table')
  for (const drive of ['intake', 'pipeline', 'archive']) {
    await expect(drives.getByText(drive, { exact: true })).toBeVisible()
  }
  expect(await listedDocuments(page)).toEqual(LIVE)
  await expect(page.getByTestId('doc-row-ledger-restricted')).toHaveCount(0)
})

test('the tombstone toggle adds the withdrawn document, marked', async ({ page }) => {
  await page.getByTestId('include-tombstoned').locator('input').check()
  await page.getByTestId('apply-query').click()
  const photo = page.getByTestId('doc-row-site-photo-0007')
  await expect(photo).toBeVisible()
  await expect(photo.getByText('tombstone')).toBeVisible()
  expect((await listedDocuments(page)).sort()).toEqual([...LIVE, 'site-photo-0007'].sort())
  await expect(page.getByTestId('doc-row-ledger-restricted')).toHaveCount(0)

  await page.getByTestId('include-tombstoned').locator('input').uncheck()
  await page.getByTestId('apply-query').click()
  await expect(photo).toHaveCount(0)
})

test('a datasource filter and a title sort narrow and order the list', async ({ page }) => {
  await page.getByTestId('filter-panel').click()
  await page.getByTestId('filter-datasourceId').locator('input').fill('ds-share')
  await page.getByTestId('apply-query').click()
  await expect.poll(() => listedDocuments(page)).toEqual(['anthologie', 'budget-q1', 'minutes-2025-02'])

  await page.getByTestId('filter-datasourceId').locator('input').fill('')
  await choose(page, 'sort-field', 'Title')
  await page.getByTestId('apply-query').click()
  await expect.poll(() => listedDocuments(page)).toEqual([
    'annual-report-2025',
    'anthologie',
    'budget-q1',
    'product-sheet',
    'charter',
    'service-manual',
    'minutes-2025-02',
  ])
})

test('paging walks the list three at a time and back', async ({ page }) => {
  await page.getByLabel('Page size').fill('3')
  await page.getByTestId('apply-query').click()
  await expect.poll(() => listedDocuments(page)).toEqual(LIVE.slice(0, 3))
  await expect(page.getByTestId('page-previous')).toBeDisabled()

  await page.getByTestId('page-next').click()
  await expect.poll(() => listedDocuments(page)).toEqual(LIVE.slice(3, 6))
  await page.getByTestId('page-next').click()
  await expect.poll(() => listedDocuments(page)).toEqual(LIVE.slice(6))
  await expect(page.getByText('Page 3')).toBeVisible()
  await expect(page.getByTestId('page-next')).toBeDisabled()

  await page.getByTestId('page-previous').click()
  await expect.poll(() => listedDocuments(page)).toEqual(LIVE.slice(3, 6))
})
