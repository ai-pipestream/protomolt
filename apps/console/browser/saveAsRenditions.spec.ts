import { readFileSync } from 'node:fs'
import { test, expect } from '@playwright/test'

test.beforeEach(async ({ page }) => {
  await page.addInitScript(() => {
    Object.defineProperty(window, 'showSaveFilePicker', { value: undefined, configurable: true })
  })
  await page.route('**/console/save-as-test', (route) =>
    route.fulfill({
      contentType: 'text/html',
      body: '<!doctype html><html><head><meta charset="utf-8"></head><body><div id="app"></div></body></html>',
    }),
  )
})

test('discovers Parquet Save-as capability, submits binary options, refreshes derived rendition provenance, and downloads verified Parquet bytes', async ({ page }) => {
  await page.setViewportSize({ width: 1280, height: 1000 })
  await page.goto('/console/save-as-test')
  await page.evaluate(async () => {
    const path = '/console/browser/saveAsHarness.ts'
    const { mountSaveAsBrowserEntry } = await import(/* @vite-ignore */ path)
    await mountSaveAsBrowserEntry({ parquetEnabled: true, staleDerived: false })
  })

  // Verify authoritative Protobuf format icon on parsed rendition
  const parsedRow = page.getByTestId('rendition-parsed')
  await expect(parsedRow).toContainText('Protobuf')
  await expect(parsedRow).toContainText('Authoritative')

  // Open Save-as dialog
  await page.getByTestId('save-as-rendition-parsed').click()
  const dialog = page.getByTestId('save-as-dialog')
  await expect(dialog).toBeVisible()
  await expect(dialog).toContainText('parquet-v1')
  await expect(dialog).toContainText('Projects scalar top-level columns only')
  // Unimplemented providers must never be invented
  await expect(dialog).not.toContainText('Markdown')
  await expect(dialog).not.toContainText('PDF')

  // Fill Parquet column projection and mapping ID
  await page.getByTestId('parquet-columns-input').locator('input').fill('doc_id, title')
  await page.getByTestId('parquet-mapping-id-input').locator('input').fill('map-browser-e2e')
  await page.getByTestId('save-as-submit-btn').click()

  await expect(page.getByTestId('save-as-job-receipt')).toContainText('v2')
  await page.getByTestId('save-as-close-btn').click()

  // Verify derived Parquet rendition now appears in rendition table with Derived badge and provenance
  const parquetRow = page.getByTestId('rendition-parquet')
  await expect(parquetRow).toContainText('Parquet')
  await expect(parquetRow).toContainText('Derived')
  await expect(parquetRow).toContainText('map-browser-e2e')

  const state = await page.evaluate(() => {
    const fn = (window as unknown as Record<string, () => { lastSubmittedColumns: string[]; lastMappingId: string; parquetBytes: number[] }>).__saveAsState
    return fn()
  })
  expect(state.lastSubmittedColumns).toEqual(['doc_id', 'title'])
  expect(state.lastMappingId).toBe('map-browser-e2e')

  // Download the derived Parquet rendition and verify exact bytes
  await page.getByTestId('select-rendition-parquet').click()
  const downloadPromise = page.waitForEvent('download')
  await page.getByTestId('download').click()
  const saved = await downloadPromise
  expect([...readFileSync((await saved.path())!)]).toEqual(state.parquetBytes)
})

test('shows Stale badge when source revision advances and reports zero-provider state cleanly when no conversion providers are enabled', async ({ page }) => {
  await page.setViewportSize({ width: 1280, height: 1000 })
  await page.goto('/console/save-as-test')
  await page.evaluate(async () => {
    const path = '/console/browser/saveAsHarness.ts'
    const { mountSaveAsBrowserEntry } = await import(/* @vite-ignore */ path)
    await mountSaveAsBrowserEntry({ parquetEnabled: false, staleDerived: true })
  })

  const parquetRow = page.getByTestId('rendition-parquet')
  await expect(parquetRow.getByTestId('format-badge-derived')).toHaveText('Derived')
  await expect(parquetRow.getByTestId('format-badge-stale')).toHaveText('Stale')
  await expect(page.getByTestId('rendition-stale-parquet')).toContainText('r1')
  await expect(page.getByTestId('rendition-stale-parquet')).toContainText('r3')

  await page.getByTestId('save-as-rendition-parsed').click()
  await expect(page.getByTestId('save-as-empty-providers')).toContainText(
    'No Save-as conversion providers are enabled on this server.',
  )
})
