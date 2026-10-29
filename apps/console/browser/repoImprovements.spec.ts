import { createHash } from 'node:crypto'
import { test, expect } from '@playwright/test'
import { create, toBinary } from '@bufbuild/protobuf'
import { UploadRenditionResponseSchema } from '../src/gen/ai/protomolt/proto/repo/archive/v1/archive_service_pb'

test.beforeEach(async ({ page }) => {
  await page.route('**/console/repo-improvements-test', (route) =>
    route.fulfill({
      contentType: 'text/html',
      body: '<!doctype html><html><head><meta charset="utf-8"><title>Repository Console</title></head><body><div id="app"></div></body></html>',
    }),
  )
})

test('Slice 1: deep-links to Validation tab and inspects retained FileDescriptorSet inline with SHA-256 verification', async ({
  page,
}) => {
  await page.setViewportSize({ width: 1360, height: 1080 })
  await page.goto('/console/repo-improvements-test')
  await page.evaluate(async () => {
    const path = '/console/browser/repoImprovementsHarness.ts'
    const { mountDocumentImprovements } = await import(/* @vite-ignore */ path)
    await mountDocumentImprovements('validation')
  })

  await expect(page.getByTestId('validation-view')).toBeVisible()
  await expect(page.getByTestId('inspect-schema-0')).toBeVisible()
  await expect(page.getByTestId('inspect-schema-1')).toBeVisible()
  await expect(page.getByTestId('open-schema-part-0')).toBeVisible()

  await page.getByTestId('inspect-schema-1').click()
  await expect(page.getByTestId('validation-descriptor-view')).toBeVisible()
  await expect(page.getByTestId('validation-descriptor-view')).toContainText('SHA-256 verified')
  await expect(page.getByTestId('validation-descriptor-view')).toContainText('google.protobuf.StringValue')

  await page.screenshot({ path: 'test-results/slice1-validation-descriptor-inspector.png', fullPage: true })
})

test('Slice 2: filters decoded protobuf message tree (including nested Any) and filters revision diffs', async ({
  page,
}) => {
  await page.setViewportSize({ width: 1360, height: 960 })
  await page.goto('/console/repo-improvements-test')
  await page.evaluate(async () => {
    const path = '/console/browser/repoImprovementsHarness.ts'
    const { mountDocumentImprovements } = await import(/* @vite-ignore */ path)
    await mountDocumentImprovements('parts')
  })

  await page.getByTestId('read-part-0').click()
  await expect(page.getByTestId('part-view')).toBeVisible()
  await expect(page.getByTestId('part-tree-filter')).toBeVisible()

  await page.getByTestId('part-tree-filter').locator('input').fill('Mutual non-disclosure')
  await expect(page.getByTestId('part-tree-match-count')).toContainText('1 match')
  await expect(page.getByTestId('part-view')).toContainText('Clause 7.2: Mutual non-disclosure obligations')
  await page.screenshot({ path: 'test-results/slice2a-decoded-tree-filter.png', fullPage: true })

  await page.getByTestId('tab-history').click()
  await page.getByTestId('compare-revision-6').click()
  await expect(page.getByTestId('compare')).toBeVisible()
  await page.getByTestId('compare-changed-only').locator('input').check()
  await page.getByTestId('compare-line-filter').locator('input').fill('Clause')
  await expect(page.getByTestId('compare')).toContainText('Clause 7.2')

  await page.screenshot({ path: 'test-results/slice2-decoded-tree-filter-and-compare.png', fullPage: true })
})

test('Slice 3: renders formatted JSON and verified raster image previews in ByteReader', async ({ page }) => {
  await page.setViewportSize({ width: 1360, height: 960 })
  await page.goto('/console/repo-improvements-test')
  await page.evaluate(async () => {
    const path = '/console/browser/repoImprovementsHarness.ts'
    const { mountDocumentImprovements } = await import(/* @vite-ignore */ path)
    await mountDocumentImprovements('files')
  })

  await page.getByTestId('select-blob-blob-json').click()
  await page.getByTestId('preview').click()
  await expect(page.getByTestId('preview-modes')).toBeVisible()
  await expect(page.getByTestId('preview-text')).toContainText('"pipeline": "intake-enrichment-v2"')
  await page.screenshot({ path: 'test-results/slice3a-byte-reader-json-preview.png', fullPage: true })

  await page.getByTestId('select-blob-blob-png').click()
  await page.getByTestId('preview').click()
  await expect(page.getByTestId('preview-image')).toBeVisible()
  await page.getByTestId('preview-image').evaluate((img: HTMLImageElement) => img.decode())

  await page.screenshot({ path: 'test-results/slice3-byte-reader-rich-preview.png', fullPage: true })
})

test('Slice 4: offers session/observed scope quick-picks and inline SKOS concept hierarchy picker on Account view', async ({
  page,
}) => {
  await page.setViewportSize({ width: 1360, height: 1280 })
  await page.goto('/console/repo-improvements-test')
  const { legalTaxonomyBase64 } = await page.evaluate(async () => {
    const path = '/console/browser/repoImprovementsHarness.ts'
    const { mountAccountImprovements } = await import(/* @vite-ignore */ path)
    return await mountAccountImprovements()
  })

  await expect(page.getByTestId('scope-quick-picks')).toBeVisible()
  await expect(page.getByTestId('quick-scope-intake')).toBeVisible()
  await expect(page.getByTestId('quick-writer-scope-ds-legal')).toBeVisible()

  await page.getByTestId('toggle-inline-concept-browser').click()
  await expect(page.getByTestId('inline-concept-browser')).toBeVisible()

  await page.getByTestId('toggle-scheme-loader').click()
  await page.getByTestId('loader-scheme-id').locator('input').fill('legal')
  await page.getByTestId('loader-scheme-version').locator('input').fill('2026.10')
  await page.getByTestId('loader-base64-input').locator('textarea').fill(legalTaxonomyBase64)
  await page.getByTestId('loader-decode-base64').click()

  await expect(page.getByTestId('scheme-chip-legal')).toContainText('Legal Matter Taxonomy')
  await page.getByTestId('tree-select-contracts').click()
  await expect(page.getByTestId('concept-detail-panel')).toContainText('legal/contracts')
  await expect(page.getByTestId('concept-apply-btn-contracts')).toBeVisible()

  await page.screenshot({ path: 'test-results/slice4-account-scope-picks-and-skos-picker.png', fullPage: true })
})

test('Slice 5: shows archive rendition ContentProfile, compares versions side by side, and uploads a new rendition', async ({
  page,
}) => {
  const samplePayload = Buffer.from('OCR page 2 text content extracted with high confidence.', 'utf-8')
  const sampleSha = createHash('sha256').update(samplePayload).digest('hex')
  const responseBytes = toBinary(
    UploadRenditionResponseSchema,
    create(UploadRenditionResponseSchema, {
      operationId: '018f4063-7d4b-7000-8000-000000000199',
      entryUuid: '00000000-0000-5000-8000-000000000101',
      version: 4n,
      sha256: sampleSha,
      replayed: false,
    }),
  )

  await page.route('**/v1/archive:upload**', (route) =>
    route.fulfill({
      status: 200,
      contentType: 'application/protobuf',
      body: Buffer.from(responseBytes),
    }),
  )

  await page.setViewportSize({ width: 1360, height: 1240 })
  await page.goto('/console/repo-improvements-test')
  await page.evaluate(async () => {
    const path = '/console/browser/repoImprovementsHarness.ts'
    const { mountArchiveEntryImprovements } = await import(/* @vite-ignore */ path)
    await mountArchiveEntryImprovements()
  })

  await expect(page.getByTestId('entry-metadata')).toContainText('department=legal')
  await expect(page.getByTestId('rendition-profile-ocr / page-1')).toContainText('OCR_TEXT · quality 0.92')

  await page.getByTestId('compare-version-1').click()
  await expect(page.getByTestId('version-compare')).toBeVisible()
  await expect(page.getByTestId('version-compare-table')).toContainText('ocr / page-1')
  await expect(page.getByTestId('version-compare-table')).toContainText('added')

  await page.getByTestId('toggle-rendition-upload').click()
  await expect(page.getByTestId('rendition-upload-panel')).toBeVisible()
  await page.getByTestId('rendition-up-name').locator('input').fill('ocr')
  await page.getByTestId('rendition-up-subkey').locator('input').fill('page-2')
  await page.getByTestId('rendition-up-module').locator('input').fill('grparse-ocr')
  await page.getByTestId('rendition-up-file').setInputFiles({
    name: 'ocr-page-2.txt',
    mimeType: 'text/plain',
    buffer: samplePayload,
  })
  await page.getByTestId('rendition-up-submit').click()
  await expect(page.getByTestId('rendition-upload-committed')).toContainText('Committed as version 4')

  await page.screenshot({ path: 'test-results/slice5-archive-version-compare-and-upload.png', fullPage: true })
})
