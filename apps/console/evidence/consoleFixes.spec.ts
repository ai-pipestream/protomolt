import { createServer, type Server } from 'node:http'
import { createHash } from 'node:crypto'
import { test, expect, type Page } from '@playwright/test'
import { create, toBinary } from '@bufbuild/protobuf'
import { UploadRenditionResponseSchema } from '../src/gen/ai/protomolt/proto/repo/archive/v1/archive_service_pb'
import { EVIDENCE_UPSTREAM_PORT } from './evidencePort'

// Screenshots land in evidence-screens/<label>/, outside test-results so a later run keeps them.
// EVIDENCE_LABEL names the build under test (`after` by default; `before` when run on a pre-fix tree).
// Each spec screenshots the state the fix is about before it asserts, so a pre-fix run still leaves
// the picture of the defect next to the failure.
const label = process.env.EVIDENCE_LABEL ?? 'after'
const shot = (page: Page, name: string) => page.screenshot({ path: `evidence-screens/${label}/${name}.png`, fullPage: true })

const rpcLog = (page: Page) => page.evaluate(() => [...window.consoleFixRpcLog])

async function mount(page: Page, which: 'mountArchiveEntry' | 'mountDocumentLinks') {
  // On a cold cache Vite optimizes the harness dependencies on first import and fails that import,
  // so a fresh page retries it a few times before the spec gives up.
  for (let attempt = 1; ; attempt++) {
    await page.goto('/console/console-fixes-evidence')
    const imported = await page.evaluate(async () => {
      const path = '/console/evidence/consoleFixesHarness.ts'
      try {
        await import(/* @vite-ignore */ path)
        return true
      } catch {
        return false
      }
    })
    if (imported) break
    if (attempt === 5) throw new Error('The evidence harness did not load after 5 attempts')
    await page.waitForTimeout(1_000)
  }
  await page.evaluate(async (fn) => {
    const path = '/console/evidence/consoleFixesHarness.ts'
    const harness = await import(/* @vite-ignore */ path)
    await harness[fn]()
  }, which)
}

test.beforeEach(async ({ page }) => {
  await page.setViewportSize({ width: 1360, height: 1100 })
  await page.route('**/console/console-fixes-evidence', (route) =>
    route.fulfill({
      contentType: 'text/html',
      body: '<!doctype html><html><head><meta charset="utf-8"><title>Console fix evidence</title></head><body><div id="app"></div></body></html>',
    }),
  )
})

test('#699: a metadata-only PutEntry edit sends no renditions, so stored markdown survives', async ({ page }) => {
  await mount(page, 'mountArchiveEntry')
  await expect(page.getByTestId('version-chip')).toContainText('version 3')

  await page.getByTestId('toggle-put-entry').click()
  await expect(page.getByTestId('put-entry-panel')).toBeVisible()
  await page.getByTestId('put-entry-title').locator('input').fill('Master Mutual NDA - Acme Corp (retitled)')
  await shot(page, '699-1-put-entry-form')

  await page.getByTestId('put-entry-submit').click()
  await expect(page.getByTestId('put-entry-outcome')).toBeVisible()
  await expect.poll(() => rpcLog(page)).toContainEqual(expect.stringContaining('ArchiveService.PutEntry'))
  await shot(page, '699-2-put-entry-sent')

  await expect(page.getByTestId('put-rendition-name-0')).toHaveCount(0)
  const put = (await rpcLog(page)).find((line) => line.startsWith('ArchiveService.PutEntry'))
  expect(put).toBe('ArchiveService.PutEntry title="Master Mutual NDA - Acme Corp (retitled)" renditions=0')
})

test('#701: Inspect bytes on a bridge result opens the version the bridge wrote', async ({ page }) => {
  await mount(page, 'mountArchiveEntry')
  await expect(page.getByTestId('version-chip')).toContainText('version 3')

  await page.getByTestId('toggle-classify-bridge').click()
  await page.getByTestId('bridge-submit').click()
  await expect(page.getByTestId('bridge-result-version')).toContainText('Entry version after run: 4')
  await page.getByTestId('bridge-inspect-original').click()
  await expect(page.getByTestId('byte-reader')).toBeVisible()
  await shot(page, '701-bridge-inspect')

  await expect(page.getByTestId('version-chip')).toContainText('version 4')
  await expect(page.getByTestId('harness-route')).toContainText('version=4')
  expect(await rpcLog(page)).toContainEqual('ArchiveService.GetEntryManifest version=4 -> 4')
})

test.describe('#702: the development server proxies /v1/archive:upload to the repository host', () => {
  const payload = Buffer.from('OCR page 2 text content extracted with high confidence.', 'utf-8')
  const seen: { url: string; bytes: Buffer }[] = []
  let upstream: Server

  test.beforeAll(async () => {
    const receipt = toBinary(
      UploadRenditionResponseSchema,
      create(UploadRenditionResponseSchema, {
        operationId: '018f4063-7d4b-7000-8000-000000000702',
        entryUuid: '00000000-0000-5000-8000-000000000101',
        version: 4n,
        sha256: createHash('sha256').update(payload).digest('hex'),
        replayed: false,
      }),
    )
    upstream = createServer(async (request, response) => {
      const chunks: Buffer[] = []
      for await (const chunk of request) chunks.push(Buffer.from(chunk))
      seen.push({ url: request.url ?? '', bytes: Buffer.concat(chunks) })
      response.writeHead(200, { 'Content-Type': 'application/protobuf' })
      response.end(Buffer.from(receipt))
    })
    await new Promise<void>((resolve) => upstream.listen(EVIDENCE_UPSTREAM_PORT, '127.0.0.1', resolve))
  })

  test.afterAll(async () => {
    await new Promise<void>((resolve) => upstream.close(() => resolve()))
  })

  test('a rendition upload from the console reaches the upstream host unchanged', async ({ page }) => {
    await mount(page, 'mountArchiveEntry')
    await page.getByTestId('toggle-rendition-upload').click()
    await page.getByTestId('rendition-up-name').locator('input').fill('ocr')
    await page.getByTestId('rendition-up-subkey').locator('input').fill('page-2')
    await page.getByTestId('rendition-up-file').setInputFiles({ name: 'ocr-page-2.txt', mimeType: 'text/plain', buffer: payload })
    await page.getByTestId('rendition-up-submit').click()
    await expect(page.getByTestId('rendition-up-submit')).not.toHaveClass(/v-btn--loading/)
    await page.waitForTimeout(500)
    await shot(page, '702-archive-upload-through-dev-proxy')

    await expect(page.getByTestId('rendition-upload-committed')).toContainText('Committed as version 4')
    expect(seen).toHaveLength(1)
    expect(seen[0].url.startsWith('/v1/archive:upload?')).toBe(true)
    expect(seen[0].bytes).toEqual(payload)
  })
})

test('#712: Resolve all stops spinning once a row resolve supersedes it', async ({ page }) => {
  await mount(page, 'mountDocumentLinks')
  const resolveAll = page.getByTestId('resolve-all-dynamic-links')

  await resolveAll.click()
  await expect(resolveAll).toHaveClass(/v-btn--loading/)
  await shot(page, '712-1-resolve-all-in-flight')

  await page.getByTestId('resolve-link-structured_data.related_agreements-0').click()
  await expect(page.getByTestId('link-status-structured_data.related_agreements-0')).toHaveText('RESOLVED (1)')
  await shot(page, '712-2-row-resolve-supersedes')
  await expect(resolveAll).not.toHaveClass(/v-btn--loading/)

  await page.evaluate(() => window.consoleFixReleaseResolve())
  await expect(resolveAll).not.toHaveClass(/v-btn--loading/)
  await expect(resolveAll).toBeEnabled()
})

test('#713: Delete entry tombstones the archive call number at the pinned version and never purges', async ({ page }) => {
  await mount(page, 'mountArchiveEntry')
  await expect(page.getByTestId('version-chip')).toContainText('version 3')

  await page.getByTestId('delete-archive-entry').click()
  const dialog = page.getByTestId('delete-dialog')
  await expect(dialog).toBeVisible()
  await expect(dialog.getByTestId('delete-call-number')).toHaveText('acct-a / archive:acct-a / executed-agreements / nda-2026-001')
  await expect(dialog.getByTestId('delete-revision')).toHaveText('3')
  await shot(page, '713-1-delete-dialog')

  await dialog.getByTestId('delete-confirm').click()
  await expect(page.getByTestId('harness-route')).toContainText('repository-document')
  await shot(page, '713-2-routed-to-tombstone')

  await expect(page.getByTestId('harness-route')).toContainText('revision=4')
  const log = await rpcLog(page)
  expect(log).toContainEqual('DocumentService.DeleteDocument acct-a / archive:acct-a / executed-agreements / nda-2026-001 expected revision=3')
  expect(log.some((line) => line.startsWith('DocumentService.PurgeDocument'))).toBe(false)
})
