import { createHash } from 'node:crypto'
import { test, expect } from '@playwright/test'
import { create, toBinary } from '@bufbuild/protobuf'
import { UploadRenditionResponseSchema } from '../src/gen/ai/protomolt/proto/repo/archive/v1/archive_service_pb'
import {
  SaveDocumentResponseSchema,
  UploadDocumentResponseSchema,
} from '../src/gen/ai/protomolt/proto/repo/v2/document_service_pb'
import { AccessionSchema } from '../src/gen/ai/protomolt/proto/repo/v2/manifest_pb'

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

test('Ticket #688: runs ClassifyEntry and BridgeEntry workbench on RepositoryArchiveEntryView', async ({ page }) => {
  await page.setViewportSize({ width: 1360, height: 1180 })
  await page.goto('/console/repo-improvements-test')
  await page.evaluate(async () => {
    const path = '/console/browser/repoImprovementsHarness.ts'
    const { mountArchiveEntryImprovements } = await import(/* @vite-ignore */ path)
    await mountArchiveEntryImprovements()
  })

  await page.getByTestId('toggle-classify-bridge').click()
  await expect(page.getByTestId('classify-bridge-panel')).toBeVisible()

  await page.getByTestId('classify-module').locator('input').fill('grparse-classifier')
  await page.getByTestId('classify-submit').click()
  await expect(page.getByTestId('classify-result')).toContainText('VERIFIED')
  await expect(page.getByTestId('classify-result')).toContainText('%PDF-1.7 header at offset 0')

  await page.getByTestId('bridge-module').locator('input').fill('grparse-bridge')
  await page.getByTestId('bridge-submit').click()
  await expect(page.getByTestId('bridge-outcomes-table')).toContainText('DOCUMENT_TEXT')
  await expect(page.getByTestId('bridge-outcomes-table')).toContainText('PRODUCED')
  await expect(page.getByTestId('bridge-outcomes-table')).toContainText('Extracted 14 pages')

  await page.screenshot({ path: 'test-results/ticket688-classify-bridge-workbench.png', fullPage: true })
})

test('Ticket #689: provisions drives, creates archives, and uploads a new archive entry with auto-suggested ID', async ({
  page,
}) => {
  const newEntryBytes = Buffer.from('%PDF-1.7 signed addendum bytes', 'utf-8')
  const newEntrySha = createHash('sha256').update(newEntryBytes).digest('hex')
  const responseBytes = toBinary(
    UploadRenditionResponseSchema,
    create(UploadRenditionResponseSchema, {
      operationId: '018f4063-7d4b-7000-8000-000000000289',
      entryUuid: '00000000-0000-5000-8000-000000000289',
      version: 1n,
      sha256: newEntrySha,
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

  await page.setViewportSize({ width: 1360, height: 1180 })
  await page.goto('/console/repo-improvements-test')
  await page.evaluate(async () => {
    const path = '/console/browser/repoImprovementsHarness.ts'
    const { mountArchiveImprovements } = await import(/* @vite-ignore */ path)
    await mountArchiveImprovements()
  })

  await page.getByTestId('toggle-new-entry-upload').click()
  await expect(page.getByTestId('new-entry-upload-panel')).toBeVisible()
  await page.getByTestId('new-entry-file').setInputFiles({
    name: '2026-Q4-Regulatory-Addendum.pdf',
    mimeType: 'application/pdf',
    buffer: newEntryBytes,
  })
  await expect(page.getByTestId('new-entry-id').locator('input')).toHaveValue('2026-q4-regulatory-addendum')
  await page.getByTestId('new-entry-submit').click()
  await expect(page.getByTestId('new-entry-committed')).toContainText('2026-q4-regulatory-addendum')
  await expect(page.getByTestId('new-entry-op-link')).toBeVisible()

  await page.screenshot({ path: 'test-results/ticket689-create-drive-archive-and-entry.png', fullPage: true })
})

test('Ticket #690: auto-resolves deep-linked ?op=... outcome on RepositoryLookupView', async ({ page }) => {
  await page.setViewportSize({ width: 1360, height: 920 })
  await page.goto('/console/repo-improvements-test')
  await page.evaluate(async () => {
    const path = '/console/browser/repoImprovementsHarness.ts'
    const { mountLookupImprovements } = await import(/* @vite-ignore */ path)
    await mountLookupImprovements({ account: 'acct-a', op: '018f4063-7d4b-7000-8000-000000000199' })
  })

  await expect(page.getByTestId('outcome-committed')).toBeVisible()
  await expect(page.getByTestId('outcome-committed')).toContainText('Revision 7 of nda-2026-001')
  await expect(page.getByTestId('open-committed')).toBeVisible()

  await page.screenshot({ path: 'test-results/ticket690-operation-outcome-deeplink.png', fullPage: true })
})

test('Ticket #691: displays URL-synced filter summary chips on RepositoryAccountView and RepositoryArchiveView', async ({
  page,
}) => {
  await page.setViewportSize({ width: 1360, height: 1080 })
  await page.goto('/console/repo-improvements-test')
  await page.evaluate(async () => {
    const path = '/console/browser/repoImprovementsHarness.ts'
    const { mountAccountImprovements } = await import(/* @vite-ignore */ path)
    await mountAccountImprovements('/repository/accounts/acct-a?graph=intake:acct-a&f.title=NDA&sort=title')
  })

  await expect(page.getByTestId('account-active-filters')).toBeVisible()
  await expect(page.getByTestId('active-filter-graph')).toContainText('intake:acct-a')
  await expect(page.getByTestId('active-filter-title')).toContainText('NDA')
  await expect(page.getByTestId('active-filter-sort')).toContainText('Title')

  await page.screenshot({ path: 'test-results/ticket691-url-synced-filters.png', fullPage: true })
})

test('Ticket #692: stages and uploads a multi-file batch queue with path-prefixed docId auto-fill', async ({
  page,
}) => {
  await page.route('**/v1/documents:upload**', (route) => {
    const url = new URL(route.request().url())
    const opId = url.searchParams.get('operation_id') ?? 'op-batch'
    const docId = url.searchParams.get('doc_id') ?? 'doc-batch'
    const body = toBinary(
      UploadDocumentResponseSchema,
      create(UploadDocumentResponseSchema, {
        answer: {
          case: 'saved',
          value: create(SaveDocumentResponseSchema, {
            operationId: opId,
            accession: create(AccessionSchema, {
              address: { accountId: 'acct-a', graphId: 'intake:acct-a', graphAddressId: 'ds-legal', docId },
              revision: 1n,
            }),
          }),
        },
      }),
    )
    return route.fulfill({
      status: 200,
      contentType: 'application/proto',
      body: Buffer.from(body),
    })
  })

  await page.setViewportSize({ width: 1360, height: 1020 })
  await page.goto('/console/repo-improvements-test')
  await page.evaluate(async () => {
    const path = '/console/browser/repoImprovementsHarness.ts'
    const { mountUploadImprovements } = await import(/* @vite-ignore */ path)
    await mountUploadImprovements()
  })

  await page.getByTestId('up-drive').locator('input[type="text"]').fill('legal-vault')
  await page.getByTestId('up-drive').locator('input[type="text"]').press('Tab')
  await page.getByTestId('upload-path-prefix').locator('input').fill('contracts/2026-q4')
  await page.getByTestId('up-file').setInputFiles([
    {
      name: 'Acme Mutual NDA (Signed).pdf',
      mimeType: 'application/pdf',
      buffer: Buffer.from('pdf payload 1'),
    },
    {
      name: 'Globex Enterprise MSA.pdf',
      mimeType: 'application/pdf',
      buffer: Buffer.from('pdf payload 2'),
    },
    {
      name: 'DPA Addendum Schedule-A.md',
      mimeType: 'text/markdown',
      buffer: Buffer.from('# Schedule A'),
    },
  ])

  await expect(page.getByTestId('batch-upload-table')).toBeVisible()
  await expect(page.getByTestId('batch-doc-id-0').locator('input')).toHaveValue(
    'contracts/2026-q4/acme-mutual-nda-signed.pdf',
  )
  await expect(page.getByTestId('batch-doc-id-1').locator('input')).toHaveValue(
    'contracts/2026-q4/globex-enterprise-msa.pdf',
  )
  await expect(page.getByTestId('batch-doc-id-2').locator('input')).toHaveValue(
    'contracts/2026-q4/dpa-addendum-schedule-a.md',
  )

  await page.getByTestId('batch-upload-submit').click()
  await expect(page.getByTestId('batch-status-0')).toHaveText('SAVED')
  await expect(page.getByTestId('batch-status-1')).toHaveText('SAVED')
  await expect(page.getByTestId('batch-status-2')).toHaveText('SAVED')

  await page.screenshot({ path: 'test-results/ticket692-batch-drag-drop-upload.png', fullPage: true })
})

test('Ticket #694: commits atomic entry metadata and inline renditions via ArchiveService.PutEntry workbench', async ({
  page,
}) => {
  await page.setViewportSize({ width: 1360, height: 1240 })
  await page.goto('/console/repo-improvements-test')
  await page.evaluate(async () => {
    const path = '/console/browser/repoImprovementsHarness.ts'
    const { mountArchiveEntryImprovements } = await import(/* @vite-ignore */ path)
    await mountArchiveEntryImprovements()
  })

  await expect(page.getByTestId('open-entry-v2-document')).toBeVisible()
  await expect(page.getByTestId('delete-archive-entry')).toBeVisible()

  await page.getByTestId('toggle-put-entry').click()
  await expect(page.getByTestId('put-entry-panel')).toBeVisible()

  await page.getByTestId('put-entry-title').locator('input').fill('Master Mutual NDA — Acme Corp (Annotated v4)')
  await page.getByTestId('put-entry-add-meta').click()
  await page.getByTestId('put-entry-meta-key-3').locator('input').fill('reviewedBy')
  await page.getByTestId('put-entry-meta-val-3').locator('input').fill('general-counsel')

  await page.getByTestId('put-entry-add-rendition').click()
  await page.getByTestId('put-rendition-name-0').locator('input').fill('markdown')
  await page.getByTestId('put-rendition-subkey-0').locator('input').fill('exec-summary')
  await page
    .getByTestId('put-rendition-text-0')
    .locator('textarea')
    .fill('# Executive Summary\n\nMutual 5-year confidentiality term verified against signed PDF.')

  await page.getByTestId('put-entry-submit').click()
  await expect(page.getByTestId('put-entry-outcome')).toBeVisible()
  await expect(page.getByTestId('put-entry-status-chip')).toHaveText('CREATED')
  await expect(page.getByTestId('put-entry-outcome')).toContainText('Committed new version 4')

  await page.screenshot({ path: 'test-results/ticket694-archive-put-entry.png', fullPage: true })
})

test('Ticket #695: renders interactive 1-hop link neighborhood graph and batch-resolves dynamic links', async ({
  page,
}) => {
  await page.setViewportSize({ width: 1360, height: 1180 })
  await page.goto('/console/repo-improvements-test')
  await page.evaluate(async () => {
    const path = '/console/browser/repoImprovementsHarness.ts'
    const { mountDocumentImprovements } = await import(/* @vite-ignore */ path)
    await mountDocumentImprovements('links')
  })

  await expect(page.getByTestId('link-neighborhood-card')).toBeVisible()
  await expect(page.getByTestId('link-neighborhood-graph')).toBeVisible()
  await expect(page.getByTestId('link-status-structured_data.related_agreements-0')).toHaveText('DYNAMIC')

  await page.getByTestId('resolve-all-dynamic-links').click()
  await expect(page.getByTestId('link-status-structured_data.related_agreements-0')).toHaveText('RESOLVED (2)')
  await expect(page.getByTestId('graph-node-sow-2026-015')).toBeVisible()

  await page.getByTestId('resolve-link-structured_data.related_agreements-0').click()
  await expect(page.getByTestId('resolved-targets')).toContainText('Statement of Work #15 — AI Platform')

  await page.screenshot({ path: 'test-results/ticket695-link-resolution-graph.png', fullPage: true })
})

test('Ticket #696: inspects DriveService.GetDrive provider details, options, metadata, and bound archives', async ({
  page,
}) => {
  await page.setViewportSize({ width: 1360, height: 1180 })
  await page.goto('/console/repo-improvements-test')
  await page.evaluate(async () => {
    const path = '/console/browser/repoImprovementsHarness.ts'
    const { mountAccountImprovements } = await import(/* @vite-ignore */ path)
    await mountAccountImprovements()
  })

  await expect(page.getByTestId('inspect-drive-legal-vault')).toBeVisible()
  await page.getByTestId('inspect-drive-legal-vault').click()

  await expect(page.getByTestId('drive-inspector-panel')).toBeVisible()
  await expect(page.getByTestId('drive-inspector-name')).toHaveText('legal-vault')
  await expect(page.getByTestId('drive-inspector-provider-summary')).toContainText('https://s3.us-east-1.acme.internal')
  await expect(page.getByTestId('drive-inspector-metadata')).toContainText('compliance=SOC2-Type-II')
  await expect(page.getByTestId('drive-inspector-archives')).toContainText('executed-agreements')

  await page.screenshot({ path: 'test-results/ticket696-drive-inspector-and-entry-v2.png', fullPage: true })
})
