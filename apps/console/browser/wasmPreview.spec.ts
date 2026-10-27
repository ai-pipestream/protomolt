import { readFileSync } from 'node:fs'
import { test, expect } from '@playwright/test'
import { create, toBinary } from '@bufbuild/protobuf'
import { AnySchema } from '@bufbuild/protobuf/wkt'
import { previewModule } from '../src/wasm/previewTestKit'

test.beforeEach(async ({ page }) => {
  await page.route('**/console/preview-test', route => route.fulfill({ contentType: 'text/html', body: '<!doctype html><html><head><meta charset="utf-8"></head><body><div id="app"></div></body></html>' }))
})

const payload = [...toBinary(AnySchema, create(AnySchema, { typeUrl: 'example/BrowserProof', value: new Uint8Array([8, 127]) }))]

test('executes the downloaded example in a real browser worker', async ({ page }) => {
  await page.goto('/console/preview-test')
  const text = await page.evaluate(async (payload) => {
    const path = '/console/src/wasm/preview.ts'
    const { runPreview } = await import(/* @vite-ignore */ path)
    const module = new Uint8Array(await (await fetch('/console/examples/type-url-preview.wasm')).arrayBuffer())
    return runPreview({ module, payload: new Uint8Array(payload), descriptorSet: new Uint8Array() }, new AbortController().signal)
  }, payload)
  expect(text).toBe('example/BrowserProof')
})

test('terminates infinite-loop modules on timeout and still runs the next preview', async ({ page }) => {
  await page.goto('/console/preview-test')
  const result = await page.evaluate(async ({ loop, good, payload }) => {
    const path = '/console/src/wasm/preview.ts'
    const { runPreview } = await import(/* @vite-ignore */ path)
    const input = { module: new Uint8Array(loop), payload: new Uint8Array(payload), descriptorSet: new Uint8Array() }
    let failure = ''
    try { await runPreview(input, new AbortController().signal) } catch (error) { failure = (error as Error).message }
    const text = await runPreview({ ...input, module: new Uint8Array(good) }, new AbortController().signal)
    return { failure, text }
  }, { loop: [...previewModule('loop')], good: [...previewModule('text', 'Recovered')], payload })
  expect(result).toEqual({ failure: 'WASM preview exceeded its 3 second deadline.', text: 'Recovered' })
})

test('cancels a running infinite loop and rejects already cancelled requests', async ({ page }) => {
  await page.goto('/console/preview-test')
  const names = await page.evaluate(async ({ loop, payload }) => {
    const path = '/console/src/wasm/preview.ts'
    const { runPreview } = await import(/* @vite-ignore */ path)
    const input = { module: new Uint8Array(loop), payload: new Uint8Array(payload), descriptorSet: new Uint8Array() }
    const controller = new AbortController()
    setTimeout(() => controller.abort(), 100)
    const names: string[] = []
    for (let i = 0; i < 2; i++) {
      try { await runPreview(input, controller.signal) } catch (error) { names.push((error as Error).name) }
    }
    return names
  }, { loop: [...previewModule('loop')], payload })
  expect(names).toEqual(['AbortError', 'AbortError'])
})


test('the document screen expands retained Any and runs a selected preview module', async ({ page }) => {
  await page.setViewportSize({ width: 1280, height: 1000 })
  await page.goto('/console/preview-test')
  const original = await page.evaluate(async () => {
    const path = '/console/browser/inspectorHarness.ts'
    const { mountInspector } = await import(/* @vite-ignore */ path)
    return mountInspector()
  })
  await page.getByTestId('read-part-0').click()
  await expect(page.getByTestId('part-view')).toContainText('This Any was decoded with its retained schema.')
  await page.getByTestId('preview-module').setInputFiles('public/examples/type-url-preview.wasm')
  await page.getByTestId('run-preview').click()
  await expect(page.getByTestId('preview-text')).toHaveText('type.googleapis.com/ai.protomolt.proto.repo.v1.Document')
  const download = page.waitForEvent('download')
  await page.getByTestId('download-part').click()
  const saved = await download
  expect(saved.suggestedFilename()).toBe('part-r7.pb')
  expect([...readFileSync((await saved.path())!)]).toEqual(original)
  await page.screenshot({ path: 'test-results/protobuf-inspector.png', fullPage: true })
})
