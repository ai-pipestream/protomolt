import { executePreview, type PreviewInput } from './previewRuntime'

self.onmessage = async (event: MessageEvent<PreviewInput>) => {
  try {
    self.postMessage({ ok: true, text: await executePreview(event.data) })
  } catch (error) {
    self.postMessage({ ok: false, error: error instanceof Error ? error.message : String(error) })
  }
}
