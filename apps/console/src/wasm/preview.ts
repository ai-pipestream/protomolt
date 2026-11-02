import { checkPreviewInput, PREVIEW_LIMITS, type PreviewInput } from './previewRuntime'

/** Each run owns its worker: timeout/cancel terminates even a guest infinite loop. */
export function runPreview(input: PreviewInput, signal: AbortSignal): Promise<string> {
  return new Promise((resolve, reject) => {
    if (signal.aborted) { reject(new DOMException('Preview cancelled.', 'AbortError')); return }
    checkPreviewInput(input)
    const worker = new Worker(new URL('./preview.worker.ts', import.meta.url), { type: 'module' })
    const finish = (error: Error | null, text = '') => {
      clearTimeout(timer)
      signal.removeEventListener('abort', cancel)
      worker.terminate()
      if (error) reject(error)
      else resolve(text)
    }
    const cancel = () => finish(new DOMException('Preview cancelled.', 'AbortError'))
    const timer = setTimeout(() => finish(new Error('WASM preview exceeded its 3 second deadline.')), PREVIEW_LIMITS.timeoutMs)
    signal.addEventListener('abort', cancel, { once: true })
    worker.onmessage = ({ data }) => {
      if (data?.ok === true && typeof data.text === 'string') finish(null, data.text)
      else finish(new Error(typeof data?.error === 'string' ? data.error : 'Invalid preview worker response.'))
    }
    worker.onerror = (event) => { event.preventDefault(); finish(new Error(event.message || 'Preview worker failed.')) }
    worker.onmessageerror = () => finish(new Error('Preview worker response could not be read.'))
    try { worker.postMessage(input) } catch (error) { finish(error instanceof Error ? error : new Error(String(error))) }
  })
}
