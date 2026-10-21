/**
 * Abort signals for a screen's calls. `next()` aborts the previous call of the
 * same scope (a newer query replaces an older one) and every signal is aborted
 * when the component unmounts, which ends server streams at the gateway.
 */
import { onBeforeUnmount } from 'vue'

export interface CallScope {
  /** A fresh signal; the scope's previous signal is aborted. */
  next(): AbortSignal
}

export function useCallScope(): CallScope {
  let current: AbortController | null = null
  const all = new Set<AbortController>()
  onBeforeUnmount(() => {
    for (const controller of all) controller.abort()
    all.clear()
  })
  return {
    next() {
      current?.abort()
      const controller = new AbortController()
      current = controller
      all.add(controller)
      controller.signal.addEventListener('abort', () => all.delete(controller), { once: true })
      return controller.signal
    },
  }
}
