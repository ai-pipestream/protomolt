/**
 * Abort signals for a screen's calls. `next()` aborts the previous call of the
 * same scope (a newer query replaces an older one) and every signal is aborted
 * when the component unmounts, which ends server streams at the gateway.
 */
import { onBeforeUnmount } from 'vue'

export interface CallScope {
  /** A fresh signal; the scope's previous signal is aborted. */
  next(): AbortSignal
  /** True once the component is unmounting: no further call should start. */
  readonly closed: boolean
}

export function useCallScope(): CallScope {
  let current: AbortController | null = null
  let unmounted = false
  const all = new Set<AbortController>()
  onBeforeUnmount(() => {
    unmounted = true
    for (const controller of all) controller.abort()
    all.clear()
  })
  return {
    get closed() {
      return unmounted
    },
    next() {
      current?.abort()
      const controller = new AbortController()
      // A call started after the screen went away, from code still unwinding,
      // never reaches the server.
      if (unmounted) {
        controller.abort()
        return controller.signal
      }
      current = controller
      all.add(controller)
      controller.signal.addEventListener('abort', () => all.delete(controller), { once: true })
      return controller.signal
    },
  }
}
