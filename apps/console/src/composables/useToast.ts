import { reactive } from 'vue'

export interface ToastMessage {
  id: number
  text: string
  kind: 'success' | 'error'
}

let nextId = 1

/** The queue ToastHost renders; module-level so any component can toast. */
export const toastQueue = reactive<ToastMessage[]>([])

function push(kind: ToastMessage['kind'], text: string) {
  const entry = { id: nextId++, text, kind }
  toastQueue.push(entry)
  setTimeout(() => {
    const at = toastQueue.findIndex((t) => t.id === entry.id)
    if (at >= 0) {
      toastQueue.splice(at, 1)
    }
  }, 4000)
}

export const toast = {
  success: (text: string) => push('success', text),
  error: (text: string) => push('error', text),
}
