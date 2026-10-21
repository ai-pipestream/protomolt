/**
 * Where a download's bytes go. In browsers with the File System Access API the
 * user picks a file and each frame is written to it as it arrives, so memory
 * stays bounded by one frame. Elsewhere the frames are kept as Blob parts and
 * handed to the browser's download when the read completes; the screen says
 * which of the two is in use.
 */

export interface DownloadSink {
  /** `disk` writes to a file the user chose; `memory` keeps the bytes until the end. */
  readonly mode: 'disk' | 'memory'
  write(chunk: Uint8Array): void | Promise<void>
  /** The read verified and completed: keep the file. */
  finish(): Promise<void>
  /** The read failed or was cancelled: discard what was written. */
  discard(): Promise<void>
}

interface SaveFilePickerWindow {
  showSaveFilePicker(options: { suggestedName?: string }): Promise<{
    createWritable(): Promise<{
      write(data: Uint8Array): Promise<void>
      close(): Promise<void>
      abort(): Promise<void>
    }>
  }>
}

export function canStreamToDisk(target: unknown = globalThis): target is SaveFilePickerWindow {
  return typeof (target as Partial<SaveFilePickerWindow>).showSaveFilePicker === 'function'
}

/** Asks the user where to save and opens that file for streaming writes. */
export async function diskSink(target: SaveFilePickerWindow, filename: string): Promise<DownloadSink> {
  const handle = await target.showSaveFilePicker({ suggestedName: filename })
  const writable = await handle.createWritable()
  return {
    mode: 'disk',
    write: (chunk) => writable.write(chunk),
    finish: () => writable.close(),
    discard: () => writable.abort(),
  }
}

/** Keeps the frames and saves them through an object URL once complete. */
export function memorySink(filename: string, mimeType: string, save: (blob: Blob, filename: string) => void): DownloadSink {
  let parts: Uint8Array<ArrayBuffer>[] = []
  return {
    mode: 'memory',
    write(chunk) {
      parts.push(new Uint8Array(chunk))
    },
    async finish() {
      save(new Blob(parts, { type: mimeType || 'application/octet-stream' }), filename)
      parts = []
    },
    async discard() {
      parts = []
    },
  }
}

/** The browser's download of a Blob, through a temporary object URL. */
export function saveBlob(blob: Blob, filename: string): void {
  const url = URL.createObjectURL(blob)
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = filename
  anchor.click()
  setTimeout(() => URL.revokeObjectURL(url), 0)
}
