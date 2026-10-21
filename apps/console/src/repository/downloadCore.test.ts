import { describe, expect, it } from 'vitest'
import { canStreamToDisk, diskSink, memorySink } from './downloadCore'

describe('download sinks', () => {
  it('keeps memory frames until finish, then saves one Blob of them', async () => {
    const saved: { blob: Blob; name: string }[] = []
    const sink = memorySink('a.bin', '', (blob, name) => saved.push({ blob, name }))
    expect(sink.mode).toBe('memory')
    await sink.write(new Uint8Array([1, 2]))
    await sink.write(new Uint8Array([3]))
    expect(saved).toHaveLength(0)
    await sink.finish()
    expect(saved[0].name).toBe('a.bin')
    expect(saved[0].blob.type).toBe('application/octet-stream')
    expect(new Uint8Array(await saved[0].blob.arrayBuffer())).toEqual(new Uint8Array([1, 2, 3]))
  })

  it('saves nothing when a memory download is discarded', async () => {
    const saved: Blob[] = []
    const sink = memorySink('a.bin', 'application/pdf', (blob) => saved.push(blob))
    await sink.write(new Uint8Array([1]))
    await sink.discard()
    expect(saved).toHaveLength(0)
  })

  it('writes disk frames as they come and aborts the file on discard', async () => {
    const log: string[] = []
    const target = {
      showSaveFilePicker: async (options: { suggestedName?: string }) => {
        log.push(`pick ${options.suggestedName}`)
        return {
          createWritable: async () => ({
            write: async (d: Uint8Array) => void log.push(`write ${d.length}`),
            close: async () => void log.push('close'),
            abort: async () => void log.push('abort'),
          }),
        }
      },
    }
    expect(canStreamToDisk(target)).toBe(true)
    expect(canStreamToDisk({})).toBe(false)
    const sink = await diskSink(target, 'r.pdf')
    await sink.write(new Uint8Array(5))
    await sink.discard()
    expect(log).toEqual(['pick r.pdf', 'write 5', 'abort'])
  })
})
