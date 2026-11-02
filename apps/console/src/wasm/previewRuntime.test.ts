import { readFileSync } from 'node:fs'
import { create, toBinary } from '@bufbuild/protobuf'
import { AnySchema, FileDescriptorSetSchema } from '@bufbuild/protobuf/wkt'
import { describe, expect, it } from 'vitest'
import { executePreview, PREVIEW_LIMITS, type PreviewInput } from './previewRuntime'
import { previewModule } from './previewTestKit'

function input(module = previewModule()): PreviewInput {
  return { module, payload: toBinary(AnySchema, create(AnySchema, { typeUrl: 'type.example/Hello', value: new Uint8Array([8, 127]) })), descriptorSet: toBinary(FileDescriptorSetSchema, create(FileDescriptorSetSchema, { file: [{ name: 'hello.proto', syntax: 'proto3' }] })) }
}

describe('WASM protobuf preview ABI', () => {
  it('runs a real module and returns literal protobuf text', async () => {
    expect(await executePreview(input())).toBe('<script>guest text</script>')
  })
  it('runs the published example against an Any envelope, including multibyte lengths', async () => {
    const example = new Uint8Array(readFileSync(new URL('../../public/examples/type-url-preview.wasm', import.meta.url)))
    const request = input(example)
    for (const typeUrl of ['type.example/Hello', `type.example/${'LongName'.repeat(30)}`]) {
      request.payload = toBinary(AnySchema, create(AnySchema, { typeUrl, value: new Uint8Array([8, 127, 0xa0, 6, 1]) }))
      const before = request.payload.slice()
      expect(await executePreview(request)).toBe(typeUrl)
      expect(request.payload).toEqual(before)
    }
  })
  it.each([
    ['trap', 'unreachable'], ['bounds', 'out-of-bounds'], ['oversize', 'display budget'],
    ['bad-proto', 'premature EOF'], ['overlap', 'overlap'], ['memory', 'must not define'],
    ['imports', 'only permitted'], ['version', 'Unsupported'],
  ] as const)('refuses %s', async (mode, message) => {
    await expect(executePreview(input(previewModule(mode)))).rejects.toThrow(message)
  })
  it('refuses malformed modules and preview budgets before compilation', async () => {
    await expect(executePreview(input(new Uint8Array([0, 1])))).rejects.toThrow('WebAssembly')
    await expect(executePreview(input(new Uint8Array(PREVIEW_LIMITS.moduleBytes + 1)))).rejects.toThrow('module exceeds')
    await expect(executePreview({ ...input(), payload: new Uint8Array(PREVIEW_LIMITS.inputBytes + 1) })).rejects.toThrow('Input exceeds')
  })
})
