/** Binary protobuf preview ABI v1. Run only in a disposable worker. */
import { fromBinary } from '@bufbuild/protobuf'
import { StringValueSchema } from '@bufbuild/protobuf/wkt'

export const PREVIEW_LIMITS = Object.freeze({ moduleBytes: 2 * 1024 * 1024, inputBytes: 4 * 1024 * 1024, outputBytes: 256 * 1024, memoryPages: 256, timeoutMs: 3000 })

export interface PreviewInput {
  module: Uint8Array
  /** google.protobuf.Any wrapping the original fragment bytes. */
  payload: Uint8Array
  /** The exact verified google.protobuf.FileDescriptorSet for the fragment. */
  descriptorSet: Uint8Array
}

export function checkPreviewInput(input: PreviewInput): void {
  if (input.module.length > PREVIEW_LIMITS.moduleBytes) throw new Error('WASM module exceeds the 2 MiB preview budget.')
  if (input.payload.length + input.descriptorSet.length > PREVIEW_LIMITS.inputBytes) throw new Error('Input exceeds the 4 MiB preview budget. The original protobuf remains available for download.')
}

/** Reject guest-owned memory/tables and start functions before instantiation. */
function checkSections(bytes: Uint8Array): void {
  const header = [0, 97, 115, 109, 1, 0, 0, 0]
  if (!header.every((byte, i) => bytes[i] === byte)) throw new Error('Expected a WebAssembly v1 module.')
  let pos = 8
  const u32 = () => {
    let value = 0
    for (let shift = 0; shift < 35; shift += 7) {
      if (pos >= bytes.length) throw new Error('Truncated WASM section length.')
      const byte = bytes[pos++]
      if (shift === 28 && byte > 15) throw new Error('Invalid WASM section length.')
      value += (byte & 127) * 2 ** shift
      if ((byte & 128) === 0) return value
    }
    throw new Error('Invalid WASM section length.')
  }
  while (pos < bytes.length) {
    const section = bytes[pos++]
    const size = u32()
    if (size > bytes.length - pos) throw new Error('Truncated WASM section.')
    // Even an unused private memory or table could evade the host memory budget.
    if (section === 4 || section === 5 || section === 8) throw new Error('Preview modules must not define tables, memories, or a start function.')
    const end = pos + size
    if (section === 1) {
      // This small ABI needs numeric function signatures only. Reject GC types
      // so a guest cannot allocate an independent, unbounded GC object heap.
      const count = u32()
      for (let i = 0; i < count; i++) {
        if (pos >= end || bytes[pos++] !== 0x60) throw new Error('Preview ABI permits only numeric function types.')
        for (let signature = 0; signature < 2; signature++) {
          const arity = u32()
          if (arity > end - pos) throw new Error('Truncated WASM function type.')
          for (let j = 0; j < arity; j++) {
            if (![0x7f, 0x7e, 0x7d, 0x7c].includes(bytes[pos++])) throw new Error('Preview ABI permits only numeric function types.')
          }
        }
      }
      if (pos !== end) throw new Error('Invalid WASM type section.')
    }
    pos = end
  }
}

/** No WASI, networking, filesystem, clock, DOM or credential imports are supplied. */
export async function executePreview(input: PreviewInput): Promise<string> {
  checkPreviewInput(input)
  checkSections(input.module)
  const module = await WebAssembly.compile(Uint8Array.from(input.module))
  const imports = WebAssembly.Module.imports(module)
  if (imports.length !== 1 || imports[0].kind !== 'memory' || imports[0].module !== 'env' || imports[0].name !== 'memory') {
    throw new Error('The only permitted preview import is env.memory.')
  }
  const memory = new WebAssembly.Memory({ initial: 16, maximum: PREVIEW_LIMITS.memoryPages })
  const instance = await WebAssembly.instantiate(module, { env: { memory } })
  const { pm_preview_abi: abi, pm_alloc: alloc, pm_preview: preview } = instance.exports
  if (typeof abi !== 'function' || typeof alloc !== 'function' || typeof preview !== 'function') throw new Error('Missing preview ABI exports.')
  if (abi() !== 1) throw new Error('Unsupported preview ABI version.')
  const range = (pointer: unknown, length: number): number => {
    if (typeof pointer !== 'number' || !Number.isInteger(pointer) || pointer < 0 || pointer > memory.buffer.byteLength - length) {
      throw new Error('Preview returned an out-of-bounds memory range.')
    }
    return pointer
  }
  const payloadAt = range(alloc(input.payload.length), input.payload.length)
  const schemaAt = range(alloc(input.descriptorSet.length), input.descriptorSet.length)
  if (payloadAt < schemaAt + input.descriptorSet.length && schemaAt < payloadAt + input.payload.length) throw new Error('Preview allocations overlap.')
  // alloc may grow memory: take the buffer only after both allocations.
  const bytes = new Uint8Array(memory.buffer)
  bytes.set(input.payload, payloadAt)
  bytes.set(input.descriptorSet, schemaAt)
  const result: unknown = preview(payloadAt, input.payload.length, schemaAt, input.descriptorSet.length)
  if (typeof result !== 'bigint') throw new Error('Preview must return an i64 pointer/length pair.')
  const pair = BigInt.asUintN(64, result)
  const length = Number(pair & 0xffffffffn)
  const pointer = Number(pair >> 32n)
  if (length > PREVIEW_LIMITS.outputBytes) throw new Error('Preview output exceeds the 256 KiB display budget.')
  range(pointer, length)
  const output = fromBinary(StringValueSchema, new Uint8Array(memory.buffer, pointer, length))
  if ((output.$unknown?.length ?? 0) !== 0) throw new Error('Preview output must be a google.protobuf.StringValue without unknown fields.')
  return output.value
}
