/** Tiny actual WASM fixtures; section encoders are test-only, not a production parser. */
function u32(n: number): number[] {
  const out: number[] = []
  do { const byte = n & 127; n >>>= 7; out.push(byte | (n ? 128 : 0)) } while (n)
  return out
}
const name = (text: string) => { const bytes = [...new TextEncoder().encode(text)]; return [...u32(bytes.length), ...bytes] }
const section = (id: number, bytes: number[]) => [id, ...u32(bytes.length), ...bytes]
const body = (code: number[]) => { const bytes = [0, ...code, 11]; return [...u32(bytes.length), ...bytes] }

export function previewModule(mode: 'text' | 'loop' | 'trap' | 'bounds' | 'oversize' | 'bad-proto' | 'overlap' | 'memory' | 'imports' | 'version' = 'text', text = '<script>guest text</script>'): Uint8Array {
  const textBytes = [...new TextEncoder().encode(text)]
  const output = mode === 'bad-proto' ? [10, 255] : [10, ...u32(textBytes.length), ...textBytes]
  const imports = mode === 'imports' ? 'other' : 'env'
  const result = mode === 'loop' ? [3, 64, 12, 0, 11, 66, 0]
    : mode === 'trap' ? [0]
    : mode === 'bounds' ? [66, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 32] // i64.const 1<<54 (pointer 4MiB)
    : mode === 'oversize' ? [66, 0x81, 0x80, 0x10] // 262145 bytes
    : [66, ...u32(output.length)] // result starts at address zero
  return new Uint8Array([
    0, 97, 115, 109, 1, 0, 0, 0,
    ...section(1, [3, 96, 0, 1, 127, 96, 1, 127, 1, 127, 96, 4, 127, 127, 127, 127, 1, 126]),
    ...section(2, [1, ...name(imports), ...name('memory'), 2, 1, 16, ...u32(256)]),
    ...section(3, [3, 0, 1, 2]),
    ...(mode === 'memory' ? section(5, [1, 0, 1]) : []),
    ...section(6, [1, 127, 1, 65, 0x80, 8, 11]), // mutable i32 heap = 1024
    ...section(7, [3, ...name('pm_preview_abi'), 0, 0, ...name('pm_alloc'), 0, 1, ...name('pm_preview'), 0, 2]),
    ...section(10, [3,
      ...body([65, mode === 'version' ? 2 : 1]),
      ...body(mode === 'overlap' ? [65, 0] : [35, 0, 35, 0, 32, 0, 106, 36, 0]),
      ...body(result),
    ]),
    ...section(11, [1, 0, 65, 0, 11, ...u32(output.length), ...output]),
  ])
}
