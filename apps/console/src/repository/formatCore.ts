/**
 * Display helpers shared by the repository screens. Every value is rendered
 * from the generated message types; nothing here invents a value the message
 * does not carry.
 */
import type { DescEnum } from '@bufbuild/protobuf'
import { timestampDate, type Timestamp } from '@bufbuild/protobuf/wkt'

/**
 * The proto name of an enum value without the enum's prefix, for example
 * `PRESENT` for `PART_STATE_PRESENT`. A number the descriptor does not name
 * (a newer server) is shown as `UNKNOWN(n)`, never as a neighbouring value.
 */
export function enumLabel(schema: DescEnum, value: number): string {
  const named = schema.value[value]
  if (named === undefined) return `UNKNOWN(${value})`
  // DescEnum.sharedPrefix is lower snake case with its underscore ("part_state_").
  const prefix = schema.sharedPrefix?.toUpperCase() ?? ''
  return prefix && named.name.startsWith(prefix) ? named.name.slice(prefix.length) : named.name
}

/** An ISO-8601 instant for a set Timestamp, or `null` when the field is unset. */
export function instant(ts: Timestamp | undefined): string | null {
  return ts === undefined ? null : timestampDate(ts).toISOString()
}

const BYTE_UNITS = ['B', 'KiB', 'MiB', 'GiB', 'TiB']

/** A byte count in binary units, keeping the exact count for small values. */
export function byteSize(bytes: bigint | number): string {
  const n = typeof bytes === 'bigint' ? Number(bytes) : bytes
  if (n < 1024) return `${n} B`
  let value = n
  let unit = 0
  while (value >= 1024 && unit < BYTE_UNITS.length - 1) {
    value /= 1024
    unit += 1
  }
  return `${value.toFixed(value >= 100 ? 0 : 1)} ${BYTE_UNITS[unit]}`
}

/** Lowercase hex of the given bytes. */
export function hex(bytes: Uint8Array): string {
  let out = ''
  for (const b of bytes) out += b.toString(16).padStart(2, '0')
  return out
}

/**
 * A classic hex dump (offset, 16 bytes, printable ASCII) of the given bytes,
 * one string per line. `baseOffset` labels a range read from the middle of a file.
 */
export function hexDump(bytes: Uint8Array, baseOffset = 0n): string[] {
  const lines: string[] = []
  for (let i = 0; i < bytes.length; i += 16) {
    const row = bytes.subarray(i, i + 16)
    const offset = (baseOffset + BigInt(i)).toString(16).padStart(8, '0')
    const cells = Array.from(row, (b) => b.toString(16).padStart(2, '0')).join(' ')
    const text = Array.from(row, (b) => (b >= 0x20 && b < 0x7f ? String.fromCharCode(b) : '.')).join('')
    lines.push(`${offset}  ${cells.padEnd(47, ' ')}  ${text}`)
  }
  return lines
}
