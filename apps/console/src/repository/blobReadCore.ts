/**
 * Raw file reads over the server-streamed ReadBlob: one header frame, then data
 * frames. Each frame goes to the caller's sink as it arrives, and the next frame
 * is not read until the sink has taken it, so memory use is whatever the sink
 * keeps. A full read is hashed incrementally and checked
 * against the header's SHA-256; a byte range cannot be checked against a
 * whole-object digest, and the result says so instead of claiming a match.
 */
import { create } from '@bufbuild/protobuf'
import type { Client } from '@connectrpc/connect'
import type { NodeAddress } from '@/gen/ai/protomolt/proto/repo/v1/address_pb'
import {
  ReadBlobRequestSchema,
  type BlobHeader,
  type DocumentService,
} from '@/gen/ai/protomolt/proto/repo/v2/document_service_pb'
import { revisionSelector } from './documentCore'
import { Sha256 } from './sha256'

/** What to read. `length` 0 means to the end of the object. */
export interface BlobRange {
  offset: bigint
  length: bigint
}

export interface BlobReadSpec {
  address: NodeAddress
  /** Null reads the current revision. */
  revision: bigint | null
  blobId: string
  range: BlobRange
}

/** How the received bytes were checked. */
export type BlobVerification =
  | { kind: 'verified'; sha256: string }
  | { kind: 'range-unverified'; wholeObjectSha256: string }

export interface BlobReadResult {
  header: BlobHeader
  receivedBytes: bigint
  verification: BlobVerification
}

/** The stream broke the ReadBlob contract or did not deliver what the header promised. */
export class BlobStreamError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'BlobStreamError'
  }
}

/** A range a user typed; refused by name when it is not a non-negative whole number. */
export function parseRange(offsetText: string, lengthText: string): BlobRange {
  const parse = (label: string, raw: string): bigint => {
    const text = raw.trim()
    if (text === '') return 0n
    let value: bigint
    try {
      value = BigInt(text)
    } catch {
      throw new Error(`${label} "${raw}" is not a whole number of bytes.`)
    }
    if (value < 0n) throw new Error(`${label} must not be negative.`)
    return value
  }
  return { offset: parse('Offset', offsetText), length: parse('Length', lengthText) }
}

export function isFullRead(range: BlobRange): boolean {
  return range.offset === 0n && range.length === 0n
}

/**
 * Reads the blob, handing each data frame to `sink` and reporting progress
 * after each. The signal ends the stream at the server when aborted.
 */
export async function readBlob(
  client: Client<typeof DocumentService>,
  spec: BlobReadSpec,
  sink: (chunk: Uint8Array) => void | Promise<void>,
  signal: AbortSignal,
  onProgress?: (received: bigint, expected: bigint) => void,
): Promise<BlobReadResult> {
  const request = create(ReadBlobRequestSchema, {
    address: spec.address,
    revision: revisionSelector(spec.revision),
    blobId: spec.blobId,
    offset: spec.range.offset,
    length: spec.range.length,
  })
  const full = isFullRead(spec.range)
  const hash = full ? new Sha256() : null
  let header: BlobHeader | null = null
  let expected = 0n
  let received = 0n
  for await (const frame of client.readBlob(request, { signal })) {
    switch (frame.frame.case) {
      case 'header':
        if (header !== null) throw new BlobStreamError('ReadBlob sent a second header frame.')
        header = frame.frame.value
        if (spec.range.offset > header.totalLength) {
          throw new BlobStreamError(
            `Offset ${spec.range.offset} is past the end of the ${header.totalLength}-byte file.`,
          )
        }
        expected = header.totalLength - spec.range.offset
        if (spec.range.length > 0n && spec.range.length < expected) expected = spec.range.length
        break
      case 'data': {
        if (header === null) throw new BlobStreamError('ReadBlob sent data before its header frame.')
        const chunk = frame.frame.value
        received += BigInt(chunk.length)
        if (received > expected) {
          throw new BlobStreamError(`ReadBlob sent ${received} bytes; the requested range holds ${expected}.`)
        }
        hash?.update(chunk)
        await sink(chunk)
        onProgress?.(received, expected)
        break
      }
      case undefined:
        throw new BlobStreamError('ReadBlob sent a frame with neither a header nor data.')
    }
  }
  if (header === null) throw new BlobStreamError('ReadBlob ended without a header frame.')
  if (received !== expected) {
    throw new BlobStreamError(`ReadBlob ended after ${received} of ${expected} bytes.`)
  }
  if (hash === null) {
    return { header, receivedBytes: received, verification: { kind: 'range-unverified', wholeObjectSha256: header.sha256 } }
  }
  const actual = hash.hex()
  if (actual !== header.sha256) {
    throw new BlobStreamError(`The received bytes hash to ${actual}, but the header declares ${header.sha256}.`)
  }
  return { header, receivedBytes: received, verification: { kind: 'verified', sha256: actual } }
}

/** The bytes a preview keeps: the first 64 KiB of the file. */
export const PREVIEW_BYTES = 64n * 1024n

/** Collects frames into one buffer, for previews of bounded size. */
export function collector(): { sink: (chunk: Uint8Array) => void; bytes: () => Uint8Array } {
  const chunks: Uint8Array[] = []
  let size = 0
  return {
    sink(chunk) {
      chunks.push(chunk)
      size += chunk.length
    },
    bytes() {
      const out = new Uint8Array(size)
      let at = 0
      for (const c of chunks) {
        out.set(c, at)
        at += c.length
      }
      return out
    },
  }
}
