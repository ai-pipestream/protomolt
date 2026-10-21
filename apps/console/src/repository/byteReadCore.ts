/**
 * The one shape every streamed byte read takes on screen, whatever the
 * service: a raw file through ReadBlob or a rendition through ReadRendition.
 */
import type { BlobRange } from './blobReadCore'

export type ByteVerification =
  | { kind: 'verified'; sha256: string }
  | { kind: 'range-unverified'; wholeObjectSha256: string }

export interface ByteReadResult {
  receivedBytes: bigint
  verification: ByteVerification
  /** The media type the service reports for the bytes; empty when it reports none. */
  mediaType: string
}

/** Reads a range into `sink`; `stopAfter` keeps only that many bytes and ends the stream. */
export type ByteRead = (
  range: BlobRange,
  sink: (chunk: Uint8Array) => void | Promise<void>,
  signal: AbortSignal,
  onProgress: (received: bigint, expected: bigint) => void,
  stopAfter?: bigint,
) => Promise<ByteReadResult>

/** True for media types a preview shows as text. */
export function isTextMediaType(mediaType: string | null): boolean {
  if (mediaType === null) return false
  const type = mediaType.split(';')[0].trim().toLowerCase()
  return type.startsWith('text/') || type === 'application/json' || type === 'application/xml' || type.endsWith('+xml') || type.endsWith('+json')
}
