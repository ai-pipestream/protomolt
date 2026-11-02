/**
 * The one shape every streamed byte read takes on screen, whatever the
 * service: a raw file through ReadBlob or a rendition through ReadRendition.
 */
import type { BlobRange } from './blobReadCore'
import { hexDump } from './formatCore'

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

export type PreviewMode = 'json' | 'text' | 'image' | 'audio' | 'hex'

export interface PreparedBytePreview {
  modes: PreviewMode[]
  defaultMode: PreviewMode
  rawText: string | null
  formattedJson: string | null
  hex: string[]
  safeMediaType: string | null
}

const SAFE_IMAGE_MEDIA_TYPES = new Set([
  'image/png',
  'image/jpeg',
  'image/webp',
  'image/gif',
])

const SAFE_AUDIO_MEDIA_TYPES = new Set([
  'audio/mpeg',
  'audio/wav',
  'audio/ogg',
  'audio/webm',
  'audio/flac',
])

export function normalizeMediaType(mediaType: string | null): string {
  if (mediaType === null) return ''
  return mediaType.split(';')[0].trim().toLowerCase()
}

/** True for media types a preview shows as text. */
export function isTextMediaType(mediaType: string | null): boolean {
  const type = normalizeMediaType(mediaType)
  if (!type) return false
  return type.startsWith('text/') || type === 'application/json' || type === 'application/xml' || type.endsWith('+xml') || type.endsWith('+json')
}

export function isJsonMediaType(mediaType: string | null): boolean {
  const type = normalizeMediaType(mediaType)
  return type === 'application/json' || type.endsWith('+json')
}

export function isSafeImageMediaType(mediaType: string | null): boolean {
  return SAFE_IMAGE_MEDIA_TYPES.has(normalizeMediaType(mediaType))
}

export function isSafeAudioMediaType(mediaType: string | null): boolean {
  return SAFE_AUDIO_MEDIA_TYPES.has(normalizeMediaType(mediaType))
}

export function tryFormatJson(rawText: string): string | null {
  try {
    return JSON.stringify(JSON.parse(rawText), null, 2)
  } catch {
    return null
  }
}

/**
 * Prepares the available preview representations for a byte slice.
 * Safe raster image and audio previews are only enabled when the full object's SHA-256 was verified.
 */
export function prepareBytePreview(
  bytes: Uint8Array,
  offset: bigint,
  mediaType: string | null,
  verifiedWholeObject: boolean,
): PreparedBytePreview {
  const hex = hexDump(bytes, offset)
  const normalized = normalizeMediaType(mediaType)
  if (verifiedWholeObject && isSafeImageMediaType(normalized)) {
    return {
      modes: ['image', 'hex'],
      defaultMode: 'image',
      rawText: null,
      formattedJson: null,
      hex,
      safeMediaType: normalized,
    }
  }
  if (verifiedWholeObject && isSafeAudioMediaType(normalized)) {
    return {
      modes: ['audio', 'hex'],
      defaultMode: 'audio',
      rawText: null,
      formattedJson: null,
      hex,
      safeMediaType: normalized,
    }
  }
  if (isTextMediaType(normalized)) {
    const rawText = new TextDecoder('utf-8', { fatal: false }).decode(bytes)
    const formattedJson = isJsonMediaType(normalized) ? tryFormatJson(rawText) : null
    if (formattedJson !== null) {
      return {
        modes: ['json', 'text', 'hex'],
        defaultMode: 'json',
        rawText,
        formattedJson,
        hex,
        safeMediaType: null,
      }
    }
    return {
      modes: ['text', 'hex'],
      defaultMode: 'text',
      rawText,
      formattedJson: null,
      hex,
      safeMediaType: null,
    }
  }
  return {
    modes: ['hex'],
    defaultMode: 'hex',
    rawText: null,
    formattedJson: null,
    hex,
    safeMediaType: null,
  }
}
