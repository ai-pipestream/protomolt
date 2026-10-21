/**
 * The read-only archive screens: archives, their stats, entries with a
 * classification filter, an entry version's manifest with its renditions,
 * version history, and rendition bytes streamed through ReadRendition.
 */
import { create } from '@bufbuild/protobuf'
import type { Client } from '@connectrpc/connect'
import { ClassificationStateSchema, type Classification } from '@/gen/ai/protomolt/proto/asset/v1/classification_pb'
import type { FormatFact } from '@/gen/ai/protomolt/proto/asset/v1/format_pb'
import {
  ReadRenditionRequestSchema,
  type ArchiveService,
  type RenditionReadHeader,
} from '@/gen/ai/protomolt/proto/repo/archive/v1/archive_service_pb'
import {
  EntryAddressSchema,
  RenditionStateSchema,
  type EntryAddress,
  type EntryInfo,
  type RenditionDescriptor,
  type VersionManifest,
} from '@/gen/ai/protomolt/proto/repo/archive/v1/entry_pb'
import { linkedController, type BlobRange } from './blobReadCore'
import { enumLabel, instant } from './formatCore'
import { Sha256 } from './sha256'

/** An entry address from route values; every part is required. */
export function entryAddress(accountId: string, archive: string, entryId: string): EntryAddress {
  for (const [label, value] of [
    ['Account ID', accountId],
    ['Archive', archive],
    ['Entry ID', entryId],
  ] as const) {
    if (value.trim() === '') throw new Error(`${label} is blank: an entry address needs the account, archive and entry IDs.`)
  }
  return create(EntryAddressSchema, { accountId, archive, entryId })
}

/** Parses a version from a route query; null means current (version 0 on the wire). */
export function parseVersion(raw: string | null | undefined): bigint | null {
  if (raw === null || raw === undefined || raw === '') return null
  let value: bigint
  try {
    value = BigInt(raw)
  } catch {
    throw new Error(`Version "${raw}" is not a whole number.`)
  }
  if (value <= 0n) throw new Error(`Version "${raw}" must be greater than zero.`)
  return value
}

function formatName(fact: FormatFact | undefined): string | null {
  return fact?.format.case ?? null
}

/** A classification as one line: its state and the declared and identified formats it records. */
export function classificationLabel(classification: Classification | undefined): string {
  if (classification === undefined) return 'not recorded'
  const parts = [enumLabel(ClassificationStateSchema, classification.state)]
  const declared = formatName(classification.declared)
  const identified = formatName(classification.identified)
  if (declared !== null) parts.push(`declared ${declared}`)
  if (identified !== null) parts.push(`identified ${identified}`)
  return parts.join(', ')
}

/** One row of the entries table. */
export interface EntryRow {
  entryId: string
  currentVersion: string
  title: string
  filename: string
  contentType: string
  updatedAt: string | null
  classification: string
}

export function entryRow(info: EntryInfo): EntryRow {
  if (info.address === undefined) throw new Error('ListEntries returned an entry without an address.')
  return {
    entryId: info.address.entryId,
    currentVersion: info.currentVersion.toString(),
    title: info.title,
    filename: info.filename,
    contentType: info.contentType,
    updatedAt: instant(info.updatedAt),
    classification: classificationLabel(info.classification),
  }
}

/** A rendition's name and sub-key as one label. */
export function renditionLabel(rendition: RenditionDescriptor | undefined): string {
  if (rendition === undefined) return '(no descriptor)'
  return rendition.subKey === '' ? rendition.name : `${rendition.name} / ${rendition.subKey}`
}

/** One rendition of a version's manifest. */
export interface RenditionRow {
  key: string
  label: string
  name: string
  subKey: string
  mediaType: string
  state: string
  sizeBytes: bigint
  sha256: string
  writtenAt: string | null
  writtenBy: string
  deletedReason: string
  readable: boolean
}

export function renditionRows(manifest: VersionManifest): RenditionRow[] {
  return manifest.renditions.map((entry) => {
    const state = enumLabel(RenditionStateSchema, entry.state)
    const by = entry.writtenBy
    return {
      key: `${entry.rendition?.name ?? ''}\u0000${entry.rendition?.subKey ?? ''}`,
      label: renditionLabel(entry.rendition),
      name: entry.rendition?.name ?? '',
      subKey: entry.rendition?.subKey ?? '',
      mediaType: entry.rendition?.mediaType ?? '',
      state,
      sizeBytes: entry.sizeBytes,
      sha256: entry.sha256,
      writtenAt: instant(entry.writtenAt),
      writtenBy: by === undefined ? '' : [by.module, by.actor].filter((x) => x !== '').join(' by '),
      deletedReason: entry.deletedReason,
      readable: state === 'PRESENT',
    }
  })
}

/** One row of the version history. */
export interface VersionRow {
  version: bigint
  createdAt: string | null
  totalBytes: bigint
  renditions: number
  rootChecksum: string
}

export function versionRow(manifest: VersionManifest): VersionRow {
  return {
    version: manifest.version,
    createdAt: instant(manifest.createdAt),
    totalBytes: manifest.totalBytes,
    renditions: manifest.renditions.length,
    rootChecksum: manifest.rootChecksum,
  }
}

/** The rendition read stream broke its contract. */
export class RenditionStreamError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'RenditionStreamError'
  }
}

export interface RenditionReadSpec {
  address: EntryAddress
  /** Null reads the current version. */
  version: bigint | null
  name: string
  subKey: string
  range: BlobRange
}

export type RenditionVerification =
  | { kind: 'verified'; sha256: string }
  | { kind: 'range-unverified'; wholeObjectSha256: string }

export interface RenditionReadResult {
  header: RenditionReadHeader
  receivedBytes: bigint
  verification: RenditionVerification
}

/**
 * Streams one rendition to `sink`, frame by frame; the next frame is not read
 * until the sink has taken this one. The header names the range the server
 * serves. A whole-object read is hashed as it arrives and must match the
 * header's SHA-256; a range read is reported unverified. With `stopAfter`, the
 * read keeps that many bytes and then ends the stream itself.
 */
export async function readRendition(
  client: Client<typeof ArchiveService>,
  spec: RenditionReadSpec,
  sink: (chunk: Uint8Array) => void | Promise<void>,
  signal: AbortSignal,
  onProgress?: (received: bigint, expected: bigint) => void,
  stopAfter?: bigint,
): Promise<RenditionReadResult> {
  const request = create(ReadRenditionRequestSchema, {
    address: spec.address,
    version: spec.version ?? 0n,
    rendition: spec.name,
    subKey: spec.subKey,
    offset: spec.range.offset,
    length: spec.range.length,
  })
  const call = linkedController(signal)
  let header: RenditionReadHeader | null = null
  let hash: Sha256 | null = null
  let expected = 0n
  let received = 0n
  let stopped = false
  try {
    for await (const frame of client.readRendition(request, { signal: call.controller.signal })) {
      switch (frame.frame.case) {
        case 'header': {
          if (header !== null) throw new RenditionStreamError('ReadRendition sent a second header frame.')
          const h = frame.frame.value
          if (spec.version !== null && h.version !== spec.version) {
            throw new RenditionStreamError(`ReadRendition answered for version ${h.version}; version ${spec.version} was requested.`)
          }
          if (h.offset !== spec.range.offset) {
            throw new RenditionStreamError(`ReadRendition serves bytes from ${h.offset}; the request starts at ${spec.range.offset}.`)
          }
          if (spec.range.length > 0n && h.length !== spec.range.length) {
            throw new RenditionStreamError(`ReadRendition serves ${h.length} bytes; the request names ${spec.range.length}.`)
          }
          if (h.offset + h.length > h.sizeBytes) {
            throw new RenditionStreamError(`ReadRendition serves [${h.offset}, +${h.length}) of a ${h.sizeBytes}-byte object.`)
          }
          header = h
          expected = stopAfter !== undefined && stopAfter < h.length ? stopAfter : h.length
          hash = h.wholeObject && stopAfter === undefined ? new Sha256() : null
          break
        }
        case 'data': {
          if (header === null) throw new RenditionStreamError('ReadRendition sent data before its header frame.')
          let chunk = frame.frame.value
          if (stopAfter !== undefined && received + BigInt(chunk.length) > expected) {
            chunk = chunk.subarray(0, Number(expected - received))
            stopped = true
          }
          received += BigInt(chunk.length)
          if (received > expected) {
            throw new RenditionStreamError(`ReadRendition sent ${received} bytes; the header declares ${expected}.`)
          }
          hash?.update(chunk)
          await sink(chunk)
          onProgress?.(received, expected)
          break
        }
        case undefined:
          throw new RenditionStreamError('ReadRendition sent a frame with neither a header nor data.')
      }
      if (stopped || (stopAfter !== undefined && header !== null && received === expected)) break
    }
  } finally {
    call.release()
  }
  if (header === null) throw new RenditionStreamError('ReadRendition ended without a header frame.')
  if (received !== expected) throw new RenditionStreamError(`ReadRendition ended after ${received} of ${expected} bytes.`)
  if (hash === null) {
    return { header, receivedBytes: received, verification: { kind: 'range-unverified', wholeObjectSha256: header.sha256 } }
  }
  const actual = hash.hex()
  if (actual !== header.sha256) {
    throw new RenditionStreamError(`The received bytes hash to ${actual}, but the manifest attests ${header.sha256}.`)
  }
  return { header, receivedBytes: received, verification: { kind: 'verified', sha256: actual } }
}
