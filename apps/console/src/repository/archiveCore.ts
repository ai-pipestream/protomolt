/**
 * The read-only archive screens: archives, their stats, entries with a
 * classification filter, an entry version's manifest with its renditions,
 * version history, and rendition bytes streamed through ReadRendition.
 */
import { create, fromBinary } from '@bufbuild/protobuf'
import { Code, type Client } from '@connectrpc/connect'
import { BridgeKind, BridgeKindSchema, BridgeStatus, BridgeStatusSchema } from '@/gen/ai/protomolt/proto/asset/v1/bridge_pb'
import { ClassificationStateSchema, type Classification } from '@/gen/ai/protomolt/proto/asset/v1/classification_pb'
import { ContentClass, ContentClassSchema, type ContentProfile } from '@/gen/ai/protomolt/proto/asset/v1/content_pb'
import {
  AvroDatasetSchema,
  DelimitedTableSchema,
  FormatFactSchema,
  GzipFileSchema,
  HeaderPresence,
  HtmlDocumentSchema,
  JsonDocumentSchema,
  MarkdownDocumentSchema,
  NdjsonDatasetSchema,
  ParquetDatasetSchema,
  PdfDocumentSchema,
  PlainTextSchema,
  PresentationDocumentSchema,
  RasterImageSchema,
  SpreadsheetDocumentSchema,
  TarArchiveSchema,
  WordDocumentSchema,
  XmlDocumentSchema,
  YamlDocumentSchema,
  ZipArchiveSchema,
  type FormatFact,
} from '@/gen/ai/protomolt/proto/asset/v1/format_pb'
import { ArchiveSchema, VersioningPolicy } from '@/gen/ai/protomolt/proto/repo/archive/v1/archive_pb'
import {
  BridgeEntryRequestSchema,
  ClassifyEntryRequestSchema,
  CreateArchiveRequestSchema,
  PutEntryRequestSchema,
  ReadRenditionRequestSchema,
  UploadRenditionResponseSchema,
  type ArchiveService,
  type BridgeEntryRequest,
  type BridgeEntryResponse,
  type ClassifyEntryRequest,
  type CreateArchiveRequest,
  type PutEntryRequest,
  type PutEntryResponse,
  type RenditionReadHeader,
  type UploadRenditionResponse,
} from '@/gen/ai/protomolt/proto/repo/archive/v1/archive_service_pb'
import {
  EntryAddressSchema,
  RenditionContentSchema,
  RenditionDescriptorSchema,
  RenditionStateSchema,
  WriteAttributionSchema,
  type EntryAddress,
  type EntryInfo,
  type RenditionDescriptor,
  type VersionManifest,
} from '@/gen/ai/protomolt/proto/repo/archive/v1/entry_pb'
import { NodeAddressSchema, type NodeAddress } from '@/gen/ai/protomolt/proto/repo/v1/address_pb'
import { DriveStatusSchema, DriveTypeSchema, type Drive } from '@/gen/ai/protomolt/proto/repo/v1/drive_pb'
import { ItemErrorSchema } from '@/gen/ai/protomolt/proto/repo/v2/errors_pb'
import { linkedController, type BlobRange } from './blobReadCore'
import type { CallNumber } from './documentCore'
import { enumLabel, instant } from './formatCore'
import { codeName, type RpcFailure } from './rpcErrorCore'
import { Sha256 } from './sha256'
import { UploadInputError, type UploadRequest } from './uploadCore'

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

/** Formats a rendition's ContentProfile (structural class and quality score/dimensions). */
export function contentProfileLabel(profile: ContentProfile | undefined): string | null {
  if (profile === undefined || profile.contentClass === ContentClass.UNSPECIFIED) return null
  const cls = enumLabel(ContentClassSchema, profile.contentClass)
  if (profile.quality === undefined) return cls
  const score = profile.quality.score.toFixed(2)
  if (profile.quality.dimensions.length === 0) return `${cls} · quality ${score}`
  const dims = profile.quality.dimensions.map((d) => `${d.name}: ${d.score.toFixed(2)}`).join(', ')
  return `${cls} · quality ${score} (${dims})`
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
  contentProfile: string | null
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
      contentProfile: contentProfileLabel(entry.contentProfile),
      readable: state === 'PRESENT',
    }
  })
}

export type DeclaredFormatCase =
  | ''
  | 'pdf'
  | 'json'
  | 'markdown'
  | 'text'
  | 'html'
  | 'xml'
  | 'yaml'
  | 'ndjson'
  | 'delimited'
  | 'parquet'
  | 'avro'
  | 'word'
  | 'spreadsheet'
  | 'presentation'
  | 'zip'
  | 'tar'
  | 'gzip'
  | 'image'

export const DECLARED_FORMAT_ITEMS: ReadonlyArray<{ title: string; value: DeclaredFormatCase }> = [
  { title: 'Characterize bytes only (no new declaration)', value: '' },
  { title: 'PDF document (.pdf)', value: 'pdf' },
  { title: 'JSON document (.json)', value: 'json' },
  { title: 'Markdown document (.md)', value: 'markdown' },
  { title: 'Plain text (.txt / .log)', value: 'text' },
  { title: 'HTML document (.html)', value: 'html' },
  { title: 'XML document (.xml)', value: 'xml' },
  { title: 'YAML document (.yaml / .yml)', value: 'yaml' },
  { title: 'NDJSON dataset (.ndjson / .jsonl)', value: 'ndjson' },
  { title: 'Delimited table (.csv / .tsv / .psv)', value: 'delimited' },
  { title: 'Parquet dataset (.parquet)', value: 'parquet' },
  { title: 'Avro dataset (.avro)', value: 'avro' },
  { title: 'Word document (.docx / .doc)', value: 'word' },
  { title: 'Spreadsheet (.xlsx / .xls / .ods)', value: 'spreadsheet' },
  { title: 'Presentation (.pptx / .ppt)', value: 'presentation' },
  { title: 'ZIP archive (.zip)', value: 'zip' },
  { title: 'TAR archive (.tar / .tar.gz / .tgz)', value: 'tar' },
  { title: 'GZIP file (.gz)', value: 'gzip' },
  { title: 'Raster image (.png / .jpg / .webp / .gif)', value: 'image' },
]

export interface ClassifyEntryForm {
  formatCase: DeclaredFormatCase
  filename: string
  delimiter: string
  header: HeaderPresence
  mediaType: string
  module: string
}

export function buildDeclaredFormatFact(form: ClassifyEntryForm): FormatFact | undefined {
  if (form.formatCase === '') return undefined
  const filename = form.filename.trim()
  if (filename === '') throw new UploadInputError('Filename', 'is required when declaring a format')
  switch (form.formatCase) {
    case 'pdf':
      return create(FormatFactSchema, { format: { case: 'pdf', value: create(PdfDocumentSchema, { filename }) } })
    case 'json':
      return create(FormatFactSchema, { format: { case: 'json', value: create(JsonDocumentSchema, { filename }) } })
    case 'markdown':
      return create(FormatFactSchema, { format: { case: 'markdown', value: create(MarkdownDocumentSchema, { filename }) } })
    case 'text':
      return create(FormatFactSchema, { format: { case: 'text', value: create(PlainTextSchema, { filename }) } })
    case 'html':
      return create(FormatFactSchema, { format: { case: 'html', value: create(HtmlDocumentSchema, { filename }) } })
    case 'xml':
      return create(FormatFactSchema, { format: { case: 'xml', value: create(XmlDocumentSchema, { filename }) } })
    case 'yaml':
      return create(FormatFactSchema, { format: { case: 'yaml', value: create(YamlDocumentSchema, { filename }) } })
    case 'ndjson':
      return create(FormatFactSchema, { format: { case: 'ndjson', value: create(NdjsonDatasetSchema, { filename }) } })
    case 'delimited': {
      const delimiter = form.delimiter === '\\t' ? '\t' : form.delimiter
      if (delimiter.length !== 1) throw new UploadInputError('Delimiter', 'must be a single character (or \\t)')
      if (form.header === HeaderPresence.UNSPECIFIED) {
        throw new UploadInputError('Header presence', 'must state PRESENT or ABSENT')
      }
      return create(FormatFactSchema, {
        format: {
          case: 'delimited',
          value: create(DelimitedTableSchema, { filename, delimiter, header: form.header }),
        },
      })
    }
    case 'parquet':
      return create(FormatFactSchema, { format: { case: 'parquet', value: create(ParquetDatasetSchema, { filename }) } })
    case 'avro':
      return create(FormatFactSchema, { format: { case: 'avro', value: create(AvroDatasetSchema, { filename }) } })
    case 'word':
      return create(FormatFactSchema, { format: { case: 'word', value: create(WordDocumentSchema, { filename }) } })
    case 'spreadsheet':
      return create(FormatFactSchema, { format: { case: 'spreadsheet', value: create(SpreadsheetDocumentSchema, { filename }) } })
    case 'presentation':
      return create(FormatFactSchema, { format: { case: 'presentation', value: create(PresentationDocumentSchema, { filename }) } })
    case 'zip':
      return create(FormatFactSchema, { format: { case: 'zip', value: create(ZipArchiveSchema, { filename }) } })
    case 'tar':
      return create(FormatFactSchema, { format: { case: 'tar', value: create(TarArchiveSchema, { filename }) } })
    case 'gzip':
      return create(FormatFactSchema, { format: { case: 'gzip', value: create(GzipFileSchema, { filename }) } })
    case 'image':
      return create(FormatFactSchema, {
        format: {
          case: 'image',
          value: create(RasterImageSchema, { filename, mediaType: form.mediaType.trim() }),
        },
      })
  }
}

export function buildClassifyEntryRequest(address: EntryAddress, form: ClassifyEntryForm): ClassifyEntryRequest {
  const declared = buildDeclaredFormatFact(form)
  const mod = form.module.trim()
  return create(ClassifyEntryRequestSchema, {
    address,
    declared,
    classifiedBy: mod !== '' ? create(WriteAttributionSchema, { module: mod }) : undefined,
  })
}

export function classificationEvidenceRows(
  classification: Classification | undefined,
): Array<{ signal: string; observation: string }> {
  if (!classification) return []
  return classification.evidence.map((e) => ({ signal: e.signal, observation: e.observation }))
}

export const BRIDGE_KIND_ITEMS: ReadonlyArray<{ title: string; value: BridgeKind }> = BridgeKindSchema.values
  .filter((v) => v.number !== BridgeKind.UNSPECIFIED)
  .map((v) => ({
    title: enumLabel(BridgeKindSchema, v.number),
    value: v.number as BridgeKind,
  }))

export function buildBridgeEntryRequest(
  address: EntryAddress,
  bridges: BridgeKind[],
  module: string,
): BridgeEntryRequest {
  const mod = module.trim()
  return create(BridgeEntryRequestSchema, {
    address,
    bridges,
    bridgedBy: mod !== '' ? create(WriteAttributionSchema, { module: mod }) : undefined,
  })
}

export interface BridgeOutcomeRow {
  bridge: string
  rendition: string
  status: string
  detail: string
  inspectable: boolean
}

export function bridgeOutcomeRows(response: BridgeEntryResponse): BridgeOutcomeRow[] {
  return response.outcomes.map((o) => ({
    bridge: enumLabel(BridgeKindSchema, o.bridge),
    rendition: o.rendition,
    status: enumLabel(BridgeStatusSchema, o.status),
    detail: o.detail,
    inspectable:
      o.rendition.trim() !== '' && (o.status === BridgeStatus.PRODUCED || o.status === BridgeStatus.UNCHANGED),
  }))
}

export interface CreateArchiveForm {
  accountId: string
  name: string
  driveName: string
  versioning: VersioningPolicy
  description: string
}

export function buildCreateArchiveRequest(form: CreateArchiveForm): CreateArchiveRequest {
  const accountId = form.accountId.trim()
  if (accountId === '') throw new UploadInputError('Account ID', 'is blank')
  const name = form.name.trim()
  if (name === '') throw new UploadInputError('Archive name', 'is blank')
  const driveName = form.driveName.trim()
  if (driveName === '') throw new UploadInputError('Drive name', 'is blank')
  const versioning = form.versioning === VersioningPolicy.UNSPECIFIED ? VersioningPolicy.RETAINED : form.versioning
  return create(CreateArchiveRequestSchema, {
    archive: create(ArchiveSchema, {
      accountId,
      name,
      driveName,
      versioning,
      description: form.description.trim(),
    }),
  })
}

export function suggestEntryIdFromFilename(filename: string): string {
  const base = filename.split(/[/\\]/).pop()?.trim() ?? ''
  if (base === '') return ''
  const dot = base.lastIndexOf('.')
  const stem = dot > 0 ? base.slice(0, dot) : base
  return stem
    .toLowerCase()
    .replace(/[^a-z0-9._-]+/g, '-')
    .replace(/^-+|-+$/g, '')
}

export type RenditionChange = 'added' | 'removed' | 'changed' | 'unchanged'

const RENDITION_DIFF_FIELDS = [
  'state',
  'mediaType',
  'sizeBytes',
  'sha256',
  'writtenBy',
  'contentProfile',
  'deletedReason',
] as const

export type RenditionDiffField = (typeof RENDITION_DIFF_FIELDS)[number]

export interface RenditionDiffRow {
  key: string
  label: string
  change: RenditionChange
  base: RenditionRow | null
  target: RenditionRow | null
  differs: RenditionDiffField[]
}

/**
 * Compares the renditions of two VersionManifests matched by rendition key (name + subKey).
 */
export function renditionDiff(base: VersionManifest, target: VersionManifest): RenditionDiffRow[] {
  const before = new Map(renditionRows(base).map((r) => [r.key, r]))
  const after = renditionRows(target)
  const out: RenditionDiffRow[] = []
  for (const row of after) {
    const old = before.get(row.key) ?? null
    before.delete(row.key)
    if (old === null) {
      out.push({ key: row.key, label: row.label, change: 'added', base: null, target: row, differs: [] })
      continue
    }
    const differs = RENDITION_DIFF_FIELDS.filter((f) => old[f] !== row[f])
    out.push({
      key: row.key,
      label: row.label,
      change: differs.length === 0 ? 'unchanged' : 'changed',
      base: old,
      target: row,
      differs,
    })
  }
  for (const row of before.values()) {
    out.push({ key: row.key, label: row.label, change: 'removed', base: row, target: null, differs: [] })
  }
  return out
}

export const ARCHIVE_UPLOAD_PATH = '/v1/archive:upload'

export interface RenditionUploadForm {
  accountId: string
  archive: string
  entryId: string
  rendition: string
  subKey: string
  contentType: string
  module: string
  expectedVersion: string
}

export interface RenditionUploadPlan {
  operationId: string
  url: string
  contentType: string
  file: Blob
  sizeBytes: number
  sha256: string
}

export type RenditionUploadResult =
  | { kind: 'saved'; response: UploadRenditionResponse }
  | { kind: 'failed'; httpStatus: number | null; failure: RpcFailure }

export function planRenditionUpload(
  form: RenditionUploadForm,
  file: File,
  operationId: string,
  sha256: string,
): RenditionUploadPlan {
  const addr = entryAddress(form.accountId, form.archive, form.entryId)
  const rendition = form.rendition.trim()
  if (rendition === '') throw new UploadInputError('Rendition', 'is blank')
  const params = new URLSearchParams({
    account_id: addr.accountId,
    archive: addr.archive,
    entry_id: addr.entryId,
    rendition,
    operation_id: operationId,
    expected_sha256: sha256,
  })
  if (form.subKey.trim() !== '') params.set('sub_key', form.subKey.trim())
  if (form.module.trim() !== '') params.set('module', form.module.trim())
  const contentType = form.contentType.trim() || file.type || 'application/octet-stream'
  params.set('content_type', contentType)
  const expectedRaw = form.expectedVersion.trim()
  if (expectedRaw !== '') {
    let ver: bigint
    try {
      ver = BigInt(expectedRaw)
    } catch {
      throw new UploadInputError('Expected version', `"${expectedRaw}" is not a whole number`)
    }
    if (ver < 0n) throw new UploadInputError('Expected version', 'must not be negative')
    params.set('expected_version', ver.toString())
  }
  return {
    operationId,
    url: `${ARCHIVE_UPLOAD_PATH}?${params.toString()}`,
    contentType,
    file,
    sizeBytes: file.size,
    sha256,
  }
}

export function sendRenditionUpload(
  plan: RenditionUploadPlan,
  newRequest: () => UploadRequest,
  onProgress: (sent: number, total: number) => void,
  signal: AbortSignal,
): Promise<RenditionUploadResult> {
  return new Promise((resolve) => {
    const request = newRequest()
    request.open('POST', plan.url)
    request.withCredentials = true
    request.responseType = 'arraybuffer'
    request.setRequestHeader('Accept', 'application/protobuf')
    request.setRequestHeader('Content-Type', plan.contentType)
    request.setRequestHeader('X-Content-Sha256', plan.sha256)
    request.upload.onprogress = (event) => onProgress(event.loaded, plan.sizeBytes)
    const onAbort = () => request.abort()
    signal.addEventListener('abort', onAbort, { once: true })
    const done = (result: RenditionUploadResult) => {
      signal.removeEventListener('abort', onAbort)
      resolve(result)
    }
    request.onload = () => {
      const type = request.getResponseHeader('content-type') ?? ''
      if (request.status === 200 && type.startsWith('application/proto')) {
        try {
          const response = fromBinary(UploadRenditionResponseSchema, new Uint8Array(request.response as ArrayBuffer))
          done({ kind: 'saved', response })
          return
        } catch (error) {
          done({
            kind: 'failed',
            httpStatus: 200,
            failure: { code: 'INTERNAL', codeNumber: Code.Internal, message: (error as Error).message },
          })
          return
        }
      }
      if (type.startsWith('application/proto')) {
        try {
          const err = fromBinary(ItemErrorSchema, new Uint8Array(request.response as ArrayBuffer))
          done({
            kind: 'failed',
            httpStatus: request.status,
            failure: {
              code: err.code >= 1 && err.code <= 16 ? codeName(err.code as Code) : `HTTP ${request.status}`,
              codeNumber: err.code || null,
              message: err.message || `Archive upload failed with HTTP ${request.status}`,
            },
          })
          return
        } catch {
          // Fall through to generic HTTP status reporting
        }
      }
      done({
        kind: 'failed',
        httpStatus: request.status,
        failure: {
          code: `HTTP ${request.status}`,
          codeNumber: null,
          message: `The archive upload route answered HTTP ${request.status} (${type || 'no content type'}).`,
        },
      })
    }
    request.onerror = () =>
      done({
        kind: 'failed',
        httpStatus: null,
        failure: { code: 'UNAVAILABLE', codeNumber: Code.Unavailable, message: 'The connection failed before the upload route answered.' },
      })
    request.onabort = () =>
      done({
        kind: 'failed',
        httpStatus: null,
        failure: { code: 'CANCELED', codeNumber: Code.Canceled, message: 'The rendition upload was cancelled.' },
      })
    request.send(plan.file)
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

const PATH_SAFE_SLUG = /^[A-Za-z0-9._-]+$/

/** One inline rendition draft in the PutEntry workbench (#694). */
export interface InlineRenditionDraft {
  name: string
  subKey: string
  mediaType: string
  schemaSubject: string
  text: string
}

/** Form state for atomic PutEntry metadata + inline renditions commit (#694). */
export interface PutEntryForm {
  title: string
  filename: string
  contentType: string
  sourceUri: string
  expectedVersion: bigint
  producingModule: string
  metadata: { key: string; value: string }[]
  renditions: InlineRenditionDraft[]
}

/** Builds a validated PutEntryRequest from the console workbench form (#694). */
export function buildPutEntryRequest(address: EntryAddress, form: PutEntryForm, operationId = ''): PutEntryRequest {
  if (form.expectedVersion < 0n) {
    throw new UploadInputError('Expected version', 'must be zero or a positive version number')
  }
  const metadata: Record<string, string> = {}
  for (let i = 0; i < form.metadata.length; i++) {
    const rawKey = form.metadata[i].key
    const rawVal = form.metadata[i].value
    if (rawKey.trim() === '' && rawVal.trim() === '') continue
    const key = rawKey.trim()
    if (key === '') throw new UploadInputError(`Metadata key ${i + 1}`, 'is blank')
    if (key.startsWith('archive.')) {
      throw new UploadInputError(`Metadata key "${key}"`, 'keys starting with "archive." are reserved by the archive service')
    }
    if (Object.prototype.hasOwnProperty.call(metadata, key)) {
      throw new UploadInputError(`Metadata key "${key}"`, 'is listed more than once')
    }
    metadata[key] = rawVal
  }
  const encoder = new TextEncoder()
  const seenSlots = new Set<string>()
  const renditions = form.renditions.map((draft, idx) => {
    const name = draft.name.trim()
    if (name === '') throw new UploadInputError(`Rendition ${idx + 1} name`, 'is blank')
    if (!PATH_SAFE_SLUG.test(name)) {
      throw new UploadInputError(`Rendition "${name}"`, 'must contain only letters, digits, dots, underscores, or hyphens')
    }
    const subKey = draft.subKey.trim()
    if (subKey !== '' && !PATH_SAFE_SLUG.test(subKey)) {
      throw new UploadInputError(`Rendition "${name}" sub-key`, 'must contain only letters, digits, dots, underscores, or hyphens')
    }
    if (name === 'parsed' && subKey !== '') {
      throw new UploadInputError('Rendition "parsed"', 'cannot carry a sub-key')
    }
    const slot = `${name}\u0000${subKey}`
    if (seenSlots.has(slot)) {
      throw new UploadInputError(`Rendition "${subKey === '' ? name : `${name}/${subKey}`}"`, 'is listed more than once')
    }
    seenSlots.add(slot)
    const mediaType = draft.mediaType.trim() || 'text/plain; charset=utf-8'
    return create(RenditionContentSchema, {
      rendition: create(RenditionDescriptorSchema, {
        name,
        subKey,
        mediaType,
        schemaSubject: draft.schemaSubject.trim(),
      }),
      data: encoder.encode(draft.text),
    })
  })
  const module = form.producingModule.trim()
  return create(PutEntryRequestSchema, {
    address,
    title: form.title.trim(),
    filename: form.filename.trim(),
    contentType: form.contentType.trim(),
    sourceUri: form.sourceUri.trim(),
    expectedVersion: form.expectedVersion,
    writtenBy: module === '' ? undefined : create(WriteAttributionSchema, { module }),
    operationId: operationId.trim(),
    metadata,
    renditions,
  })
}

export interface PutEntryOutcomeSummary {
  version: bigint
  rootChecksum: string
  totalBytes: bigint
  deduplicated: boolean
  replayed: boolean
  operationId: string
  statusLabel: 'CREATED' | 'DEDUPLICATED' | 'REPLAYED'
  headline: string
}

/** Summarizes a PutEntryResponse for operator display (#694). */
export function describePutEntryOutcome(response: PutEntryResponse): PutEntryOutcomeSummary {
  const statusLabel: PutEntryOutcomeSummary['statusLabel'] = response.replayed
    ? 'REPLAYED'
    : response.deduplicated
      ? 'DEDUPLICATED'
      : 'CREATED'
  const headline = response.replayed
    ? `Replayed existing outcome at version ${response.version} (no new write).`
    : response.deduplicated
      ? `Unchanged: root checksum matched version ${response.version} (deduplicated).`
      : `Committed new version ${response.version}.`
  return {
    version: response.version,
    rootChecksum: response.rootChecksum,
    totalBytes: response.totalBytes,
    deduplicated: response.deduplicated,
    replayed: response.replayed,
    operationId: response.operationId,
    statusLabel,
    headline,
  }
}

/** Maps an archive EntryAddress to its underlying v2 DocumentService CallNumber (#696). */
export function entryCallNumber(address: EntryAddress): CallNumber {
  return {
    accountId: address.accountId,
    graphId: `archive:${address.accountId}`,
    graphAddressId: address.archive,
    docId: address.entryId,
  }
}

/** Maps an archive EntryAddress to its underlying v2 DocumentService NodeAddress (#696). */
export function entryNodeAddress(address: EntryAddress): NodeAddress {
  const call = entryCallNumber(address)
  return create(NodeAddressSchema, call)
}

/** Detailed view of a Drive returned by DriveService.GetDrive (#696). */
export interface DriveDetailsView {
  driveId: string
  name: string
  accountId: string
  driveType: string
  status: string
  provider: string
  bucket: string
  prefix: string
  location: string
  region: string
  credentialsRef: string
  createdAt: string | null
  providerSummary: string
  providerOptions: { key: string; value: string }[]
  metadata: { key: string; value: string }[]
}

/** Formats a Drive message into a structured inspector view (#696). */
export function describeDriveDetails(drive: Drive): DriveDetailsView {
  const location = drive.prefix === '' ? drive.bucket : `${drive.bucket}/${drive.prefix}`
  let providerSummary = 'Default provider settings'
  const cfg = drive.providerConfig?.config
  if (cfg?.case === 's3') {
    const parts: string[] = ['S3']
    if (cfg.value.endpointOverride !== '') parts.push(`endpoint ${cfg.value.endpointOverride}`)
    parts.push(cfg.value.forcePathStyle ? 'path-style=true' : 'path-style=false')
    providerSummary = parts.join(' · ')
  } else if (cfg?.case === 'redis') {
    const parts: string[] = [`Redis (${cfg.value.uri || 'default URI'})`]
    if (cfg.value.keyPrefix !== '') parts.push(`keyPrefix=${cfg.value.keyPrefix}`)
    if (cfg.value.ttlSeconds > 0) parts.push(`ttl=${cfg.value.ttlSeconds}s`)
    providerSummary = parts.join(' · ')
  } else if (cfg?.case === 'remote') {
    providerSummary = `Remote (${cfg.value.target} / drive ${cfg.value.driveName})`
  }
  const providerOptions = Object.entries(drive.providerConfig?.options ?? {})
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([key, value]) => ({ key, value }))
  const metadata = Object.entries(drive.metadata)
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([key, value]) => ({ key, value }))
  return {
    driveId: drive.driveId,
    name: drive.name,
    accountId: drive.accountId,
    driveType: enumLabel(DriveTypeSchema, drive.driveType),
    status: enumLabel(DriveStatusSchema, drive.status),
    provider: drive.provider,
    bucket: drive.bucket,
    prefix: drive.prefix,
    location,
    region: drive.region || '(default)',
    credentialsRef: drive.credentialsRef || '(service default chain)',
    createdAt: instant(drive.createdAt),
    providerSummary,
    providerOptions,
    metadata,
  }
}
