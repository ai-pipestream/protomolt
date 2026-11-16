/**
 * Pure view-models, request builders and options encoders for Repository Save-as
 * (`RepositorySinkService`): session access, sink sources, capability cards, Parquet options and
 * the publish request. Job lifecycle helpers live in `saveAsJobCore`, rendition provenance and
 * occurrence options in `saveAsProvenanceCore`.
 */
import { create, toBinary } from '@bufbuild/protobuf'
import type { ConsoleSessionStatus } from '@/gen/ai/protomolt/proto/authz/v1/console_session_pb'
import { ParquetMaskStrategy, ParquetRendererOptionsSchema } from '@/gen/ai/protomolt/proto/emit/parquet/v1/parquet_renderer_pb'
import {
  EntryAddressSchema,
  RenditionDescriptorSchema,
  WriteAttributionSchema,
  type EntryAddress,
} from '@/gen/ai/protomolt/proto/repo/archive/v1/entry_pb'
import {
  ArchiveRenditionSourceSchema,
  ConversionSpecSchema,
  DocumentPartSourceSchema,
  ListSaveAsCapabilitiesRequestSchema,
  PublishRepositorySinkRequestSchema,
  RepositoryRevisionSourceSchema,
  SaveAsCompatibilityStatus,
  SinkDestinationSchema,
  SinkSourceSchema,
  type ListSaveAsCapabilitiesResponse,
  type PublishRepositorySinkRequest,
  type SaveAsCapability,
  type SinkSource,
} from '@/gen/ai/protomolt/proto/repo/sink/v1/repository_sink_pb'
import { NodeAddressSchema, type NodeAddress } from '@/gen/ai/protomolt/proto/repo/v1/address_pb'
import { RevisionSelectorSchema, type PartSelector } from '@/gen/ai/protomolt/proto/repo/v2/manifest_pb'
import {
  classifyFormat,
  isProtobufMediaType,
  sanitizeDisplayMetadata,
  type FormatDescriptor,
} from './formatIcons'
import { scopesIn } from './sessionWritersCore'

export const PARQUET_OPTIONS_TYPE = 'ai.protomolt.proto.emit.parquet.v1.ParquetRendererOptions'
export const EMPTY_OPTIONS_TYPE = 'google.protobuf.Empty'
export const ZERO_PROVIDERS_MESSAGE = 'No Save-as conversion providers are enabled on this server.'

export class SaveAsInputError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'SaveAsInputError'
  }
}

export interface SaveAsSessionAccess {
  readOnly: boolean
  reason: string | null
  principal: string | null
}

/**
 * Evaluates whether the signed-in console session can submit Save-as writes
 * in `accountId`. When session status is absent (null), returns advisory
 * non-blocking access so server authorization remains authoritative.
 */
export function evaluateSaveAsSessionAccess(
  session: ConsoleSessionStatus | null,
  accountId: string,
): SaveAsSessionAccess {
  if (session === null) {
    return { readOnly: false, reason: null, principal: null }
  }
  if (!session.authenticated) {
    return {
      readOnly: true,
      reason: 'Sign in with a writer session to save derived renditions.',
      principal: null,
    }
  }
  const acct = accountId.trim()
  const scopes = acct !== '' ? scopesIn(session.intakeWriters, acct) : session.intakeWriters
  if (scopes.length === 0) {
    return {
      readOnly: true,
      reason: acct !== ''
        ? `Session ${session.principal || '(signed-in)'} has read-only repository access for account ${acct}.`
        : `Session ${session.principal || '(signed-in)'} has read-only repository access.`,
      principal: session.principal || null,
    }
  }
  return {
    readOnly: false,
    reason: null,
    principal: session.principal || null,
  }
}

export interface ArchiveRenditionSourceInput {
  address: EntryAddress
  rendition: {
    name: string
    subKey?: string
    mediaType?: string
  }
  /** Explicit pinned entry version (> 0n) so Save-as binds to the viewed revision. */
  revision: bigint
  expectedSchemaFingerprint?: string
}

export interface DocumentPartSourceInput {
  address: NodeAddress
  selector: PartSelector
  /** Explicit pinned document revision (> 0n) so Save-as binds to the viewed revision. */
  revision: bigint
  occurrenceFieldPath?: string
  expectedSchemaFingerprint?: string
}

/** Build a `SinkSource` pinned to an exact archive entry version and rendition. */
export function buildArchiveRenditionSinkSource(input: ArchiveRenditionSourceInput): SinkSource {
  if (input.revision <= 0n) {
    throw new SaveAsInputError('Source revision: Save-as requires an explicit positive entry version.')
  }
  return create(SinkSourceSchema, {
    kind: {
      case: 'repositoryRevision',
      value: create(RepositoryRevisionSourceSchema, {
        target: {
          case: 'archiveRendition',
          value: create(ArchiveRenditionSourceSchema, {
            address: create(EntryAddressSchema, {
              accountId: input.address.accountId.trim(),
              archive: input.address.archive.trim(),
              entryId: input.address.entryId.trim(),
            }),
            revision: create(RevisionSelectorSchema, {
              selector: { case: 'revision', value: input.revision },
            }),
            rendition: create(RenditionDescriptorSchema, {
              name: input.rendition.name.trim(),
              subKey: (input.rendition.subKey ?? '').trim(),
              mediaType: (input.rendition.mediaType ?? '').trim(),
            }),
            expectedSchemaFingerprint: (input.expectedSchemaFingerprint ?? '').trim(),
          }),
        },
      }),
    },
  })
}

/** Build a `SinkSource` pinned to an exact v2 document revision and part (plus optional occurrence path). */
export function buildDocumentPartSinkSource(input: DocumentPartSourceInput): SinkSource {
  if (input.revision <= 0n) {
    throw new SaveAsInputError('Source revision: Save-as requires an explicit positive document revision.')
  }
  return create(SinkSourceSchema, {
    kind: {
      case: 'repositoryRevision',
      value: create(RepositoryRevisionSourceSchema, {
        target: {
          case: 'documentPart',
          value: create(DocumentPartSourceSchema, {
            address: create(NodeAddressSchema, {
              accountId: input.address.accountId.trim(),
              graphId: input.address.graphId.trim(),
              graphAddressId: input.address.graphAddressId.trim(),
              docId: input.address.docId.trim(),
            }),
            revision: create(RevisionSelectorSchema, {
              selector: { case: 'revision', value: input.revision },
            }),
            selector: input.selector,
            occurrenceFieldPath: (input.occurrenceFieldPath ?? '').trim(),
            expectedSchemaFingerprint: (input.expectedSchemaFingerprint ?? '').trim(),
          }),
        },
      }),
    },
  })
}

export function buildListSaveAsCapabilitiesRequest(source: SinkSource) {
  return create(ListSaveAsCapabilitiesRequestSchema, { source })
}

export interface SaveAsCapabilityItem {
  key: string
  providerId: string
  providerVersion: string
  sourceProfile: string
  mediaType: string
  extension: string
  defaultRenditionName: string
  optionsType: string
  optionsFingerprint: string
  lossDescription: string
  maxInputBytes: bigint
  maxOutputBytes: bigint
  compatible: boolean
  incompatibilityReason: string
  format: FormatDescriptor
  optionsKind: 'parquet' | 'empty' | 'generic-default'
}

export interface SaveAsCapabilitiesView {
  sourceSchemaType: string
  sourceSchemaFingerprint: string
  sourceRevision: bigint
  sourceSizeBytes: bigint
  compatible: SaveAsCapabilityItem[]
  incompatible: SaveAsCapabilityItem[]
  hasAnyProviders: boolean
  emptyStateMessage: string | null
}

function isDownloadableRenditionMediaType(mediaType: string): boolean {
  const normalized = mediaType.trim().toLowerCase()
  if (normalized === '') return false
  // External event transports (e.g. kafka:// or x-kafka) are never downloadable archive renditions.
  if (normalized.startsWith('kafka/') || normalized.includes('://')) return false
  return normalized.includes('/')
}

export function defaultRenditionSlotName(extension: string, providerId: string): string {
  let cleanedExt = extension.trim().toLowerCase()
  while (cleanedExt.startsWith('.')) {
    cleanedExt = cleanedExt.slice(1)
  }
  const candidate = cleanedExt !== '' ? cleanedExt : providerId.trim().toLowerCase()
  let slug = ''
  for (let i = 0; i < candidate.length; i++) {
    const c = candidate.charCodeAt(i)
    const isAlphaNum = (c >= 0x61 && c <= 0x7a) || (c >= 0x30 && c <= 0x39)
    if (isAlphaNum) {
      slug += candidate[i]
    } else if ((c === 0x2d || c === 0x5f || c === 0x2e || c === 0x3a) && slug !== '' && !slug.endsWith('-')) {
      slug += '-'
    }
  }
  while (slug.endsWith('-')) {
    slug = slug.slice(0, -1)
  }
  if (slug === '' || slug === 'parsed' || slug === 'original') {
    return slug === '' ? 'derived' : `${slug}-derived`
  }
  return slug.slice(0, 64)
}

function mapSaveAsCapability(entry: SaveAsCapability): SaveAsCapabilityItem | null {
  const cap = entry.capability
  if (cap === undefined) return null
  if (!isDownloadableRenditionMediaType(cap.mediaType)) return null

  const providerId = sanitizeDisplayMetadata(cap.providerId, 128)
  const providerVersion = sanitizeDisplayMetadata(cap.providerVersion, 64)
  const sourceProfile = sanitizeDisplayMetadata(cap.sourceProfile, 128)
  const mediaType = cap.mediaType.trim()
  const extension = sanitizeDisplayMetadata(cap.extension, 32)
  const optionsType = cap.optionsType.trim()
  const optionsFingerprint = cap.optionsFingerprint.trim()
  const lossDescription = sanitizeDisplayMetadata(cap.lossDescription, 512)
  const compatible = entry.status === SaveAsCompatibilityStatus.COMPATIBLE
  const incompatibilityReason = sanitizeDisplayMetadata(entry.incompatibilityReason, 512)

  const format = classifyFormat(mediaType, {
    filename: extension !== '' ? `output${extension.startsWith('.') ? extension : `.${extension}`}` : undefined,
    derived: true,
  })

  const optionsKind =
    optionsType === PARQUET_OPTIONS_TYPE
      ? 'parquet'
      : optionsType === '' || optionsType === EMPTY_OPTIONS_TYPE
        ? 'empty'
        : 'generic-default'

  return {
    key: `${cap.providerId}@${cap.providerVersion}:${cap.sourceProfile}:${mediaType}`,
    providerId,
    providerVersion,
    sourceProfile,
    mediaType,
    extension,
    defaultRenditionName: defaultRenditionSlotName(extension, cap.providerId),
    optionsType,
    optionsFingerprint,
    lossDescription,
    maxInputBytes: BigInt(cap.maxInputBytes),
    maxOutputBytes: BigInt(cap.maxOutputBytes),
    compatible,
    incompatibilityReason:
      incompatibilityReason !== ''
        ? incompatibilityReason
        : compatible
          ? ''
          : 'This conversion provider is incompatible with the selected protobuf source.',
    format,
    optionsKind,
  }
}

/**
 * Transform a `ListSaveAsCapabilitiesResponse` into compatible and incompatible
 * provider lists, never inventing unconfigured formats when empty.
 */
export function describeSaveAsCapabilities(response: ListSaveAsCapabilitiesResponse): SaveAsCapabilitiesView {
  const compatible: SaveAsCapabilityItem[] = []
  const incompatible: SaveAsCapabilityItem[] = []

  for (const raw of response.capabilities) {
    const item = mapSaveAsCapability(raw)
    if (item === null) continue
    if (item.compatible) {
      compatible.push(item)
    } else {
      incompatible.push(item)
    }
  }

  const hasAnyProviders = compatible.length > 0 || incompatible.length > 0
  return {
    sourceSchemaType: sanitizeDisplayMetadata(response.sourceSchemaType, 256),
    sourceSchemaFingerprint: sanitizeDisplayMetadata(response.sourceSchemaFingerprint, 128),
    sourceRevision: response.resolvedSourceRevision,
    sourceSizeBytes: response.sourceSizeBytes,
    compatible,
    incompatible,
    hasAnyProviders,
    emptyStateMessage: hasAnyProviders ? null : ZERO_PROVIDERS_MESSAGE,
  }
}

export interface ParquetOptionsFormInput {
  /** Comma- or newline-separated top-level protobuf field names; empty means all supported scalar fields. */
  columnsText: string
  /** Comma- or newline-separated sensitivity classes to mask. */
  maskedSensitivityClassesText: string
  maskStrategy: ParquetMaskStrategy
  /** Optional positive decimal integer limit in bytes; empty or "0" uses capability default. */
  maxOutputBytesText: string
  mappingId: string
  policyId: string
}

function splitCleanTokens(raw: string): string[] {
  const result: string[] = []
  let current = ''
  for (let i = 0; i < raw.length; i++) {
    const ch = raw[i]!
    if (ch === ',' || ch === '\n' || ch === '\r') {
      const trimmed = current.trim()
      if (trimmed !== '') result.push(trimmed)
      current = ''
    } else {
      current += ch
    }
  }
  const tail = current.trim()
  if (tail !== '') result.push(tail)
  return result
}

function isProtoIdentifier(value: string): boolean {
  if (value.length === 0 || value.length > 128) return false
  for (let i = 0; i < value.length; i++) {
    const c = value.charCodeAt(i)
    const isLower = c >= 0x61 && c <= 0x7a
    const isUpper = c >= 0x41 && c <= 0x5a
    const isDigit = c >= 0x30 && c <= 0x39
    const isUnderscore = c === 0x5f
    if (i === 0 && isDigit) return false
    if (!isLower && !isUpper && !isDigit && !isUnderscore) return false
  }
  return true
}

function isSafeClassOrPolicyToken(value: string): boolean {
  if (value.length === 0 || value.length > 128) return false
  for (let i = 0; i < value.length; i++) {
    const c = value.charCodeAt(i)
    if (c < 0x21 || c > 0x7e) return false
  }
  return true
}

function parseOptionalNonNegativeBigInt(raw: string, fieldLabel: string): bigint {
  const trimmed = raw.trim()
  if (trimmed === '') return 0n
  for (let i = 0; i < trimmed.length; i++) {
    const c = trimmed.charCodeAt(i)
    if (c < 0x30 || c > 0x39) {
      throw new SaveAsInputError(`${fieldLabel}: must be a whole number of bytes.`)
    }
  }
  return BigInt(trimmed)
}

/**
 * Validate and serialize `ParquetRendererOptions` into canonical binary protobuf.
 */
export function encodeParquetRendererOptions(
  form: ParquetOptionsFormInput,
  capabilityMaxOutputBytes = 0n,
): { bytes: Uint8Array; mappingId: string; policyId: string } {
  const columns = splitCleanTokens(form.columnsText)
  const seenColumns = new Set<string>()
  for (const col of columns) {
    if (col.includes('.')) {
      throw new SaveAsInputError(`Projected columns: '${col}' must be a top-level field name without dots.`)
    }
    if (!isProtoIdentifier(col)) {
      throw new SaveAsInputError(`Projected columns: '${col}' is not a valid protobuf field identifier.`)
    }
    if (seenColumns.has(col)) {
      throw new SaveAsInputError(`Projected columns: duplicate column '${col}'.`)
    }
    seenColumns.add(col)
  }

  const sensitivityClasses = splitCleanTokens(form.maskedSensitivityClassesText)
  const seenClasses = new Set<string>()
  for (const cls of sensitivityClasses) {
    if (!isSafeClassOrPolicyToken(cls)) {
      throw new SaveAsInputError(`Sensitivity classes: '${cls}' contains invalid characters.`)
    }
    if (!seenClasses.has(cls)) {
      seenClasses.add(cls)
    }
  }
  const uniqueClasses = [...seenClasses]

  if (
    uniqueClasses.length > 0
    && form.maskStrategy === ParquetMaskStrategy.UNSPECIFIED
  ) {
    throw new SaveAsInputError('Mask strategy: choose REDACT or REMOVE when sensitivity classes are specified.')
  }

  const maxOutputBytes = parseOptionalNonNegativeBigInt(form.maxOutputBytesText, 'Max output bytes')
  if (capabilityMaxOutputBytes > 0n && maxOutputBytes > capabilityMaxOutputBytes) {
    throw new SaveAsInputError(
      `Max output bytes: ${maxOutputBytes.toString()} exceeds provider ceiling of ${capabilityMaxOutputBytes.toString()} bytes.`,
    )
  }

  const mappingId = form.mappingId.trim()
  if (mappingId !== '' && !isSafeClassOrPolicyToken(mappingId)) {
    throw new SaveAsInputError('Mapping ID: contains invalid whitespace or control characters.')
  }
  const policyId = form.policyId.trim()
  if (policyId !== '' && !isSafeClassOrPolicyToken(policyId)) {
    throw new SaveAsInputError('Policy ID: contains invalid whitespace or control characters.')
  }

  const message = create(ParquetRendererOptionsSchema, {
    columns,
    maskedSensitivityClasses: uniqueClasses,
    maskStrategy: form.maskStrategy,
    maxOutputBytes,
    mappingId,
    policyId,
  })
  return {
    bytes: toBinary(ParquetRendererOptionsSchema, message),
    mappingId,
    policyId,
  }
}

export interface SaveAsDestinationInput {
  accountId: string
  archive: string
  entryId: string
  renditionName: string
  subKey?: string
  preconditionMode: 'unconditional' | 'expected-version' | 'create-only'
  expectedDestinationVersionText?: string
  writtenByModule?: string
}

export interface BuildPublishSaveAsInput {
  operationId: string
  source: SinkSource
  capability: SaveAsCapabilityItem
  destination: SaveAsDestinationInput
  parquetOptions?: ParquetOptionsFormInput
  genericMappingId?: string
  genericPolicyId?: string
  async?: boolean
}

function validateSafeSegment(value: string, label: string, maxLen = 128): string {
  const trimmed = value.trim()
  if (trimmed === '') {
    throw new SaveAsInputError(`${label}: required.`)
  }
  if (trimmed.length > maxLen) {
    throw new SaveAsInputError(`${label}: must be at most ${maxLen} characters.`)
  }
  if (trimmed === '.' || trimmed === '..' || trimmed.includes('/') || trimmed.includes('\\')) {
    throw new SaveAsInputError(`${label}: must not contain path separators or relative segments.`)
  }
  for (let i = 0; i < trimmed.length; i++) {
    const c = trimmed.charCodeAt(i)
    if (c < 0x20 || c === 0x7f) {
      throw new SaveAsInputError(`${label}: must not contain control characters.`)
    }
  }
  return trimmed
}

/**
 * Construct a validated `PublishRepositorySinkRequest` bound to a deterministic
 * `operationId`, pinned `SinkSource`, compatible `SaveAsCapabilityItem`, and
 * explicit `SinkDestination`.
 */
export function buildPublishSaveAsRequest(input: BuildPublishSaveAsInput): PublishRepositorySinkRequest {
  const operationId = input.operationId.trim()
  if (operationId === '') {
    throw new SaveAsInputError('Operation ID: required.')
  }
  if (!input.capability.compatible) {
    throw new SaveAsInputError('Conversion provider: selected capability is incompatible with this source.')
  }

  const accountId = validateSafeSegment(input.destination.accountId, 'Destination account', 128)
  const archive = validateSafeSegment(input.destination.archive, 'Destination archive', 128)
  const entryId = validateSafeSegment(input.destination.entryId, 'Destination entry ID', 256)
  const renditionName = validateSafeSegment(input.destination.renditionName, 'Rendition name', 128)
  if (renditionName === 'parsed' && !isProtobufMediaType(input.capability.mediaType)) {
    throw new SaveAsInputError("Rendition name: converted non-protobuf renditions cannot overwrite the 'parsed' protobuf slot.")
  }
  const subKeyRaw = (input.destination.subKey ?? '').trim()
  const subKey = subKeyRaw !== '' ? validateSafeSegment(subKeyRaw, 'Rendition sub-key', 128) : ''

  let expectedDestinationVersion = 0n
  let createOnly = false
  if (input.destination.preconditionMode === 'create-only') {
    createOnly = true
  } else if (input.destination.preconditionMode === 'expected-version') {
    expectedDestinationVersion = parseOptionalNonNegativeBigInt(
      input.destination.expectedDestinationVersionText ?? '',
      'Expected destination version',
    )
    if (expectedDestinationVersion <= 0n) {
      throw new SaveAsInputError('Expected destination version: must be at least 1 when version precondition is selected.')
    }
  }

  let optionsBytes: Uint8Array = new Uint8Array(0)
  let mappingId = (input.genericMappingId ?? '').trim()
  let policyId = (input.genericPolicyId ?? '').trim()

  if (input.capability.optionsKind === 'parquet') {
    const encoded = encodeParquetRendererOptions(
      input.parquetOptions ?? {
        columnsText: '',
        maskedSensitivityClassesText: '',
        maskStrategy: ParquetMaskStrategy.UNSPECIFIED,
        maxOutputBytesText: '',
        mappingId: '',
        policyId: '',
      },
      input.capability.maxOutputBytes,
    )
    optionsBytes = encoded.bytes
    mappingId = encoded.mappingId
    policyId = encoded.policyId
  } else {
    if (mappingId !== '' && !isSafeClassOrPolicyToken(mappingId)) {
      throw new SaveAsInputError('Mapping ID: contains invalid whitespace or control characters.')
    }
    if (policyId !== '' && !isSafeClassOrPolicyToken(policyId)) {
      throw new SaveAsInputError('Policy ID: contains invalid whitespace or control characters.')
    }
  }

  return create(PublishRepositorySinkRequestSchema, {
    operationId,
    source: input.source,
    conversion: create(ConversionSpecSchema, {
      providerId: input.capability.providerId,
      providerVersion: input.capability.providerVersion,
      sourceProfile: input.capability.sourceProfile,
      mediaType: input.capability.mediaType,
      extension: input.capability.extension,
      optionsType: input.capability.optionsType,
      optionsFingerprint: input.capability.optionsFingerprint,
      options: optionsBytes,
      mappingId,
      policyId,
    }),
    destination: create(SinkDestinationSchema, {
      address: create(EntryAddressSchema, {
        accountId,
        archive,
        entryId,
      }),
      rendition: create(RenditionDescriptorSchema, {
        name: renditionName,
        subKey,
        mediaType: input.capability.mediaType,
      }),
      expectedDestinationVersion,
      createOnly,
      writtenBy: create(WriteAttributionSchema, {
        module: (input.destination.writtenByModule ?? '').trim(),
      }),
    }),
    async: input.async ?? true,
  })
}
