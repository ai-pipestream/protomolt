/**
 * Pure rendition provenance enrichment and document-part occurrence options for Repository
 * Save-as.
 */
import {
  create,
  equals,
  ScalarType,
  type DescField,
  type DescMessage,
} from '@bufbuild/protobuf'
import { EntryAddressSchema, type EntryAddress } from '@/gen/ai/protomolt/proto/repo/archive/v1/entry_pb'
import {
  ListRenditionProvenanceRequestSchema,
  type ListRenditionProvenanceResponse,
  type RenditionProvenanceEntry,
} from '@/gen/ai/protomolt/proto/repo/sink/v1/repository_sink_pb'
import type { RepositorySchemaOccurrenceStep } from '@/gen/ai/protomolt/proto/repo/v1/schema_occurrence_pb'
import { PartSelectorSchema, type PartSelector, type RevisionValidation } from '@/gen/ai/protomolt/proto/repo/v2/manifest_pb'
import type { RenditionRow } from './archiveCore'
import { instant } from './formatCore'
import {
  classifyFormat,
  isProtobufMediaType,
  sanitizeDisplayMetadata,
  type FormatDescriptor,
} from './formatIcons'
import { safeExternalHref } from './linksCore'
import { typeNameOf } from './partReadCore'

export function buildListRenditionProvenanceRequest(address: EntryAddress, version: bigint) {
  return create(ListRenditionProvenanceRequestSchema, {
    address: create(EntryAddressSchema, {
      accountId: address.accountId.trim(),
      archive: address.archive.trim(),
      entryId: address.entryId.trim(),
    }),
    version: version > 0n ? version : 0n,
  })
}

export interface EnrichedRenditionRow extends RenditionRow {
  format: FormatDescriptor
  authoritativeProtobuf: boolean
  isDerived: boolean
  isStale: boolean
  staleExplanation: string | null
  sourceRevision: bigint
  currentSourceRevision: bigint
  providerId: string
  providerVersion: string
  sourceProfile: string
  lossDescription: string
  mappingId: string
  policyId: string
  schemaType: string
  outputSha256: string
  committedAt: string
  safeExternalUrl: string | null
  canSaveAsSource: boolean
}

function renditionKey(name: string, subKey: string): string {
  return `${name.length}:${name}|${subKey.length}:${subKey}`
}

/**
 * Enrich manifest `RenditionRow`s with `ListRenditionProvenanceResponse` entries,
 * computing format icons, authoritative vs derived badges, stale indicators,
 * safe external links, and Save-as source eligibility.
 */
export function mergeRenditionsWithProvenance(
  rows: readonly RenditionRow[],
  provenanceResponse: ListRenditionProvenanceResponse | null,
  options?: {
    viewedEntryVersion?: bigint
    originalFilename?: string
    sourceUri?: string
  },
): EnrichedRenditionRow[] {
  const byKey = new Map<string, RenditionProvenanceEntry>()
  if (provenanceResponse !== null) {
    for (const entry of provenanceResponse.entries) {
      if (entry.rendition === undefined) continue
      byKey.set(renditionKey(entry.rendition.name, entry.rendition.subKey), entry)
    }
  }

  return rows.map((row) => {
    const provEntry = byKey.get(renditionKey(row.name, row.subKey))
    const prov = provEntry?.provenance
    const hasProvider = Boolean(prov && prov.providerId.trim() !== '')
    const authoritativeProtobuf = prov !== undefined
      ? prov.authoritativeProtobuf
      : row.name === 'parsed' || isProtobufMediaType(row.mediaType)
    const isDerived = prov !== undefined ? !prov.authoritativeProtobuf || hasProvider : false

    const sourceRevision = prov?.sourceRevision ?? 0n
    const currentSourceRevision = provEntry?.currentSourceRevision ?? 0n
    const viewedVersion = options?.viewedEntryVersion
    const sameEntryAdvanced =
      isDerived
      && sourceRevision > 0n
      && viewedVersion !== undefined
      && viewedVersion > sourceRevision
      && prov?.sourceEntry !== undefined
      && prov.destinationAddress !== undefined
      && prov.sourceEntry.accountId === prov.destinationAddress.accountId
      && prov.sourceEntry.archive === prov.destinationAddress.archive
      && prov.sourceEntry.entryId === prov.destinationAddress.entryId

    const isStale = Boolean(
      isDerived
      && (
        provEntry?.sourceStale
        || (sourceRevision > 0n && currentSourceRevision > 0n && currentSourceRevision !== sourceRevision)
        || sameEntryAdvanced
      ),
    )

    let staleExplanation: string | null = null
    if (isStale) {
      if (sourceRevision > 0n && currentSourceRevision > 0n && currentSourceRevision !== sourceRevision) {
        staleExplanation = `Derived from source revision r${sourceRevision.toString()}; current source is r${currentSourceRevision.toString()}.`
      } else if (sourceRevision > 0n && sameEntryAdvanced && viewedVersion !== undefined) {
        staleExplanation = `Derived from revision r${sourceRevision.toString()}; viewing r${viewedVersion.toString()}.`
      } else {
        staleExplanation = 'Source revision has changed or been removed since this rendition was derived.'
      }
    }

    const fallbackFilename = row.name === 'original' ? options?.originalFilename : undefined
    const format = classifyFormat(row.mediaType, {
      filename: fallbackFilename,
      authoritativeProtobuf,
      derived: isDerived,
      stale: isStale,
    })

    const canSaveAsSource =
      row.readable
      && !isDerived
      && (authoritativeProtobuf || row.name === 'parsed' || isProtobufMediaType(row.mediaType))

    return {
      ...row,
      format,
      authoritativeProtobuf,
      isDerived,
      isStale,
      staleExplanation,
      sourceRevision,
      currentSourceRevision,
      providerId: prov ? sanitizeDisplayMetadata(prov.providerId, 128) : '',
      providerVersion: prov ? sanitizeDisplayMetadata(prov.providerVersion, 64) : '',
      sourceProfile: prov ? sanitizeDisplayMetadata(prov.sourceProfile, 128) : '',
      lossDescription: prov ? sanitizeDisplayMetadata(prov.lossDescription, 512) : '',
      mappingId: prov ? sanitizeDisplayMetadata(prov.mappingId, 128) : '',
      policyId: prov ? sanitizeDisplayMetadata(prov.policyId, 128) : '',
      schemaType: prov ? sanitizeDisplayMetadata(prov.schemaType, 256) : '',
      outputSha256: prov ? sanitizeDisplayMetadata(prov.outputSha256, 128) : '',
      committedAt: prov?.committedAt ? (instant(prov.committedAt) ?? '') : '',
      safeExternalUrl:
        row.name === 'original' && options?.sourceUri
          ? safeExternalHref(options.sourceUri)
          : null,
      canSaveAsSource,
    }
  })
}

export interface ResolvedOccurrenceOption {
  fieldPath: string
  typeName: string
  typeUrl: string
  payloadSizeBytes: bigint
}

function escapeMapStringSubscript(value: string): string {
  let escaped = '"'
  for (let i = 0; i < value.length; i++) {
    const ch = value[i]!
    if (ch === '\\' || ch === '"') {
      escaped += `\\${ch}`
    } else {
      escaped += ch
    }
  }
  escaped += '"'
  return escaped
}

function formatMapSubscript(step: RepositorySchemaOccurrenceStep): string | null {
  if (step.step.case !== 'mapKey') return null
  const mk = step.step.value
  switch (mk.value.case) {
    case 'stringValue':
      return escapeMapStringSubscript(mk.value.value)
    case 'boolValue':
    case 'signedValue':
    case 'unsignedValue':
      return String(mk.value.value)
    default:
      return null
  }
}

/**
 * Reconstruct selectable `occurrence_field_path` options for resolved top-level
 * or nested message `Any` occurrences within a decoded document part.
 */
export function listPartOccurrenceOptions(
  validation: RevisionValidation | null,
  part: PartSelector,
  rootDesc: DescMessage | null,
  lookupTypeDesc?: (typeUrl: string) => DescMessage | null,
): ResolvedOccurrenceOption[] {
  if (validation === null || rootDesc === null) return []
  const results: ResolvedOccurrenceOption[] = []
  const seen = new Set<string>()

  for (const resolved of validation.resolvedOccurrences) {
    const locator = resolved.occurrence
    const schema = resolved.schema
    if (locator?.part === undefined || schema === undefined) continue
    if (!equals(PartSelectorSchema, locator.part, part)) continue

    let curDesc: DescMessage | null = rootDesc
    let curField: DescField | null = null
    const segments: string[] = []
    let supported = true

    for (const step of locator.steps) {
      if (curDesc === null) {
        supported = false
        break
      }
      if (step.step.case === 'anyBoundary') {
        const boundaryTypeUrl = step.step.value.typeUrl
        if (curDesc.typeName === 'google.protobuf.Any' && lookupTypeDesc !== undefined) {
          curDesc = lookupTypeDesc(boundaryTypeUrl)
        }
        continue
      }
      if (step.step.case === 'fieldNumber') {
        const found: DescField | undefined = curDesc.fields.find((f) => f.number === step.step.value)
        if (found === undefined) {
          supported = false
          break
        }
        curField = found
        segments.push(found.name)
        if (found.fieldKind === 'message') {
          curDesc = found.message
        } else if (found.fieldKind === 'list' && found.listKind === 'message') {
          curDesc = found.message
        } else if (found.fieldKind === 'map' && found.mapKind === 'message') {
          curDesc = found.message
        } else {
          supported = false
          break
        }
        continue
      }
      if (step.step.case === 'repeatedIndex') {
        if (segments.length === 0 || curField?.fieldKind !== 'list') {
          supported = false
          break
        }
        segments[segments.length - 1] = `${segments[segments.length - 1]}[${step.step.value}]`
        continue
      }
      if (step.step.case === 'mapKey') {
        if (segments.length === 0 || curField?.fieldKind !== 'map') {
          supported = false
          break
        }
        const sub = formatMapSubscript(step)
        if (sub === null) {
          supported = false
          break
        }
        segments[segments.length - 1] = `${segments[segments.length - 1]}[${sub}]`
        continue
      }
      supported = false
      break
    }

    if (!supported || segments.length === 0) continue
    const fieldPath = segments.join('.')
    if (seen.has(fieldPath)) continue
    seen.add(fieldPath)
    results.push({
      fieldPath,
      typeName: typeNameOf(schema.typeUrl),
      typeUrl: schema.typeUrl,
      payloadSizeBytes: resolved.payloadLength,
    })
  }
  return results
}

export { ScalarType }
