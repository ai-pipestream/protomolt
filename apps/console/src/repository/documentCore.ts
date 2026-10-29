/**
 * The document detail screen: the manifest parts table, metadata, the
 * validation record and the schema artifacts a revision retained, all read
 * from GetManifest and GetDocument answers.
 */
import { create } from '@bufbuild/protobuf'
import type { NodeAddress } from '@/gen/ai/protomolt/proto/repo/v1/address_pb'
import { NodeAddressSchema } from '@/gen/ai/protomolt/proto/repo/v1/address_pb'
import type { Blob as RepoBlob, Document } from '@/gen/ai/protomolt/proto/repo/v1/document_pb'
import type { RepositorySchemaAssetReference } from '@/gen/ai/protomolt/proto/repo/v1/schema_asset_reference_pb'
import {
  DocumentPart,
  DocumentPartSchema,
  PartStateSchema,
  RevisionSelectorSchema,
  RuleResultStateSchema,
  ValidationStatusSchema,
  type DocumentManifest,
  type PartSelector,
  type RevisionSelector,
  type RevisionValidation,
  type SaveOwner,
} from '@/gen/ai/protomolt/proto/repo/v2/manifest_pb'
import { enumLabel } from './formatCore'

/** The four fields of a call number, as route query values. */
export interface CallNumber {
  accountId: string
  graphId: string
  graphAddressId: string
  docId: string
}

/** A call number field that is blank; `field` is the label the form shows. */
export class CallNumberError extends Error {
  constructor(readonly field: string) {
    super(`${field} is blank: a call number needs the account, graph, graph address and document IDs.`)
    this.name = 'CallNumberError'
  }
}

const CALL_NUMBER_FIELDS: { key: keyof CallNumber; label: string }[] = [
  { key: 'accountId', label: 'Account ID' },
  { key: 'graphId', label: 'Graph ID' },
  { key: 'graphAddressId', label: 'Graph address ID' },
  { key: 'docId', label: 'Document ID' },
]

/** The NodeAddress of a call number; every field is required. */
export function nodeAddress(call: CallNumber): NodeAddress {
  for (const { key, label } of CALL_NUMBER_FIELDS) {
    if (call[key].trim() === '') throw new CallNumberError(label)
  }
  return create(NodeAddressSchema, {
    accountId: call.accountId,
    graphId: call.graphId,
    graphAddressId: call.graphAddressId,
    docId: call.docId,
  })
}

export function callNumberOf(address: NodeAddress): CallNumber {
  return {
    accountId: address.accountId,
    graphId: address.graphId,
    graphAddressId: address.graphAddressId,
    docId: address.docId,
  }
}

/** Current, or an exact revision. An exact revision that does not exist is NOT_FOUND. */
export function revisionSelector(revision: bigint | null): RevisionSelector {
  return create(
    RevisionSelectorSchema,
    revision === null ? { selector: { case: 'current', value: true } } : { selector: { case: 'revision', value: revision } },
  )
}

/** Parses an exact revision from a route query; null means current. */
export function parseRevision(raw: string | null | undefined): bigint | null {
  if (raw === null || raw === undefined || raw === '') return null
  let value: bigint
  try {
    value = BigInt(raw)
  } catch {
    throw new Error(`Revision "${raw}" is not a whole number.`)
  }
  if (value <= 0n) throw new Error(`Revision "${raw}" must be greater than zero.`)
  return value
}

export const DOCUMENT_TABS = ['parts', 'metadata', 'validation', 'links', 'history', 'files'] as const
export type DocumentTab = (typeof DOCUMENT_TABS)[number] | 'compare'

/** Parses a bookmarkable document tab from a route query; null when absent or unknown. */
export function parseDocumentTab(raw: string | null | undefined): DocumentTab | null {
  if (raw === null || raw === undefined || raw === '') return null
  return (DOCUMENT_TABS as readonly string[]).includes(raw) ? (raw as DocumentTab) : null
}

/** A part selector as a short label: `CORE`, `chunks seg-1`, `embeddings seg-1 / set-a`. */
export function partLabel(selector: PartSelector | undefined): string {
  if (selector === undefined) return '(no selector)'
  const s = selector.selector
  switch (s.case) {
    case 'part':
      return enumLabel(DocumentPartSchema, s.value)
    case 'segmentationId':
      return `chunks ${s.value}`
    case 'embeddingSet':
      return `embeddings ${s.value.segmentationId} / ${s.value.embeddingSetId}`
    case undefined:
      return '(empty selector)'
  }
}

/** Who wrote a revision or part. */
export function ownerLabel(owner: SaveOwner | undefined): string {
  if (owner === undefined) return '(no owner)'
  const o = owner.owner
  switch (o.case) {
    case 'intake':
      return o.value.connectorId === undefined
        ? `intake ${o.value.datasourceId}`
        : `intake ${o.value.datasourceId} via ${o.value.connectorId}`
    case 'step': {
      const step = o.value
      const where = [step.nodeId && `node ${step.nodeId}`, step.graphId && `graph ${step.graphId}`]
        .filter(Boolean)
        .join(', ')
      const version = step.graphVersion > 0n ? ` v${step.graphVersion}` : ''
      return `step ${step.moduleId}${where ? ` (${where}${version})` : ''}`
    }
    case undefined:
      return '(owner without kind)'
  }
}

/** One row of the manifest parts table. */
export interface PartRow {
  label: string
  state: string
  sizeBytes: bigint
  sha256: string
  writtenInRevision: string
  owner: string
  opaque: boolean
  deletedReason: string
  segmentationDigest: string
}

export function partRows(manifest: DocumentManifest): PartRow[] {
  return manifest.parts.map((entry) => ({
    label: partLabel(entry.selector),
    state: enumLabel(PartStateSchema, entry.state),
    sizeBytes: entry.sizeBytes,
    sha256: entry.sha256,
    writtenInRevision: entry.writtenInRevision.toString(),
    owner: ownerLabel(entry.owner),
    opaque: entry.opaque,
    deletedReason: entry.deletedReason,
    segmentationDigest: entry.segmentationDigest,
  }))
}

/** One retained schema artifact a revision references. */
export interface SchemaArtifact {
  /** Where the revision binds it: a part's fragment schema or a resolved Any occurrence. */
  usage: string
  /** Label of the part this artifact is bound within, when bound to a manifest part. */
  partLabel: string | null
  typeUrl: string
  descriptorSha256: string
  metadataSha256: string
  sourceSha256: string | null
}

function artifact(usage: string, ref: RepositorySchemaAssetReference | undefined, boundPart: PartSelector | undefined = undefined): SchemaArtifact {
  if (ref === undefined) throw new Error(`The validation record has a ${usage} binding without a schema reference.`)
  return {
    usage,
    partLabel: boundPart === undefined ? null : partLabel(boundPart),
    typeUrl: ref.typeUrl,
    descriptorSha256: ref.descriptorSha256,
    metadataSha256: ref.metadataSha256,
    sourceSha256: ref.sourceSha256 ?? null,
  }
}

/** The validation record as the screen shows it. */
export interface ValidationView {
  status: string
  candidateSha256: string
  validatorProfile: string
  authority: string
  schemas: SchemaArtifact[]
  opaque: { at: string; typeUrl: string; sha256: string; length: string; owner: string }[]
  rules: { ruleId: string; fieldPath: string; state: string }[]
}

export function validationView(validation: RevisionValidation): ValidationView {
  const authority = validation.writeAuthority
  return {
    status: enumLabel(ValidationStatusSchema, validation.status),
    candidateSha256: validation.candidateSha256,
    validatorProfile: validation.validatorProfile,
    authority:
      authority.case === 'writerGrant'
        ? `writer grant ${authority.value.writerGrantId} (generation ${authority.value.credentialGeneration})`
        : authority.case === 'processAuthority'
          ? `process authority ${authority.value.principal}`
          : 'none recorded',
    schemas: [
      ...validation.partSchemas.map((b) => artifact(`${partLabel(b.part)} fragment`, b.fragmentSchema, b.part)),
      ...validation.resolvedOccurrences.map((o) =>
        artifact(`Any in ${partLabel(o.occurrence?.part)} (${o.payloadLength} bytes)`, o.schema, o.occurrence?.part),
      ),
    ],
    opaque: validation.opaqueOccurrences.map((o) => ({
      at: partLabel(o.occurrence?.part),
      typeUrl: o.typeUrl,
      sha256: o.sha256,
      length: o.length.toString(),
      owner: o.owner?.moduleId ?? '(no owner)',
    })),
    rules: validation.ruleResults.map((r) => ({
      ruleId: r.ruleId,
      fieldPath: r.fieldPath,
      state: enumLabel(RuleResultStateSchema, r.state),
    })),
  }
}

/** The descriptor digest bound to a part's fragment, or null when the revision binds none. */
export function fragmentDescriptor(validation: RevisionValidation, part: DocumentPart): SchemaArtifact | null {
  for (const binding of validation.partSchemas) {
    const s = binding.part?.selector
    if (s?.case === 'part' && s.value === part) return artifact(`${enumLabel(DocumentPartSchema, part)} fragment`, binding.fragmentSchema, binding.part)
  }
  return null
}

/** One raw file of a document, from its BLOBS part. */
export interface BlobRow {
  blobId: string
  /** Null when the document did not record one. */
  filename: string | null
  mimeType: string | null
  sizeBytes: bigint
}

/** The raw files a BLOBS read assembled, single or many. */
export function blobRows(document: Document | undefined): BlobRow[] {
  const bag = document?.blobBag?.blobData
  if (bag === undefined || bag.case === undefined) return []
  const blobs: RepoBlob[] = bag.case === 'blob' ? [bag.value] : bag.value.blob
  return blobs.map((b) => ({
    blobId: b.blobId,
    filename: b.filename ?? null,
    mimeType: b.mimeType ?? null,
    sizeBytes: b.sizeBytes,
  }))
}

/** Metadata entries sorted by key, so the table order is stable. */
export function metadataEntries(metadata: Record<string, string>): { key: string; value: string }[] {
  return Object.keys(metadata)
    .sort()
    .map((key) => ({ key, value: metadata[key] }))
}

export { DocumentPart }
