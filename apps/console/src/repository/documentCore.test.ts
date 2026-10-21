import { create } from '@bufbuild/protobuf'
import { describe, expect, it } from 'vitest'
import { BlobBagSchema, BlobSchema, BlobsSchema, DocumentSchema } from '@/gen/ai/protomolt/proto/repo/v1/document_pb'
import { WriteProvenanceSchema } from '@/gen/ai/protomolt/proto/repo/v1/manifest_pb'
import { RepositorySchemaAssetReferenceSchema } from '@/gen/ai/protomolt/proto/repo/v1/schema_asset_reference_pb'
import {
  DocumentManifestSchema,
  DocumentPart,
  OccurrenceLocatorSchema,
  OpaqueOccurrenceSchema,
  PartManifestEntrySchema,
  PartSchemaBindingSchema,
  PartSelectorSchema,
  PartState,
  ProcessAuthorityEvidenceSchema,
  ResolvedOccurrenceSchema,
  RevisionValidationSchema,
  RuleResultSchema,
  RuleResultState,
  SaveOwnerSchema,
  ValidationStatus,
} from '@/gen/ai/protomolt/proto/repo/v2/manifest_pb'
import {
  CallNumberError,
  blobRows,
  fragmentDescriptor,
  metadataEntries,
  nodeAddress,
  ownerLabel,
  parseRevision,
  partLabel,
  partRows,
  revisionSelector,
  validationView,
} from './documentCore'
import { testAddress } from './repositoryTestKit'

const core = create(PartSelectorSchema, { selector: { case: 'part', value: DocumentPart.CORE } })
const chunks = create(PartSelectorSchema, { selector: { case: 'segmentationId', value: 'seg-1' } })
const embeddings = create(PartSelectorSchema, {
  selector: { case: 'embeddingSet', value: { segmentationId: 'seg-1', embeddingSetId: 'minilm' } },
})
const intake = create(SaveOwnerSchema, { owner: { case: 'intake', value: { datasourceId: 'ds-files' } } })
const step = create(SaveOwnerSchema, {
  owner: { case: 'step', value: create(WriteProvenanceSchema, { moduleId: 'chunker', nodeId: 'n-2', graphId: 'g-1', graphVersion: 4n }) },
})

describe('call numbers and revisions', () => {
  it('needs all four call number fields, naming the blank one', () => {
    expect(nodeAddress({ accountId: 'a', graphId: 'g', graphAddressId: 'n', docId: 'd' }).docId).toBe('d')
    expect(() => nodeAddress({ accountId: 'a', graphId: 'g', graphAddressId: ' ', docId: 'd' })).toThrow(CallNumberError)
    expect(() => nodeAddress({ accountId: 'a', graphId: '', graphAddressId: 'n', docId: 'd' })).toThrow('Graph ID is blank')
  })

  it('reads a revision query as exact or current, refusing anything else', () => {
    expect(parseRevision(undefined)).toBeNull()
    expect(parseRevision('')).toBeNull()
    expect(parseRevision('12')).toBe(12n)
    expect(() => parseRevision('0')).toThrow('greater than zero')
    expect(() => parseRevision('two')).toThrow('not a whole number')
    expect(revisionSelector(null).selector).toEqual({ case: 'current', value: true })
    expect(revisionSelector(5n).selector).toEqual({ case: 'revision', value: 5n })
  })
})

describe('labels', () => {
  it('names parts by kind, chunk segmentation and embedding set', () => {
    expect(partLabel(core)).toBe('CORE')
    expect(partLabel(chunks)).toBe('chunks seg-1')
    expect(partLabel(embeddings)).toBe('embeddings seg-1 / minilm')
    expect(partLabel(create(PartSelectorSchema, { selector: { case: 'part', value: 42 as DocumentPart } }))).toBe('UNKNOWN(42)')
  })

  it('names intake and step owners with what they carry', () => {
    expect(ownerLabel(intake)).toBe('intake ds-files')
    expect(ownerLabel(step)).toBe('step chunker (node n-2, graph g-1 v4)')
    expect(ownerLabel(undefined)).toBe('(no owner)')
  })
})

describe('partRows', () => {
  it('lists every manifest entry with its state, size, digest, revision and owner', () => {
    const manifest = create(DocumentManifestSchema, {
      address: testAddress(),
      revision: 4n,
      parts: [
        create(PartManifestEntrySchema, { selector: core, state: PartState.PRESENT, sizeBytes: 1234n, sha256: 'a'.repeat(64), writtenInRevision: 4n, owner: intake }),
        create(PartManifestEntrySchema, { selector: chunks, state: PartState.DELETED, writtenInRevision: 3n, owner: step, deletedReason: 'resegmented' }),
        create(PartManifestEntrySchema, { selector: embeddings, state: PartState.EMPTY, writtenInRevision: 2n, owner: step, opaque: true }),
      ],
    })
    expect(partRows(manifest)).toEqual([
      expect.objectContaining({ label: 'CORE', state: 'PRESENT', sizeBytes: 1234n, writtenInRevision: '4', owner: 'intake ds-files' }),
      expect.objectContaining({ label: 'chunks seg-1', state: 'DELETED', deletedReason: 'resegmented' }),
      expect.objectContaining({ label: 'embeddings seg-1 / minilm', state: 'EMPTY', opaque: true }),
    ])
  })
})

describe('validationView', () => {
  const ref = (typeUrl: string, digest: string) =>
    create(RepositorySchemaAssetReferenceSchema, { typeUrl, descriptorSha256: digest, metadataSha256: 'm'.repeat(64) })

  it('lists part fragment schemas and resolved Any schemas, opaque values and unevaluated rules', () => {
    const view = validationView(
      create(RevisionValidationSchema, {
        status: ValidationStatus.OPAQUE_ACCEPTED,
        candidateSha256: 'c'.repeat(64),
        validatorProfile: 'house-v1',
        writeAuthority: { case: 'processAuthority', value: create(ProcessAuthorityEvidenceSchema, { principal: 'operator' }) },
        partSchemas: [create(PartSchemaBindingSchema, { part: core, fragmentSchema: ref('t/Doc', '1'.repeat(64)) })],
        resolvedOccurrences: [
          create(ResolvedOccurrenceSchema, {
            occurrence: create(OccurrenceLocatorSchema, { part: core }),
            schema: ref('t/Order', '2'.repeat(64)),
            payloadLength: 88n,
          }),
        ],
        opaqueOccurrences: [
          create(OpaqueOccurrenceSchema, {
            occurrence: create(OccurrenceLocatorSchema, { part: core }),
            typeUrl: 't/Unknown',
            sha256: '3'.repeat(64),
            length: 10n,
            owner: create(WriteProvenanceSchema, { moduleId: 'parser' }),
          }),
        ],
        ruleResults: [create(RuleResultSchema, { ruleId: 'r1', fieldPath: 'a.b', state: RuleResultState.NOT_EVALUATED })],
      }),
    )
    expect(view.status).toBe('OPAQUE_ACCEPTED')
    expect(view.authority).toBe('process authority operator')
    expect(view.schemas.map((s) => [s.usage, s.typeUrl, s.descriptorSha256])).toEqual([
      ['CORE fragment', 't/Doc', '1'.repeat(64)],
      ['Any in CORE (88 bytes)', 't/Order', '2'.repeat(64)],
    ])
    expect(view.opaque).toEqual([{ at: 'CORE', typeUrl: 't/Unknown', sha256: '3'.repeat(64), length: '10', owner: 'parser' }])
    expect(view.rules).toEqual([{ ruleId: 'r1', fieldPath: 'a.b', state: 'NOT_EVALUATED' }])
    expect(fragmentDescriptor(create(RevisionValidationSchema, { partSchemas: [create(PartSchemaBindingSchema, { part: core, fragmentSchema: ref('t/Doc', '1'.repeat(64)) })] }), DocumentPart.CORE)?.typeUrl).toBe('t/Doc')
  })

  it('says when no write authority is recorded rather than inventing one', () => {
    expect(validationView(create(RevisionValidationSchema, { status: ValidationStatus.NOT_APPLICABLE })).authority).toBe('none recorded')
  })
})

describe('blobRows and metadataEntries', () => {
  it('lists a single blob and a blob collection alike, keeping unrecorded names null', () => {
    const one = create(DocumentSchema, {
      blobBag: create(BlobBagSchema, { blobData: { case: 'blob', value: create(BlobSchema, { blobId: 'b1', filename: 'a.pdf', mimeType: 'application/pdf', sizeBytes: 9n }) } }),
    })
    expect(blobRows(one)).toEqual([{ blobId: 'b1', filename: 'a.pdf', mimeType: 'application/pdf', sizeBytes: 9n }])
    const many = create(DocumentSchema, {
      blobBag: create(BlobBagSchema, {
        blobData: { case: 'blobs', value: create(BlobsSchema, { blob: [create(BlobSchema, { blobId: 'b1' }), create(BlobSchema, { blobId: 'b2', filename: 'x' })] }) },
      }),
    })
    expect(blobRows(many)).toEqual([
      { blobId: 'b1', filename: null, mimeType: null, sizeBytes: 0n },
      { blobId: 'b2', filename: 'x', mimeType: null, sizeBytes: 0n },
    ])
    expect(blobRows(create(DocumentSchema))).toEqual([])
  })

  it('sorts metadata by key', () => {
    expect(metadataEntries({ b: '2', a: '1' })).toEqual([{ key: 'a', value: '1' }, { key: 'b', value: '2' }])
  })
})
