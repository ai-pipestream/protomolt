import { create, createFileRegistry, fromBinary, setExtension, toBinary, type DescMessage } from '@bufbuild/protobuf'
import {
  FieldDescriptorProto_Label,
  FieldDescriptorProto_Type,
  FieldOptionsSchema,
  FileDescriptorProtoSchema,
  FileDescriptorSetSchema,
  anyPack,
  type FileDescriptorProto,
} from '@bufbuild/protobuf/wkt'
import { createHash } from 'node:crypto'
import { describe, expect, it } from 'vitest'
import { file_ai_protomolt_proto_types_v1_concepts } from '@/gen/ai/protomolt/proto/types/v1/concepts_pb'
import { DocumentSchema, SearchMetadataSchema } from '@/gen/ai/protomolt/proto/repo/v1/document_pb'
import { RepositorySchemaAssetReferenceSchema } from '@/gen/ai/protomolt/proto/repo/v1/schema_asset_reference_pb'
import { DocumentService, GetSchemaDescriptorResponseSchema } from '@/gen/ai/protomolt/proto/repo/v2/document_service_pb'
import {
  DocumentManifestSchema,
  DocumentPart,
  PartManifestEntrySchema,
  PartSelectorSchema,
  PartState,
  ResolvedOccurrenceSchema,
  RevisionValidationSchema,
  SaveOwnerSchema,
} from '@/gen/ai/protomolt/proto/repo/v2/manifest_pb'
import { FieldRulesSchema, field as fieldRules, file_ai_protomolt_proto_validate_v1_validate } from '@/gen/ai/protomolt/proto/validate/v1/validate_pb'
import { linkRow } from './linksCore'
import { AnySchemas, flatten, linkLines, partDiff, searchMetadataLines, sectionDiff } from './revisionDiffCore'
import { descriptorSetOf, routerClients, testAddress } from './repositoryTestKit'
import { ResolvedLinkSchema, StaticLinkStateSchema } from '@/gen/ai/protomolt/proto/repo/v2/document_service_pb'

const part = (value: DocumentPart) => create(PartSelectorSchema, { selector: { case: 'part', value } })
const owner = (datasourceId: string) => create(SaveOwnerSchema, { owner: { case: 'intake', value: { datasourceId } } })

describe('partDiff', () => {
  it('matches parts by selector and names the columns that changed', () => {
    const base = create(DocumentManifestSchema, {
      revision: 1n,
      parts: [
        create(PartManifestEntrySchema, { selector: part(DocumentPart.CORE), state: PartState.PRESENT, sha256: 'a'.repeat(64), sizeBytes: 10n, writtenInRevision: 1n, owner: owner('ds') }),
        create(PartManifestEntrySchema, { selector: part(DocumentPart.BLOBS), state: PartState.PRESENT, sha256: 'b'.repeat(64), sizeBytes: 5n, writtenInRevision: 1n, owner: owner('ds') }),
        create(PartManifestEntrySchema, { selector: part(DocumentPart.PARSED), state: PartState.PRESENT, sha256: 'c'.repeat(64), writtenInRevision: 1n, owner: owner('ds') }),
      ],
    })
    const target = create(DocumentManifestSchema, {
      revision: 3n,
      parts: [
        create(PartManifestEntrySchema, { selector: part(DocumentPart.CORE), state: PartState.PRESENT, sha256: 'd'.repeat(64), sizeBytes: 12n, writtenInRevision: 3n, owner: owner('ds') }),
        create(PartManifestEntrySchema, { selector: part(DocumentPart.BLOBS), state: PartState.PRESENT, sha256: 'b'.repeat(64), sizeBytes: 5n, writtenInRevision: 1n, owner: owner('ds') }),
        create(PartManifestEntrySchema, { selector: create(PartSelectorSchema, { selector: { case: 'segmentationId', value: 'seg-1' } }), state: PartState.PRESENT, writtenInRevision: 3n, owner: owner('ds') }),
      ],
    })
    const rows = partDiff(base, target)
    expect(rows.map((r) => [r.label, r.change])).toEqual([
      ['CORE', 'changed'],
      ['BLOBS', 'unchanged'],
      ['chunks seg-1', 'added'],
      ['PARSED', 'removed'],
    ])
    expect(rows[0].differs).toEqual(['sizeBytes', 'sha256', 'writtenInRevision'])
  })

  it('reads every part of a revision a tombstone replaced as removed', () => {
    const base = create(DocumentManifestSchema, { revision: 1n, parts: [create(PartManifestEntrySchema, { selector: part(DocumentPart.CORE), state: PartState.PRESENT })] })
    const tombstone = create(DocumentManifestSchema, { revision: 2n, tombstone: true })
    expect(partDiff(base, tombstone).map((r) => r.change)).toEqual(['removed'])
  })

  it('reports an emptied or deleted part with its reason', () => {
    const base = create(DocumentManifestSchema, { revision: 1n, parts: [create(PartManifestEntrySchema, { selector: part(DocumentPart.PARSED), state: PartState.PRESENT })] })
    const target = create(DocumentManifestSchema, {
      revision: 2n,
      parts: [create(PartManifestEntrySchema, { selector: part(DocumentPart.PARSED), state: PartState.DELETED, deletedReason: 'reparse requested' })],
    })
    const [row] = partDiff(base, target)
    expect(row.change).toBe('changed')
    expect(row.differs).toEqual(['state', 'deletedReason'])
    expect(row.target?.state).toBe('DELETED')
  })
})

/** A schema with two bound ConceptRef fields, an unbound one and a title, beside the real concepts and validate files. */
function taggedSchema(): { set: Uint8Array; desc: DescMessage } {
  const bound = () => {
    const options = create(FieldOptionsSchema)
    setExtension(options, fieldRules, create(FieldRulesSchema, { conceptSchemes: ['topics'] }))
    return options
  }
  const ref = '.ai.protomolt.proto.types.v1.ConceptRef'
  const file: FileDescriptorProto = create(FileDescriptorProtoSchema, {
    name: 'test/v1/tagged.proto',
    package: 'test.v1',
    syntax: 'proto3',
    dependency: ['ai/protomolt/proto/types/v1/concepts.proto', 'ai/protomolt/proto/validate/v1/validate.proto'],
    messageType: [
      {
        name: 'Tagged',
        field: [
          { name: 'subject', number: 1, jsonName: 'subject', label: FieldDescriptorProto_Label.OPTIONAL, type: FieldDescriptorProto_Type.MESSAGE, typeName: ref, options: bound() },
          { name: 'extra', number: 2, jsonName: 'extra', label: FieldDescriptorProto_Label.REPEATED, type: FieldDescriptorProto_Type.MESSAGE, typeName: ref, options: bound() },
          { name: 'untagged', number: 3, jsonName: 'untagged', label: FieldDescriptorProto_Label.OPTIONAL, type: FieldDescriptorProto_Type.MESSAGE, typeName: ref },
          { name: 'title', number: 4, jsonName: 'title', label: FieldDescriptorProto_Label.OPTIONAL, type: FieldDescriptorProto_Type.STRING },
        ],
      },
    ],
  })
  const files = new Map<string, FileDescriptorProto>()
  for (const dep of [file_ai_protomolt_proto_types_v1_concepts, file_ai_protomolt_proto_validate_v1_validate]) {
    for (const f of fromBinary(FileDescriptorSetSchema, descriptorSetOf(dep)).file) files.set(f.name, f)
  }
  files.set(file.name, file)
  const set = toBinary(FileDescriptorSetSchema, create(FileDescriptorSetSchema, { file: [...files.values()] }))
  const desc = createFileRegistry(fromBinary(FileDescriptorSetSchema, set)).getMessage('test.v1.Tagged')
  if (desc === undefined) throw new Error('test.v1.Tagged did not register')
  return { set, desc }
}

describe('flatten', () => {
  it('lists one line per set leaf and the bound ConceptRef fields as tags', () => {
    const { desc } = taggedSchema()
    const message = create(desc, {
      subject: { schemeId: 'topics', conceptId: 'energy' },
      extra: [{ schemeId: 'topics', conceptId: 'solar' }],
      untagged: { schemeId: 'topics', conceptId: 'not-a-tag' },
      title: 'Q3 "draft"',
    } as never)
    const flat = flatten(desc, message, AnySchemas.none())
    expect(flat.lines).toEqual([
      'subject.scheme_id = "topics"',
      'subject.concept_id = "energy"',
      'extra[0].scheme_id = "topics"',
      'extra[0].concept_id = "solar"',
      'untagged.scheme_id = "topics"',
      'untagged.concept_id = "not-a-tag"',
      'title = "Q3 \\"draft\\""',
    ])
    expect(flat.concepts).toEqual(['subject = topics/energy', 'extra[0] = topics/solar'])
  })

  it('enters a typed Any only with the schema the revision retained for that exact payload', async () => {
    const { set, desc } = taggedSchema()
    const payload = create(desc, { subject: { schemeId: 'topics', conceptId: 'wind' } } as never)
    const document = create(DocumentSchema, {
      docId: 'doc-1',
      searchMetadata: create(SearchMetadataSchema, { title: 'Wind report' }),
      structuredData: anyPack(desc, payload),
    })
    const packed = document.structuredData!
    const setSha = createHash('sha256').update(set).digest('hex')
    const validation = create(RevisionValidationSchema, {
      resolvedOccurrences: [
        create(ResolvedOccurrenceSchema, {
          schema: create(RepositorySchemaAssetReferenceSchema, { typeUrl: packed.typeUrl, descriptorSha256: setSha }),
          payloadSha256: createHash('sha256').update(packed.value).digest('hex'),
          payloadLength: BigInt(packed.value.length),
        }),
      ],
    })
    const asked: string[] = []
    const clients = routerClients((router) =>
      router.service(DocumentService, {
        getSchemaDescriptor(request) {
          asked.push(request.descriptorSha256)
          return create(GetSchemaDescriptorResponseSchema, { descriptorSet: set })
        },
      }),
    )
    const schemas = await AnySchemas.load(clients.documents, testAddress(), 2n, validation, new AbortController().signal)
    const flat = flatten(DocumentSchema, document, schemas)
    expect(asked).toEqual([setSha])
    expect(searchMetadataLines(flat.lines)).toEqual(['search_metadata.title = "Wind report"'])
    expect(flat.lines).toContain(`structured_data @type = ${packed.typeUrl}`)
    expect(flat.lines).toContain('structured_data.subject.concept_id = "wind"')
    expect(flat.concepts).toEqual(['structured_data.subject = topics/wind'])

    // Another payload of the same type was not resolved at this revision: it is not entered.
    const other = create(DocumentSchema, { structuredData: anyPack(desc, create(desc, { title: 'other' } as never)) })
    const unresolved = flatten(DocumentSchema, other, schemas)
    expect(unresolved.lines).toEqual([
      `structured_data @type = ${packed.typeUrl}`,
      `structured_data = ${other.structuredData!.value.length} B, no schema retained for this payload`,
    ])
    expect(unresolved.concepts).toEqual([])
  })

  it('refuses a descriptor set that does not hash to the retained digest', async () => {
    const { set } = taggedSchema()
    const validation = create(RevisionValidationSchema, {
      resolvedOccurrences: [
        create(ResolvedOccurrenceSchema, {
          schema: create(RepositorySchemaAssetReferenceSchema, { typeUrl: 'type.googleapis.com/test.v1.Tagged', descriptorSha256: 'f'.repeat(64) }),
          payloadSha256: 'e'.repeat(64),
        }),
      ],
    })
    const clients = routerClients((router) =>
      router.service(DocumentService, { getSchemaDescriptor: () => create(GetSchemaDescriptorResponseSchema, { descriptorSet: set }) }),
    )
    await expect(AnySchemas.load(clients.documents, testAddress(), 2n, validation, new AbortController().signal)).rejects.toThrow(
      `but revision 2 retained ${'f'.repeat(64)}`,
    )
  })
})

describe('linkLines and sectionDiff', () => {
  it('diffs links by field, relation and target, never by the target status now', () => {
    const link = (docId: string, unavailable: boolean) =>
      linkRow(
        create(ResolvedLinkSchema, {
          fieldPath: 'structured_data.cites',
          relation: 'references',
          target: { case: 'staticTarget', value: create(StaticLinkStateSchema, { address: testAddress({ docId }), targetUnavailable: unavailable }) },
        }),
      )
    const before = linkLines([link('doc-2', false), link('doc-3', false)])
    const after = linkLines([link('doc-2', true), link('doc-4', false)])
    expect(before[0]).toBe('structured_data.cites[0] references -> intake:acct-a/ds-files/doc-2')
    const diff = sectionDiff(before, after)
    expect(diff.stats).toEqual({ added: 1, removed: 1, unchanged: 1 })
    expect(diff.lines.filter((l) => l.op !== 'same').map((l) => `${l.op} ${l.text}`)).toEqual([
      'del structured_data.cites[0] references -> intake:acct-a/ds-files/doc-3',
      'add structured_data.cites[0] references -> intake:acct-a/ds-files/doc-4',
    ])
  })
})

