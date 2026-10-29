import { clone, create, createFileRegistry, fromBinary, setExtension, toBinary, type DescMessage } from '@bufbuild/protobuf'
import {
  FieldDescriptorProto_Label,
  FieldDescriptorProto_Type,
  FieldOptionsSchema,
  FileDescriptorProtoSchema,
  FileDescriptorSetSchema,
  anyPack,
  AnySchema,
  Int64ValueSchema,
  type Any,
  type FileDescriptorProto,
} from '@bufbuild/protobuf/wkt'
import { createHash } from 'node:crypto'
import { describe, expect, it, vi } from 'vitest'
import { file_ai_protomolt_proto_types_v1_concepts } from '@/gen/ai/protomolt/proto/types/v1/concepts_pb'
import { DocumentSchema, SearchMetadataSchema } from '@/gen/ai/protomolt/proto/repo/v1/document_pb'
import { RepositorySchemaAssetReferenceSchema } from '@/gen/ai/protomolt/proto/repo/v1/schema_asset_reference_pb'
import { RepositoryOccurrenceKeyType, RepositorySchemaOccurrenceStepSchema, type RepositorySchemaOccurrenceStep } from '@/gen/ai/protomolt/proto/repo/v1/schema_occurrence_pb'
import { DocumentService, GetSchemaDescriptorResponseSchema, GetManifestResponseSchema, ReadDocumentResponseSchema, ListLinksResponseSchema } from '@/gen/ai/protomolt/proto/repo/v2/document_service_pb'
import {
  DocumentManifestSchema,
  DocumentPart,
  OpaqueOccurrenceSchema,
  PartManifestEntrySchema,
  PartSelectorSchema,
  PartState,
  ResolvedOccurrenceSchema,
  RevisionValidationSchema,
  SaveOwnerSchema,
  type PartSelector,
  type ResolvedOccurrence,
  type RevisionValidation,
} from '@/gen/ai/protomolt/proto/repo/v2/manifest_pb'
import { FieldRulesSchema, field as fieldRules, file_ai_protomolt_proto_validate_v1_validate } from '@/gen/ai/protomolt/proto/validate/v1/validate_pb'
import { linkRow } from './linksCore'
import { AnySchemas, filterSectionDiff, flatten, linkLines, partDiff, readSide, searchMetadataLines, sectionDiff } from './revisionDiffCore'
import { descriptorSetOf, routerClients, testAddress } from './repositoryTestKit'
import { decodedTree } from './decodedTreeCore'
import { filterViewTree, type ViewNode } from '@/lib/message-view/messageTree'
import { Sha256 } from './sha256'
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
function taggedSchema(subjectName = 'subject'): { set: Uint8Array; desc: DescMessage } {
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
          { name: subjectName, number: 1, jsonName: subjectName, label: FieldDescriptorProto_Label.OPTIONAL, type: FieldDescriptorProto_Type.MESSAGE, typeName: ref, options: bound() },
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

const digest = (bytes: Uint8Array) => createHash('sha256').update(bytes).digest('hex')
const fieldStep = (number: number) => create(RepositorySchemaOccurrenceStepSchema, { step: { case: 'fieldNumber', value: number } })
const indexStep = (index: number) => create(RepositorySchemaOccurrenceStepSchema, { step: { case: 'repeatedIndex', value: index } })

function boundaryStep(packed: Any, set: Uint8Array): RepositorySchemaOccurrenceStep {
  return create(RepositorySchemaOccurrenceStepSchema, { step: { case: 'anyBoundary', value: {
    typeUrl: packed.typeUrl, valueSha256: digest(packed.value), valueSizeBytes: BigInt(packed.value.length),
    outcome: { case: 'resolved', value: { artifactSha256: digest(set), schema: { typeName: packed.typeUrl.slice(packed.typeUrl.lastIndexOf('/') + 1) } } },
  } } })
}

function resolved(packed: Any, set: Uint8Array, steps: RepositorySchemaOccurrenceStep[], selector = part(DocumentPart.CORE)): ResolvedOccurrence {
  return create(ResolvedOccurrenceSchema, {
    occurrence: { part: selector, steps: [...steps, boundaryStep(packed, set)] },
    schema: { typeUrl: packed.typeUrl, descriptorSha256: digest(set) },
    payloadSha256: digest(packed.value), payloadLength: BigInt(packed.value.length),
  })
}

function anyContainer(mapType = FieldDescriptorProto_Type.STRING): { set: Uint8Array; desc: DescMessage } {
  const optional = FieldDescriptorProto_Label.OPTIONAL
  const repeated = FieldDescriptorProto_Label.REPEATED
  const message = FieldDescriptorProto_Type.MESSAGE
  const file = create(FileDescriptorProtoSchema, {
    name: 'container.proto', package: 'test', syntax: 'proto3', dependency: ['google/protobuf/any.proto'],
    messageType: [{
      name: 'Container',
      field: [
        { name: 'first', number: 1, label: optional, type: message, typeName: '.google.protobuf.Any' },
        { name: 'second', number: 2, label: optional, type: message, typeName: '.google.protobuf.Any' },
        { name: 'items', number: 3, label: repeated, type: message, typeName: '.google.protobuf.Any' },
        { name: 'entries', number: 4, label: repeated, type: message, typeName: '.test.Container.EntriesEntry' },
        { name: 'ordinary', number: 5, label: optional, type: message, typeName: '.test.Container.Ordinary' },
      ],
      nestedType: [
        { name: 'EntriesEntry', options: { mapEntry: true }, field: [
          { name: 'key', number: 1, label: optional, type: mapType },
          { name: 'value', number: 2, label: optional, type: message, typeName: '.google.protobuf.Any' },
        ] },
        { name: 'Ordinary', field: [{ name: 'payload', number: 7, label: optional, type: message, typeName: '.google.protobuf.Any' }] },
      ],
    }],
  })
  const set = toBinary(FileDescriptorSetSchema, create(FileDescriptorSetSchema, {
    file: [...fromBinary(FileDescriptorSetSchema, descriptorSetOf(AnySchema.file)).file, file],
  }))
  return { set, desc: createFileRegistry(fromBinary(FileDescriptorSetSchema, set)).getMessage('test.Container')! }
}

async function loadSchemas(validation: RevisionValidation, sets: Uint8Array[]) {
  const asked: string[] = []
  const clients = routerClients(router => router.service(DocumentService, {
    getSchemaDescriptor(request) {
      asked.push(request.descriptorSha256)
      const descriptorSet = sets.find(set => digest(set) === request.descriptorSha256)
      if (descriptorSet === undefined) throw new Error(`Unexpected descriptor ${request.descriptorSha256}`)
      return create(GetSchemaDescriptorResponseSchema, { descriptorSet })
    },
  }))
  const schemas = await AnySchemas.load(clients.documents, testAddress(), 2n, validation, new AbortController().signal)
  return { schemas, asked }
}

describe('flatten', () => {
  it.each(['type URL', 'digest', 'length'])('refuses a payload with a different %s at the retained location', async (change) => {
    const tagged = taggedSchema()
    const root = anyContainer()
    const packed = anyPack(tagged.desc, create(tagged.desc, { subject: { schemeId: 'topics', conceptId: 'wind' } } as never))
    const { schemas } = await loadSchemas(create(RevisionValidationSchema, {
      resolvedOccurrences: [resolved(packed, tagged.set, [fieldStep(1)])],
    }), [tagged.set])
    const changed = clone(AnySchema, packed)
    if (change === 'type URL') changed.typeUrl = `other.example/${tagged.desc.typeName}`
    else if (change === 'length') changed.value = new Uint8Array([...packed.value, 0])
    else changed.value[changed.value.length - 1] ^= 1
    const flat = flatten(root.desc, create(root.desc, { first: changed } as never), schemas, part(DocumentPart.CORE))
    expect(flat.concepts).toEqual([])
    expect(flat.lines).toEqual([`first @type = ${changed.typeUrl}`, `first = ${changed.value.length} B, no schema retained for this payload`])
  })

  it('distinguishes parts and named chunk sets at the same locator', async () => {
    const tagged = taggedSchema()
    const root = anyContainer()
    const packed = anyPack(tagged.desc, create(tagged.desc, { subject: { schemeId: 'topics', conceptId: 'wind' } } as never))
    const chunk = (name: string) => create(PartSelectorSchema, { selector: { case: 'segmentationId', value: name } })
    const selectors = [part(DocumentPart.CORE), part(DocumentPart.PARSED), chunk('1'), chunk('other')]
    const message = create(root.desc, { first: packed } as never)
    for (const selector of selectors) {
      const { schemas } = await loadSchemas(create(RevisionValidationSchema, {
        resolvedOccurrences: [resolved(packed, tagged.set, [fieldStep(1)], selector)],
      }), [tagged.set])
      for (const current of selectors) {
        expect(flatten(root.desc, message, schemas, current).concepts).toEqual(current === selector ? ['first.subject = topics/wind'] : [])
      }
    }
  })

  it('crosses consecutive Any boundaries only where each envelope was resolved', async () => {
    const tagged = taggedSchema()
    const root = anyContainer()
    const packed = anyPack(tagged.desc, create(tagged.desc, { subject: { schemeId: 'topics', conceptId: 'wind' } } as never))
    const outer = anyPack(AnySchema, packed)
    const anySet = descriptorSetOf(AnySchema.file)
    const validation = create(RevisionValidationSchema, { resolvedOccurrences: [
      resolved(outer, anySet, [fieldStep(1)]),
      resolved(packed, tagged.set, [fieldStep(1), boundaryStep(outer, anySet)]),
      resolved(outer, anySet, [fieldStep(2)]),
    ] })
    const { schemas } = await loadSchemas(validation, [anySet, tagged.set])
    const flat = flatten(root.desc, create(root.desc, { first: outer, second: outer } as never), schemas, part(DocumentPart.CORE))
    expect(flat.concepts).toEqual(['first.subject = topics/wind'])
    expect(flat.lines).toContain(`second = ${packed.value.length} B, no schema retained for this payload`)
  })

  it('propagates invalid typed protobuf bytes instead of treating them as opaque', async () => {
    const tagged = taggedSchema()
    const root = anyContainer()
    const packed = create(AnySchema, { typeUrl: `type.googleapis.com/${tagged.desc.typeName}`, value: new Uint8Array([10, 255]) })
    const { schemas } = await loadSchemas(create(RevisionValidationSchema, {
      resolvedOccurrences: [resolved(packed, tagged.set, [fieldStep(1)])],
    }), [tagged.set])
    expect(() => flatten(root.desc, create(root.desc, { first: packed } as never), schemas, part(DocumentPart.CORE))).toThrow()
  })

  it.each(['duplicate', 'opaque', 'terminal', 'missing locator'])('rejects conflicting or incomplete retained evidence: %s', async (failure) => {
    const tagged = taggedSchema()
    const packed = anyPack(tagged.desc, create(tagged.desc))
    const occurrence = resolved(packed, tagged.set, [fieldStep(1)])
    const validation = create(RevisionValidationSchema, { resolvedOccurrences: [occurrence] })
    let expected = 'conflicting Any occurrence bindings'
    if (failure === 'duplicate') validation.resolvedOccurrences.push(resolved(packed, tagged.set, [fieldStep(1)]))
    else if (failure === 'opaque') validation.opaqueOccurrences.push(create(OpaqueOccurrenceSchema, {
      occurrence: { part: part(DocumentPart.CORE), steps: [fieldStep(1)] },
      typeUrl: packed.typeUrl, sha256: digest(packed.value), length: BigInt(packed.value.length),
    }))
    else if (failure === 'terminal') {
      occurrence.payloadLength += 1n
      expected = 'Any boundary that differs from its retained binding'
    } else {
      occurrence.occurrence = undefined
      expected = 'no complete locator'
    }
    await expect(loadSchemas(validation, [tagged.set])).rejects.toThrow(expected)
  })

  it('keeps distinct retained schemas for identical bytes and type URLs', async () => {
    const first = taggedSchema()
    const second = taggedSchema('category')
    const root = anyContainer()
    const packed = anyPack(first.desc, create(first.desc, { subject: { schemeId: 'topics', conceptId: 'wind' } } as never))
    const validation = create(RevisionValidationSchema, { resolvedOccurrences: [
      resolved(packed, first.set, [fieldStep(1)]),
      resolved(packed, second.set, [fieldStep(2)]),
      resolved(packed, first.set, [fieldStep(3), indexStep(0)]),
    ] })
    const { schemas, asked } = await loadSchemas(validation, [first.set, second.set])
    const flat = flatten(root.desc, create(root.desc, { first: packed, second: packed, items: [packed, packed] } as never), schemas, part(DocumentPart.CORE))
    expect(flat.concepts).toEqual(['first.subject = topics/wind', 'second.category = topics/wind', 'items[0].subject = topics/wind'])
    expect(flat.lines).toContain(`items[1] = ${packed.value.length} B, no schema retained for this payload`)
    expect(asked).toEqual([digest(first.set), digest(second.set)])
  })

  it('requires each ordinary field, repeated index and intermediate Any boundary', async () => {
    const tagged = taggedSchema()
    const root = anyContainer()
    const packed = anyPack(tagged.desc, create(tagged.desc, { subject: { schemeId: 'topics', conceptId: 'wind' } } as never))
    const inner = anyPack(root.desc, create(root.desc, { first: packed, second: packed, items: [packed, packed], ordinary: { payload: packed } } as never))
    const prefix = [fieldStep(1), boundaryStep(inner, root.set)]
    const validation = create(RevisionValidationSchema, { resolvedOccurrences: [
      resolved(inner, root.set, [fieldStep(1)]),
      resolved(packed, tagged.set, [...prefix, fieldStep(2)]),
      resolved(packed, tagged.set, [...prefix, fieldStep(3), indexStep(1)]),
      resolved(packed, tagged.set, [...prefix, fieldStep(5), fieldStep(7)]),
    ], opaqueOccurrences: [{
      occurrence: { part: part(DocumentPart.CORE), steps: [...prefix, fieldStep(1)] },
      typeUrl: packed.typeUrl, sha256: digest(packed.value), length: BigInt(packed.value.length),
    }] })
    const { schemas } = await loadSchemas(validation, [root.set, tagged.set])
    const flat = flatten(root.desc, create(root.desc, { first: inner, second: inner } as never), schemas, part(DocumentPart.CORE))
    expect(flat.concepts).toEqual(['first.second.subject = topics/wind', 'first.items[1].subject = topics/wind', 'first.ordinary.payload.subject = topics/wind'])
    expect(flat.lines).toContain(`first.first = ${packed.value.length} B, no schema retained for this payload`)
    expect(flat.lines).toContain(`second = ${inner.value.length} B, no schema retained for this payload`)
    for (const change of ['typeUrl', 'valueSha256', 'valueSizeBytes', 'artifactSha256', 'typeName', 'descriptorFingerprint'] as const) {
      const altered = clone(RevisionValidationSchema, validation)
      const nested = altered.resolvedOccurrences.slice(1)
      for (const occurrence of nested) {
        const boundary = occurrence.occurrence!.steps[1].step
        if (boundary.case !== 'anyBoundary' || boundary.value.outcome.case !== 'resolved') throw new Error('Expected boundary')
        if (change === 'artifactSha256') boundary.value.outcome.value.artifactSha256 = 'f'.repeat(64)
        else if (change === 'typeName' || change === 'descriptorFingerprint') boundary.value.outcome.value.schema![change] += 'changed'
        else if (change === 'valueSizeBytes') boundary.value.valueSizeBytes += 1n
        else boundary.value[change] += 'changed'
      }
      const alteredSchemas = await loadSchemas(altered, [root.set, tagged.set])
      expect(flatten(root.desc, create(root.desc, { first: inner } as never), alteredSchemas.schemas, part(DocumentPart.CORE)).concepts).toEqual([])
    }
  })

  it('reuses precomputed Any boundary digests without re-hashing ancestor steps', async () => {
    const tagged = taggedSchema()
    const root = anyContainer()
    const leaf = anyPack(tagged.desc, create(tagged.desc, { subject: { schemeId: 'topics', conceptId: 'wind' }, title: 'Leaf' } as never))
    const middle = anyPack(root.desc, create(root.desc, { first: leaf } as never))
    const outer = anyPack(root.desc, create(root.desc, { first: middle } as never))
    const validation = create(RevisionValidationSchema, {
      resolvedOccurrences: [
        resolved(outer, root.set, [fieldStep(1)]),
        resolved(middle, root.set, [fieldStep(1), boundaryStep(outer, root.set), fieldStep(1)]),
        resolved(leaf, tagged.set, [fieldStep(1), boundaryStep(outer, root.set), fieldStep(1), boundaryStep(middle, root.set), fieldStep(1)]),
      ],
    })
    const { schemas } = await loadSchemas(validation, [root.set, tagged.set])
    const spy = vi.spyOn(Sha256.prototype, 'update')
    try {
      const flat = flatten(root.desc, create(root.desc, { first: outer } as never), schemas, part(DocumentPart.CORE))
      expect(flat.concepts).toEqual(['first.first.first.subject = topics/wind'])
      expect(flat.lines).toContain('first.first.first.title = "Leaf"')
      expect(spy).toHaveBeenCalledTimes(3)
    } finally {
      spy.mockRestore()
    }
  })

  it.each([
    [FieldDescriptorProto_Type.STRING, RepositoryOccurrenceKeyType.STRING, 'stringValue', '', 'x'],
    [FieldDescriptorProto_Type.STRING, RepositoryOccurrenceKeyType.STRING, 'stringValue', 'a\u0000:b[\"/雪', 'a'],
    [FieldDescriptorProto_Type.BOOL, RepositoryOccurrenceKeyType.BOOL, 'boolValue', false, true],
    [FieldDescriptorProto_Type.INT32, RepositoryOccurrenceKeyType.INT32, 'signedValue', -2147483648n, 0n],
    [FieldDescriptorProto_Type.SINT32, RepositoryOccurrenceKeyType.SINT32, 'signedValue', 0n, -1n],
    [FieldDescriptorProto_Type.SFIXED32, RepositoryOccurrenceKeyType.SFIXED32, 'signedValue', 2147483647n, 0n],
    [FieldDescriptorProto_Type.UINT32, RepositoryOccurrenceKeyType.UINT32, 'unsignedValue', 4294967295n, 0n],
    [FieldDescriptorProto_Type.FIXED32, RepositoryOccurrenceKeyType.FIXED32, 'unsignedValue', 0n, 4294967295n],
    [FieldDescriptorProto_Type.INT64, RepositoryOccurrenceKeyType.INT64, 'signedValue', -9223372036854775808n, 0n],
    [FieldDescriptorProto_Type.SINT64, RepositoryOccurrenceKeyType.SINT64, 'signedValue', 0n, -1n],
    [FieldDescriptorProto_Type.SFIXED64, RepositoryOccurrenceKeyType.SFIXED64, 'signedValue', 9223372036854775807n, 0n],
    [FieldDescriptorProto_Type.UINT64, RepositoryOccurrenceKeyType.UINT64, 'unsignedValue', 18446744073709551615n, 0n],
    [FieldDescriptorProto_Type.FIXED64, RepositoryOccurrenceKeyType.FIXED64, 'unsignedValue', 0n, 18446744073709551615n],
  ] as const)('matches typed map keys exactly (%s, %s, %s, %s)', async (mapType, keyType, kind, key, other) => {
    const tagged = taggedSchema()
    const root = anyContainer(mapType)
    const packed = anyPack(tagged.desc, create(tagged.desc, { subject: { schemeId: 'topics', conceptId: 'wind' } } as never))
    const mapStep = create(RepositorySchemaOccurrenceStepSchema, { step: { case: 'mapKey', value: {
      type: keyType, value: { case: kind, value: key } as never,
    } } })
    const validation = create(RevisionValidationSchema, { resolvedOccurrences: [resolved(packed, tagged.set, [fieldStep(4), mapStep])] })
    const { schemas } = await loadSchemas(validation, [tagged.set])
    const message = create(root.desc, { entries: { [String(other)]: packed, [String(key)]: packed } } as never)
    const flat = flatten(root.desc, message, schemas, part(DocumentPart.CORE))
    expect(flat.concepts).toEqual([`entries[${JSON.stringify(String(key))}].subject = topics/wind`])
    expect(flat.lines).toContain(`entries[${JSON.stringify(String(other))}] = ${packed.value.length} B, no schema retained for this payload`)
    if (mapStep.step.case !== 'mapKey') throw new Error('Expected map key')
    mapStep.step.value.type = keyType === RepositoryOccurrenceKeyType.STRING ? RepositoryOccurrenceKeyType.BOOL : RepositoryOccurrenceKeyType.STRING
    const mismatched = await loadSchemas(create(RevisionValidationSchema, { resolvedOccurrences: [resolved(packed, tagged.set, [fieldStep(4), mapStep])] }), [tagged.set])
    expect(flatten(root.desc, message, mismatched.schemas, part(DocumentPart.CORE)).concepts).toEqual([])
  })

  it('keeps equal bytes at an opaque sibling separate from a typed occurrence', async () => {
    const { set, desc } = taggedSchema()
    const packed = anyPack(desc, create(desc, { subject: { schemeId: 'topics', conceptId: 'wind' } } as never))
    const wrapper = create(FileDescriptorProtoSchema, {
      name: 'siblings.proto', package: 'test', syntax: 'proto3', dependency: ['google/protobuf/any.proto'],
      messageType: [{ name: 'Siblings', field: ['opaque', 'typed'].map((name, index) => ({
        name, number: index + 1, label: FieldDescriptorProto_Label.OPTIONAL,
        type: FieldDescriptorProto_Type.MESSAGE, typeName: '.google.protobuf.Any',
      })) }],
    })
    const registry = createFileRegistry(create(FileDescriptorSetSchema, {
      file: [...fromBinary(FileDescriptorSetSchema, descriptorSetOf(AnySchema.file)).file, wrapper],
    }))
    const root = registry.getMessage('test.Siblings')!
    const validation = create(RevisionValidationSchema, { resolvedOccurrences: [{
      occurrence: { part: part(DocumentPart.CORE), steps: [{ step: { case: 'fieldNumber', value: 2 } }] },
      schema: { typeUrl: packed.typeUrl, descriptorSha256: createHash('sha256').update(set).digest('hex') },
      payloadSha256: createHash('sha256').update(packed.value).digest('hex'),
      payloadLength: BigInt(packed.value.length),
    }] })
    const clients = routerClients(router => router.service(DocumentService, {
      getSchemaDescriptor: () => create(GetSchemaDescriptorResponseSchema, { descriptorSet: set }),
    }))
    const schemas = await AnySchemas.load(clients.documents, testAddress(), 2n, validation, new AbortController().signal)
    const flat = flatten(root, create(root, { opaque: packed, typed: packed } as never), schemas, part(DocumentPart.CORE))
    expect(flat.concepts).toEqual(['typed.subject = topics/wind'])
    expect(flat.lines).toContain(`opaque = ${packed.value.length} B, no schema retained for this payload`)
  })

  it('lists one line per set leaf and the bound ConceptRef fields as tags', () => {
    const { desc } = taggedSchema()
    const message = create(desc, {
      subject: { schemeId: 'topics', conceptId: 'energy' },
      extra: [{ schemeId: 'topics', conceptId: 'solar' }],
      untagged: { schemeId: 'topics', conceptId: 'not-a-tag' },
      title: 'Q3 "draft"',
    } as never)
    const flat = flatten(desc, message, AnySchemas.none(), part(DocumentPart.CORE))
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
          occurrence: { part: part(DocumentPart.CORE), steps: [{ step: { case: 'fieldNumber', value: 4 } }] },
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
    const flat = flatten(DocumentSchema, document, schemas, part(DocumentPart.CORE))
    expect(asked).toEqual([setSha])
    expect(searchMetadataLines(flat.lines)).toEqual(['search_metadata.title = "Wind report"'])
    expect(flat.lines).toContain(`structured_data @type = ${packed.typeUrl}`)
    expect(flat.lines).toContain('structured_data.subject.concept_id = "wind"')
    expect(flat.concepts).toEqual(['structured_data.subject = topics/wind'])

    // Another payload of the same type was not resolved at this revision: it is not entered.
    const other = create(DocumentSchema, { structuredData: anyPack(desc, create(desc, { title: 'other' } as never)) })
    const unresolved = flatten(DocumentSchema, other, schemas, part(DocumentPart.CORE))
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
          occurrence: { part: part(DocumentPart.CORE), steps: [{ step: { case: 'fieldNumber', value: 4 } }] },
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

describe('readSide', () => {
  it.each([false, true])('uses the selected part through retained fragment decoding (PARSED typed: %s)', async (parsedTyped) => {
    const root = anyContainer()
    const tagged = taggedSchema()
    const alternate = taggedSchema('category')
    const core = part(DocumentPart.CORE)
    const parsed = part(DocumentPart.PARSED)
    const packed = anyPack(tagged.desc, create(tagged.desc, { subject: { schemeId: 'topics', conceptId: 'wind' } } as never))
    const bytes = toBinary(root.desc, create(root.desc, { first: packed } as never))
    const manifest = create(DocumentManifestSchema, { revision: 2n, parts: [core, parsed].map(selector => ({
      selector, state: PartState.PRESENT, contentIncluded: true, sizeBytes: BigInt(bytes.length), sha256: digest(bytes),
    })) })
    const validation = create(RevisionValidationSchema, {
      partSchemas: [core, parsed].map(selector => ({ part: selector, fragmentSchema: { typeUrl: `type.googleapis.com/${root.desc.typeName}`, descriptorSha256: digest(root.set) } })),
      resolvedOccurrences: [resolved(packed, tagged.set, [fieldStep(1)], core), ...(parsedTyped ? [resolved(packed, alternate.set, [fieldStep(1)], parsed)] : [])],
    })
    const read: PartSelector[] = []
    const clients = routerClients(router => router.service(DocumentService, {
      getManifest(request) {
        expect(request.revision?.selector).toEqual({ case: 'revision', value: 2n })
        return create(GetManifestResponseSchema, { manifest, validation })
      },
      getSchemaDescriptor(request) {
        expect(request.revision?.selector).toEqual({ case: 'revision', value: 2n })
        const descriptorSet = [root.set, tagged.set, alternate.set].find(set => digest(set) === request.descriptorSha256)
        if (descriptorSet === undefined) throw new Error('Unexpected descriptor')
        return create(GetSchemaDescriptorResponseSchema, { descriptorSet })
      },
      async *readDocument(request) {
        expect(request.revision?.selector).toEqual({ case: 'revision', value: 2n })
        const selector = request.selection!.selectors[0]
        read.push(selector)
        yield create(ReadDocumentResponseSchema, { frame: { case: 'manifest', value: manifest } })
        yield create(ReadDocumentResponseSchema, { frame: { case: 'partData', value: { selector, data: bytes, last: true } } })
        yield create(ReadDocumentResponseSchema, { frame: { case: 'complete', value: {} } })
      },
      listLinks: () => create(ListLinksResponseSchema),
    }))
    const side = await readSide(clients.documents, testAddress(), 2n, new AbortController().signal)
    expect(read).toEqual([core, parsed])
    expect(side.core).toMatchObject({ kind: 'lines', flattened: { concepts: ['first.subject = topics/wind'] } })
    expect(side.parsed).toMatchObject({ kind: 'lines', flattened: { concepts: parsedTyped ? ['first.category = topics/wind'] : [] } })
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


/** Fully expand only in tests; the screen expands branches on demand. */
function inspected(nodes: ViewNode[]): string[] {
  return nodes.flatMap(node => [node.label, node.display ?? '', node.detail ?? '', ...inspected(node.loadChildren?.() ?? node.children ?? [])])
}

describe('retained Any inspector', () => {
  it('preserves exact int64 and unknown fields inside a resolved Any', async () => {
    const root = anyContainer()
    const set = descriptorSetOf(Int64ValueSchema.file)
    const packed = anyPack(Int64ValueSchema, create(Int64ValueSchema, { value: 9007199254740993n }))
    packed.value = new Uint8Array([...packed.value, 0xa0, 6, 1])
    const { schemas } = await loadSchemas(create(RevisionValidationSchema, { resolvedOccurrences: [resolved(packed, set, [fieldStep(1)])] }), [set])
    const tree = decodedTree(root.desc, create(root.desc, { first: packed } as never), { schemas, location: { part: part(DocumentPart.CORE), steps: [] } })
    expect(inspected(tree)).toContain('9007199254740993')
    expect(inspected(tree)).toContain('#100')
  })

  it('stops a very deep ordinary message at an explicit display boundary', () => {
    const file = create(FileDescriptorProtoSchema, { name: 'deep.proto', package: 'test', syntax: 'proto3', messageType: [{ name: 'Node', field: [{ name: 'child', number: 1, type: FieldDescriptorProto_Type.MESSAGE, typeName: '.test.Node', label: FieldDescriptorProto_Label.OPTIONAL }] }] })
    const desc = createFileRegistry(create(FileDescriptorSetSchema, { file: [file] })).getMessage('test.Node')!
    const message = create(desc)
    let at = message as unknown as { child?: unknown }
    for (let i = 0; i < 10000; i++) { const child = create(desc); at.child = child; at = child as unknown as { child?: unknown } }
    const tree = decodedTree(desc, message, { schemas: AnySchemas.none(), location: { part: part(DocumentPart.CORE), steps: [] } })
    expect(inspected(tree).join(' ')).toContain('Inspection depth limit reached')
  })

  it('lazily expands consecutive Any envelopes and distinguishes opaque same-byte siblings', async () => {
    const tagged = taggedSchema()
    const root = anyContainer()
    const packed = anyPack(tagged.desc, create(tagged.desc, { title: 'Only typed content' } as never))
    const outer = anyPack(AnySchema, packed)
    const anySet = descriptorSetOf(AnySchema.file)
    const { schemas } = await loadSchemas(create(RevisionValidationSchema, {
      resolvedOccurrences: [resolved(outer, anySet, [fieldStep(1)]), resolved(packed, tagged.set, [fieldStep(1), boundaryStep(outer, anySet)])],
      opaqueOccurrences: [{ occurrence: { part: part(DocumentPart.CORE), steps: [fieldStep(2)] }, typeUrl: outer.typeUrl, sha256: digest(outer.value), length: BigInt(outer.value.length) }],
    }), [anySet, tagged.set])
    const tree = decodedTree(root.desc, create(root.desc, { first: outer, second: outer } as never), { schemas, location: { part: part(DocumentPart.CORE), steps: [] } })
    expect(tree[0].children).toBeUndefined()
    expect(tree[0].loadChildren).toBeTypeOf('function')
    expect(inspected([tree[0]])).toContain('Only typed content')
    expect(inspected([tree[1]]).join(' ')).toContain('Opaque: payload not decoded')
    expect(inspected([tree[1]])).not.toContain('Only typed content')
  })

  it('uses repeated indices and map keys without lending schemas to neighbors', async () => {
    const tagged = taggedSchema()
    const root = anyContainer()
    const packed = anyPack(tagged.desc, create(tagged.desc, { title: 'Resolved' } as never))
    const mapKey = create(RepositorySchemaOccurrenceStepSchema, { step: { case: 'mapKey', value: { type: RepositoryOccurrenceKeyType.STRING, value: { case: 'stringValue', value: 'yes' } } } })
    const { schemas } = await loadSchemas(create(RevisionValidationSchema, { resolvedOccurrences: [
      resolved(packed, tagged.set, [fieldStep(3), indexStep(1)]), resolved(packed, tagged.set, [fieldStep(4), mapKey]),
    ] }), [tagged.set])
    const tree = decodedTree(root.desc, create(root.desc, { items: [packed, packed], entries: { no: packed, yes: packed } } as never), { schemas, location: { part: part(DocumentPart.CORE), steps: [] } })
    const items = tree[0].loadChildren!()
    expect(inspected([items[0]])).not.toContain('Resolved')
    expect(inspected([items[1]])).toContain('Resolved')
    const entries = tree[1].loadChildren!()
    expect(inspected([entries[0]])).not.toContain('Resolved')
    expect(inspected([entries[1]])).toContain('Resolved')
  })

  it('shows a mismatched binding and malformed typed bytes as different failures', async () => {
    const tagged = taggedSchema()
    const root = anyContainer()
    const packed = create(AnySchema, { typeUrl: `type.googleapis.com/${tagged.desc.typeName}`, value: new Uint8Array([10, 255]) })
    const changed = clone(AnySchema, packed)
    changed.value = new Uint8Array([10, 0])
    const { schemas } = await loadSchemas(create(RevisionValidationSchema, { resolvedOccurrences: [resolved(packed, tagged.set, [fieldStep(1)]), resolved(packed, tagged.set, [fieldStep(2)])] }), [tagged.set])
    const tree = decodedTree(root.desc, create(root.desc, { first: packed, second: changed } as never), { schemas, location: { part: part(DocumentPart.CORE), steps: [] } })
    expect(inspected([tree[0]])).toContain('(invalid payload)')
    expect(inspected([tree[1]]).join(' ')).toContain('Payload differs from its retained binding')
    expect(inspected([tree[1]])).not.toContain('(decoded payload)')
  })

  it('uses each occurrence’s own descriptor even when names and bytes match', async () => {
    const first = taggedSchema()
    const second = taggedSchema('category')
    const root = anyContainer()
    const packed = anyPack(first.desc, create(first.desc, { subject: { schemeId: 'topics', conceptId: 'wind' } } as never))
    const { schemas } = await loadSchemas(create(RevisionValidationSchema, { resolvedOccurrences: [resolved(packed, first.set, [fieldStep(1)]), resolved(packed, second.set, [fieldStep(2)])] }), [first.set, second.set])
    const tree = decodedTree(root.desc, create(root.desc, { first: packed, second: packed } as never), { schemas, location: { part: part(DocumentPart.CORE), steps: [] } })
    expect(inspected([tree[0]])).toContain('subject')
    expect(inspected([tree[0]])).not.toContain('category')
    expect(inspected([tree[1]])).toContain('category')
  })

  it('pages large collections lazily without hiding the remaining entries', () => {
    const root = anyContainer()
    const tree = decodedTree(root.desc, create(root.desc, { items: Array.from({ length: 10000 }, () => create(AnySchema)) } as never), { schemas: AnySchemas.none(), location: { part: part(DocumentPart.CORE), steps: [] } })
    expect(tree[0].children).toBeUndefined()
    const children = tree[0].loadChildren!()
    expect(children).toHaveLength(201)
    expect(children[200].label).toBe('(more entries)')
    expect(children[200].loadChildren!()[0].label).toBe('[200]')
    expect(tree[0].loadChildren!()).toBe(children)
  })

  it('filters a decoded tree across lazy Any nodes and filters section diffs by line or changed-only', async () => {
    const tagged = taggedSchema()
    const root = anyContainer()
    const packed = anyPack(tagged.desc, create(tagged.desc, { title: 'Needle in Any' } as never))
    const { schemas } = await loadSchemas(
      create(RevisionValidationSchema, { resolvedOccurrences: [resolved(packed, tagged.set, [fieldStep(1)])] }),
      [tagged.set],
    )
    const tree = decodedTree(root.desc, create(root.desc, { first: packed } as never), {
      schemas,
      location: { part: part(DocumentPart.CORE), steps: [] },
    })
    const hit = filterViewTree(tree, 'needle')
    expect(hit.matchCount).toBe(1)
    expect(hit.nodes).toHaveLength(1)
    expect(hit.nodes[0].defaultOpen).toBe(true)
    expect(inspected(hit.nodes)).toContain('Needle in Any')
    expect(filterViewTree(tree, 'nonexistent-value').nodes).toEqual([])

    const diff = sectionDiff(['title = "Draft"', 'language = "en"'], ['title = "Final"', 'language = "en"'])
    const changed = filterSectionDiff(diff, '', true)
    expect(changed.lines.map((l) => l.op)).toEqual(['del', 'add'])
    const byText = filterSectionDiff(diff, 'final', false)
    expect(byText.lines).toHaveLength(1)
    expect(byText.lines[0].text).toContain('Final')
  })
})
