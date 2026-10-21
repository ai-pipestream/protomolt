import { createHash } from 'node:crypto'
import { create, toBinary } from '@bufbuild/protobuf'
import { BinaryWriter, WireType } from '@bufbuild/protobuf/wire'
import { describe, expect, it } from 'vitest'
import {
  DocumentSchema,
  SearchMetadataSchema,
  file_ai_protomolt_proto_repo_v1_document,
} from '@/gen/ai/protomolt/proto/repo/v1/document_pb'
import { RepositorySchemaAssetReferenceSchema } from '@/gen/ai/protomolt/proto/repo/v1/schema_asset_reference_pb'
import { file_ai_protomolt_proto_repo_v1_address } from '@/gen/ai/protomolt/proto/repo/v1/address_pb'
import {
  DocumentService,
  GetSchemaDescriptorResponseSchema,
  PartDataSchema,
  ReadCompleteSchema,
  ReadDocumentResponseSchema,
  type GetSchemaDescriptorRequest,
  type ReadDocumentRequest,
  type ReadDocumentResponse,
} from '@/gen/ai/protomolt/proto/repo/v2/document_service_pb'
import {
  DocumentManifestSchema,
  DocumentPart,
  PartManifestEntrySchema,
  PartSchemaBindingSchema,
  PartState,
  PartSelectorSchema,
  RevisionValidationSchema,
  ValidationStatus,
} from '@/gen/ai/protomolt/proto/repo/v2/manifest_pb'
import { decodedTree } from './decodedTreeCore'
import { PartStreamError, decodePart, readPart, typeNameOf } from './partReadCore'
import { at, descriptorSetOf, routerClients, testAddress } from './repositoryTestKit'

const CORE = create(PartSelectorSchema, { selector: { case: 'part', value: DocumentPart.CORE } })
const PARSED = create(PartSelectorSchema, { selector: { case: 'part', value: DocumentPart.PARSED } })

// A stored CORE fragment, plus one field (number 900) the retained schema does
// not define, the way a newer writer's bytes look to an older schema.
const document = create(DocumentSchema, {
  docId: 'doc-1',
  searchMetadata: create(SearchMetadataSchema, {
    title: 'Quarterly report',
    creationDate: at('2026-01-02T03:04:05Z'),
    metadata: { zeta: 'last', alpha: 'first' },
  }),
})
const unknownTail = new BinaryWriter().tag(900, WireType.LengthDelimited).string('from a newer schema').finish()
const STORED = new Uint8Array([...toBinary(DocumentSchema, document), ...unknownTail])

const DESCRIPTORS = descriptorSetOf(file_ai_protomolt_proto_repo_v1_document)
const DESCRIPTORS_SHA = createHash('sha256').update(DESCRIPTORS).digest('hex')

function manifestFrame(revision = 3n, contentIncluded = true): ReadDocumentResponse {
  return create(ReadDocumentResponseSchema, {
    frame: {
      case: 'manifest',
      value: create(DocumentManifestSchema, {
        address: testAddress(),
        revision,
        parts: [create(PartManifestEntrySchema, { selector: CORE, state: PartState.PRESENT, writtenInRevision: revision, contentIncluded })],
      }),
    },
  })
}
function dataFrame(offset: number, bytes: Uint8Array, last: boolean, selector = CORE): ReadDocumentResponse {
  return create(ReadDocumentResponseSchema, {
    frame: { case: 'partData', value: create(PartDataSchema, { selector, offset: BigInt(offset), data: bytes, last }) },
  })
}
const completeFrame = create(ReadDocumentResponseSchema, { frame: { case: 'complete', value: create(ReadCompleteSchema) } })

function validation(descriptorSha256 = DESCRIPTORS_SHA) {
  return create(RevisionValidationSchema, {
    status: ValidationStatus.TYPED_VALID,
    partSchemas: [
      create(PartSchemaBindingSchema, {
        part: CORE,
        fragmentSchema: create(RepositorySchemaAssetReferenceSchema, {
          typeUrl: 'type.googleapis.com/ai.protomolt.proto.repo.v1.Document',
          descriptorSha256,
        }),
      }),
    ],
  })
}

const signal = () => new AbortController().signal

describe('readPart', () => {
  it('assembles the part from contiguous frames and reports the pinned revision', async () => {
    const seen: ReadDocumentRequest[] = []
    const clients = routerClients((router) =>
      router.service(DocumentService, {
        async *readDocument(request) {
          seen.push(request)
          yield manifestFrame(3n)
          yield dataFrame(0, STORED.subarray(0, 10), false)
          yield dataFrame(10, STORED.subarray(10), true)
          yield completeFrame
        },
      }),
    )
    const part = await readPart(clients.documents, testAddress(), null, CORE, false, signal())
    expect(part.revision).toBe(3n)
    expect(part.bytes).toEqual(STORED)
    expect(seen[0].allowOpaque).toBe(false)
    expect(seen[0].selection?.selectors).toHaveLength(1)
  })

  it.each([
    ['ends without the completion frame', [manifestFrame(), dataFrame(0, STORED, true)], 'without its completion frame'],
    ['skips bytes', [manifestFrame(), dataFrame(0, STORED.subarray(0, 4), false), dataFrame(8, STORED.subarray(8), true), completeFrame], 'offset 8; the next byte is 4'],
    ['sends another part', [manifestFrame(), dataFrame(0, STORED, true, PARSED), completeFrame], 'bytes for PARSED, which was not requested'],
    ['completes before the last frame', [manifestFrame(), dataFrame(0, STORED, false), completeFrame], 'completed before the last frame'],
    ['sends data before the manifest', [dataFrame(0, STORED, true), completeFrame], 'before its manifest frame'],
    ['sends nothing', [], 'without a manifest frame'],
    ['completes with no frame for a part with content', [manifestFrame(), completeFrame], 'completed before the last frame of CORE'],
    ['answers for another revision', [manifestFrame(2n), dataFrame(0, STORED, true), completeFrame], 'answered for revision 2; revision 3 was asked for'],
    ['includes no content for the part', [manifestFrame(3n, false), completeFrame], 'CORE has no content at revision 3'],
  ])('refuses a stream that %s', async (_name, frames, message) => {
    const clients = routerClients((router) =>
      router.service(DocumentService, {
        async *readDocument() {
          yield* frames as ReadDocumentResponse[]
        },
      }),
    )
    const reading = readPart(clients.documents, testAddress(), 3n, CORE, false, signal())
    await expect(reading).rejects.toThrow(PartStreamError)
    await expect(readPart(clients.documents, testAddress(), 3n, CORE, false, signal())).rejects.toThrow(message)
  })
})

describe('decodePart', () => {
  function descriptorServer(bytes = DESCRIPTORS, seen: GetSchemaDescriptorRequest[] = []) {
    return routerClients((router) =>
      router.service(DocumentService, {
        getSchemaDescriptor(request) {
          seen.push(request)
          return create(GetSchemaDescriptorResponseSchema, {
            revision: 3n,
            descriptorSha256: request.descriptorSha256,
            descriptorSet: bytes,
          })
        },
      }),
    ).documents
  }

  it('decodes with the descriptor set retained by digest, keeping unknown fields', async () => {
    const seen: GetSchemaDescriptorRequest[] = []
    const decoded = await decodePart(descriptorServer(DESCRIPTORS, seen), testAddress(), 3n, validation(), CORE, STORED, signal())
    expect(seen[0].descriptorSha256).toBe(DESCRIPTORS_SHA)
    expect(seen[0].revision?.selector).toEqual({ case: 'revision', value: 3n })
    if (decoded.kind !== 'decoded') throw new Error('expected a decoded part')
    expect(decoded.desc.typeName).toBe('ai.protomolt.proto.repo.v1.Document')
    // The retained descriptor is a different object from the generated one.
    expect(decoded.desc).not.toBe(DocumentSchema)
    const tree = decodedTree(decoded.desc, decoded.message)
    expect(tree.map((n) => n.label)).toEqual(['doc_id', 'search_metadata', '(unknown fields)'])
    const meta = tree[1].children!
    expect(meta.find((n) => n.label === 'title')?.display).toBe('Quarterly report')
    expect(meta.find((n) => n.label === 'creation_date')?.display).toBe('2026-01-02T03:04:05.000Z')
    expect(meta.find((n) => n.label === 'metadata')?.children?.map((c) => c.label)).toEqual(['alpha', 'zeta'])
    expect(tree[2].children?.[0].label).toBe('#900')
  })

  it('shows raw bytes, saying why, when the revision binds no schema to the part', async () => {
    const decoded = await decodePart(descriptorServer(), testAddress(), 3n, validation(), PARSED, STORED, signal())
    expect(decoded).toEqual({ kind: 'raw', reason: 'Revision 3 binds no fragment schema to PARSED.', descriptorSha256: null })
  })

  it('refuses a descriptor set that does not hash to the retained digest', async () => {
    const other = descriptorSetOf(file_ai_protomolt_proto_repo_v1_address)
    await expect(
      decodePart(descriptorServer(other), testAddress(), 3n, validation(), CORE, STORED, signal()),
    ).rejects.toThrow(`but revision 3 retained ${DESCRIPTORS_SHA}`)
  })

  it('refuses a retained set that does not define the bound type', async () => {
    const other = descriptorSetOf(file_ai_protomolt_proto_repo_v1_address)
    const otherSha = createHash('sha256').update(other).digest('hex')
    await expect(
      decodePart(descriptorServer(other), testAddress(), 3n, validation(otherSha), CORE, STORED, signal()),
    ).rejects.toThrow(`does not define ai.protomolt.proto.repo.v1.Document`)
  })
})

describe('typeNameOf', () => {
  it('takes the name after the last slash', () => {
    expect(typeNameOf('type.googleapis.com/a.b.C')).toBe('a.b.C')
    expect(typeNameOf('a.b.C')).toBe('a.b.C')
  })
})
