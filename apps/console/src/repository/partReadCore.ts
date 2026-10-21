/**
 * Reading one stored part and decoding it with the schema its revision
 * retained. ReadDocument streams the manifest (which pins the revision), the
 * part's bytes and a completion frame; a stream without completion is not a
 * result. GetSchemaDescriptor returns the descriptor set by the digest the
 * revision's validation record binds, which is checked again here before use.
 * A revision with no binding for the part is shown as raw bytes, saying why.
 */
import { create, createFileRegistry, equals, fromBinary, type DescMessage, type Message } from '@bufbuild/protobuf'
import { FileDescriptorSetSchema } from '@bufbuild/protobuf/wkt'
import type { Client } from '@connectrpc/connect'
import type { NodeAddress } from '@/gen/ai/protomolt/proto/repo/v1/address_pb'
import {
  GetSchemaDescriptorRequestSchema,
  ReadDocumentRequestSchema,
  type DocumentService,
} from '@/gen/ai/protomolt/proto/repo/v2/document_service_pb'
import {
  PartSelectionSchema,
  PartSelectorSchema,
  type DocumentManifest,
  type PartSelector,
  type RevisionValidation,
} from '@/gen/ai/protomolt/proto/repo/v2/manifest_pb'
import { linkedController } from './blobReadCore'
import { partLabel, revisionSelector } from './documentCore'
import { Sha256 } from './sha256'

/** The stream broke the ReadDocument contract. */
export class PartStreamError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'PartStreamError'
  }
}

export interface PartBytes {
  /** The revision the manifest frame pinned. */
  revision: bigint
  bytes: Uint8Array
}

/**
 * Reads one part's stored bytes. `allowOpaque` is the request flag of the same
 * name; it is never set unless the user asks for it.
 */
export async function readPart(
  client: Client<typeof DocumentService>,
  address: NodeAddress,
  revision: bigint | null,
  selector: PartSelector,
  allowOpaque: boolean,
  signal: AbortSignal,
): Promise<PartBytes> {
  const request = create(ReadDocumentRequestSchema, {
    address,
    revision: revisionSelector(revision),
    selection: create(PartSelectionSchema, { selectors: [selector] }),
    allowOpaque,
  })
  let manifest: DocumentManifest | null = null
  const chunks: Uint8Array[] = []
  let size = 0n
  let last = false
  let complete = false
  const call = linkedController(signal)
  try {
    for await (const response of client.readDocument(request, { signal: call.controller.signal })) {
      const frame = response.frame
      if (complete) throw new PartStreamError('ReadDocument sent a frame after its completion frame.')
      switch (frame.case) {
        case 'manifest':
          if (manifest !== null) throw new PartStreamError('ReadDocument sent a second manifest frame.')
          manifest = frame.value
          break
        case 'partData': {
          if (manifest === null) throw new PartStreamError('ReadDocument sent part data before its manifest frame.')
          const data = frame.value
          if (data.selector === undefined || !equals(PartSelectorSchema, data.selector, selector)) {
            throw new PartStreamError(`ReadDocument sent bytes for ${partLabel(data.selector)}, which was not requested.`)
          }
          if (last) throw new PartStreamError(`ReadDocument sent bytes for ${partLabel(selector)} after its last frame.`)
          if (data.offset !== size) {
            throw new PartStreamError(`ReadDocument sent a frame at offset ${data.offset}; the next byte is ${size}.`)
          }
          chunks.push(data.data)
          size += BigInt(data.data.length)
          last = data.last
          break
        }
        case 'complete':
          complete = true
          break
        case undefined:
          throw new PartStreamError('ReadDocument sent an empty frame.')
      }
    }
  } finally {
    call.release()
  }
  if (manifest === null) throw new PartStreamError('ReadDocument ended without a manifest frame.')
  if (revision !== null && manifest.revision !== revision) {
    throw new PartStreamError(`ReadDocument answered for revision ${manifest.revision}; revision ${revision} was asked for.`)
  }
  if (!complete) throw new PartStreamError('ReadDocument ended without its completion frame; the read is incomplete.')
  const entry = manifest.parts.find((p) => p.selector !== undefined && equals(PartSelectorSchema, p.selector, selector))
  if (entry === undefined || !entry.contentIncluded) {
    throw new PartStreamError(`${partLabel(selector)} has no content at revision ${manifest.revision}.`)
  }
  // The server ends every included part with a last frame, an empty part included.
  if (!last) throw new PartStreamError(`ReadDocument completed before the last frame of ${partLabel(selector)}.`)
  const bytes = new Uint8Array(Number(size))
  let at = 0
  for (const c of chunks) {
    bytes.set(c, at)
    at += c.length
  }
  return { revision: manifest.revision, bytes }
}

/** A part decoded with its retained schema, or the reason it is shown raw. */
export type DecodedPart =
  | { kind: 'decoded'; desc: DescMessage; message: Message; typeUrl: string; descriptorSha256: string }
  | { kind: 'raw'; reason: string; descriptorSha256: string | null }

/** The message name a type URL names: the text after its last slash. */
export function typeNameOf(typeUrl: string): string {
  const slash = typeUrl.lastIndexOf('/')
  return slash < 0 ? typeUrl : typeUrl.slice(slash + 1)
}

/**
 * Decodes `bytes` as the fragment type the revision's validation record binds
 * to `selector`, using the descriptor set retained under that digest.
 */
export async function decodePart(
  client: Client<typeof DocumentService>,
  address: NodeAddress,
  revision: bigint,
  validation: RevisionValidation,
  selector: PartSelector,
  bytes: Uint8Array,
  signal: AbortSignal,
): Promise<DecodedPart> {
  const binding = validation.partSchemas.find(
    (b) => b.part !== undefined && equals(PartSelectorSchema, b.part, selector),
  )
  if (binding === undefined) {
    return { kind: 'raw', reason: `Revision ${revision} binds no fragment schema to ${partLabel(selector)}.`, descriptorSha256: null }
  }
  const schema = binding.fragmentSchema
  if (schema === undefined) throw new Error(`The fragment schema binding for ${partLabel(selector)} has no reference.`)
  const response = await client.getSchemaDescriptor(
    create(GetSchemaDescriptorRequestSchema, {
      address,
      revision: revisionSelector(revision),
      descriptorSha256: schema.descriptorSha256,
    }),
    { signal },
  )
  const actual = new Sha256().update(response.descriptorSet).hex()
  if (actual !== schema.descriptorSha256) {
    throw new Error(`The descriptor set hashes to ${actual}, but revision ${revision} retained ${schema.descriptorSha256}.`)
  }
  const registry = createFileRegistry(fromBinary(FileDescriptorSetSchema, response.descriptorSet))
  const typeName = typeNameOf(schema.typeUrl)
  const desc = registry.getMessage(typeName)
  if (desc === undefined) {
    throw new Error(`The retained descriptor set ${schema.descriptorSha256} does not define ${typeName}.`)
  }
  return {
    kind: 'decoded',
    desc,
    message: fromBinary(desc, bytes),
    typeUrl: schema.typeUrl,
    descriptorSha256: schema.descriptorSha256,
  }
}
