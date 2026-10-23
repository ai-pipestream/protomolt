/**
 * Comparing two revisions of one document: the manifest's parts entry by
 * entry, and line diffs of the decoded CORE and PARSED fragments, their
 * search metadata, their outbound links and their bound SKOS concept tags.
 *
 * A decoded fragment is flattened into one line per set leaf
 * (`search_metadata.title = "Q3"`), so the textDiff line diff lines up
 * changes by field path. Lines come from protobuf reflection over the message
 * decoded with the schema the revision retained, never from JSON. A typed Any
 * is entered without a path segment of its own, as the repository walks it,
 * only where the revision's validation record resolved it: the same part and
 * occurrence path, ending at a boundary with the payload's type URL, value
 * SHA-256 and size. Anywhere else it is shown as its type URL and length.
 */
import { ScalarType, create, equals, fromBinary, getOption, type DescField, type DescMessage, type Message } from '@bufbuild/protobuf'
import { reflect, type ReflectList, type ReflectMap, type ReflectMessage } from '@bufbuild/protobuf/reflect'
import { timestampDate, type Any, type Timestamp } from '@bufbuild/protobuf/wkt'
import type { Client } from '@connectrpc/connect'
import type { NodeAddress } from '@/gen/ai/protomolt/proto/repo/v1/address_pb'
import { RepositoryAnyResolutionSchema } from '@/gen/ai/protomolt/proto/repo/v1/any_resolution_pb'
import {
  RepositoryOccurrenceKeyType,
  RepositoryOccurrenceMapKeySchema,
  RepositorySchemaOccurrenceStepSchema,
  type RepositorySchemaOccurrenceStep,
} from '@/gen/ai/protomolt/proto/repo/v1/schema_occurrence_pb'
import { GetManifestRequestSchema, type DocumentService } from '@/gen/ai/protomolt/proto/repo/v2/document_service_pb'
import {
  DocumentPart,
  PartSelectorSchema,
  PartState,
  PartStateSchema,
  type DocumentManifest,
  type PartSelector,
  type RevisionValidation,
} from '@/gen/ai/protomolt/proto/repo/v2/manifest_pb'
import { field as fieldRules } from '@/gen/ai/protomolt/proto/validate/v1/validate_pb'
import { diffLines, diffStats, type DiffLine, type DiffStats } from '@/services/textDiff'
import { partRows, revisionSelector, type PartRow } from './documentCore'
import { enumLabel, instant } from './formatCore'
import { linkRows, listLinksRequest, type LinkRow } from './linksCore'
import { decodePart, readPart, retainedRegistry, typeNameOf } from './partReadCore'
import { rpcFailure, type RpcFailure } from './rpcErrorCore'
import { Sha256 } from './sha256'

const CONCEPT_REF = 'ai.protomolt.proto.types.v1.ConceptRef'
const ANY = 'google.protobuf.Any'
const TIMESTAMP = 'google.protobuf.Timestamp'

/** The compared revision pair, from the route: `revision` is shown, `compare` is the base. */
export interface ComparePair {
  base: bigint
  target: bigint
}

/** What changed for one part between the two manifests. */
export type PartChange = 'added' | 'removed' | 'changed' | 'unchanged'

/** The PartRow columns a part comparison reports. */
const PART_FIELDS = ['state', 'sizeBytes', 'sha256', 'writtenInRevision', 'owner', 'opaque', 'deletedReason'] as const
export type PartField = (typeof PART_FIELDS)[number]

export interface PartDiffRow {
  label: string
  change: PartChange
  base: PartRow | null
  target: PartRow | null
  /** The columns that differ; empty unless `change` is `changed`. */
  differs: PartField[]
}

/**
 * The parts of both manifests, matched by selector label, in the target's
 * order followed by the parts only the base holds. A tombstone holds no parts,
 * so every base part reads as removed.
 */
export function partDiff(base: DocumentManifest, target: DocumentManifest): PartDiffRow[] {
  const before = new Map(partRows(base).map((row) => [row.label, row]))
  const after = partRows(target)
  const out: PartDiffRow[] = []
  for (const row of after) {
    const old = before.get(row.label) ?? null
    before.delete(row.label)
    if (old === null) {
      out.push({ label: row.label, change: 'added', base: null, target: row, differs: [] })
      continue
    }
    const differs = PART_FIELDS.filter((f) => old[f] !== row[f])
    out.push({ label: row.label, change: differs.length === 0 ? 'unchanged' : 'changed', base: old, target: row, differs })
  }
  for (const row of before.values()) out.push({ label: row.label, change: 'removed', base: row, target: null, differs: [] })
  return out
}

/** One side's revision facts shown above the comparison. */
export interface RevisionFacts {
  revision: bigint
  committedAt: string | null
  tombstone: boolean
}

export function revisionFacts(manifest: DocumentManifest): RevisionFacts {
  return { revision: manifest.revision, committedAt: instant(manifest.committedAt), tombstone: manifest.tombstone }
}

/**
 * Schemas for the typed Any payloads of one revision, from the occurrences
 * its validation record resolved. An occurrence is identified by its part and
 * its exact step path (field numbers, repeated indexes, map keys and the Any
 * boundaries crossed to reach it, ending at its own boundary with the
 * payload's type URL, value SHA-256 and size), as the repository records it.
 * A payload is entered only at the place the revision resolved it, so an
 * opaque payload with the same bytes as a typed one elsewhere stays opaque.
 * Each distinct descriptor set is fetched once and checked against its digest.
 */
export class AnySchemas {
  private constructor(private readonly resolved: { part: PartSelector; steps: RepositorySchemaOccurrenceStep[]; desc: DescMessage }[]) {}

  static none(): AnySchemas {
    return new AnySchemas([])
  }

  static async load(
    client: Client<typeof DocumentService>,
    address: NodeAddress,
    revision: bigint,
    validation: RevisionValidation,
    signal: AbortSignal,
  ): Promise<AnySchemas> {
    const registries = new Map<string, Awaited<ReturnType<typeof retainedRegistry>>>()
    const resolved: { part: PartSelector; steps: RepositorySchemaOccurrenceStep[]; desc: DescMessage }[] = []
    for (const occurrence of validation.resolvedOccurrences) {
      const schema = occurrence.schema
      if (schema === undefined) throw new Error(`Revision ${revision} resolved an Any occurrence without a schema reference.`)
      const part = occurrence.occurrence?.part
      if (part === undefined) throw new Error(`Revision ${revision} resolved an Any occurrence without a part.`)
      let registry = registries.get(schema.descriptorSha256)
      if (registry === undefined) {
        registry = await retainedRegistry(client, address, revision, schema.descriptorSha256, signal)
        registries.set(schema.descriptorSha256, registry)
      }
      const typeName = typeNameOf(schema.typeUrl)
      const desc = registry.getMessage(typeName)
      if (desc === undefined) {
        throw new Error(`The retained descriptor set ${schema.descriptorSha256} does not define ${typeName}.`)
      }
      resolved.push({ part, steps: occurrence.occurrence?.steps ?? [], desc })
    }
    return new AnySchemas(resolved)
  }

  /**
   * The schema the revision resolved for the Any reached by `steps` in `part`,
   * whose own boundary step `boundary` names; null when it resolved none there.
   */
  schemaAt(part: PartSelector, steps: RepositorySchemaOccurrenceStep[], boundary: RepositorySchemaOccurrenceStep): DescMessage | null {
    const path = [...steps, boundary]
    const found = this.resolved.find(
      (r) => equals(PartSelectorSchema, r.part, part) && r.steps.length === path.length && r.steps.every((step, i) => sameStep(step, path[i])),
    )
    return found?.desc ?? null
  }
}

/**
 * Whether two occurrence steps name the same place. An Any boundary is the
 * same when its type URL, value SHA-256 and size match; the schema it resolved
 * to is what is being looked up, so it is not part of the identity.
 */
function sameStep(recorded: RepositorySchemaOccurrenceStep, walked: RepositorySchemaOccurrenceStep): boolean {
  const a = recorded.step
  const b = walked.step
  if (a.case === 'anyBoundary' && b.case === 'anyBoundary') {
    return a.value.typeUrl === b.value.typeUrl && a.value.valueSha256 === b.value.valueSha256 && a.value.valueSizeBytes === b.value.valueSizeBytes
  }
  return equals(RepositorySchemaOccurrenceStepSchema, recorded, walked)
}

function fieldStep(number: number): RepositorySchemaOccurrenceStep {
  return create(RepositorySchemaOccurrenceStepSchema, { step: { case: 'fieldNumber', value: number } })
}

function indexStep(index: number): RepositorySchemaOccurrenceStep {
  return create(RepositorySchemaOccurrenceStepSchema, { step: { case: 'repeatedIndex', value: index } })
}

const KEY_TYPES: Partial<Record<ScalarType, { type: RepositoryOccurrenceKeyType; kind: 'string' | 'bool' | 'signed' | 'unsigned' }>> = {
  [ScalarType.STRING]: { type: RepositoryOccurrenceKeyType.STRING, kind: 'string' },
  [ScalarType.BOOL]: { type: RepositoryOccurrenceKeyType.BOOL, kind: 'bool' },
  [ScalarType.INT32]: { type: RepositoryOccurrenceKeyType.INT32, kind: 'signed' },
  [ScalarType.SINT32]: { type: RepositoryOccurrenceKeyType.SINT32, kind: 'signed' },
  [ScalarType.SFIXED32]: { type: RepositoryOccurrenceKeyType.SFIXED32, kind: 'signed' },
  [ScalarType.UINT32]: { type: RepositoryOccurrenceKeyType.UINT32, kind: 'unsigned' },
  [ScalarType.FIXED32]: { type: RepositoryOccurrenceKeyType.FIXED32, kind: 'unsigned' },
  [ScalarType.INT64]: { type: RepositoryOccurrenceKeyType.INT64, kind: 'signed' },
  [ScalarType.SINT64]: { type: RepositoryOccurrenceKeyType.SINT64, kind: 'signed' },
  [ScalarType.SFIXED64]: { type: RepositoryOccurrenceKeyType.SFIXED64, kind: 'signed' },
  [ScalarType.UINT64]: { type: RepositoryOccurrenceKeyType.UINT64, kind: 'unsigned' },
  [ScalarType.FIXED64]: { type: RepositoryOccurrenceKeyType.FIXED64, kind: 'unsigned' },
}

/** The typed map-key step the repository records for one key of a map whose keys are `scalar`. */
function mapKeyStep(scalar: ScalarType, key: unknown): RepositorySchemaOccurrenceStep {
  const typed = KEY_TYPES[scalar]
  if (typed === undefined) throw new Error(`A map key of type ${ScalarType[scalar]} has no occurrence step.`)
  const value =
    typed.kind === 'string' ? { case: 'stringValue' as const, value: String(key) }
    : typed.kind === 'bool' ? { case: 'boolValue' as const, value: key === true || key === 'true' }
    : typed.kind === 'signed' ? { case: 'signedValue' as const, value: BigInt(key as string | number | bigint) }
    : { case: 'unsignedValue' as const, value: BigInt(key as string | number | bigint) }
  return create(RepositorySchemaOccurrenceStepSchema, {
    step: { case: 'mapKey', value: create(RepositoryOccurrenceMapKeySchema, { type: typed.type, value }) },
  })
}

function boundaryStep(any: Any): RepositorySchemaOccurrenceStep {
  return create(RepositorySchemaOccurrenceStepSchema, {
    step: {
      case: 'anyBoundary',
      value: create(RepositoryAnyResolutionSchema, {
        typeUrl: any.typeUrl,
        valueSha256: new Sha256().update(any.value).hex(),
        valueSizeBytes: BigInt(any.value.length),
      }),
    },
  })
}

/** The flattened lines of one decoded fragment and the concept tags found in it. */
export interface Flattened {
  lines: string[]
  concepts: string[]
}

/** What the walk carries: where it writes, the revision's Any schemas and the part walked. */
interface Walk {
  out: Flattened
  schemas: AnySchemas
  part: PartSelector
}

/**
 * One line per set leaf of `message`, the decoded fragment of `part`, in field
 * number order. A ConceptRef field that declares `concept_schemes` is a tag:
 * it is listed in `concepts` as `path = scheme/concept` as well as in `lines`.
 */
export function flatten(desc: DescMessage, message: Message, schemas: AnySchemas, part: PartSelector): Flattened {
  const walker: Walk = { out: { lines: [], concepts: [] }, schemas, part }
  walk(reflect(desc, message), '', [], walker)
  return walker.out
}

function join(path: string, name: string): string {
  return path === '' ? name : `${path}.${name}`
}

function scalarText(scalar: ScalarType, value: unknown): string {
  if (scalar === ScalarType.STRING) return JSON.stringify(value)
  if (scalar === ScalarType.BYTES) {
    const bytes = value as Uint8Array
    return `${bytes.length} B sha256:${new Sha256().update(bytes).hex()}`
  }
  return String(value)
}

function enumText(field: DescField, value: number): string {
  const e = field.fieldKind === 'enum' ? field.enum : field.fieldKind === 'list' && field.listKind === 'enum' ? field.enum : null
  const named = e?.value[value]
  return named?.name ?? `UNKNOWN(${value})`
}

function boundConcept(field: DescField): boolean {
  const message = field.fieldKind === 'message' ? field.message : field.fieldKind === 'list' && field.listKind === 'message' ? field.message : null
  if (message?.typeName !== CONCEPT_REF) return false
  return getOption(field, fieldRules).conceptSchemes.length > 0
}

function walk(r: ReflectMessage, path: string, steps: RepositorySchemaOccurrenceStep[], w: Walk): void {
  const out = w.out
  let any = false
  for (const field of r.sortedFields) {
    if (!r.isSet(field)) continue
    any = true
    const at = join(path, field.name)
    const here = [...steps, fieldStep(field.number)]
    switch (field.fieldKind) {
      case 'scalar':
        out.lines.push(`${at} = ${scalarText(field.scalar, r.get(field))}`)
        break
      case 'enum':
        out.lines.push(`${at} = ${enumText(field, r.get(field) as number)}`)
        break
      case 'message':
        entry(r.get(field) as ReflectMessage, at, here, w, boundConcept(field))
        break
      case 'list': {
        const list = r.get(field) as ReflectList
        const tag = boundConcept(field)
        for (const [i, item] of list.entries()) {
          const element = `${at}[${i}]`
          if (field.listKind === 'message') entry(item as ReflectMessage, element, [...here, indexStep(i)], w, tag)
          else if (field.listKind === 'enum') out.lines.push(`${element} = ${enumText(field, item as number)}`)
          else out.lines.push(`${element} = ${scalarText(field.scalar, item)}`)
        }
        break
      }
      case 'map': {
        const map = r.get(field) as ReflectMap
        const entries = [...map.entries()].sort(([a], [b]) => (String(a) < String(b) ? -1 : String(a) > String(b) ? 1 : 0))
        for (const [key, value] of entries) {
          const element = `${at}[${JSON.stringify(String(key))}]`
          if (field.mapKind === 'message') entry(value as ReflectMessage, element, [...here, mapKeyStep(field.mapKey, key)], w, false)
          else if (field.mapKind === 'enum') out.lines.push(`${element} = ${field.enum.value[value as number]?.name ?? `UNKNOWN(${value})`}`)
          else out.lines.push(`${element} = ${scalarText(field.scalar, value)}`)
        }
        break
      }
    }
  }
  for (const unknown of r.getUnknown() ?? []) {
    any = true
    out.lines.push(`${join(path, `#${unknown.no}`)} = unknown field, wire type ${unknown.wireType}, ${unknown.data.length} B`)
  }
  if (!any && path !== '') out.lines.push(`${path} = {}`)
}

function entry(r: ReflectMessage, at: string, steps: RepositorySchemaOccurrenceStep[], w: Walk, conceptTag: boolean): void {
  const out = w.out
  const typeName = r.desc.typeName
  if (typeName === TIMESTAMP) {
    out.lines.push(`${at} = ${timestampDate(r.message as Timestamp).toISOString()}`)
    return
  }
  if (typeName === ANY) {
    const any = r.message as Any
    out.lines.push(`${at} @type = ${any.typeUrl}`)
    const boundary = boundaryStep(any)
    const desc = w.schemas.schemaAt(w.part, steps, boundary)
    if (desc === null) {
      out.lines.push(`${at} = ${any.value.length} B, not resolved at this place in this revision`)
      return
    }
    walk(reflect(desc, fromBinary(desc, any.value)), at, [...steps, boundary], w)
    return
  }
  if (conceptTag && typeName === CONCEPT_REF) {
    const scheme = r.desc.fields.find((f) => f.name === 'scheme_id')
    const concept = r.desc.fields.find((f) => f.name === 'concept_id')
    out.concepts.push(
      scheme === undefined || concept === undefined
        ? `${at} = ConceptRef without scheme_id and concept_id in the retained schema`
        : `${at} = ${String(r.get(scheme))}/${String(r.get(concept))}`,
    )
  }
  walk(r, at, steps, w)
}

/** The lines of a fragment under `search_metadata`, the document's lookup fields. */
export function searchMetadataLines(lines: string[]): string[] {
  return lines.filter((line) => line.startsWith('search_metadata.') || line.startsWith('search_metadata '))
}

/** One line per outbound link, without its target status, which is the target's state now, not the revision's. */
export function linkLines(rows: LinkRow[]): string[] {
  return rows.map((row) => {
    const at = `${row.fieldPath}[${row.ordinal}] ${row.relation}${row.label === null ? '' : ` "${row.label}"`}`
    if (row.target.kind === 'static') {
      const c = row.target.call
      const pin = row.target.pinnedRevision === null ? '' : ` @${row.target.pinnedRevision}`
      return `${at} -> ${c.graphId}/${c.graphAddressId}/${c.docId}${pin}`
    }
    return `${at} -> dynamic: ${row.target.conditions.join('; ')}${row.target.order === null ? '' : `; ordered by ${row.target.order}`}`
  })
}

/** A section's line diff and its counts. */
export interface SectionDiff {
  lines: DiffLine[]
  stats: DiffStats
}

export function sectionDiff(base: string[], target: string[]): SectionDiff {
  const lines = diffLines(base.join('\n'), target.join('\n'))
  return { lines, stats: diffStats(lines) }
}

/** A fragment read for comparison: its flattened lines, or why it has none. */
export type FragmentSide =
  | { kind: 'absent'; reason: string }
  | { kind: 'lines'; flattened: Flattened; decodedAs: string | null }
  | { kind: 'failed'; failure: RpcFailure }

/** Everything one revision contributes to the comparison. */
export interface RevisionSide {
  manifest: DocumentManifest
  validation: RevisionValidation
  core: FragmentSide
  parsed: FragmentSide
  links: { kind: 'lines'; lines: string[] } | { kind: 'failed'; failure: RpcFailure }
}

const CORE = create(PartSelectorSchema, { selector: { case: 'part', value: DocumentPart.CORE } })
const PARSED = create(PartSelectorSchema, { selector: { case: 'part', value: DocumentPart.PARSED } })

/**
 * Reads one side of a comparison at an exact revision: the manifest and
 * validation record, the CORE and PARSED fragments decoded with the schemas
 * the revision retained, and its links. A failed fragment or link read is
 * kept as that section's failure; a failed manifest read fails the side.
 */
export async function readSide(
  client: Client<typeof DocumentService>,
  address: NodeAddress,
  revision: bigint,
  signal: AbortSignal,
): Promise<RevisionSide> {
  const answer = await client.getManifest(create(GetManifestRequestSchema, { address, revision: revisionSelector(revision) }), { signal })
  if (answer.manifest === undefined || answer.validation === undefined) {
    throw new Error(`GetManifest answered revision ${revision} without a manifest or validation record.`)
  }
  if (answer.manifest.revision !== revision) {
    throw new Error(`GetManifest answered for revision ${answer.manifest.revision}; revision ${revision} was asked for.`)
  }
  const manifest = answer.manifest
  const validation = answer.validation
  let schemas: AnySchemas | RpcFailure
  try {
    schemas = validation.resolvedOccurrences.length === 0 ? AnySchemas.none() : await AnySchemas.load(client, address, revision, validation, signal)
  } catch (error) {
    schemas = rpcFailure(error)
  }
  const fragment = async (selector: PartSelector, name: string): Promise<FragmentSide> => {
    if (manifest.tombstone) return { kind: 'absent', reason: `Revision ${revision} is a tombstone.` }
    const entry = manifest.parts.find((p) => p.selector !== undefined && equals(PartSelectorSchema, p.selector, selector))
    if (entry === undefined) return { kind: 'absent', reason: `Revision ${revision} has no ${name} part.` }
    if (entry.state !== PartState.PRESENT) {
      return { kind: 'absent', reason: `${name} is ${enumLabel(PartStateSchema, entry.state)} at revision ${revision}.` }
    }
    if ('code' in schemas) return { kind: 'failed', failure: schemas }
    try {
      const part = await readPart(client, address, revision, selector, false, signal)
      const decoded = await decodePart(client, address, revision, validation, selector, part.bytes, signal)
      if (decoded.kind === 'raw') {
        return {
          kind: 'lines',
          decodedAs: null,
          flattened: { lines: [`(raw) ${decoded.reason}`, `(raw) ${part.bytes.length} B sha256:${new Sha256().update(part.bytes).hex()}`], concepts: [] },
        }
      }
      return { kind: 'lines', decodedAs: decoded.typeUrl, flattened: flatten(decoded.desc, decoded.message, schemas, selector) }
    } catch (error) {
      return { kind: 'failed', failure: rpcFailure(error) }
    }
  }
  const core = await fragment(CORE, 'CORE')
  const parsed = await fragment(PARSED, 'PARSED')
  let links: RevisionSide['links']
  if (manifest.tombstone) {
    links = { kind: 'lines', lines: [] }
  } else {
    try {
      const answer = await client.listLinks(listLinksRequest(address, revision), { signal })
      links = { kind: 'lines', lines: linkLines(linkRows(answer)) }
    } catch (error) {
      links = { kind: 'failed', failure: rpcFailure(error) }
    }
  }
  return { manifest, validation, core, parsed, links }
}

/** The lines a fragment side contributes; an absent or failed side contributes none. */
export function fragmentLines(side: FragmentSide, pick: (f: Flattened) => string[]): string[] {
  return side.kind === 'lines' ? pick(side.flattened) : []
}
