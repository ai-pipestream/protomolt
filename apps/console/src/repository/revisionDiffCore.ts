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
 * when the revision retained a schema for that exact occurrence and payload;
 * otherwise it is shown as its type URL and length.
 */
import { ScalarType, create, equals, fromBinary, getOption, type DescField, type DescMessage, type Message } from '@bufbuild/protobuf'
import { reflect, type ReflectList, type ReflectMap, type ReflectMessage } from '@bufbuild/protobuf/reflect'
import { timestampDate, type Any, type Timestamp } from '@bufbuild/protobuf/wkt'
import type { Client } from '@connectrpc/connect'
import type { NodeAddress } from '@/gen/ai/protomolt/proto/repo/v1/address_pb'
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
import { decodePart, readPart } from './partReadCore'
import { rpcFailure, type RpcFailure } from './rpcErrorCore'
import { Sha256 } from './sha256'

export { AnySchemas } from './anySchemas'
import { AnySchemas, mapStepKey, withStep, type AnyLocation } from './anySchemas'

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

/** The flattened lines of one decoded fragment and the concept tags found in it. */
export interface Flattened {
  lines: string[]
  concepts: string[]
}

/**
 * One line per set leaf of `message`, in field number order. A ConceptRef
 * field that declares `concept_schemes` is a tag: it is listed in `concepts`
 * as `path = scheme/concept` as well as in `lines`.
 */
export function flatten(desc: DescMessage, message: Message, schemas: AnySchemas, part: PartSelector): Flattened {
  const out: Flattened = { lines: [], concepts: [] }
  walk(reflect(desc, message), '', out, schemas, { part, steps: [] })
  return out
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

function walk(r: ReflectMessage, path: string, out: Flattened, schemas: AnySchemas, location: AnyLocation): void {
  let any = false
  for (const field of r.sortedFields) {
    if (!r.isSet(field)) continue
    any = true
    const at = join(path, field.name)
    switch (field.fieldKind) {
      case 'scalar':
        out.lines.push(`${at} = ${scalarText(field.scalar, r.get(field))}`)
        break
      case 'enum':
        out.lines.push(`${at} = ${enumText(field, r.get(field) as number)}`)
        break
      case 'message':
        withStep(location, `fieldNumber:${field.number}`, () => {
          entry(r.get(field) as ReflectMessage, at, out, schemas, location, boundConcept(field))
        })
        break
      case 'list': {
        const list = r.get(field) as ReflectList
        const tag = boundConcept(field)
        if (field.listKind === 'message') {
          withStep(location, `fieldNumber:${field.number}`, () => {
            for (const [i, item] of list.entries()) {
              withStep(location, `repeatedIndex:${i}`, () => {
                entry(item as ReflectMessage, `${at}[${i}]`, out, schemas, location, tag)
              })
            }
          })
        } else {
          for (const [i, item] of list.entries()) {
            const element = `${at}[${i}]`
            if (field.listKind === 'enum') out.lines.push(`${element} = ${enumText(field, item as number)}`)
            else out.lines.push(`${element} = ${scalarText(field.scalar, item)}`)
          }
        }
        break
      }
      case 'map': {
        const map = r.get(field) as ReflectMap
        const entries = [...map.entries()].sort(([a], [b]) => (String(a) < String(b) ? -1 : String(a) > String(b) ? 1 : 0))
        if (field.mapKind === 'message') {
          withStep(location, `fieldNumber:${field.number}`, () => {
            for (const [key, value] of entries) {
              withStep(location, mapStepKey(field.mapKey, key), () => {
                entry(value as ReflectMessage, `${at}[${JSON.stringify(String(key))}]`, out, schemas, location, false)
              })
            }
          })
        } else {
          for (const [key, value] of entries) {
            const element = `${at}[${JSON.stringify(String(key))}]`
            if (field.mapKind === 'enum') out.lines.push(`${element} = ${field.enum.value[value as number]?.name ?? `UNKNOWN(${value})`}`)
            else out.lines.push(`${element} = ${scalarText(field.scalar, value)}`)
          }
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

function entry(r: ReflectMessage, at: string, out: Flattened, schemas: AnySchemas, location: AnyLocation, conceptTag: boolean): void {
  const typeName = r.desc.typeName
  if (typeName === TIMESTAMP) {
    out.lines.push(`${at} = ${timestampDate(r.message as Timestamp).toISOString()}`)
    return
  }
  if (typeName === ANY) {
    const any = r.message as Any
    out.lines.push(`${at} @type = ${any.typeUrl}`)
    const binding = schemas.schemaOf(any, location)
    if (binding === null) {
      out.lines.push(`${at} = ${any.value.length} B, no schema retained for this payload`)
      return
    }
    const decoded = reflect(binding.desc, fromBinary(binding.desc, any.value))
    withStep(location, binding.boundary, () => {
      if (binding.desc.typeName === ANY) entry(decoded, at, out, schemas, location, false)
      else walk(decoded, at, out, schemas, location)
    })
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
  walk(r, at, out, schemas, location)
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

/**
 * Filters a section diff by line substring and/or changed-only operations.
 */
export function filterSectionDiff(diff: SectionDiff, query: string, changedOnly: boolean): SectionDiff {
  const needle = query.trim().toLowerCase()
  if (needle === '' && !changedOnly) {
    return diff
  }
  const lines = diff.lines.filter((line) => {
    if (changedOnly && line.op === 'same') return false
    if (needle !== '' && !line.text.toLowerCase().includes(needle)) return false
    return true
  })
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
