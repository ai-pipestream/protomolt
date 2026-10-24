/**
 * SKOS concept scheme inspection and concept-filtered document lookup for the
 * repository Look up screen.
 *
 * The repository wire surface exposes SKOS taxonomy querying through
 * `DocumentService.ListDocuments` (`DocumentFilter.concept: ConceptFilter`)
 * and returns `ConceptFilterEvidence` (`expanded_concepts`,
 * `mounted_schemes`, `unresolved_mappings`). Scheme definitions follow
 * `ai.protomolt.proto.types.v1.ConceptScheme` and `Concept`: `narrower` is the
 * inverse of in-scheme `broader`, `related` is symmetric within a scheme,
 * deprecated concepts may name `replaced_by`, `hidden_label` values participate
 * in search matching without replacing the display label, and concept IRIs or
 * notation datatype IRIs become clickable hrefs only when `safeExternalHref`
 * accepts them as `http:` or `https:`.
 */
import { fromBinary } from '@bufbuild/protobuf'
import {
  ConceptSchemeSchema,
  type Concept,
  type ConceptRef,
  type ConceptScheme,
  type LangLabel,
} from '@/gen/ai/protomolt/proto/types/v1/concepts_pb'
import type { ListDocumentsRequest } from '@/gen/ai/protomolt/proto/repo/v2/document_service_pb'
import { safeExternalHref, type EvidenceView } from './linksCore'
import {
  DEFAULT_PAGE_SIZE,
  ListingInputError,
  buildListRequest,
  listingRouteQuery,
  newListingQuery,
  type ListingQuery,
} from './listingCore'

const DEFAULT_PREFERRED_LANG = 'en'

/** A language-tagged SKOS label or note with blank text removed. */
export interface FormattedLabel {
  text: string
  lang: string | null
}

/** A SKOS notation code and its optional datatype IRI. */
export interface ConceptNotationView {
  value: string
  datatypeIri: string | null
  safeDatatypeHref: string | null
}

/** An in-scheme semantic relation target (`broader`, `narrower`, `related`, `replaced_by`). */
export interface ConceptRelationTarget {
  conceptId: string
  primaryLabel: string
  primaryLang: string | null
  known: boolean
  deprecated: boolean
}

/** The five SKOS cross-scheme mapping relations on `Concept`. */
export type ConceptMappingKind = 'exactMatch' | 'closeMatch' | 'broadMatch' | 'narrowMatch' | 'relatedMatch'

/** One cross-scheme `ConceptRef` target. */
export interface ConceptMappingTarget {
  schemeId: string
  conceptId: string
  key: string
}

/** Grouped cross-scheme mappings for one SKOS mapping relation. */
export interface ConceptMappingGroup {
  kind: ConceptMappingKind
  label: string
  expandsWithFilter: 'always' | 'closeMatch' | 'never'
  targets: ConceptMappingTarget[]
}

/** One concept within a `ConceptSchemeView` with its resolved hierarchy and mappings. */
export interface ConceptNodeView {
  schemeId: string
  id: string
  refKey: string
  primaryLabel: string
  primaryLang: string | null
  prefLabels: FormattedLabel[]
  altLabels: FormattedLabel[]
  hiddenLabels: FormattedLabel[]
  definitions: FormattedLabel[]
  scopeNotes: FormattedLabel[]
  notations: ConceptNotationView[]
  deprecated: boolean
  replacedBy: ConceptRelationTarget[]
  replaces: ConceptRelationTarget[]
  iri: string | null
  safeIriHref: string | null
  broader: ConceptRelationTarget[]
  narrower: ConceptRelationTarget[]
  related: ConceptRelationTarget[]
  mappings: ConceptMappingGroup[]
}

/** A node in the broader/narrower hierarchy tree of a `ConceptSchemeView`. */
export interface ConceptTreeNode {
  concept: ConceptNodeView
  children: ConceptTreeNode[]
  cycleDetected: boolean
}

/** A decoded and indexed SKOS `ConceptScheme`. */
export interface ConceptSchemeView {
  schemeId: string
  version: string | null
  primaryLabel: string
  primaryLang: string | null
  prefLabels: FormattedLabel[]
  concepts: ConceptNodeView[]
  byId: ReadonlyMap<string, ConceptNodeView>
  topConcepts: ConceptNodeView[]
  tree: ConceptTreeNode[]
}

/**
 * In-scheme expansion preview for a concept under the current `narrower` and
 * `close_match` toggles, mirroring the repository's `ConceptSchemeMounts` rules.
 */
export interface InSchemeExpansionPreview {
  requestedId: string
  canonicalIds: string[]
  replacedRedirected: boolean
  inSchemeConcepts: ConceptNodeView[]
  crossSchemeTargets: { kind: 'exactMatch' | 'closeMatch'; schemeId: string; conceptId: string; key: string }[]
  unresolvedInScheme: { fromId: string; relation: 'broader' | 'replaced_by'; targetId: string }[]
}

/** The concept lookup form state on the Look up screen. */
export interface ConceptLookupDraft {
  accountId: string
  graphId: string
  graphAddressId: string
  schemeId: string
  conceptId: string
  narrower: boolean
  closeMatch: boolean
  includeTombstoned: boolean
  pageSize: number
}

const MAPPING_SPECS: readonly {
  kind: ConceptMappingKind
  label: string
  expandsWithFilter: 'always' | 'closeMatch' | 'never'
}[] = [
  { kind: 'exactMatch', label: 'Exact match', expandsWithFilter: 'always' },
  { kind: 'closeMatch', label: 'Close match', expandsWithFilter: 'closeMatch' },
  { kind: 'broadMatch', label: 'Broad match', expandsWithFilter: 'never' },
  { kind: 'narrowMatch', label: 'Narrow match', expandsWithFilter: 'never' },
  { kind: 'relatedMatch', label: 'Related match', expandsWithFilter: 'never' },
]

/** Normalizes `LangLabel` entries, dropping blank strings and trimming tags. */
export function formatLangLabels(labels: readonly LangLabel[]): FormattedLabel[] {
  const out: FormattedLabel[] = []
  for (const entry of labels) {
    const text = entry.text.trim()
    if (text === '') continue
    const lang = entry.language.trim()
    out.push({ text, lang: lang === '' ? null : lang })
  }
  return out
}

/**
 * Picks the primary display label from `pref_label` entries: exact preferred
 * language match first, then language prefix match (`en-US` for `en`), then the
 * first non-empty label, or `fallback` when none are set.
 */
export function primaryLabel(
  labels: readonly LangLabel[],
  fallback: string,
  preferredLang = DEFAULT_PREFERRED_LANG,
): FormattedLabel {
  const formatted = formatLangLabels(labels)
  if (formatted.length === 0) return { text: fallback, lang: null }
  const target = preferredLang.trim().toLowerCase()
  if (target !== '') {
    const exact = formatted.find((l) => l.lang?.toLowerCase() === target)
    if (exact !== undefined) return exact
    const prefix = formatted.find((l) => l.lang?.toLowerCase().startsWith(`${target}-`))
    if (prefix !== undefined) return prefix
  }
  return formatted[0]
}

function uniqueStrings(values: Iterable<string>): string[] {
  const seen = new Set<string>()
  const out: string[] = []
  for (const raw of values) {
    const trimmed = raw.trim()
    if (trimmed !== '' && !seen.has(trimmed)) {
      seen.add(trimmed)
      out.push(trimmed)
    }
  }
  return out
}

function formatMappings(concept: Concept): ConceptMappingGroup[] {
  const out: ConceptMappingGroup[] = []
  for (const spec of MAPPING_SPECS) {
    const refs: readonly ConceptRef[] = concept[spec.kind]
    const seen = new Set<string>()
    const targets: ConceptMappingTarget[] = []
    for (const ref of refs) {
      const schemeId = ref.schemeId.trim()
      const conceptId = ref.conceptId.trim()
      if (schemeId === '' || conceptId === '') continue
      const key = `${schemeId}/${conceptId}`
      if (seen.has(key)) continue
      seen.add(key)
      targets.push({ schemeId, conceptId, key })
    }
    if (targets.length > 0) {
      out.push({
        kind: spec.kind,
        label: spec.label,
        expandsWithFilter: spec.expandsWithFilter,
        targets,
      })
    }
  }
  return out
}

function compareTargets(a: ConceptRelationTarget, b: ConceptRelationTarget): number {
  const byLabel = a.primaryLabel.localeCompare(b.primaryLabel)
  return byLabel !== 0 ? byLabel : a.conceptId.localeCompare(b.conceptId)
}

function compareNodes(a: ConceptNodeView, b: ConceptNodeView): number {
  const byLabel = a.primaryLabel.localeCompare(b.primaryLabel)
  return byLabel !== 0 ? byLabel : a.id.localeCompare(b.id)
}

/**
 * Indexes a `ConceptScheme` protobuf message into a `ConceptSchemeView` with
 * inverse `narrower` edges, symmetric `related` links, deprecation chains,
 * top concepts, and cycle-guarded hierarchy trees.
 */
export function buildConceptSchemeView(
  rawSchemeId: string,
  scheme: ConceptScheme,
  version: string | null = null,
): ConceptSchemeView {
  const schemeId = rawSchemeId.trim()
  if (schemeId === '') throw new ListingInputError('Concept scheme ID', 'is blank')

  const rawById = new Map<string, Concept>()
  for (const concept of scheme.concepts) {
    const id = concept.id.trim()
    if (id === '' || rawById.has(id)) continue
    rawById.set(id, concept)
  }

  const labelById = new Map<string, FormattedLabel>()
  for (const [id, concept] of rawById.entries()) {
    labelById.set(id, primaryLabel(concept.prefLabel, id))
  }

  const narrowerIds = new Map<string, Set<string>>()
  const relatedIds = new Map<string, Set<string>>()
  const replacesIds = new Map<string, Set<string>>()
  for (const id of rawById.keys()) {
    narrowerIds.set(id, new Set())
    relatedIds.set(id, new Set())
    replacesIds.set(id, new Set())
  }

  for (const [id, concept] of rawById.entries()) {
    for (const parentId of uniqueStrings(concept.broader)) {
      if (parentId === id) continue
      narrowerIds.get(parentId)?.add(id)
    }
    for (const relId of uniqueStrings(concept.related)) {
      if (relId === id) continue
      relatedIds.get(id)?.add(relId)
      relatedIds.get(relId)?.add(id)
    }
    for (const replacement of uniqueStrings(concept.replacedBy)) {
      if (replacement !== id) {
        replacesIds.get(replacement)?.add(id)
      }
    }
  }

  const toTarget = (targetId: string): ConceptRelationTarget => {
    const target = rawById.get(targetId)
    const label = labelById.get(targetId) ?? { text: targetId, lang: null }
    return {
      conceptId: targetId,
      primaryLabel: label.text,
      primaryLang: label.lang,
      known: target !== undefined,
      deprecated: target?.deprecated ?? false,
    }
  }

  const byId = new Map<string, ConceptNodeView>()
  for (const [id, concept] of rawById.entries()) {
    const primary = labelById.get(id)!
    const replacedBy = uniqueStrings(concept.replacedBy)
      .filter((targetId) => targetId !== id)
      .map(toTarget)
      .sort(compareTargets)
    const iri = concept.iri?.trim() ?? ''
    const notations: ConceptNotationView[] = []
    for (const notation of concept.notation) {
      const value = notation.value.trim()
      if (value === '') continue
      const datatypeIri = notation.datatypeIri.trim()
      notations.push({
        value,
        datatypeIri: datatypeIri === '' ? null : datatypeIri,
        safeDatatypeHref: datatypeIri === '' ? null : safeExternalHref(datatypeIri),
      })
    }
    const broader = uniqueStrings(concept.broader)
      .filter((parentId) => parentId !== id)
      .map(toTarget)
      .sort(compareTargets)
    const narrower = [...(narrowerIds.get(id) ?? [])].map(toTarget).sort(compareTargets)
    const related = [...(relatedIds.get(id) ?? [])].map(toTarget).sort(compareTargets)
    const replaces = [...(replacesIds.get(id) ?? [])].map(toTarget).sort(compareTargets)

    byId.set(id, {
      schemeId,
      id,
      refKey: `${schemeId}/${id}`,
      primaryLabel: primary.text,
      primaryLang: primary.lang,
      prefLabels: formatLangLabels(concept.prefLabel),
      altLabels: formatLangLabels(concept.altLabel),
      hiddenLabels: formatLangLabels(concept.hiddenLabel),
      definitions: formatLangLabels(concept.definition),
      scopeNotes: formatLangLabels(concept.scopeNote),
      notations,
      deprecated: concept.deprecated,
      replacedBy,
      replaces,
      iri: iri === '' ? null : iri,
      safeIriHref: iri === '' ? null : safeExternalHref(iri),
      broader,
      narrower,
      related,
      mappings: formatMappings(concept),
    })
  }

  const concepts = [...byId.values()].sort(compareNodes)
  const roots = concepts.filter((node) => !node.broader.some((parent) => parent.known))
  const topConcepts = roots.length > 0 || concepts.length === 0 ? roots : [concepts[0]]

  const buildTree = (node: ConceptNodeView, ancestors: ReadonlySet<string>): ConceptTreeNode => {
    const nextAncestors = new Set(ancestors)
    nextAncestors.add(node.id)
    let cycleDetected = false
    const children: ConceptTreeNode[] = []
    for (const childRef of node.narrower) {
      const child = byId.get(childRef.conceptId)
      if (child === undefined) continue
      if (nextAncestors.has(child.id)) {
        cycleDetected = true
        continue
      }
      children.push(buildTree(child, nextAncestors))
    }
    return { concept: node, children, cycleDetected }
  }

  const tree = topConcepts.map((root) => buildTree(root, new Set()))
  const schemePrimary = primaryLabel(scheme.prefLabel, schemeId)
  const trimmedVersion = version?.trim() ?? ''

  return {
    schemeId,
    version: trimmedVersion === '' ? null : trimmedVersion,
    primaryLabel: schemePrimary.text,
    primaryLang: schemePrimary.lang,
    prefLabels: formatLangLabels(scheme.prefLabel),
    concepts,
    byId,
    topConcepts,
    tree,
  }
}

/** Decodes binary `ConceptScheme` protobuf bytes into a `ConceptSchemeView`. */
export function decodeConceptScheme(
  schemeId: string,
  bytes: Uint8Array,
  version: string | null = null,
): ConceptSchemeView {
  if (bytes.byteLength === 0) {
    throw new ListingInputError('ConceptScheme payload', 'is empty')
  }
  let decoded: ConceptScheme
  try {
    decoded = fromBinary(ConceptSchemeSchema, bytes)
  } catch (error) {
    const detail = error instanceof Error ? error.message : String(error)
    throw new ListingInputError('ConceptScheme payload', `is not valid ConceptScheme protobuf bytes (${detail})`)
  }
  return buildConceptSchemeView(schemeId, decoded, version)
}

/** Decodes base64 or base64url `ConceptScheme` protobuf bytes into a `ConceptSchemeView`. */
export function decodeConceptSchemeBase64(
  schemeId: string,
  rawBase64: string,
  version: string | null = null,
): ConceptSchemeView {
  const compact = rawBase64.replace(/\s+/g, '').replace(/-/g, '+').replace(/_/g, '/')
  if (compact === '') throw new ListingInputError('ConceptScheme base64', 'is blank')
  if (!/^[A-Za-z0-9+/]*={0,2}$/.test(compact) || compact.length % 4 === 1) {
    throw new ListingInputError('ConceptScheme base64', 'is not valid base64')
  }
  const padded = compact + '='.repeat((4 - (compact.length % 4)) % 4)
  let binary: string
  try {
    binary = atob(padded)
  } catch {
    throw new ListingInputError('ConceptScheme base64', 'is not valid base64')
  }
  const bytes = new Uint8Array(binary.length)
  for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i)
  return decodeConceptScheme(schemeId, bytes, version)
}

/**
 * Filters concepts in `scheme` by case-insensitive substring across concept ID,
 * preferred labels, alternate labels, hidden labels (SKOS lexical search target),
 * notations, definitions, and IRI.
 */
export function filterConcepts(scheme: ConceptSchemeView, rawQuery: string): ConceptNodeView[] {
  const needle = rawQuery.trim().toLowerCase()
  if (needle === '') return scheme.concepts
  return scheme.concepts.filter((node) => {
    if (node.id.toLowerCase().includes(needle)) return true
    if (node.iri !== null && node.iri.toLowerCase().includes(needle)) return true
    for (const label of node.prefLabels) if (label.text.toLowerCase().includes(needle)) return true
    for (const label of node.altLabels) if (label.text.toLowerCase().includes(needle)) return true
    for (const label of node.hiddenLabels) if (label.text.toLowerCase().includes(needle)) return true
    for (const notation of node.notations) if (notation.value.toLowerCase().includes(needle)) return true
    for (const def of node.definitions) if (def.text.toLowerCase().includes(needle)) return true
    return false
  })
}

/**
 * Computes the in-scheme expansion preview for `conceptId` under `options`,
 * following deprecated `replaced_by` redirection, deprecated predecessors,
 * transitive `narrower` descendants when enabled, and cross-scheme
 * `exact_match` / `close_match` targets.
 */
export function expandConceptInScheme(
  scheme: ConceptSchemeView,
  rawConceptId: string,
  options: { narrower: boolean; closeMatch: boolean },
): InSchemeExpansionPreview | null {
  const requestedId = rawConceptId.trim()
  const start = scheme.byId.get(requestedId)
  if (start === undefined) return null

  const unresolvedInScheme: { fromId: string; relation: 'broader' | 'replaced_by'; targetId: string }[] = []
  const canonicalNodes: ConceptNodeView[] = []
  const canonicalSeen = new Set<string>()
  const forwardVisited = new Set<string>()
  let replacedRedirected = false
  if (start.deprecated && start.replacedBy.length > 0) {
    const forwardQueue: ConceptNodeView[] = [start]
    forwardVisited.add(start.id)
    while (forwardQueue.length > 0) {
      const node = forwardQueue.shift()!
      for (const replacement of node.replacedBy) {
        if (replacement.known) {
          const targetNode = scheme.byId.get(replacement.conceptId)!
          if (targetNode.id !== start.id) replacedRedirected = true
          if (targetNode.deprecated && targetNode.replacedBy.length > 0) {
            if (!forwardVisited.has(targetNode.id)) {
              forwardVisited.add(targetNode.id)
              forwardQueue.push(targetNode)
            }
          } else if (!canonicalSeen.has(targetNode.id)) {
            canonicalSeen.add(targetNode.id)
            canonicalNodes.push(targetNode)
          }
        } else {
          unresolvedInScheme.push({
            fromId: node.id,
            relation: 'replaced_by',
            targetId: replacement.conceptId,
          })
        }
      }
    }
  }
  if (canonicalNodes.length === 0) {
    canonicalNodes.push(start)
  }

  const expandedIds = new Set<string>()
  const queue: ConceptNodeView[] = []
  for (const node of canonicalNodes) {
    if (!expandedIds.has(node.id)) {
      expandedIds.add(node.id)
      queue.push(node)
    }
  }
  if (replacedRedirected) {
    for (const id of forwardVisited) {
      const node = scheme.byId.get(id)
      if (node !== undefined && !expandedIds.has(node.id)) {
        expandedIds.add(node.id)
        queue.push(node)
      }
    }
  }

  while (queue.length > 0) {
    const current = queue.shift()!
    for (const parent of current.broader) {
      if (!parent.known) {
        unresolvedInScheme.push({ fromId: current.id, relation: 'broader', targetId: parent.conceptId })
      }
    }
    for (const pred of current.replaces) {
      const predNode = scheme.byId.get(pred.conceptId)
      if (predNode !== undefined && !expandedIds.has(predNode.id)) {
        expandedIds.add(predNode.id)
        queue.push(predNode)
      }
    }
    if (options.narrower) {
      for (const childRef of current.narrower) {
        const child = scheme.byId.get(childRef.conceptId)
        if (child !== undefined && !expandedIds.has(child.id)) {
          expandedIds.add(child.id)
          queue.push(child)
        }
      }
    }
  }

  const inSchemeConcepts = [...expandedIds]
    .map((id) => scheme.byId.get(id)!)
    .filter((node): node is ConceptNodeView => node !== undefined)
    .sort(compareNodes)

  const seenCross = new Set<string>()
  const crossSchemeTargets: { kind: 'exactMatch' | 'closeMatch'; schemeId: string; conceptId: string; key: string }[] = []
  for (const node of inSchemeConcepts) {
    for (const group of node.mappings) {
      if (group.kind !== 'exactMatch' && !(options.closeMatch && group.kind === 'closeMatch')) continue
      for (const target of group.targets) {
        if (seenCross.has(target.key)) continue
        seenCross.add(target.key)
        crossSchemeTargets.push({
          kind: group.kind,
          schemeId: target.schemeId,
          conceptId: target.conceptId,
          key: target.key,
        })
      }
    }
  }

  return {
    requestedId: start.id,
    canonicalIds: canonicalNodes.map((n) => n.id),
    replacedRedirected,
    inSchemeConcepts,
    crossSchemeTargets,
    unresolvedInScheme,
  }
}

/** Creates an empty `ConceptLookupDraft` for the given account. */
export function newConceptLookupDraft(accountId = ''): ConceptLookupDraft {
  return {
    accountId,
    graphId: '',
    graphAddressId: '',
    schemeId: '',
    conceptId: '',
    narrower: true,
    closeMatch: false,
    includeTombstoned: false,
    pageSize: DEFAULT_PAGE_SIZE,
  }
}

/** Converts a `ConceptLookupDraft` into a `ListingQuery` for `DocumentService.listDocuments`. */
export function toListingQuery(draft: ConceptLookupDraft): ListingQuery {
  const query = newListingQuery(draft.accountId.trim())
  query.graphId = draft.graphId.trim()
  query.graphAddressId = draft.graphAddressId.trim()
  query.includeTombstoned = draft.includeTombstoned
  query.pageSize = draft.pageSize
  query.filter.concept = {
    schemeId: draft.schemeId.trim(),
    conceptId: draft.conceptId.trim(),
    narrower: draft.narrower,
    closeMatch: draft.closeMatch,
  }
  return query
}

/**
 * Validates `draft` and builds a `ListDocumentsRequest` with its `ConceptFilter`.
 * Refuses blank account, scheme, or concept IDs and unpaired graph/node scope.
 */
export function buildConceptLookupRequest(draft: ConceptLookupDraft, pageToken = ''): ListDocumentsRequest {
  if (draft.accountId.trim() === '') throw new ListingInputError('Account', 'is blank')
  if (draft.schemeId.trim() === '') {
    throw new ListingInputError('Concept scheme ID', 'is blank; a concept is named within its scheme')
  }
  if (draft.conceptId.trim() === '') {
    throw new ListingInputError('Concept ID', 'is blank; a scheme alone names no concept')
  }
  const hasGraph = draft.graphId.trim() !== ''
  const hasNode = draft.graphAddressId.trim() !== ''
  if (hasGraph !== hasNode) {
    throw new ListingInputError(
      hasGraph ? 'Graph address ID' : 'Graph ID',
      'must be given together with the other scope field, or both left blank',
    )
  }
  return buildListRequest(toListingQuery(draft), pageToken)
}

/** Route query for opening the concept filter in `RepositoryAccountView`. */
export function conceptAccountRouteQuery(draft: ConceptLookupDraft): Record<string, string | string[]> {
  return listingRouteQuery(toListingQuery(draft))
}

/**
 * Merges mounted scheme versions reported in `ConceptFilterEvidence` into the
 * list of observed mounted schemes, keeping the newest version per `schemeId`
 * and sorting by `schemeId`.
 */
export function mergeMountedSchemes(
  existing: readonly { schemeId: string; version: string }[],
  evidence: EvidenceView | null,
): { schemeId: string; version: string }[] {
  const byScheme = new Map<string, string>()
  for (const item of existing) {
    const id = item.schemeId.trim()
    if (id !== '') byScheme.set(id, item.version)
  }
  if (evidence !== null) {
    for (const item of evidence.schemes) {
      const id = item.schemeId.trim()
      if (id !== '') byScheme.set(id, item.version)
    }
  }
  return [...byScheme.entries()]
    .map(([schemeId, version]) => ({ schemeId, version }))
    .sort((a, b) => a.schemeId.localeCompare(b.schemeId))
}
