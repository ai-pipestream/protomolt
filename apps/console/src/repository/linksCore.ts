/**
 * The document links panel: a revision's outbound links from ListLinks, one
 * page of a dynamic link's targets from ResolveLink, and the static links into
 * the document from ListBacklinks (requirements §17). Every value is read from
 * the answer; a link relation or concept URI is text, never a navigable href,
 * unless it is an absolute http or https URL.
 */
import { create } from '@bufbuild/protobuf'
import type { NodeAddress } from '@/gen/ai/protomolt/proto/repo/v1/address_pb'
import {
  DocumentSortFieldSchema,
  ListBacklinksRequestSchema,
  ListLinksRequestSchema,
  ResolveLinkRequestSchema,
  type Backlink,
  type ConceptFilterEvidence,
  type DocumentFilter,
  type DocumentSort,
  type ListBacklinksRequest,
  type ListLinksRequest,
  type ListLinksResponse,
  type ResolveLinkRequest,
  type ResolvedLink,
} from '@/gen/ai/protomolt/proto/repo/v2/document_service_pb'
import { callNumberOf, revisionSelector, type CallNumber } from './documentCore'
import { enumLabel, instant } from './formatCore'

/** Targets per ResolveLink page and backlinks per ListBacklinks page. */
export const LINK_PAGE_SIZE = 50

/**
 * The href an extension relation or concept URI may carry: the URL itself when
 * it parses as an absolute `http:` or `https:` URL, otherwise null and the
 * value is shown as text. `javascript:`, `data:`, `vbscript:`, relative and
 * unparseable values never become a link.
 */
export function safeExternalHref(raw: string): string | null {
  let url: URL
  try {
    url = new URL(raw)
  } catch {
    return null
  }
  return url.protocol === 'https:' || url.protocol === 'http:' ? url.href : null
}

/**
 * A static target as the reader sees it. RESOLVED is current and readable;
 * UNAVAILABLE covers missing, purged and unreadable alike, because the server
 * does not tell them apart to a reader access hides them from.
 */
export type TargetStatus = 'RESOLVED' | 'TOMBSTONED' | 'UNAVAILABLE'

export type LinkTarget =
  | { kind: 'static'; call: CallNumber; pinnedRevision: bigint | null; status: TargetStatus }
  | { kind: 'dynamic'; conditions: string[]; order: string | null }

/** One outbound link row. */
export interface LinkRow {
  key: string
  fieldPath: string
  ordinal: number
  relation: string
  relationHref: string | null
  label: string | null
  target: LinkTarget
}

function staticStatus(tombstoned: boolean, unavailable: boolean): TargetStatus {
  if (unavailable) return 'UNAVAILABLE'
  return tombstoned ? 'TOMBSTONED' : 'RESOLVED'
}

export function linkRow(link: ResolvedLink): LinkRow {
  const base = {
    key: `${link.fieldPath}\u0000${link.ordinal}`,
    fieldPath: link.fieldPath,
    ordinal: link.ordinal,
    relation: link.relation,
    relationHref: safeExternalHref(link.relation),
    label: link.label ?? null,
  }
  const target = link.target
  switch (target.case) {
    case 'staticTarget': {
      const state = target.value
      if (state.address === undefined) throw new Error(`ListLinks returned a static link at ${link.fieldPath} without a target address.`)
      return {
        ...base,
        target: {
          kind: 'static',
          call: callNumberOf(state.address),
          pinnedRevision: state.pinnedRevision ?? null,
          status: staticStatus(state.targetTombstoned, state.targetUnavailable),
        },
      }
    }
    case 'dynamicTarget': {
      const selector = target.value
      if (selector.filter === undefined) throw new Error(`ListLinks returned a dynamic link at ${link.fieldPath} without a filter.`)
      return { ...base, target: { kind: 'dynamic', conditions: filterConditions(selector.filter), order: sortLabel(selector.sort) } }
    }
    case undefined:
      throw new Error(`ListLinks returned a link at ${link.fieldPath} with neither a static nor a dynamic target.`)
  }
}

export function linkRows(response: ListLinksResponse): LinkRow[] {
  return response.links.map(linkRow)
}

/** A dynamic selector's filter as one line per condition, in DocumentFilter field order. */
export function filterConditions(filter: DocumentFilter): string[] {
  const out: string[] = []
  const exact: [string, string | undefined][] = [
    ['doc_id', filter.docId],
    ['title', filter.title],
    ['document_type', filter.documentType],
    ['source_uri', filter.sourceUri],
    ['source_mime_type', filter.sourceMimeType],
    ['language', filter.language],
    ['author', filter.author],
    ['category', filter.category],
    ['datasource_id', filter.datasourceId],
    ['connector_id', filter.connectorId],
  ]
  for (const [name, value] of exact) if (value !== undefined) out.push(`${name} = ${value}`)
  const after = instant(filter.committedAfter)
  if (after !== null) out.push(`committed at or after ${after}`)
  const before = instant(filter.committedBefore)
  if (before !== null) out.push(`committed before ${before}`)
  for (const m of filter.metadata) out.push(`metadata ${m.key} = ${m.value}`)
  if (filter.owner.case === 'ownerDatasourceId') out.push(`owner datasource ${filter.owner.value}`)
  if (filter.owner.case === 'ownerModuleId') out.push(`owner module ${filter.owner.value}`)
  const concept = filter.concept?.concept
  if (concept !== undefined) {
    const steps = [filter.concept?.expansion?.narrower && 'narrower', filter.concept?.expansion?.closeMatch && 'close match']
      .filter(Boolean)
      .join(', ')
    out.push(`concept ${concept.schemeId}/${concept.conceptId}${steps ? ` (with ${steps})` : ''}`)
  }
  if (out.length === 0) out.push('every current document in the account')
  return out
}

function sortLabel(sort: DocumentSort | undefined): string | null {
  if (sort === undefined) return null
  return `${enumLabel(DocumentSortFieldSchema, sort.field)}${sort.descending ? ' descending' : ''}`
}

/** What a concept filter's expansion read, as the panel shows it. */
export interface EvidenceView {
  expandedConcepts: number
  schemes: { schemeId: string; version: string }[]
  unresolved: { from: string; relation: string; target: string; reason: string }[]
}

export function evidenceView(evidence: ConceptFilterEvidence | undefined): EvidenceView | null {
  if (evidence === undefined) return null
  const ref = (r: { schemeId: string; conceptId: string } | undefined) => (r === undefined ? '(none)' : `${r.schemeId}/${r.conceptId}`)
  return {
    expandedConcepts: evidence.expandedConcepts,
    schemes: evidence.mountedSchemes.map((s) => ({ schemeId: s.schemeId, version: s.version })),
    unresolved: evidence.unresolvedMappings.map((u) => ({
      from: ref(u.from),
      relation: u.relation,
      target: ref(u.target),
      reason: u.reason,
    })),
  }
}

/** One inbound link row. */
export interface BacklinkRow {
  key: string
  call: CallNumber
  revision: bigint
  fieldPath: string
  ordinal: number
  relation: string
  relationHref: string | null
  pinnedRevision: bigint | null
}

export function backlinkRow(backlink: Backlink): BacklinkRow {
  if (backlink.address === undefined) throw new Error('ListBacklinks returned a backlink without a linking address.')
  const call = callNumberOf(backlink.address)
  return {
    key: `${call.graphId}\u0000${call.graphAddressId}\u0000${call.docId}\u0000${backlink.fieldPath}\u0000${backlink.ordinal}`,
    call,
    revision: backlink.revision,
    fieldPath: backlink.fieldPath,
    ordinal: backlink.ordinal,
    relation: backlink.relation,
    relationHref: safeExternalHref(backlink.relation),
    pinnedRevision: backlink.pinnedRevision ?? null,
  }
}

/** The links of exactly the revision the screen shows. */
export function listLinksRequest(address: NodeAddress, revision: bigint): ListLinksRequest {
  return create(ListLinksRequestSchema, { address, revision: revisionSelector(revision) })
}

/** One page of a dynamic link's targets, pinned to the linking revision the screen shows. */
export function resolveLinkRequest(address: NodeAddress, revision: bigint, row: LinkRow, pageToken: string): ResolveLinkRequest {
  if (row.target.kind !== 'dynamic') throw new Error(`The link at ${row.fieldPath} is static; only a dynamic link resolves.`)
  return create(ResolveLinkRequestSchema, {
    address,
    revision: revisionSelector(revision),
    fieldPath: row.fieldPath,
    ordinal: row.ordinal,
    pageSize: LINK_PAGE_SIZE,
    pageToken,
  })
}

export function listBacklinksRequest(target: NodeAddress, pageToken: string): ListBacklinksRequest {
  return create(ListBacklinksRequestSchema, { target, pageSize: LINK_PAGE_SIZE, pageToken })
}

/** The console route of a call number, at an exact revision or the current one. */
export function documentRoute(call: CallNumber, revision: bigint | null) {
  const query: Record<string, string> = { account: call.accountId, graph: call.graphId, node: call.graphAddressId, doc: call.docId }
  if (revision !== null) query.revision = revision.toString()
  return { name: 'repository-document', query }
}
