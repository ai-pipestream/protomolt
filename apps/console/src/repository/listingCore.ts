/**
 * The documents table: turns the filter form into a ListDocumentsRequest and
 * keeps the page tokens for paging. A blank form field means "no filter on
 * this field"; anything else that cannot be sent as typed is refused with the
 * field named, before a request is built.
 */
import { create, type MessageInitShape } from '@bufbuild/protobuf'
import { timestampFromDate } from '@bufbuild/protobuf/wkt'
import { ConceptExpansionSchema, ConceptRefSchema } from '@/gen/ai/protomolt/proto/types/v1/concepts_pb'
import {
  ConceptFilterSchema,
  DocumentFilterSchema,
  DocumentSortField,
  DocumentSortSchema,
  ListDocumentsRequestSchema,
  MetadataMatchSchema,
  type DocumentFilter,
  type DocumentSummary,
  type ListDocumentsRequest,
} from '@/gen/ai/protomolt/proto/repo/v2/document_service_pb'
import type { SaveOwner } from '@/gen/ai/protomolt/proto/repo/v2/manifest_pb'
import { instant } from './formatCore'
import { ownerLabel } from './documentCore'

/** The exact-match text filters, by DocumentFilter field name. */
export const TEXT_FILTERS = [
  { field: 'docId', label: 'Document ID' },
  { field: 'title', label: 'Title' },
  { field: 'documentType', label: 'Document type' },
  { field: 'sourceUri', label: 'Source URI' },
  { field: 'sourceMimeType', label: 'Source MIME type' },
  { field: 'language', label: 'Language' },
  { field: 'author', label: 'Author' },
  { field: 'category', label: 'Category' },
  { field: 'datasourceId', label: 'Datasource ID' },
  { field: 'connectorId', label: 'Connector ID' },
] as const

export type TextFilterField = (typeof TEXT_FILTERS)[number]['field']

/** The filter form as the screen edits it. */
export interface FilterForm {
  text: Record<TextFilterField, string>
  /** `datetime-local` values (local time, no zone); blank is unset. */
  committedAfter: string
  committedBefore: string
  metadata: { key: string; value: string }[]
  owner: { kind: 'any' | 'datasource' | 'module'; id: string }
  /** The SKOS concept filter; blank scheme and concept mean no concept filter. */
  concept: ConceptForm
}

/** A concept filter as the form edits it: the ConceptRef and its optional expansion steps. */
export interface ConceptForm {
  schemeId: string
  conceptId: string
  narrower: boolean
  closeMatch: boolean
}

/** Everything a listing query names besides the page token. */
export interface ListingQuery {
  accountId: string
  graphId: string
  graphAddressId: string
  includeTombstoned: boolean
  pageSize: number
  filter: FilterForm
  sortField: DocumentSortField
  descending: boolean
}

/** A form value that cannot be sent; `field` is the label the form shows. */
export class ListingInputError extends Error {
  constructor(
    readonly field: string,
    message: string,
  ) {
    super(`${field}: ${message}`)
    this.name = 'ListingInputError'
  }
}

export function emptyFilterForm(): FilterForm {
  return {
    text: Object.fromEntries(TEXT_FILTERS.map((f) => [f.field, ''])) as Record<TextFilterField, string>,
    committedAfter: '',
    committedBefore: '',
    metadata: [],
    owner: { kind: 'any', id: '' },
    concept: { schemeId: '', conceptId: '', narrower: false, closeMatch: false },
  }
}

/** The page size the table opens with; the server accepts 1 to 1000. */
export const DEFAULT_PAGE_SIZE = 50

export function newListingQuery(accountId: string): ListingQuery {
  return {
    accountId,
    graphId: '',
    graphAddressId: '',
    includeTombstoned: false,
    pageSize: DEFAULT_PAGE_SIZE,
    filter: emptyFilterForm(),
    sortField: DocumentSortField.UNSPECIFIED,
    descending: false,
  }
}

// A value that is present but only whitespace is a mistake the server would
// refuse; name it here instead of sending it or dropping it.
function present(label: string, raw: string): string | undefined {
  if (raw === '') return undefined
  if (raw.trim() === '') throw new ListingInputError(label, 'is only whitespace')
  return raw
}

function localInstant(label: string, raw: string): Date | undefined {
  if (raw === '') return undefined
  const date = new Date(raw)
  if (Number.isNaN(date.getTime())) throw new ListingInputError(label, `"${raw}" is not a date and time`)
  return date
}

/** The DocumentFilter the form describes, or undefined when nothing is set. */
export function buildFilter(form: FilterForm): DocumentFilter | undefined {
  const init: MessageInitShape<typeof DocumentFilterSchema> = {}
  let any = false
  for (const { field, label } of TEXT_FILTERS) {
    const value = present(label, form.text[field])
    if (value !== undefined) {
      init[field] = value
      any = true
    }
  }
  const after = localInstant('Committed at or after', form.committedAfter)
  const before = localInstant('Committed before', form.committedBefore)
  if (after && before && before.getTime() <= after.getTime()) {
    throw new ListingInputError('Committed before', 'must be later than "committed at or after"')
  }
  if (after) {
    init.committedAfter = timestampFromDate(after)
    any = true
  }
  if (before) {
    init.committedBefore = timestampFromDate(before)
    any = true
  }
  const seen = new Map<string, string>()
  const metadata = form.metadata.map(({ key, value }, i) => {
    const name = present(`Metadata key ${i + 1}`, key)
    if (name === undefined) throw new ListingInputError(`Metadata key ${i + 1}`, 'is blank')
    const earlier = seen.get(name)
    if (earlier !== undefined && earlier !== value) {
      throw new ListingInputError(`Metadata key ${i + 1}`, `"${name}" is already required to equal "${earlier}"`)
    }
    seen.set(name, value)
    return create(MetadataMatchSchema, { key: name, value })
  })
  if (metadata.length > 0) {
    init.metadata = metadata
    any = true
  }
  if (form.owner.kind !== 'any') {
    const label = form.owner.kind === 'datasource' ? 'Owner datasource ID' : 'Owner module ID'
    const id = present(label, form.owner.id)
    if (id === undefined) throw new ListingInputError(label, 'is blank')
    init.owner =
      form.owner.kind === 'datasource'
        ? { case: 'ownerDatasourceId', value: id }
        : { case: 'ownerModuleId', value: id }
    any = true
  }
  const concept = conceptFilter(form.concept)
  if (concept !== undefined) {
    init.concept = concept
    any = true
  }
  return any ? create(DocumentFilterSchema, init) : undefined
}

/**
 * The ConceptFilter the concept fields describe, or undefined when both IDs are
 * blank. A scheme without a concept (or the reverse) names no concept and is
 * refused by name; expansion steps without a concept are refused too, so a
 * checked box is never silently dropped. The expansion is always sent with a
 * concept, so the request states both steps explicitly.
 */
export function conceptFilter(form: ConceptForm) {
  const schemeId = present('Concept scheme ID', form.schemeId)
  const conceptId = present('Concept ID', form.conceptId)
  if (schemeId === undefined && conceptId === undefined) {
    if (form.narrower || form.closeMatch) throw new ListingInputError('Concept ID', 'is blank; expansion steps need a concept')
    return undefined
  }
  if (schemeId === undefined) throw new ListingInputError('Concept scheme ID', 'is blank; a concept is named within its scheme')
  if (conceptId === undefined) throw new ListingInputError('Concept ID', 'is blank; a scheme alone names no concept')
  return create(ConceptFilterSchema, {
    concept: create(ConceptRefSchema, { schemeId, conceptId }),
    expansion: create(ConceptExpansionSchema, { narrower: form.narrower, closeMatch: form.closeMatch }),
  })
}

/** The request for one page of the query. */
export function buildListRequest(query: ListingQuery, pageToken: string): ListDocumentsRequest {
  if (query.accountId.trim() === '') throw new ListingInputError('Account', 'is blank')
  if (!Number.isInteger(query.pageSize) || query.pageSize < 1 || query.pageSize > 1000) {
    throw new ListingInputError('Page size', 'must be a whole number from 1 to 1000')
  }
  if (query.descending && query.sortField === DocumentSortField.UNSPECIFIED) {
    throw new ListingInputError('Descending', 'needs a sort field; call number order is always ascending')
  }
  const filter = buildFilter(query.filter)
  return create(ListDocumentsRequestSchema, {
    accountId: query.accountId,
    graphId: present('Graph ID', query.graphId) ?? '',
    graphAddressId: present('Graph address ID', query.graphAddressId) ?? '',
    includeTombstoned: query.includeTombstoned,
    pageSize: query.pageSize,
    pageToken,
    filter,
    sort:
      query.sortField === DocumentSortField.UNSPECIFIED
        ? undefined
        : create(DocumentSortSchema, { field: query.sortField, descending: query.descending }),
  })
}

/**
 * Page tokens of the current query. Page 1 has the empty token; each page the
 * server answers adds the next one. A token is bound to the query it was
 * issued for, so a changed query starts over at page 1.
 */
export class PageCursor {
  private tokens: string[] = ['']
  private nextToken = ''

  get page(): number {
    return this.tokens.length
  }
  get token(): string {
    return this.tokens[this.tokens.length - 1]
  }
  get hasNext(): boolean {
    return this.nextToken !== ''
  }
  get hasPrevious(): boolean {
    return this.tokens.length > 1
  }
  /** Records the server's next_page_token for the page just shown. */
  answered(nextPageToken: string): void {
    this.nextToken = nextPageToken
  }
  forward(): void {
    if (!this.hasNext) throw new Error('There is no next page.')
    this.tokens.push(this.nextToken)
    this.nextToken = ''
  }
  back(): void {
    if (!this.hasPrevious) throw new Error('There is no previous page.')
    this.tokens.pop()
    this.nextToken = ''
  }
  reset(): void {
    this.tokens = ['']
    this.nextToken = ''
  }
}

/** One table row. Lookup columns are null when the document does not carry the field. */
export interface DocumentRow {
  key: string
  graphId: string
  graphAddressId: string
  docId: string
  revision: string
  committedAt: string | null
  tombstone: boolean
  title: string | null
  documentType: string | null
  sourceMimeType: string | null
  owner: string | null
}

export function documentRow(summary: DocumentSummary): DocumentRow {
  const address = summary.address
  if (address === undefined) throw new Error('ListDocuments returned a summary without an address.')
  const lookup = summary.lookup
  return {
    key: `${address.graphId}\u0000${address.graphAddressId}\u0000${address.docId}`,
    graphId: address.graphId,
    graphAddressId: address.graphAddressId,
    docId: address.docId,
    revision: summary.revision.toString(),
    committedAt: instant(summary.committedAt),
    tombstone: summary.tombstone,
    title: lookup?.title ?? null,
    documentType: lookup?.documentType ?? null,
    sourceMimeType: lookup?.sourceMimeType ?? null,
    owner: summary.owner === undefined ? null : ownerLabel(summary.owner as SaveOwner),
  }
}

/** A route query as vue-router hands it over: a value, a repeated value, or absent. */
export type RouteQuery = Record<string, string | null | (string | null)[] | undefined>

const SORT_NAMES: Record<string, DocumentSortField> = {
  committed_at: DocumentSortField.COMMITTED_AT,
  title: DocumentSortField.TITLE,
}

/**
 * The listing query as route query values, so a listed query survives a
 * reload, the back button and a shared link. Only what differs from a new
 * query is written; page tokens never are, because a token is bound to the
 * query it was issued for and a restored query starts at page 1.
 */
export function listingRouteQuery(query: ListingQuery): Record<string, string | string[]> {
  const out: Record<string, string | string[]> = {}
  if (query.graphId !== '') out.graph = query.graphId
  if (query.graphAddressId !== '') out.node = query.graphAddressId
  if (query.includeTombstoned) out.tombstoned = '1'
  if (query.pageSize !== DEFAULT_PAGE_SIZE) out.size = String(query.pageSize)
  const sort = Object.entries(SORT_NAMES).find(([, field]) => field === query.sortField)
  if (sort !== undefined) out.sort = sort[0]
  if (query.descending) out.desc = '1'
  const f = query.filter
  for (const { field } of TEXT_FILTERS) if (f.text[field] !== '') out[`f.${field}`] = f.text[field]
  if (f.committedAfter !== '') out.after = f.committedAfter
  if (f.committedBefore !== '') out.before = f.committedBefore
  if (f.metadata.length > 0) {
    out.mk = f.metadata.map((m) => m.key)
    out.mv = f.metadata.map((m) => m.value)
  }
  if (f.owner.kind !== 'any') {
    out.owner = f.owner.kind
    out.ownerId = f.owner.id
  }
  if (f.concept.schemeId !== '') out.scheme = f.concept.schemeId
  if (f.concept.conceptId !== '') out.concept = f.concept.conceptId
  if (f.concept.narrower) out.narrower = '1'
  if (f.concept.closeMatch) out.closeMatch = '1'
  return out
}

function one(query: RouteQuery, key: string): string {
  const value = query[key]
  if (value === undefined || value === null) return ''
  if (Array.isArray(value)) throw new ListingInputError(`Link parameter "${key}"`, 'is given more than once')
  return value
}

function many(query: RouteQuery, key: string): string[] {
  const value = query[key]
  if (value === undefined || value === null) return []
  return (Array.isArray(value) ? value : [value]).map((v) => v ?? '')
}

function flag(query: RouteQuery, key: string): boolean {
  const value = one(query, key)
  if (value === '' || value === '0') return false
  if (value === '1') return true
  throw new ListingInputError(`Link parameter "${key}"`, `"${value}" is not 1 or 0`)
}

/**
 * The listing query a route names; the inverse of {@link listingRouteQuery}.
 * A value that does not read back as written (an unknown sort, a page size
 * that is not a number, unpaired metadata) is refused by name rather than
 * replaced by a default.
 */
export function listingQueryFromRoute(accountId: string, query: RouteQuery): ListingQuery {
  const out = newListingQuery(accountId)
  out.graphId = one(query, 'graph')
  out.graphAddressId = one(query, 'node')
  out.includeTombstoned = flag(query, 'tombstoned')
  const size = one(query, 'size')
  if (size !== '') {
    const parsed = Number(size)
    if (!Number.isInteger(parsed)) throw new ListingInputError('Page size', `"${size}" is not a whole number`)
    out.pageSize = parsed
  }
  const sort = one(query, 'sort')
  if (sort !== '') {
    const field = SORT_NAMES[sort]
    if (field === undefined) throw new ListingInputError('Sort by', `"${sort}" is not committed_at or title`)
    out.sortField = field
  }
  out.descending = flag(query, 'desc')
  for (const { field } of TEXT_FILTERS) out.filter.text[field] = one(query, `f.${field}`)
  out.filter.committedAfter = one(query, 'after')
  out.filter.committedBefore = one(query, 'before')
  const keys = many(query, 'mk')
  const values = many(query, 'mv')
  if (keys.length !== values.length) {
    throw new ListingInputError('Metadata', `${keys.length} keys but ${values.length} values in the link`)
  }
  out.filter.metadata = keys.map((key, i) => ({ key, value: values[i] }))
  const owner = one(query, 'owner')
  if (owner !== '') {
    if (owner !== 'datasource' && owner !== 'module') throw new ListingInputError('Owner', `"${owner}" is not datasource or module`)
    out.filter.owner = { kind: owner, id: one(query, 'ownerId') }
  }
  out.filter.concept = {
    schemeId: one(query, 'scheme'),
    conceptId: one(query, 'concept'),
    narrower: flag(query, 'narrower'),
    closeMatch: flag(query, 'closeMatch'),
  }
  return out
}

/** Whether two route queries hold the same values, whatever their key order. */
export function sameRouteQuery(a: RouteQuery, b: RouteQuery): boolean {
  const keys = new Set([...Object.keys(a), ...Object.keys(b)].filter((k) => a[k] !== undefined || b[k] !== undefined))
  for (const key of keys) {
    const left = many(a, key)
    const right = many(b, key)
    if (left.length !== right.length || left.some((v, i) => v !== right[i])) return false
  }
  return true
}
