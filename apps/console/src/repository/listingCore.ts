/**
 * The documents table: turns the filter form into a ListDocumentsRequest and
 * keeps the page tokens for paging. A blank form field means "no filter on
 * this field"; anything else that cannot be sent as typed is refused with the
 * field named, before a request is built.
 */
import { create, type MessageInitShape } from '@bufbuild/protobuf'
import { timestampFromDate } from '@bufbuild/protobuf/wkt'
import {
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
  return any ? create(DocumentFilterSchema, init) : undefined
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
