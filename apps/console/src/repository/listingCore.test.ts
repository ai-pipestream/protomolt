import { create } from '@bufbuild/protobuf'
import { timestampDate } from '@bufbuild/protobuf/wkt'
import { describe, expect, it } from 'vitest'
import {
  DocumentLookupSchema,
  DocumentService,
  DocumentSortField,
  DocumentSummarySchema,
  ListDocumentsResponseSchema,
  type ListDocumentsRequest,
} from '@/gen/ai/protomolt/proto/repo/v2/document_service_pb'
import { SaveOwnerSchema } from '@/gen/ai/protomolt/proto/repo/v2/manifest_pb'
import {
  ListingInputError,
  PageCursor,
  buildFilter,
  buildListRequest,
  conceptFilter,
  documentRow,
  emptyFilterForm,
  listingQueryFromRoute,
  listingRouteQuery,
  newListingQuery,
  sameRouteQuery,
} from './listingCore'
import { at, routerClients, testAddress } from './repositoryTestKit'

describe('buildListRequest', () => {
  it('sends no filter and no sort for a fresh query', () => {
    const request = buildListRequest(newListingQuery('acct-a'), '')
    expect(request.accountId).toBe('acct-a')
    expect(request.pageSize).toBe(50)
    expect(request.filter).toBeUndefined()
    expect(request.sort).toBeUndefined()
    expect(request.includeTombstoned).toBe(false)
  })

  it('carries every set filter as its typed field and leaves blank ones absent', () => {
    const query = newListingQuery('acct-a')
    query.graphId = 'intake:acct-a'
    query.includeTombstoned = true
    query.filter.text.title = 'Quarterly report'
    query.filter.text.sourceMimeType = 'application/pdf'
    query.filter.committedAfter = '2026-10-01T00:00'
    query.filter.committedBefore = '2026-10-02T00:00'
    query.filter.metadata = [{ key: 'team', value: 'search' }, { key: 'team', value: 'search' }]
    query.filter.owner = { kind: 'module', id: 'parser' }
    query.sortField = DocumentSortField.TITLE
    query.descending = true
    const request = buildListRequest(query, 'tok-2')
    expect(request.graphId).toBe('intake:acct-a')
    expect(request.graphAddressId).toBe('')
    expect(request.pageToken).toBe('tok-2')
    expect(request.includeTombstoned).toBe(true)
    const filter = request.filter!
    expect(filter.title).toBe('Quarterly report')
    expect(filter.sourceMimeType).toBe('application/pdf')
    expect(filter.author).toBeUndefined()
    expect(filter.docId).toBeUndefined()
    expect(timestampDate(filter.committedAfter!).getTime()).toBe(new Date('2026-10-01T00:00').getTime())
    expect(filter.metadata.map((m) => [m.key, m.value])).toEqual([
      ['team', 'search'],
      ['team', 'search'],
    ])
    expect(filter.owner).toEqual({ case: 'ownerModuleId', value: 'parser' })
    expect(request.sort?.field).toBe(DocumentSortField.TITLE)
    expect(request.sort?.descending).toBe(true)
  })

  it('keeps an empty metadata value, since an exact match on "" is a real filter', () => {
    const form = emptyFilterForm()
    form.metadata = [{ key: 'tag', value: '' }]
    expect(buildFilter(form)!.metadata[0].value).toBe('')
  })

  it.each([
    ['whitespace title', (q: ReturnType<typeof newListingQuery>) => (q.filter.text.title = '   '), 'Title: is only whitespace'],
    ['blank metadata key', (q: ReturnType<typeof newListingQuery>) => (q.filter.metadata = [{ key: '', value: 'x' }]), 'Metadata key 1: is blank'],
    [
      'contradictory metadata',
      (q: ReturnType<typeof newListingQuery>) =>
        (q.filter.metadata = [{ key: 'team', value: 'a' }, { key: 'team', value: 'b' }]),
      'Metadata key 2: "team" is already required to equal "a"',
    ],
    [
      'unordered committed range',
      (q: ReturnType<typeof newListingQuery>) => {
        q.filter.committedAfter = '2026-10-02T00:00'
        q.filter.committedBefore = '2026-10-02T00:00'
      },
      'Committed before: must be later',
    ],
    ['unparsable date', (q: ReturnType<typeof newListingQuery>) => (q.filter.committedAfter = 'yesterday'), 'Committed at or after: "yesterday" is not a date'],
    ['owner kind without an id', (q: ReturnType<typeof newListingQuery>) => (q.filter.owner = { kind: 'datasource', id: '' }), 'Owner datasource ID: is blank'],
    ['descending without a field', (q: ReturnType<typeof newListingQuery>) => (q.descending = true), 'Descending: needs a sort field'],
    ['page size zero', (q: ReturnType<typeof newListingQuery>) => (q.pageSize = 0), 'Page size'],
    ['page size above the server bound', (q: ReturnType<typeof newListingQuery>) => (q.pageSize = 1001), 'Page size'],
    ['whitespace graph scope', (q: ReturnType<typeof newListingQuery>) => (q.graphId = ' '), 'Graph ID: is only whitespace'],
  ])('refuses %s by name', (_name, edit, message) => {
    const query = newListingQuery('acct-a')
    edit(query)
    expect(() => buildListRequest(query, '')).toThrow(ListingInputError)
    expect(() => buildListRequest(query, '')).toThrow(message)
  })
})

describe('PageCursor', () => {
  it('walks forward on next_page_token and back to page 1, and starts over on reset', () => {
    const cursor = new PageCursor()
    expect(cursor.page).toBe(1)
    expect(cursor.token).toBe('')
    expect(cursor.hasNext).toBe(false)
    cursor.answered('t2')
    cursor.forward()
    expect(cursor.page).toBe(2)
    expect(cursor.token).toBe('t2')
    cursor.answered('t3')
    cursor.forward()
    expect(cursor.token).toBe('t3')
    cursor.answered('')
    expect(cursor.hasNext).toBe(false)
    expect(() => cursor.forward()).toThrow('no next page')
    cursor.back()
    expect(cursor.token).toBe('t2')
    cursor.reset()
    expect(cursor.page).toBe(1)
    expect(cursor.hasPrevious).toBe(false)
    expect(() => cursor.back()).toThrow('no previous page')
  })
})

describe('documentRow', () => {
  it('shows lookup columns only when the document carries the field, and tombstones without lookup', () => {
    const live = documentRow(
      create(DocumentSummarySchema, {
        address: testAddress(),
        revision: 3n,
        committedAt: at('2026-10-01T12:00:00Z'),
        lookup: create(DocumentLookupSchema, { title: 'Report', datasourceId: 'ds-files' }),
        owner: create(SaveOwnerSchema, { owner: { case: 'intake', value: { datasourceId: 'ds-files', connectorId: 'fs' } } }),
      }),
    )
    expect(live).toMatchObject({
      docId: 'doc-1',
      revision: '3',
      committedAt: '2026-10-01T12:00:00.000Z',
      title: 'Report',
      documentType: null,
      owner: 'intake ds-files via fs',
      tombstone: false,
    })
    const tomb = documentRow(
      create(DocumentSummarySchema, { address: testAddress({ docId: 'doc-2' }), revision: 4n, committedAt: at('2026-10-02T00:00:00Z'), tombstone: true }),
    )
    expect(tomb).toMatchObject({ tombstone: true, title: null, owner: null })
  })

  it('refuses a summary without an address', () => {
    expect(() => documentRow(create(DocumentSummarySchema, { revision: 1n }))).toThrow('without an address')
  })
})

describe('ListDocuments over the in-memory Connect router', () => {
  it('round-trips the typed filter and sort in binary protobuf', async () => {
    const seen: ListDocumentsRequest[] = []
    const clients = routerClients((router) =>
      router.service(DocumentService, {
        listDocuments(request) {
          seen.push(request)
          return create(ListDocumentsResponseSchema, {
            documents: [create(DocumentSummarySchema, { address: testAddress(), revision: 1n, committedAt: at('2026-10-01T00:00:00Z') })],
            nextPageToken: 'next',
          })
        },
      }),
    )
    const query = newListingQuery('acct-a')
    query.filter.metadata = [{ key: 'k', value: 'v' }]
    query.filter.owner = { kind: 'datasource', id: 'ds-files' }
    query.sortField = DocumentSortField.COMMITTED_AT
    const response = await clients.documents.listDocuments(buildListRequest(query, ''))
    expect(response.nextPageToken).toBe('next')
    expect(response.documents.map(documentRow)[0].docId).toBe('doc-1')
    expect(seen[0].filter?.owner).toEqual({ case: 'ownerDatasourceId', value: 'ds-files' })
    expect(seen[0].filter?.metadata[0].key).toBe('k')
    expect(seen[0].sort?.field).toBe(DocumentSortField.COMMITTED_AT)
  })
})

describe('conceptFilter', () => {
  it('sends the ConceptRef with both expansion steps stated', () => {
    const filter = buildFilter({ ...emptyFilterForm(), concept: { schemeId: 'topics', conceptId: 'energy', narrower: true, closeMatch: false } })
    expect(filter?.concept?.concept).toMatchObject({ schemeId: 'topics', conceptId: 'energy' })
    expect(filter?.concept?.expansion).toMatchObject({ narrower: true, closeMatch: false })
    const close = conceptFilter({ schemeId: 'topics', conceptId: 'energy', narrower: false, closeMatch: true })
    expect(close?.expansion).toMatchObject({ narrower: false, closeMatch: true })
  })

  it('sends no concept for blank IDs and refuses a half-named concept by field', () => {
    expect(buildFilter(emptyFilterForm())).toBeUndefined()
    const half = (schemeId: string, conceptId: string, narrower = false) => () =>
      conceptFilter({ schemeId, conceptId, narrower, closeMatch: false })
    expect(half('topics', '')).toThrow('Concept ID: is blank; a scheme alone names no concept')
    expect(half('', 'energy')).toThrow('Concept scheme ID: is blank')
    expect(half('', '', true)).toThrow('Concept ID: is blank; expansion steps need a concept')
    expect(half('  ', 'energy')).toThrow('Concept scheme ID: is only whitespace')
    expect(half('topics', '', false)).toThrow(ListingInputError)
  })
})

describe('listing route query', () => {
  it('writes only what differs from a new query', () => {
    expect(listingRouteQuery(newListingQuery('acct-a'))).toEqual({})
  })

  it('round-trips every query field, the concept filter included', () => {
    const query = newListingQuery('acct-a')
    query.graphId = 'intake:acct-a'
    query.graphAddressId = 'ds-files'
    query.includeTombstoned = true
    query.pageSize = 20
    query.sortField = DocumentSortField.TITLE
    query.descending = true
    query.filter.text.title = 'Q3 & Q4 = report?'
    query.filter.text.datasourceId = 'ds-files'
    query.filter.committedAfter = '2026-10-01T00:00'
    query.filter.metadata = [{ key: 'a=b', value: 'c&d' }, { key: 'team', value: '' }]
    query.filter.owner = { kind: 'module', id: 'parser' }
    query.filter.concept = { schemeId: 'topics', conceptId: 'energy', narrower: true, closeMatch: true }
    const route = listingRouteQuery(query)
    expect(route).toMatchObject({ scheme: 'topics', concept: 'energy', narrower: '1', closeMatch: '1', sort: 'title', desc: '1' })
    const back = listingQueryFromRoute('acct-a', route)
    expect(back).toEqual(query)
    expect(buildListRequest(back, '')).toEqual(buildListRequest(query, ''))
  })

  it('refuses a route value it cannot read instead of defaulting it', () => {
    expect(() => listingQueryFromRoute('a', { sort: 'size' })).toThrow('Sort by: "size" is not committed_at or title')
    expect(() => listingQueryFromRoute('a', { size: 'ten' })).toThrow('Page size: "ten" is not a whole number')
    expect(() => listingQueryFromRoute('a', { narrower: 'yes' })).toThrow('"yes" is not 1 or 0')
    expect(() => listingQueryFromRoute('a', { mk: ['k1', 'k2'], mv: 'v1' })).toThrow('2 keys but 1 values')
    expect(() => listingQueryFromRoute('a', { scheme: ['s1', 's2'] })).toThrow('is given more than once')
    expect(() => listingQueryFromRoute('a', { owner: 'anyone' })).toThrow('"anyone" is not datasource or module')
  })

  it('compares route queries by value, not key order', () => {
    expect(sameRouteQuery({ a: '1', b: ['x', 'y'] }, { b: ['x', 'y'], a: '1' })).toBe(true)
    expect(sameRouteQuery({ a: '1' }, { a: '1', b: 'x' })).toBe(false)
    expect(sameRouteQuery({ b: ['x', 'y'] }, { b: ['y', 'x'] })).toBe(false)
    expect(sameRouteQuery({}, {})).toBe(true)
  })
})
