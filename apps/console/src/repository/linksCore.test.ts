import { create } from '@bufbuild/protobuf'
import { describe, expect, it } from 'vitest'
import { ConceptExpansionSchema, ConceptRefSchema } from '@/gen/ai/protomolt/proto/types/v1/concepts_pb'
import {
  ExternalBindingLifecycle,
  ExternalEndpointSummarySchema,
  ExternalLinkSchema,
  ExternalProjectionLifecycle,
  ExternalProviderEvidenceSchema,
  ExternalProviderSettlement,
  ExternalRetentionPolicy,
  ExternalServingVisibility,
  ExternalSystemBindingSchema,
  ManagedSearchIdentitySchema,
} from '@/gen/ai/protomolt/proto/repo/external/v1/external_publication_pb'
import {
  BacklinkSchema,
  ConceptFilterEvidenceSchema,
  ConceptFilterSchema,
  DocumentFilterSchema,
  DocumentSortField,
  DocumentSortSchema,
  DynamicLinkSelectorSchema,
  ListLinksResponseSchema,
  MetadataMatchSchema,
  MountedConceptSchemeSchema,
  ResolvedLinkSchema,
  StaticLinkStateSchema,
  UnresolvedConceptMappingSchema,
} from '@/gen/ai/protomolt/proto/repo/v2/document_service_pb'
import {
  backlinkRow,
  buildLinkNeighborhoodGraph,
  documentRoute,
  evidenceView,
  externalLinkRow,
  externalLinkRows,
  filterConditions,
  linkRow,
  linkRows,
  listBacklinksRequest,
  listLinksRequest,
  resolveLinkRequest,
  safeExternalHref,
} from './linksCore'
import { at, testAddress } from './repositoryTestKit'

function staticLink(state: { tombstoned?: boolean; unavailable?: boolean; pinned?: bigint } = {}) {
  return create(ResolvedLinkSchema, {
    fieldPath: 'structured_data.cites',
    ordinal: 1,
    relation: 'references',
    label: 'Prior report',
    target: {
      case: 'staticTarget',
      value: create(StaticLinkStateSchema, {
        address: testAddress({ docId: 'doc-2' }),
        pinnedRevision: state.pinned,
        targetTombstoned: state.tombstoned ?? false,
        targetUnavailable: state.unavailable ?? false,
      }),
    },
  })
}

describe('safeExternalHref', () => {
  it('keeps only absolute http and https URLs without credentials or bearer tokens', () => {
    expect(safeExternalHref('https://example.org/rel/reviews')).toBe('https://example.org/rel/reviews')
    expect(safeExternalHref('http://purl.org/dc/terms/references')).toBe('http://purl.org/dc/terms/references')
    expect(safeExternalHref('https://console.example.org/search?collection=claims&page=2')).toBe(
      'https://console.example.org/search?collection=claims&page=2',
    )
  })

  it.each([
    'javascript:alert(1)',
    'JavaScript:alert(document.cookie)',
    ' javascript:alert(1)',
    ' https://example.org/padded ',
    'data:text/html,<script>alert(1)</script>',
    'vbscript:msgbox(1)',
    'file:///etc/passwd',
    'https://user:secret@example.org/items/1',
    'https://bearer-token@example.org/items/1',
    'https://example.org/items/1?access_token=eyJ0eXAi',
    'https://example.org/items/1?view=full&Bearer=secret',
    'https://example.org/items/1?api_key=12345',
    'https://example.org/items/1?X-Amz-Signature=deadbeef',
    'references',
    '/relative/path',
    '//example.org/scheme-relative',
    '',
  ])('refuses %j', (raw) => {
    expect(safeExternalHref(raw)).toBeNull()
  })
})

describe('linkRow', () => {
  it('reads a static link with its target and status', () => {
    const row = linkRow(staticLink({ pinned: 3n }))
    expect(row).toMatchObject({ fieldPath: 'structured_data.cites', ordinal: 1, relation: 'references', relationHref: null, label: 'Prior report' })
    expect(row.target).toEqual({
      kind: 'static',
      call: { accountId: 'acct-a', graphId: 'intake:acct-a', graphAddressId: 'ds-files', docId: 'doc-2' },
      pinnedRevision: 3n,
      status: 'RESOLVED',
    })
  })

  it('names a tombstoned and an unavailable target', () => {
    expect(linkRow(staticLink({ tombstoned: true })).target).toMatchObject({ status: 'TOMBSTONED', pinnedRevision: null })
    expect(linkRow(staticLink({ unavailable: true })).target).toMatchObject({ status: 'UNAVAILABLE' })
  })

  it('describes a dynamic selector condition by condition', () => {
    const row = linkRow(
      create(ResolvedLinkSchema, {
        fieldPath: 'structured_data.related',
        relation: 'https://example.org/rel/same-topic',
        target: {
          case: 'dynamicTarget',
          value: create(DynamicLinkSelectorSchema, {
            filter: create(DocumentFilterSchema, {
              documentType: 'report',
              concept: create(ConceptFilterSchema, {
                concept: create(ConceptRefSchema, { schemeId: 'topics', conceptId: 'energy' }),
                expansion: create(ConceptExpansionSchema, { narrower: true }),
              }),
            }),
            sort: create(DocumentSortSchema, { field: DocumentSortField.COMMITTED_AT, descending: true }),
          }),
        },
      }),
    )
    expect(row.relationHref).toBe('https://example.org/rel/same-topic')
    expect(row.target).toEqual({
      kind: 'dynamic',
      conditions: ['document_type = report', 'concept topics/energy (with narrower)'],
      order: 'COMMITTED_AT descending',
    })
  })

  it('refuses a link the server sent without a target', () => {
    expect(() => linkRow(create(ResolvedLinkSchema, { fieldPath: 'f', relation: 'references' }))).toThrow(
      'neither a static nor a dynamic target',
    )
    expect(() =>
      linkRow(create(ResolvedLinkSchema, { fieldPath: 'f', relation: 'references', target: { case: 'staticTarget', value: create(StaticLinkStateSchema) } })),
    ).toThrow('without a target address')
  })

  it('keeps ListLinks order', () => {
    const rows = linkRows(create(ListLinksResponseSchema, { revision: 2n, links: [staticLink(), { ...staticLink(), ordinal: 0 }] }))
    expect(rows.map((r) => r.ordinal)).toEqual([1, 0])
  })
})

describe('filterConditions', () => {
  it('lists every set field and says when nothing narrows the selector', () => {
    expect(
      filterConditions(
        create(DocumentFilterSchema, {
          title: 'Q3',
          committedAfter: at('2026-10-01T00:00:00Z'),
          metadata: [create(MetadataMatchSchema, { key: 'team', value: 'a' })],
          owner: { case: 'ownerModuleId', value: 'parser' },
          concept: create(ConceptFilterSchema, {
            concept: create(ConceptRefSchema, { schemeId: 's', conceptId: 'c' }),
            expansion: create(ConceptExpansionSchema, { narrower: true, closeMatch: true }),
          }),
        }),
      ),
    ).toEqual([
      'title = Q3',
      'committed at or after 2026-10-01T00:00:00.000Z',
      'metadata team = a',
      'owner module parser',
      'concept s/c (with narrower, close match)',
    ])
    expect(filterConditions(create(DocumentFilterSchema))).toEqual(['every current document in the account'])
  })
})

describe('evidenceView', () => {
  it('shows the consulted scheme versions and the mappings skipped', () => {
    expect(evidenceView(undefined)).toBeNull()
    expect(
      evidenceView(
        create(ConceptFilterEvidenceSchema, {
          expandedConcepts: 4,
          mountedSchemes: [create(MountedConceptSchemeSchema, { schemeId: 'topics', version: 'v7' })],
          unresolvedMappings: [
            create(UnresolvedConceptMappingSchema, {
              from: create(ConceptRefSchema, { schemeId: 'topics', conceptId: 'energy' }),
              relation: 'close_match',
              target: create(ConceptRefSchema, { schemeId: 'eurovoc', conceptId: '1234' }),
              reason: 'scheme eurovoc is not mounted',
            }),
          ],
        }),
      ),
    ).toEqual({
      expandedConcepts: 4,
      schemes: [{ schemeId: 'topics', version: 'v7' }],
      unresolved: [{ from: 'topics/energy', relation: 'close_match', target: 'eurovoc/1234', reason: 'scheme eurovoc is not mounted' }],
    })
  })
})

describe('requests', () => {
  it('pins ListLinks and ResolveLink to the revision shown', () => {
    const links = listLinksRequest(testAddress(), 3n)
    expect(links.revision?.selector).toEqual({ case: 'revision', value: 3n })
    const dynamic = linkRow(
      create(ResolvedLinkSchema, {
        fieldPath: 'structured_data.related',
        ordinal: 2,
        relation: 'related',
        target: { case: 'dynamicTarget', value: create(DynamicLinkSelectorSchema, { filter: create(DocumentFilterSchema) }) },
      }),
    )
    const resolve = resolveLinkRequest(testAddress(), 3n, dynamic, 'p2')
    expect(resolve).toMatchObject({ fieldPath: 'structured_data.related', ordinal: 2, pageSize: 50, pageToken: 'p2' })
    expect(resolve.revision?.selector).toEqual({ case: 'revision', value: 3n })
    expect(() => resolveLinkRequest(testAddress(), 3n, linkRow(staticLink()), '')).toThrow('only a dynamic link resolves')
    expect(listBacklinksRequest(testAddress(), '')).toMatchObject({ pageSize: 50, pageToken: '' })
  })

  it('routes to a call number at an exact or the current revision', () => {
    const call = { accountId: 'a', graphId: 'g', graphAddressId: 'n', docId: 'd' }
    expect(documentRoute(call, 4n)).toEqual({ name: 'repository-document', query: { account: 'a', graph: 'g', node: 'n', doc: 'd', revision: '4' } })
    expect(documentRoute(call, null).query).not.toHaveProperty('revision')
  })
})

describe('backlinkRow', () => {
  it('reads the linking document, its revision and the pin', () => {
    const row = backlinkRow(
      create(BacklinkSchema, { address: testAddress({ docId: 'src' }), revision: 5n, fieldPath: 'structured_data.cites', relation: 'javascript:alert(1)', pinnedRevision: 2n }),
    )
    expect(row).toMatchObject({ revision: 5n, fieldPath: 'structured_data.cites', relationHref: null, pinnedRevision: 2n })
    expect(row.call.docId).toBe('src')
    expect(() => backlinkRow(create(BacklinkSchema, { revision: 1n }))).toThrow('without a linking address')
  })
})

describe('buildLinkNeighborhoodGraph (#695)', () => {
  it('lays out inbound backlinks, the current document, and outbound static/dynamic targets in 3 columns', () => {
    const current = { accountId: 'acct-a', graphId: 'intake:acct-a', graphAddressId: 'ds-files', docId: 'doc-1' }
    const staticOut = linkRow(staticLink({ pinned: 3n }))
    const dynamicOut = linkRow(
      create(ResolvedLinkSchema, {
        fieldPath: 'structured_data.related',
        ordinal: 0,
        relation: 'related',
        target: {
          case: 'dynamicTarget',
          value: create(DynamicLinkSelectorSchema, {
            filter: create(DocumentFilterSchema, { documentType: 'report' }),
          }),
        },
      }),
    )
    const inbound = backlinkRow(
      create(BacklinkSchema, {
        address: testAddress({ docId: 'citing-doc' }),
        revision: 4n,
        fieldPath: 'structured_data.cites',
        relation: 'references',
      }),
    )

    const beforeResolve = buildLinkNeighborhoodGraph(current, 3n, [staticOut, dynamicOut], [inbound])
    expect(beforeResolve.nodes.map((n) => ({ docId: n.docId, column: n.column, status: n.status }))).toEqual([
      { docId: 'citing-doc', column: 'inbound', status: 'RESOLVED' },
      { docId: 'doc-1', column: 'current', status: 'CURRENT' },
      { docId: 'doc-2', column: 'outbound', status: 'RESOLVED' },
      { docId: 'structured_data.related[0]', column: 'outbound', status: 'DYNAMIC' },
    ])
    expect(beforeResolve.edges).toHaveLength(3)

    const afterResolve = buildLinkNeighborhoodGraph(current, 3n, [staticOut, dynamicOut], [inbound], {
      [dynamicOut.key]: [{ graphId: 'intake:acct-a', graphAddressId: 'ds-files', docId: 'solar-report', revision: '2' }],
    })
    expect(afterResolve.nodes.find((n) => n.docId === 'solar-report')).toMatchObject({
      column: 'outbound',
      status: 'RESOLVED',
    })
  })
})

describe('externalLinkRow (#814)', () => {
  function sampleExternalLink(overrides: { displayUrl?: string; externalVersion?: string; retired?: boolean } = {}) {
    const retired = overrides.retired ?? false
    return create(ExternalLinkSchema, {
      bindingId: 'search-primary',
      bindingRevision: 2n,
      bindingSnapshot: create(ExternalSystemBindingSchema, {
        bindingId: 'search-primary',
        accountId: 'acct-a',
        bindingRevision: 2n,
        provider: 'protomolt-search',
        serviceProfileId: 'search-profile',
        serviceEndpointId: 'grpc-1',
        dataset: 'claims-index',
        namespace: 'prod-us',
        contractVersion: 'managed-search/v1',
        endpointSummary: create(ExternalEndpointSummarySchema, {
          host: 'search.internal.example.org',
          port: 8443,
          tlsEnabled: true,
        }),
        lifecycle: retired ? ExternalBindingLifecycle.RETIRED : ExternalBindingLifecycle.ACTIVE,
        retentionPolicy: ExternalRetentionPolicy.RETAIN_HISTORY,
        createdAt: at('2026-10-01T00:00:00Z'),
        retiredReason: retired ? 'rotated to v3' : undefined,
        retiredAt: retired ? at('2026-10-02T00:00:00Z') : undefined,
      }),
      externalObjectId: 'claims-index:doc-1',
      externalVersion: overrides.externalVersion,
      sourceAddress: testAddress({ docId: 'doc-1' }),
      sourceRevision: 7n,
      managedSearch: create(ManagedSearchIdentitySchema, {
        historyId: new Uint8Array(16),
        documentKey: new Uint8Array([1, 2, 3]),
        indexKey: new Uint8Array([4, 5]),
        acceptedVersion: 3n,
        acceptedSequence: 42n,
        publishedCatalogEpoch: 9n,
        afterManifestSha256: 'a'.repeat(64),
        servingVisibility: ExternalServingVisibility.CURRENT_ROWS,
      }),
      displayUrl: overrides.displayUrl,
      confirmedByOperationId: 'op-ext-7',
      confirmationEvidence: create(ExternalProviderEvidenceSchema, {
        provider: 'protomolt-search',
        contractVersion: 'managed-search/v1',
        verifiedServiceProfileId: 'search-profile',
        verifiedServiceEndpointId: 'grpc-1',
        credentialBindingSha256: 'a'.repeat(64),
        externalObjectId: 'claims-index:doc-1',
        externalVersion: overrides.externalVersion,
        requestIntentSha256: 'b'.repeat(64),
        receiptSha256: 'c'.repeat(64),
        providerReceiptId: 'decision-42',
        settlement: ExternalProviderSettlement.PUBLISHED,
        verifiedAt: at('2026-10-03T12:00:00Z'),
      }),
      projectionLifecycle: ExternalProjectionLifecycle.ACTIVE,
      confirmedAt: at('2026-10-03T12:00:00Z'),
    })
  }

  it('projects canonical external identity, historical binding snapshot, and guarded displayHref', () => {
    const row = externalLinkRow(
      sampleExternalLink({
        displayUrl: 'https://console.example.org/search/claims-index/doc-1',
        externalVersion: 'v3-epoch9',
        retired: true,
      }),
    )
    expect(row).toEqual({
      key: 'search-primary\u00002\u0000intake:acct-a\u0000ds-files\u0000doc-1\u00007',
      bindingId: 'search-primary',
      bindingRevision: 2n,
      provider: 'protomolt-search',
      dataset: 'claims-index',
      namespace: 'prod-us',
      contractVersion: 'managed-search/v1',
      endpointSummary: 'tls://search.internal.example.org:8443',
      bindingLifecycle: 'RETIRED',
      externalObjectId: 'claims-index:doc-1',
      externalVersion: 'v3-epoch9',
      sourceCall: { accountId: 'acct-a', graphId: 'intake:acct-a', graphAddressId: 'ds-files', docId: 'doc-1' },
      sourceRevision: 7n,
      projectionLifecycle: 'ACTIVE',
      confirmedByOperationId: 'op-ext-7',
      confirmedAt: '2026-10-03T12:00:00.000Z',
      providerReceiptId: 'decision-42',
      servingVisibility: 'CURRENT_ROWS',
      displayHref: 'https://console.example.org/search/claims-index/doc-1',
    })
  })

  it('preserves explicit absence of externalVersion and strips unsafe or bearer displayUrl without altering canonical identity', () => {
    const rows = externalLinkRows([
      sampleExternalLink({ displayUrl: 'https://token@console.example.org/doc-1' }),
      sampleExternalLink({ displayUrl: 'https://console.example.org/doc-1?access_token=secret' }),
      sampleExternalLink({ displayUrl: 'javascript:alert(1)' }),
    ])
    for (const row of rows) {
      expect(row.externalObjectId).toBe('claims-index:doc-1')
      expect(row.externalVersion).toBeNull()
      expect(row.displayHref).toBeNull()
    }
  })

  it('rejects incomplete wire ExternalLink records', () => {
    expect(() => externalLinkRow(create(ExternalLinkSchema, { bindingId: 'b1' }))).toThrow('missing sourceAddress')
  })
})
