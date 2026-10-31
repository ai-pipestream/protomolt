// @vitest-environment jsdom
import { create } from '@bufbuild/protobuf'
import { Code, ConnectError, type ConnectRouter } from '@connectrpc/connect'
import { describe, expect, it } from 'vitest'
import { ConceptExpansionSchema, ConceptRefSchema } from '@/gen/ai/protomolt/proto/types/v1/concepts_pb'
import {
  BacklinkSchema,
  ConceptFilterEvidenceSchema,
  ConceptFilterSchema,
  DocumentFilterSchema,
  DocumentLookupSchema,
  DocumentService,
  DocumentSummarySchema,
  DynamicLinkSelectorSchema,
  GetDocumentResponseSchema,
  GetManifestResponseSchema,
  ListBacklinksResponseSchema,
  ListLinksResponseSchema,
  MountedConceptSchemeSchema,
  ResolveLinkResponseSchema,
  ResolvedLinkSchema,
  StaticLinkStateSchema,
  UnresolvedConceptMappingSchema,
  type ListBacklinksRequest,
  type ListLinksRequest,
  type ResolveLinkRequest,
} from '@/gen/ai/protomolt/proto/repo/v2/document_service_pb'
import { DocumentManifestSchema, RevisionValidationSchema, ValidationStatus } from '@/gen/ai/protomolt/proto/repo/v2/manifest_pb'
import { at, testAddress } from '@/repository/repositoryTestKit'
import { mountRepositoryView, settle } from '@/repository/repositoryViewKit'
import RepositoryDocumentView from './RepositoryDocumentView.vue'

const DOC = '/repository/document?account=acct-a&graph=intake:acct-a&node=ds-files&doc=doc-1'

function staticLink(fieldPath: string, docId: string, relation: string, state: { tombstoned?: boolean; unavailable?: boolean; pinned?: bigint } = {}) {
  return create(ResolvedLinkSchema, {
    fieldPath,
    relation,
    target: {
      case: 'staticTarget',
      value: create(StaticLinkStateSchema, {
        address: testAddress({ docId }),
        pinnedRevision: state.pinned,
        targetTombstoned: state.tombstoned ?? false,
        targetUnavailable: state.unavailable ?? false,
      }),
    },
  })
}

const DYNAMIC = create(ResolvedLinkSchema, {
  fieldPath: 'structured_data.related',
  relation: 'related',
  label: 'Same topic',
  target: {
    case: 'dynamicTarget',
    value: create(DynamicLinkSelectorSchema, {
      filter: create(DocumentFilterSchema, {
        concept: create(ConceptFilterSchema, {
          concept: create(ConceptRefSchema, { schemeId: 'topics', conceptId: 'energy' }),
          expansion: create(ConceptExpansionSchema, { narrower: true }),
        }),
      }),
    }),
  },
})

interface Seen {
  links: ListLinksRequest[]
  resolves: ResolveLinkRequest[]
  backlinks: ListBacklinksRequest[]
}

/** doc-1 at revision 3 (current), linking out and linked into. */
function repository(seen: Seen, links = [
  staticLink('structured_data.cites', 'doc-2', 'references', { pinned: 1n }),
  staticLink('structured_data.replaces', 'doc-old', 'replaces', { tombstoned: true }),
  staticLink('structured_data.hidden', 'doc-secret', 'requires', { unavailable: true }),
  DYNAMIC,
], holdResolve?: Promise<void>) {
  return (router: ConnectRouter) =>
    router.service(DocumentService, {
      getManifest(request) {
        const s = request.revision?.selector
        const revision = s?.case === 'revision' ? s.value : 3n
        return create(GetManifestResponseSchema, {
          manifest: create(DocumentManifestSchema, { address: request.address, revision }),
          validation: create(RevisionValidationSchema, { status: ValidationStatus.TYPED_VALID }),
        })
      },
      getDocument: () => create(GetDocumentResponseSchema, { validation: create(RevisionValidationSchema) }),
      listLinks(request) {
        seen.links.push(request)
        const s = request.revision?.selector
        return create(ListLinksResponseSchema, { revision: s?.case === 'revision' ? s.value : 3n, links })
      },
      async resolveLink(request) {
        seen.resolves.push(request)
        if (holdResolve !== undefined && seen.resolves.length === 1) await holdResolve
        const first = request.pageToken === ''
        return create(ResolveLinkResponseSchema, {
          documents: [
            create(DocumentSummarySchema, {
              address: testAddress({ docId: first ? 'match-1' : 'match-2' }),
              revision: 2n,
              committedAt: at('2026-10-01T00:00:00Z'),
              lookup: create(DocumentLookupSchema, { title: first ? 'Solar' : 'Wind' }),
            }),
          ],
          nextPageToken: first ? 'p2' : '',
          conceptEvidence: create(ConceptFilterEvidenceSchema, {
            expandedConcepts: 3,
            mountedSchemes: [create(MountedConceptSchemeSchema, { schemeId: 'topics', version: '2026-10-20' })],
            unresolvedMappings: [
              create(UnresolvedConceptMappingSchema, {
                from: create(ConceptRefSchema, { schemeId: 'topics', conceptId: 'energy' }),
                relation: 'exact_match',
                target: create(ConceptRefSchema, { schemeId: 'eurovoc', conceptId: '754' }),
                reason: 'scheme eurovoc is not mounted',
              }),
            ],
          }),
        })
      },
      listBacklinks(request) {
        seen.backlinks.push(request)
        return create(ListBacklinksResponseSchema, {
          backlinks: [
            create(BacklinkSchema, { address: testAddress({ docId: 'citing' }), revision: 7n, fieldPath: 'structured_data.cites', relation: 'references' }),
            create(BacklinkSchema, { address: testAddress({ docId: 'pinning' }), revision: 2n, fieldPath: 'structured_data.source', relation: 'source', pinnedRevision: 1n }),
          ],
        })
      },
    })
}

async function openLinks(seen: Seen, location = DOC, links?: Parameters<typeof repository>[1], holdResolve?: Promise<void>) {
  const mounted = await mountRepositoryView(RepositoryDocumentView, location, repository(seen, links, holdResolve))
  await mounted.wrapper.get('[data-testid="tab-links"]').trigger('click')
  await settle()
  return mounted
}

function fresh(): Seen {
  return { links: [], resolves: [], backlinks: [] }
}

describe('RepositoryDocumentView links', () => {
  it('lists the outbound links of the revision shown with each static target status', async () => {
    const seen = fresh()
    const { wrapper } = await openLinks(seen, `${DOC}&revision=2`)
    expect(seen.links).toHaveLength(1)
    expect(seen.links[0].revision?.selector).toEqual({ case: 'revision', value: 2n })
    const cites = wrapper.get('[data-testid="link-structured_data.cites-0"]').text()
    expect(cites).toContain('references')
    expect(cites).toContain('static')
    expect(cites).toContain('doc-2')
    expect(cites).toContain('pinned to revision 1')
    expect(wrapper.get('[data-testid="link-status-structured_data.cites-0"]').text()).toBe('RESOLVED')
    expect(wrapper.get('[data-testid="link-status-structured_data.replaces-0"]').text()).toBe('TOMBSTONED')
    expect(wrapper.get('[data-testid="link-status-structured_data.hidden-0"]').text()).toBe('UNAVAILABLE')
    // An unavailable target has nothing the reader may open.
    expect(wrapper.find('[data-testid="open-link-structured_data.hidden-0"]').exists()).toBe(false)
    const dynamic = wrapper.get('[data-testid="link-structured_data.related-0"]').text()
    expect(dynamic).toContain('dynamic')
    expect(dynamic).toContain('Same topic')
    expect(dynamic).toContain('concept topics/energy (with narrower)')
    wrapper.unmount()
  })

  it('opens a static target at its pinned revision and an unpinned one at current', async () => {
    const seen = fresh()
    const { wrapper, router } = await openLinks(seen)
    await wrapper.get('[data-testid="open-link-structured_data.cites-0"]').trigger('click')
    await settle()
    expect(router.currentRoute.value.query).toEqual({ account: 'acct-a', graph: 'intake:acct-a', node: 'ds-files', doc: 'doc-2', revision: '1' })
    await wrapper.get('[data-testid="tab-links"]').trigger('click')
    await settle()
    await wrapper.get('[data-testid="open-link-structured_data.replaces-0"]').trigger('click')
    await settle()
    expect(router.currentRoute.value.query.doc).toBe('doc-old')
    expect(router.currentRoute.value.query.revision).toBeUndefined()
    wrapper.unmount()
  })

  it('resolves a dynamic link page by page with its concept expansion evidence', async () => {
    const seen = fresh()
    const { wrapper, router } = await openLinks(seen)
    await wrapper.get('[data-testid="resolve-link-structured_data.related-0"]').trigger('click')
    await settle()
    expect(seen.resolves[0]).toMatchObject({ fieldPath: 'structured_data.related', ordinal: 0, pageSize: 50, pageToken: '' })
    expect(seen.resolves[0].revision?.selector).toEqual({ case: 'revision', value: 3n })
    const targets = wrapper.get('[data-testid="resolved-targets"]')
    expect(targets.text()).toContain('Solar')
    const evidence = wrapper.get('[data-testid="concept-evidence"]').text()
    expect(evidence).toContain('Matched against 3 concepts')
    expect(wrapper.get('[data-testid="evidence-scheme-topics"]').text()).toContain('2026-10-20')
    expect(wrapper.get('[data-testid="evidence-unresolved"]').text()).toContain('topics/energy exact_match eurovoc/754: scheme eurovoc is not mounted')

    await wrapper.get('[data-testid="resolve-more"]').trigger('click')
    await settle()
    expect(seen.resolves[1].pageToken).toBe('p2')
    expect(wrapper.get('[data-testid="resolved-targets"]').text()).toContain('Wind')
    expect(wrapper.get('[data-testid="resolved-targets"]').text()).toContain('Solar')
    expect(wrapper.find('[data-testid="resolve-more"]').exists()).toBe(false)

    await wrapper.get('[data-testid="resolved-match-2"]').trigger('click')
    await settle()
    expect(router.currentRoute.value.query).toEqual({ account: 'acct-a', graph: 'intake:acct-a', node: 'ds-files', doc: 'match-2' })
    wrapper.unmount()
  })

  it('lists the backlinks into the document and opens the linking revision', async () => {
    const seen = fresh()
    const { wrapper, router } = await openLinks(seen)
    expect(seen.backlinks[0].target?.docId).toBe('doc-1')
    expect(seen.backlinks[0]).toMatchObject({ pageSize: 50, pageToken: '' })
    const table = wrapper.get('[data-testid="backlinks-table"]').text()
    expect(table).toContain('citing')
    expect(table).toContain('current')
    expect(wrapper.get('[data-testid="backlink-pinning-structured_data.source"]').text()).toContain('revision 1')
    await wrapper.get('[data-testid="backlink-citing-structured_data.cites"]').trigger('click')
    await settle()
    expect(router.currentRoute.value.query).toMatchObject({ doc: 'citing', revision: '7' })
    wrapper.unmount()
  })

  it('never renders a javascript:, data: or other non-http relation as an href', async () => {
    const seen = fresh()
    const { wrapper } = await openLinks(seen, DOC, [
      staticLink('structured_data.a', 'doc-2', 'javascript:alert(document.cookie)'),
      staticLink('structured_data.b', 'doc-2', 'data:text/html,<script>alert(1)</script>'),
      staticLink('structured_data.c', 'doc-2', 'vbscript:msgbox(1)'),
      staticLink('structured_data.d', 'doc-2', 'https://example.org/rel/reviews'),
    ])
    const hrefs = wrapper.findAll('[data-testid="links"] a[href]').map((a) => a.attributes('href'))
    expect(hrefs).toEqual(['https://example.org/rel/reviews'])
    for (const a of wrapper.findAll('[data-testid="links"] a[href]')) {
      expect(a.attributes('rel')).toBe('noopener noreferrer')
      expect(a.attributes('target')).toBe('_blank')
    }
    // The refused relations are still shown, as text.
    const text = wrapper.get('[data-testid="links-table"]').text()
    expect(text).toContain('javascript:alert(document.cookie)')
    expect(text).toContain('data:text/html,<script>alert(1)</script>')
    expect(document.body.innerHTML).not.toContain('href="javascript:')
    expect(document.body.innerHTML).not.toContain('href="data:')
    wrapper.unmount()
  })

  it('shows a failing link read by its code without hiding the backlinks', async () => {
    const { wrapper } = await mountRepositoryView(RepositoryDocumentView, DOC, (router) => {
      router.service(DocumentService, {
        getManifest: (request) =>
          create(GetManifestResponseSchema, {
            manifest: create(DocumentManifestSchema, { address: request.address, revision: 3n }),
            validation: create(RevisionValidationSchema),
          }),
        getDocument: () => create(GetDocumentResponseSchema, { validation: create(RevisionValidationSchema) }),
        listLinks() {
          throw new ConnectError('revision 3 CORE is not readable', Code.FailedPrecondition)
        },
        listBacklinks: () => create(ListBacklinksResponseSchema),
      })
    })
    await wrapper.get('[data-testid="tab-links"]').trigger('click')
    await settle()
    const failure = wrapper.get('[data-testid="rpc-failure"]').text()
    expect(failure).toContain('FAILED_PRECONDITION')
    expect(failure).toContain('revision 3 CORE is not readable')
    expect(wrapper.find('[data-testid="no-links"]').exists()).toBe(false)
    expect(wrapper.get('[data-testid="no-backlinks"]').text()).toContain('No document you can read links here.')
    wrapper.unmount()
  })

  it('renders the 1-hop link neighborhood graph, resolves all dynamic links in batch, and navigates on node click (#695)', async () => {
    const seen = fresh()
    const { wrapper, router } = await openLinks(seen)
    expect(wrapper.find('[data-testid="link-neighborhood-card"]').exists()).toBe(true)
    expect(wrapper.find('[data-testid="link-neighborhood-graph"]').exists()).toBe(true)
    expect(wrapper.get('[data-testid="link-status-structured_data.related-0"]').text()).toBe('DYNAMIC')

    await wrapper.get('[data-testid="resolve-all-dynamic-links"]').trigger('click')
    await settle()
    expect(seen.resolves).toHaveLength(1)
    expect(wrapper.get('[data-testid="link-status-structured_data.related-0"]').text()).toBe('RESOLVED (1)')
    expect(wrapper.find('[data-testid="graph-node-match-1"]').exists()).toBe(true)

    await wrapper.get('[data-testid="graph-node-citing"]').trigger('click')
    await settle()
    expect(router.currentRoute.value.query).toMatchObject({ doc: 'citing', revision: '7' })
    wrapper.unmount()
  })

  it('stops showing Resolve all as loading once a single resolve supersedes it', async () => {
    const seen = fresh()
    let release = () => {}
    const held = new Promise<void>((resolve) => { release = resolve })
    const { wrapper } = await openLinks(seen, DOC, undefined, held)
    const resolveAll = () => wrapper.get('[data-testid="resolve-all-dynamic-links"]')

    await resolveAll().trigger('click')
    await settle()
    expect(resolveAll().classes()).toContain('v-btn--loading')

    await wrapper.get('[data-testid="resolve-link-structured_data.related-0"]').trigger('click')
    await settle()
    expect(resolveAll().classes()).not.toContain('v-btn--loading')
    expect(seen.resolves).toHaveLength(2)
    expect(wrapper.get('[data-testid="link-status-structured_data.related-0"]').text()).toBe('RESOLVED (1)')

    release()
    await settle()
    expect(resolveAll().classes()).not.toContain('v-btn--loading')
    wrapper.unmount()
  })
})
