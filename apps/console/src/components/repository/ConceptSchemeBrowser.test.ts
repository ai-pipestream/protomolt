// @vitest-environment jsdom
import { create, toBinary } from '@bufbuild/protobuf'
import type { ConnectRouter } from '@connectrpc/connect'
import { defineComponent } from 'vue'
import { describe, expect, it } from 'vitest'
import {
  ConceptNotationSchema,
  ConceptRefSchema,
  ConceptSchema,
  ConceptSchemeSchema,
  LangLabelSchema,
} from '@/gen/ai/protomolt/proto/types/v1/concepts_pb'
import {
  ConceptFilterEvidenceSchema,
  DocumentLookupSchema,
  DocumentService,
  DocumentSummarySchema,
  ListDocumentsResponseSchema,
  MountedConceptSchemeSchema,
  UnresolvedConceptMappingSchema,
} from '@/gen/ai/protomolt/proto/repo/v2/document_service_pb'
import ConceptSchemeBrowser from '@/components/repository/ConceptSchemeBrowser.vue'
import { buildConceptSchemeView } from '@/repository/conceptBrowserCore'
import { at, testAddress } from '@/repository/repositoryTestKit'
import { mountRepositoryView, settle } from '@/repository/repositoryViewKit'

const label = (text: string, language = 'en') => create(LangLabelSchema, { text, language })
const ref = (schemeId: string, conceptId: string) => create(ConceptRefSchema, { schemeId, conceptId })

function testEnergySchemeView() {
  const proto = create(ConceptSchemeSchema, {
    prefLabel: [label('Energy Taxonomy', 'en')],
    concepts: [
      create(ConceptSchema, {
        id: 'energy',
        prefLabel: [label('Energy', 'en'), label('Énergie', 'fr')],
        altLabel: [label('Power', 'en')],
        definition: [label('All forms of energy.', 'en')],
        scopeNote: [label('Root concept.', 'en')],
        notation: [create(ConceptNotationSchema, { value: 'E-00', datatypeIri: 'https://example.org/dt' })],
        iri: 'https://example.org/concepts/energy',
        related: ['climate'],
        exactMatch: [ref('eurovoc', 'c-100')],
      }),
      create(ConceptSchema, {
        id: 'wind',
        prefLabel: [label('Wind Power', 'en')],
        hiddenLabel: [label('aeolian', 'en')],
        broader: ['energy'],
        closeMatch: [ref('wikidata', 'Q43302')],
      }),
      create(ConceptSchema, {
        id: 'old-wind',
        prefLabel: [label('Legacy Wind', 'en')],
        deprecated: true,
        replacedBy: ['wind'],
        iri: 'javascript:alert(1)',
      }),
      create(ConceptSchema, {
        id: 'climate',
        prefLabel: [label('Climate', 'en')],
      }),
    ],
  })
  return { proto, view: buildConceptSchemeView('topics', proto, 'v2') }
}

function mountBrowser(
  schemes = [testEnergySchemeView().view],
  routes: (router: ConnectRouter) => void = () => {},
) {
  const Host = defineComponent({
    components: { ConceptSchemeBrowser },
    setup: () => ({ schemes }),
    template: '<ConceptSchemeBrowser account-id="acct-a" :initial-schemes="schemes" />',
  })
  return mountRepositoryView(Host, { name: 'repository-lookup', query: { account: 'acct-a' } }, routes)
}

describe('ConceptSchemeBrowser', () => {
  it('renders scheme hierarchy tree, navigates relations, and guards unsafe IRIs', async () => {
    const { wrapper } = await mountBrowser()

    expect(wrapper.get('[data-testid="scheme-chip-topics"]').text()).toContain('Energy Taxonomy')
    expect(wrapper.find('[data-testid="concept-hierarchy-tree"]').exists()).toBe(true)

    // Select 'energy' in tree
    await wrapper.get('[data-testid="tree-select-energy"]').trigger('click')
    await settle()
    const detail = wrapper.get('[data-testid="concept-detail-panel"]')
    expect(detail.text()).toContain('topics/energy')
    expect(detail.text()).toContain('All forms of energy.')
    expect(detail.get('[data-testid="concept-iri-link"]').attributes('href')).toBe('https://example.org/concepts/energy')

    // Navigate narrower relation -> wind
    await wrapper.get('[data-testid="relation-narrower-wind"]').trigger('click')
    await settle()
    expect(wrapper.get('[data-testid="concept-detail-panel"]').text()).toContain('topics/wind')

    // Navigate replaces relation -> old-wind (which has javascript: IRI)
    await wrapper.get('[data-testid="relation-replaces-old-wind"]').trigger('click')
    await settle()
    expect(wrapper.find('[data-testid="concept-deprecated-chip"]').exists()).toBe(true)
    expect(wrapper.find('[data-testid="concept-iri-link"]').exists()).toBe(false)
    expect(wrapper.get('[data-testid="concept-iri-text"]').text()).toBe('javascript:alert(1)')
    expect(wrapper.get('[data-testid="expansion-redirected"]').text()).toContain('Redirects deprecated old-wind to wind')

    wrapper.unmount()
  })

  it('filters concepts by hidden_label and decodes base64 ConceptScheme payloads', async () => {
    const { proto, view } = testEnergySchemeView()
    const { wrapper } = await mountBrowser([view])

    // Filter by hidden_label 'aeolian'
    await wrapper.get('[data-testid="scheme-concept-filter"] input').setValue('aeolian')
    await settle()
    const filtered = wrapper.get('[data-testid="concept-filtered-list"]')
    expect(filtered.text()).toContain('Wind Power')
    await wrapper.get('[data-testid="filtered-concept-wind"]').trigger('click')
    await settle()
    expect(wrapper.get('[data-testid="concept-detail-panel"]').text()).toContain('topics/wind')

    // Decode a second scheme via base64 loader
    await wrapper.get('[data-testid="toggle-scheme-loader"]').trigger('click')
    await settle()
    await wrapper.get('[data-testid="loader-scheme-id"] input').setValue('eurovoc')
    await wrapper.get('[data-testid="loader-scheme-version"] input').setValue('2026-04')
    await wrapper.get('[data-testid="loader-base64-input"] textarea').setValue('!!!invalid-base64!!!')
    await wrapper.get('[data-testid="loader-decode-base64"]').trigger('click')
    await settle()
    expect(wrapper.get('[data-testid="loader-error"]').text()).toContain('is not valid base64')

    const validBase64 = Buffer.from(toBinary(ConceptSchemeSchema, proto)).toString('base64')
    await wrapper.get('[data-testid="loader-base64-input"] textarea').setValue(validBase64)
    await wrapper.get('[data-testid="loader-decode-base64"]').trigger('click')
    await settle()
    expect(wrapper.get('[data-testid="scheme-chip-eurovoc"]').text()).toContain('eurovoc@2026-04')

    wrapper.unmount()
  })

  it('queries ListDocuments with ConceptFilter, renders ConceptEvidence, and navigates to document and account views', async () => {
    const { view } = testEnergySchemeView()
    const seenRequests: { accountId: string; schemeId: string; conceptId: string; narrower: boolean; closeMatch: boolean }[] = []

    const { wrapper, router } = await mountBrowser([view], (r) =>
      r.service(DocumentService, {
        listDocuments(req) {
          seenRequests.push({
            accountId: req.accountId,
            schemeId: req.filter?.concept?.concept?.schemeId ?? '',
            conceptId: req.filter?.concept?.concept?.conceptId ?? '',
            narrower: req.filter?.concept?.expansion?.narrower ?? false,
            closeMatch: req.filter?.concept?.expansion?.closeMatch ?? false,
          })
          return create(ListDocumentsResponseSchema, {
            documents: [
              create(DocumentSummarySchema, {
                address: testAddress({ docId: 'doc-wind-1' }),
                revision: 3n,
                committedAt: at('2026-10-02T00:00:00Z'),
                lookup: create(DocumentLookupSchema, { title: 'Offshore Wind Report', documentType: 'report' }),
              }),
            ],
            conceptEvidence: create(ConceptFilterEvidenceSchema, {
              expandedConcepts: 4,
              mountedSchemes: [create(MountedConceptSchemeSchema, { schemeId: 'topics', version: 'v2' })],
              unresolvedMappings: [
                create(UnresolvedConceptMappingSchema, {
                  from: ref('topics', 'wind'),
                  relation: 'exact_match',
                  target: ref('wikidata', 'Q43302'),
                  reason: 'scheme_not_mounted',
                }),
              ],
            }),
          })
        },
      }),
    )

    // Select 'energy' and click Query documents
    await wrapper.get('[data-testid="tree-select-energy"]').trigger('click')
    await settle()
    await wrapper.get('[data-testid="concept-query-btn-energy"]').trigger('click')
    await settle()

    expect(seenRequests).toEqual([
      {
        accountId: 'acct-a',
        schemeId: 'topics',
        conceptId: 'energy',
        narrower: true,
        closeMatch: false,
      },
    ])
    expect(wrapper.get('[data-testid="concept-evidence"]').text()).toContain('Matched against 4 concepts')
    expect(wrapper.get('[data-testid="mounted-scheme-chip-topics"]').text()).toContain('topics@v2')
    expect(wrapper.get('[data-testid="concept-doc-row-doc-wind-1"]').text()).toContain('Offshore Wind Report')

    // Open in account documents
    await wrapper.get('[data-testid="concept-open-in-account"]').trigger('click')
    await settle()
    expect(router.currentRoute.value.name).toBe('repository-account')
    expect(router.currentRoute.value.params).toEqual({ accountId: 'acct-a' })
    expect(router.currentRoute.value.query).toMatchObject({
      scheme: 'topics',
      concept: 'energy',
      narrower: '1',
    })

    wrapper.unmount()
  })
})
