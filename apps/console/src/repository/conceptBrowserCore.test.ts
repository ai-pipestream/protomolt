import { create, toBinary } from '@bufbuild/protobuf'
import { describe, expect, it } from 'vitest'
import {
  ConceptNotationSchema,
  ConceptRefSchema,
  ConceptSchema,
  ConceptSchemeSchema,
  LangLabelSchema,
} from '@/gen/ai/protomolt/proto/types/v1/concepts_pb'
import { listingQueryFromRoute } from './listingCore'
import {
  buildConceptLookupRequest,
  buildConceptSchemeView,
  conceptAccountRouteQuery,
  decodeConceptScheme,
  decodeConceptSchemeBase64,
  expandConceptInScheme,
  filterConcepts,
  formatLangLabels,
  mergeMountedSchemes,
  newConceptLookupDraft,
  primaryLabel,
} from './conceptBrowserCore'

const label = (text: string, language = 'en') => create(LangLabelSchema, { text, language })
const ref = (schemeId: string, conceptId: string) => create(ConceptRefSchema, { schemeId, conceptId })

function sampleEnergyScheme() {
  return create(ConceptSchemeSchema, {
    prefLabel: [label('Énergie', 'fr'), label('Energy Taxonomy', 'en-US')],
    concepts: [
      create(ConceptSchema, {
        id: 'energy',
        prefLabel: [label('Energy', 'en'), label('Énergie', 'fr')],
        altLabel: [label('Power', 'en')],
        hiddenLabel: [label('erg-root', 'en')],
        definition: [label('All forms of energy generation and storage.', 'en')],
        scopeNote: [label('Use narrower terms when possible.', 'en')],
        notation: [
          create(ConceptNotationSchema, { value: 'E-00', datatypeIri: 'https://example.org/datatypes/code' }),
          create(ConceptNotationSchema, { value: 'BAD-DT', datatypeIri: 'javascript:alert(1)' }),
        ],
        iri: 'https://example.org/concepts/energy',
        related: ['climate'],
        exactMatch: [ref('eurovoc', 'c-100')],
      }),
      create(ConceptSchema, {
        id: 'renewables',
        prefLabel: [label('Renewable Energy', 'en')],
        broader: ['energy'],
        closeMatch: [ref('wikidata', 'Q12705')],
        broadMatch: [ref('eurovoc', 'c-010')],
      }),
      create(ConceptSchema, {
        id: 'wind',
        prefLabel: [label('Wind Power', 'en')],
        hiddenLabel: [label('aeolian', 'en')],
        broader: ['renewables', 'missing-parent'],
        exactMatch: [ref('wikidata', 'Q43302')],
        closeMatch: [ref('agrovoc', 'c-wind')],
      }),
      create(ConceptSchema, {
        id: 'old-wind',
        prefLabel: [label('Legacy Wind Term', 'en')],
        deprecated: true,
        replacedBy: ['wind'],
        iri: 'javascript:evil()',
      }),
      create(ConceptSchema, {
        id: 'orphan-deprecated',
        prefLabel: [label('Orphan Deprecated', 'en')],
        deprecated: true,
        replacedBy: ['ghost-target'],
      }),
      create(ConceptSchema, {
        id: 'climate',
        prefLabel: [label('Climate', 'en')],
      }),
    ],
  })
}

describe('formatLangLabels and primaryLabel', () => {
  it('trims labels, drops blank entries, and selects preferred language or prefix', () => {
    const labels = [
      label('   ', 'en'),
      label('  Énergie  ', 'fr'),
      label('Energy (US)', 'en-US'),
    ]
    expect(formatLangLabels(labels)).toEqual([
      { text: 'Énergie', lang: 'fr' },
      { text: 'Energy (US)', lang: 'en-US' },
    ])
    expect(primaryLabel(labels, 'fallback', 'en')).toEqual({ text: 'Energy (US)', lang: 'en-US' })
    expect(primaryLabel(labels, 'fallback', 'fr')).toEqual({ text: 'Énergie', lang: 'fr' })
    expect(primaryLabel(labels, 'fallback', 'de')).toEqual({ text: 'Énergie', lang: 'fr' })
    expect(primaryLabel([], 'fallback-id')).toEqual({ text: 'fallback-id', lang: null })
  })
})

describe('buildConceptSchemeView and binary decoding', () => {
  it('computes top concepts, inverse narrower edges, symmetric related links, notations, and safe IRIs', () => {
    const view = buildConceptSchemeView('topics', sampleEnergyScheme(), 'v3')
    expect(view.schemeId).toBe('topics')
    expect(view.version).toBe('v3')
    expect(view.primaryLabel).toBe('Energy Taxonomy')
    expect(view.topConcepts.map((c) => c.id)).toEqual(['climate', 'energy', 'old-wind', 'orphan-deprecated'])

    const energy = view.byId.get('energy')!
    expect(energy.narrower.map((n) => n.conceptId)).toEqual(['renewables'])
    expect(energy.related.map((r) => r.conceptId)).toEqual(['climate'])
    expect(energy.safeIriHref).toBe('https://example.org/concepts/energy')
    expect(energy.notations).toEqual([
      { value: 'E-00', datatypeIri: 'https://example.org/datatypes/code', safeDatatypeHref: 'https://example.org/datatypes/code' },
      { value: 'BAD-DT', datatypeIri: 'javascript:alert(1)', safeDatatypeHref: null },
    ])

    // Symmetric related edge on climate
    const climate = view.byId.get('climate')!
    expect(climate.related.map((r) => r.conceptId)).toEqual(['energy'])

    // Deprecation chain on wind / old-wind
    const wind = view.byId.get('wind')!
    expect(wind.replaces.map((r) => r.conceptId)).toEqual(['old-wind'])
    const oldWind = view.byId.get('old-wind')!
    expect(oldWind.replacedBy.map((r) => r.conceptId)).toEqual(['wind'])
    expect(oldWind.replacedBy[0]?.known).toBe(true)
    expect(oldWind.safeIriHref).toBeNull()

    // Tree structure under energy -> renewables -> wind
    const energyTree = view.tree.find((node) => node.concept.id === 'energy')!
    expect(energyTree.children.map((c) => c.concept.id)).toEqual(['renewables'])
    expect(energyTree.children[0].children.map((c) => c.concept.id)).toEqual(['wind'])
  })

  it('guards against broader/narrower cycles without infinite recursion', () => {
    const cyclic = create(ConceptSchemeSchema, {
      concepts: [
        create(ConceptSchema, { id: 'a', prefLabel: [label('A')], broader: ['b'] }),
        create(ConceptSchema, { id: 'b', prefLabel: [label('B')], broader: ['a'] }),
      ],
    })
    const view = buildConceptSchemeView('cyclic-scheme', cyclic)
    expect(view.topConcepts).toHaveLength(1)
    expect(view.tree).toHaveLength(1)
    expect(view.tree[0].concept.id).toBe('a')
    expect(view.tree[0].children[0].concept.id).toBe('b')
    expect(view.tree[0].children[0].cycleDetected).toBe(true)
    expect(view.tree[0].children[0].children).toEqual([])
  })

  it('decodes binary and base64 ConceptScheme payloads and rejects malformed input', () => {
    const bytes = toBinary(ConceptSchemeSchema, sampleEnergyScheme())
    const fromBytes = decodeConceptScheme('topics', bytes, 'v1')
    expect(fromBytes.byId.has('wind')).toBe(true)

    const base64 = Buffer.from(bytes).toString('base64')
    const fromB64 = decodeConceptSchemeBase64('topics', `  ${base64}\n`, 'v1')
    expect(fromB64.byId.get('wind')?.primaryLabel).toBe('Wind Power')

    expect(() => decodeConceptScheme('', bytes)).toThrow('Concept scheme ID: is blank')
    expect(() => decodeConceptScheme('topics', new Uint8Array(0))).toThrow('ConceptScheme payload: is empty')
    expect(() => decodeConceptScheme('topics', new Uint8Array([255, 255, 255]))).toThrow('is not valid ConceptScheme protobuf bytes')
    expect(() => decodeConceptSchemeBase64('topics', '   ')).toThrow('ConceptScheme base64: is blank')
    expect(() => decodeConceptSchemeBase64('topics', 'not-valid-base64!!!')).toThrow('ConceptScheme base64: is not valid base64')
  })
})

describe('filterConcepts and expandConceptInScheme', () => {
  it('matches hidden_label, alt_label, notation, definition, and IRI without exposing hidden_label as primaryLabel', () => {
    const view = buildConceptSchemeView('topics', sampleEnergyScheme())
    expect(view.byId.get('wind')?.primaryLabel).toBe('Wind Power')
    expect(filterConcepts(view, 'aeolian').map((c) => c.id)).toEqual(['wind'])
    expect(filterConcepts(view, 'power').map((c) => c.id)).toEqual(['energy', 'wind'])
    expect(filterConcepts(view, 'e-00').map((c) => c.id)).toEqual(['energy'])
    expect(filterConcepts(view, 'storage').map((c) => c.id)).toEqual(['energy'])
    expect(filterConcepts(view, '').length).toBe(view.concepts.length)
  })

  it('previews in-scheme expansion across narrower descendants, deprecated predecessors, and cross-scheme mappings', () => {
    const view = buildConceptSchemeView('topics', sampleEnergyScheme())

    const direct = expandConceptInScheme(view, 'energy', { narrower: false, closeMatch: false })!
    expect(direct.inSchemeConcepts.map((c) => c.id)).toEqual(['energy'])
    expect(direct.crossSchemeTargets.map((t) => t.key)).toEqual(['eurovoc/c-100'])

    const expanded = expandConceptInScheme(view, 'energy', { narrower: true, closeMatch: true })!
    expect(expanded.inSchemeConcepts.map((c) => c.id)).toEqual(['energy', 'old-wind', 'renewables', 'wind'])
    expect(expanded.crossSchemeTargets.map((t) => `${t.kind}:${t.key}`)).toEqual([
      'exactMatch:eurovoc/c-100',
      'closeMatch:wikidata/Q12705',
      'exactMatch:wikidata/Q43302',
      'closeMatch:agrovoc/c-wind',
    ])
    expect(expanded.unresolvedInScheme).toEqual([
      { fromId: 'wind', relation: 'broader', targetId: 'missing-parent' },
    ])

    // Deprecated concept redirects to its canonical replacement
    const redirected = expandConceptInScheme(view, 'old-wind', { narrower: false, closeMatch: false })!
    expect(redirected.replacedRedirected).toBe(true)
    expect(redirected.canonicalIds).toEqual(['wind'])
    expect(redirected.inSchemeConcepts.map((c) => c.id)).toEqual(['old-wind', 'wind'])

    // Deprecated concept with unresolved replacement reports unresolvedInScheme
    const unresolvedDep = expandConceptInScheme(view, 'orphan-deprecated', { narrower: false, closeMatch: false })!
    expect(unresolvedDep.replacedRedirected).toBe(false)
    expect(unresolvedDep.unresolvedInScheme).toEqual([
      { fromId: 'orphan-deprecated', relation: 'replaced_by', targetId: 'ghost-target' },
    ])
  })
})

describe('buildConceptLookupRequest, conceptAccountRouteQuery, and mergeMountedSchemes', () => {
  it('builds a ListDocumentsRequest with ConceptFilter and round-trips to RepositoryAccountView route query', () => {
    const draft = newConceptLookupDraft('acct-a')
    draft.schemeId = 'topics'
    draft.conceptId = 'energy'
    draft.narrower = true
    draft.closeMatch = true
    draft.graphId = 'intake:acct-a'
    draft.graphAddressId = 'ds-1'

    const req = buildConceptLookupRequest(draft, 'tok-2')
    expect(req.accountId).toBe('acct-a')
    expect(req.graphId).toBe('intake:acct-a')
    expect(req.graphAddressId).toBe('ds-1')
    expect(req.pageToken).toBe('tok-2')
    expect(req.filter?.concept?.concept).toMatchObject({ schemeId: 'topics', conceptId: 'energy' })
    expect(req.filter?.concept?.expansion).toMatchObject({ narrower: true, closeMatch: true })

    const routeQuery = conceptAccountRouteQuery(draft)
    const restored = listingQueryFromRoute('acct-a', routeQuery)
    expect(restored.filter.concept).toEqual({
      schemeId: 'topics',
      conceptId: 'energy',
      narrower: true,
      closeMatch: true,
    })
  })

  it('refuses blank account, scheme, or concept IDs and unpaired graph/node scope', () => {
    const draft = newConceptLookupDraft('')
    expect(() => buildConceptLookupRequest(draft)).toThrow('Account: is blank')
    draft.accountId = 'acct-a'
    expect(() => buildConceptLookupRequest(draft)).toThrow('Concept scheme ID: is blank')
    draft.schemeId = 'topics'
    expect(() => buildConceptLookupRequest(draft)).toThrow('Concept ID: is blank')
    draft.conceptId = 'wind'
    draft.graphId = 'intake:acct-a'
    expect(() => buildConceptLookupRequest(draft)).toThrow('Graph address ID: must be given together')
  })

  it('merges mounted scheme versions from ConceptFilterEvidence', () => {
    const initial = [{ schemeId: 'topics', version: 'v1' }]
    const merged = mergeMountedSchemes(initial, {
      expandedConcepts: 3,
      schemes: [
        { schemeId: 'topics', version: 'v2' },
        { schemeId: 'eurovoc', version: '2026-01' },
      ],
      unresolved: [],
    })
    expect(merged).toEqual([
      { schemeId: 'eurovoc', version: '2026-01' },
      { schemeId: 'topics', version: 'v2' },
    ])
  })
})
