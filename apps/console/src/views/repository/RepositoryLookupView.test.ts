// @vitest-environment jsdom
import { create } from '@bufbuild/protobuf'
import { Code, ConnectError } from '@connectrpc/connect'
import { describe, expect, it } from 'vitest'
import {
  ConceptFilterEvidenceSchema,
  DocumentLookupSchema,
  DocumentService,
  DocumentSummarySchema,
  GetOperationOutcomeResponseSchema,
  ListDocumentsResponseSchema,
  MountedConceptSchemeSchema,
  NoRecordedOutcomeSchema,
  RecordedOutcomeSchema,
} from '@/gen/ai/protomolt/proto/repo/v2/document_service_pb'
import { RejectionReason, SaveRejectionSchema } from '@/gen/ai/protomolt/proto/repo/v2/errors_pb'
import { AccessionSchema } from '@/gen/ai/protomolt/proto/repo/v2/manifest_pb'
import RepositoryLookupView from '@/views/repository/RepositoryLookupView.vue'
import { at, testAddress } from '@/repository/repositoryTestKit'
import { mountRepositoryView, settle } from '@/repository/repositoryViewKit'

const outcomes: Record<string, () => ReturnType<typeof create<typeof GetOperationOutcomeResponseSchema>>> = {
  'op-done': () =>
    create(GetOperationOutcomeResponseSchema, {
      state: {
        case: 'committed',
        value: create(RecordedOutcomeSchema, {
          result: { case: 'accession', value: create(AccessionSchema, { address: testAddress(), revision: 4n }) },
          recordedAt: at('2026-10-01T00:00:00Z'),
        }),
      },
    }),
  'op-rejected': () =>
    create(GetOperationOutcomeResponseSchema, {
      state: {
        case: 'committed',
        value: create(RecordedOutcomeSchema, {
          result: { case: 'rejection', value: create(SaveRejectionSchema, { reason: RejectionReason.SCHEMA_MISSING, message: 'no schema for type x' }) },
          recordedAt: at('2026-10-01T00:00:00Z'),
        }),
      },
    }),
  'op-pending': () => create(GetOperationOutcomeResponseSchema, { state: { case: 'none', value: create(NoRecordedOutcomeSchema) } }),
  'op-down': () => {
    throw new ConnectError('outcome ledger unreachable', Code.Unavailable)
  },
  'op-other': () => {
    throw new ConnectError('account acct-a is not bound', Code.PermissionDenied)
  },
}

async function lookup(operationId: string) {
  const mounted = await mountRepositoryView(RepositoryLookupView, { name: 'repository-lookup', query: { account: 'acct-a' } }, (router) =>
    router.service(DocumentService, { getOperationOutcome: (r) => outcomes[r.operationId]() }),
  )
  await mounted.wrapper.get('[data-testid="op-id"] input').setValue(operationId)
  await mounted.wrapper.get('[data-testid="lookup-op"]').trigger('click')
  await settle()
  return mounted
}

describe('RepositoryLookupView, by operation ID', () => {
  it('shows a committed save with a link to its exact revision', async () => {
    const { wrapper, router } = await lookup('op-done')
    expect(wrapper.get('[data-testid="outcome-committed"]').text()).toContain('Revision 4 of doc-1')
    await wrapper.get('[data-testid="open-committed"]').trigger('click')
    await settle()
    expect(router.currentRoute.value.query).toMatchObject({ doc: 'doc-1', revision: '4', graph: 'intake:acct-a' })
    wrapper.unmount()
  })

  it('shows committed, rejected, no outcome and lookup unavailable as four distinct answers', async () => {
    const shown: string[] = []
    for (const op of ['op-done', 'op-rejected', 'op-pending', 'op-down']) {
      const { wrapper } = await lookup(op)
      const answer = wrapper.find('[data-testid^="outcome-"]')
      shown.push(answer.attributes('data-testid')!)
      if (op === 'op-rejected') expect(answer.text()).toContain('SCHEMA_MISSING')
      if (op === 'op-pending') expect(answer.text()).toContain('does not mean the save failed')
      if (op === 'op-down') expect(answer.text()).toContain('outcome ledger unreachable')
      wrapper.unmount()
    }
    expect(shown).toEqual(['outcome-committed', 'outcome-rejected', 'outcome-none', 'outcome-unavailable'])
  })

  it('shows any other status as a failed call, not as an outcome', async () => {
    const { wrapper } = await lookup('op-other')
    expect(wrapper.find('[data-testid^="outcome-"]').exists()).toBe(false)
    expect(wrapper.get('[data-testid="rpc-failure"]').text()).toContain('PERMISSION_DENIED')
    wrapper.unmount()
  })
})

describe('RepositoryLookupView, by call number', () => {
  it('opens the document route with the typed call number and exact revision', async () => {
    const { wrapper, router } = await mountRepositoryView(RepositoryLookupView, { name: 'repository-lookup' }, () => {})
    await wrapper.get('[data-testid="call-account"] input').setValue('acct-a')
    await wrapper.get('[data-testid="call-graph"] input').setValue('intake:acct-a')
    await wrapper.get('[data-testid="call-node"] input').setValue('ds/files')
    await wrapper.get('[data-testid="call-doc"] input').setValue('doc 1')
    await wrapper.get('[data-testid="call-revision"] input').setValue('2')
    await wrapper.get('[data-testid="open-call"]').trigger('click')
    await settle()
    expect(router.currentRoute.value.query).toEqual({ account: 'acct-a', graph: 'intake:acct-a', node: 'ds/files', doc: 'doc 1', revision: '2' })
    wrapper.unmount()
  })

  it('names the blank field instead of opening a partial call number', async () => {
    const { wrapper, router } = await mountRepositoryView(RepositoryLookupView, { name: 'repository-lookup' }, () => {})
    await wrapper.get('[data-testid="call-account"] input').setValue('acct-a')
    await wrapper.get('[data-testid="open-call"]').trigger('click')
    await settle()
    expect(wrapper.get('[data-testid="call-error"]').text()).toContain('Graph ID is blank')
    expect(router.currentRoute.value.name).toBe('repository-lookup')
    wrapper.unmount()
  })

  it('mounts the SKOS concept browser and syncs account ID when route.query.account changes', async () => {
    const { wrapper, router } = await mountRepositoryView(
      RepositoryLookupView,
      { name: 'repository-lookup', query: { account: 'acct-a' } },
      (r) =>
        r.service(DocumentService, {
          listDocuments(req) {
            expect(req.accountId).toBe('acct-b')
            expect(req.filter?.concept?.concept).toMatchObject({ schemeId: 'topics', conceptId: 'wind' })
            return create(ListDocumentsResponseSchema, {
              documents: [
                create(DocumentSummarySchema, {
                  address: testAddress({ accountId: 'acct-b', docId: 'doc-99' }),
                  revision: 1n,
                  lookup: create(DocumentLookupSchema, { title: 'Wind Farm' }),
                }),
              ],
              conceptEvidence: create(ConceptFilterEvidenceSchema, {
                expandedConcepts: 2,
                mountedSchemes: [create(MountedConceptSchemeSchema, { schemeId: 'topics', version: 'v5' })],
              }),
            })
          },
        }),
    )

    expect(wrapper.find('[data-testid="concept-scheme-browser"]').exists()).toBe(true)
    await router.push({ name: 'repository-lookup', query: { account: 'acct-b' } })
    await settle()
    expect((wrapper.get('[data-testid="call-account"] input').element as HTMLInputElement).value).toBe('acct-b')
    expect((wrapper.get('[data-testid="concept-lookup-account"] input').element as HTMLInputElement).value).toBe('acct-b')

    await wrapper.get('[data-testid="concept-lookup-scheme"] input').setValue('topics')
    await wrapper.get('[data-testid="concept-lookup-concept"] input').setValue('wind')
    await wrapper.get('[data-testid="concept-lookup-submit"]').trigger('click')
    await settle()

    expect(wrapper.get('[data-testid="concept-doc-row-doc-99"]').text()).toContain('Wind Farm')
    expect(wrapper.get('[data-testid="mounted-scheme-chip-topics"]').text()).toContain('topics@v5')
    wrapper.unmount()
  })
})
