// @vitest-environment jsdom
import { create } from '@bufbuild/protobuf'
import { Code, ConnectError, type ConnectRouter } from '@connectrpc/connect'
import { describe, expect, it } from 'vitest'
import {
  DeleteDocumentResponseSchema,
  DocumentService,
  GetDocumentResponseSchema,
  GetManifestResponseSchema,
  ListRevisionsResponseSchema,
  RevisionSummarySchema,
  type DeleteDocumentRequest,
} from '@/gen/ai/protomolt/proto/repo/v2/document_service_pb'
import { RevisionConflictSchema } from '@/gen/ai/protomolt/proto/repo/v2/errors_pb'
import {
  AccessionSchema,
  DocumentManifestSchema,
  DocumentPart,
  ExpectedRevisionSchema,
  PartManifestEntrySchema,
  PartSelectorSchema,
  PartState,
  RevisionValidationSchema,
  SaveOwnerSchema,
  ValidationStatus,
} from '@/gen/ai/protomolt/proto/repo/v2/manifest_pb'
import { at, testAddress } from '@/repository/repositoryTestKit'
import { mountRepositoryView, settle } from '@/repository/repositoryViewKit'
import RepositoryDocumentView from './RepositoryDocumentView.vue'

const owner = create(SaveOwnerSchema, { owner: { case: 'intake', value: { datasourceId: 'ds-files' } } })
const CORE = create(PartSelectorSchema, { selector: { case: 'part', value: DocumentPart.CORE } })

/** A document at revision 4 that the delete handler may turn into a tombstone at 5. */
function repository(onDelete: (request: DeleteDocumentRequest) => 'commit' | Error, seen: DeleteDocumentRequest[]) {
  let current = 4n
  return (router: ConnectRouter) =>
    router.service(DocumentService, {
      getManifest(request) {
        const s = request.revision?.selector
        const revision = s?.case === 'revision' ? s.value : current
        const tombstone = revision === 5n
        if (s?.case !== 'revision' && tombstone) throw new ConnectError('Document is deleted', Code.NotFound)
        return create(GetManifestResponseSchema, {
          manifest: create(DocumentManifestSchema, {
            address: testAddress(),
            revision,
            tombstone,
            parts: tombstone ? [] : [create(PartManifestEntrySchema, { selector: CORE, state: PartState.PRESENT, writtenInRevision: revision, owner })],
          }),
          validation: create(RevisionValidationSchema, { status: tombstone ? ValidationStatus.NOT_APPLICABLE : ValidationStatus.TYPED_VALID }),
        })
      },
      getDocument: () => create(GetDocumentResponseSchema, { manifest: create(DocumentManifestSchema, { address: testAddress(), revision: current }), validation: create(RevisionValidationSchema) }),
      listRevisions: () =>
        create(ListRevisionsResponseSchema, {
          revisions: Array.from({ length: Number(current) }, (_, i) => current - BigInt(i)).map((revision) =>
            create(RevisionSummarySchema, {
              revision,
              previousRevision: revision - 1n,
              committedAt: at('2026-10-21T00:00:00Z'),
              operationId: `op-${revision}`,
              tombstone: revision === 5n,
              owner: revision === 5n ? undefined : owner,
              validationStatus: revision === 5n ? ValidationStatus.NOT_APPLICABLE : ValidationStatus.TYPED_VALID,
            }),
          ),
        }),
      deleteDocument(request) {
        seen.push(request)
        const outcome = onDelete(request)
        if (outcome instanceof Error) throw outcome
        current = 5n
        return create(DeleteDocumentResponseSchema, {
          operationId: request.operationId,
          accession: create(AccessionSchema, { address: testAddress(), revision: 5n, previousRevision: 4n, tombstone: true }),
        })
      },
    })
}

const DOC = { name: 'repository-document', query: { account: 'acct-a', graph: 'intake:acct-a', node: 'ds-files', doc: 'doc-1' } }

async function openDialog(onDelete: (request: DeleteDocumentRequest) => 'commit' | Error) {
  const seen: DeleteDocumentRequest[] = []
  const mounted = await mountRepositoryView(RepositoryDocumentView, DOC, repository(onDelete, seen))
  await mounted.wrapper.get('[data-testid="delete-document"]').trigger('click')
  await settle()
  const dialog = () => document.body.querySelector('[data-testid="delete-dialog"]') as HTMLElement | null
  const click = async (testId: string) => {
    ;(document.body.querySelector(`[data-testid="${testId}"]`) as HTMLElement).click()
    await settle()
  }
  return { ...mounted, seen, dialog, click }
}

describe('Tombstone delete', () => {
  it('names the call number and current revision, sends the expected revision, and opens the tombstone in history', async () => {
    const { wrapper, router, seen, dialog, click } = await openDialog(() => 'commit')
    const text = dialog()!.textContent!
    expect(text).toContain('acct-a / intake:acct-a / ds-files / doc-1')
    expect(dialog()!.querySelector('[data-testid="delete-revision"]')!.textContent).toBe('4')
    const operationId = dialog()!.querySelector('[data-testid="delete-operation"]')!.textContent!
    expect(operationId).toMatch(/^[0-9a-f-]{36}$/)
    await click('delete-confirm')
    expect(seen).toHaveLength(1)
    expect(seen[0].operationId).toBe(operationId)
    expect(seen[0].expected?.expected).toEqual({ case: 'revision', value: 4n })
    expect(router.currentRoute.value.query.revision).toBe('5')
    expect(wrapper.get('[data-testid="deleted-notice"]').text()).toContain('revision 5 is a tombstone after revision 4')
    expect(wrapper.get('[data-testid="revision-chip"]').text()).toBe('revision 5')
    expect(wrapper.find('[data-testid="delete-document"]').exists()).toBe(false)
    await wrapper.get('[data-testid="tab-history"]').trigger('click')
    await settle()
    expect(wrapper.get('[data-testid="revision-5"]').text()).toContain('tombstone')
    expect(wrapper.find('[data-testid="open-revision-4"]').exists()).toBe(true)
    wrapper.unmount()
  })

  it('shows a stale revision with the current one and reloads on request, never retrying', async () => {
    const { wrapper, seen, dialog, click } = await openDialog(
      () =>
        new ConnectError('expected revision 4, current 6', Code.Aborted, undefined, [
          {
            desc: RevisionConflictSchema,
            value: { address: testAddress(), expected: create(ExpectedRevisionSchema, { expected: { case: 'revision', value: 4n } }), currentRevision: 6n },
          },
        ]),
    )
    await click('delete-confirm')
    expect(seen).toHaveLength(1)
    expect(dialog()!.querySelector('[data-testid="delete-stale"]')!.textContent).toContain(
      'Revision 4 is no longer current; the current revision is 6.',
    )
    expect((dialog()!.querySelector('[data-testid="delete-confirm"]') as HTMLButtonElement).disabled).toBe(true)
    await click('delete-reload')
    expect(dialog()).toBeNull()
    expect(seen).toHaveLength(1)
    wrapper.unmount()
  })

  it('shows a write refusal with its reason', async () => {
    const { wrapper, dialog, click } = await openDialog(() => new ConnectError('The caller may not delete this document', Code.PermissionDenied))
    await click('delete-confirm')
    const text = dialog()!.textContent!
    expect(text).toContain('PERMISSION_DENIED')
    expect(text).toContain('The caller may not delete this document')
    wrapper.unmount()
  })

  it('sends a failed delete again only when asked, under the same operation ID', async () => {
    let attempts = 0
    const { wrapper, router, seen, dialog, click } = await openDialog(() =>
      attempts++ === 0 ? new ConnectError('catalog unreachable', Code.Unavailable) : 'commit',
    )
    await click('delete-confirm')
    expect(seen).toHaveLength(1)
    expect(dialog()!.textContent).toContain('UNAVAILABLE')
    expect(dialog()!.querySelector('[data-testid="delete-confirm"]')!.textContent).toContain('Send again')
    await click('delete-confirm')
    expect(seen).toHaveLength(2)
    expect(seen[1].operationId).toBe(seen[0].operationId)
    expect(router.currentRoute.value.query.revision).toBe('5')
    wrapper.unmount()
  })

  it('offers no delete on an exact older revision or on a tombstone', async () => {
    const { wrapper } = await mountRepositoryView(RepositoryDocumentView, { ...DOC, query: { ...DOC.query, revision: '3' } }, repository(() => 'commit', []))
    expect(wrapper.find('[data-testid="delete-document"]').exists()).toBe(false)
    wrapper.unmount()
  })
})
