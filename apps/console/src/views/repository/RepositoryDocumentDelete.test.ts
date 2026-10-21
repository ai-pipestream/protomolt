// @vitest-environment jsdom
import { create } from '@bufbuild/protobuf'
import { Code, ConnectError, type ConnectRouter } from '@connectrpc/connect'
import { describe, expect, it } from 'vitest'
import {
  DeleteDocumentResponseSchema,
  DocumentService,
  GetOperationOutcomeResponseSchema,
  NoRecordedOutcomeSchema,
  RecordedOutcomeSchema,
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
function repository(onDelete: (request: DeleteDocumentRequest, commit: () => void) => 'commit' | Error, seen: DeleteDocumentRequest[]) {
  let current = 4n
  const tombstone = () => create(AccessionSchema, { address: testAddress(), revision: 5n, previousRevision: 4n, tombstone: true })
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
      getOperationOutcome: () =>
        current === 5n
          ? create(GetOperationOutcomeResponseSchema, {
              state: { case: 'committed', value: create(RecordedOutcomeSchema, { result: { case: 'accession', value: tombstone() }, recordedAt: at('2026-10-21T00:00:00Z') }) },
            })
          : create(GetOperationOutcomeResponseSchema, { state: { case: 'none', value: create(NoRecordedOutcomeSchema) } }),
      deleteDocument(request) {
        seen.push(request)
        const outcome = onDelete(request, () => (current = 5n))
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

async function openDialog(onDelete: (request: DeleteDocumentRequest, commit: () => void) => 'commit' | Error) {
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

  it('resends exactly the first request, reason included, and freezes the reason after the first send', async () => {
    let attempts = 0
    const { wrapper, seen, dialog, click } = await openDialog(() =>
      attempts++ === 0 ? new ConnectError('deadline passed', Code.DeadlineExceeded) : 'commit',
    )
    const reason = () => dialog()!.querySelector('[data-testid="delete-reason"] textarea') as HTMLTextAreaElement
    reason().value = 'duplicate scan'
    reason().dispatchEvent(new Event('input'))
    await settle()
    await click('delete-confirm')
    expect(reason().readOnly).toBe(true)
    reason().value = 'edited later'
    reason().dispatchEvent(new Event('input'))
    await settle()
    await click('delete-confirm')
    expect(seen).toHaveLength(2)
    expect(seen.map((r) => r.reason)).toEqual(['duplicate scan', 'duplicate scan'])
    wrapper.unmount()
  })

  it('treats a definite answer such as NOT_FOUND as final, with no resend', async () => {
    const { wrapper, dialog, click } = await openDialog(() => new ConnectError('Document was purged', Code.NotFound))
    await click('delete-confirm')
    expect(dialog()!.textContent).toContain('NOT_FOUND')
    expect(dialog()!.querySelector('[data-testid="delete-unsettled"]')).toBeNull()
    expect((dialog()!.querySelector('[data-testid="delete-confirm"]') as HTMLButtonElement).disabled).toBe(true)
    wrapper.unmount()
  })

  it('looks up what an already-used operation ID recorded, and opens the tombstone it committed', async () => {
    const { wrapper, router, dialog, click } = await openDialog((_request, commit) => {
      commit()
      return new ConnectError('Operation ID was used with a different intent', Code.AlreadyExists)
    })
    await click('delete-confirm')
    expect(dialog()).toBeNull()
    expect(router.currentRoute.value.query.revision).toBe('5')
    wrapper.unmount()
  })

  it('closes the dialog when the route moves to another document', async () => {
    const { wrapper, router, dialog } = await openDialog(() => 'commit')
    expect(dialog()).not.toBeNull()
    await router.push({ ...DOC, query: { ...DOC.query, doc: 'doc-2' } })
    await settle()
    await new Promise((resolve) => setTimeout(resolve, 50))
    await settle()
    // doc-2 has loaded and is deletable; the dialog must not come back for it.
    expect(wrapper.find('[data-testid="delete-document"]').exists()).toBe(true)
    expect((wrapper.vm as unknown as { deleting: boolean }).deleting).toBe(false)
    expect(dialog()).toBeNull()
    wrapper.unmount()
  })

  it('opens the tombstone another tab recorded when Reload follows a stale delete', async () => {
    const { wrapper, router, dialog, click } = await openDialog((_request, commit) => {
      commit()
      return new ConnectError('expected revision 4, current 5', Code.Aborted, undefined, [
        {
          desc: RevisionConflictSchema,
          value: {
            address: testAddress(),
            expected: create(ExpectedRevisionSchema, { expected: { case: 'revision', value: 4n } }),
            currentRevision: 5n,
            currentIsTombstone: true,
          },
        },
      ])
    })
    await click('delete-confirm')
    expect(dialog()!.textContent).toContain('the current revision is 5, a tombstone')
    await click('delete-reload')
    expect(router.currentRoute.value.query.revision).toBe('5')
    expect(wrapper.get('[data-testid="revision-chip"]').text()).toBe('revision 5')
    expect(wrapper.find('[data-testid="rpc-failure"]').exists()).toBe(false)
    wrapper.unmount()
  })

  it('offers the tombstone when the current revision of a deleted document is NOT_FOUND', async () => {
    const { wrapper, router, click } = await openDialog(() => 'commit')
    await click('delete-confirm')
    // The document is now a tombstone at 5; opening its current revision is NOT_FOUND.
    await router.push(DOC)
    await settle()
    expect(wrapper.get('[data-testid="rpc-failure"]').text()).toContain('NOT_FOUND')
    expect(wrapper.get('[data-testid="tombstone-hint"]').text()).toContain('Open revision 5')
    await wrapper.get('[data-testid="open-tombstone"]').trigger('click')
    await settle()
    expect(router.currentRoute.value.query.revision).toBe('5')
    wrapper.unmount()
  })
})
