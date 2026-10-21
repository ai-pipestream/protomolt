// @vitest-environment jsdom
import { create } from '@bufbuild/protobuf'
import { Code, ConnectError } from '@connectrpc/connect'
import { describe, expect, it } from 'vitest'
import { DriveSchema, DriveService, DriveStatus, DriveType, ListDrivesResponseSchema } from '@/gen/ai/protomolt/proto/repo/v1/drive_pb'
import {
  DocumentLookupSchema,
  DocumentService,
  DocumentSortField,
  DocumentSummarySchema,
  ListDocumentsResponseSchema,
  type ListDocumentsRequest,
} from '@/gen/ai/protomolt/proto/repo/v2/document_service_pb'
import { ArchiveService } from '@/gen/ai/protomolt/proto/repo/archive/v1/archive_service_pb'
import RepositoryAccountView from '@/views/repository/RepositoryAccountView.vue'
import { at, testAddress } from '@/repository/repositoryTestKit'
import { mountRepositoryView, settle } from '@/repository/repositoryViewKit'

function summary(docId: string, title?: string) {
  return create(DocumentSummarySchema, {
    address: testAddress({ docId }),
    revision: 2n,
    committedAt: at('2026-10-01T00:00:00Z'),
    lookup: create(DocumentLookupSchema, { title, datasourceId: 'ds-files' }),
  })
}

function drives() {
  return (router: Parameters<Parameters<typeof mountRepositoryView>[2]>[0]) =>
    router.service(DriveService, {
      listDrives(request) {
        return create(ListDrivesResponseSchema, {
          drives: [
            create(DriveSchema, {
              driveId: 'drv-1',
              name: 'intake',
              accountId: request.accountId,
              driveType: DriveType.INTAKE,
              status: DriveStatus.ACTIVE,
              provider: 's3',
              bucket: 'docs',
              prefix: 'acct-a',
            }),
          ],
        })
      },
    })
}

describe('RepositoryAccountView', () => {
  it('shows the drives and the first page of documents for the account', async () => {
    const seen: ListDocumentsRequest[] = []
    const { wrapper } = await mountRepositoryView(RepositoryAccountView, '/repository/accounts/acct-a', (router) => {
      drives()(router)
      router.service(DocumentService, {
        listDocuments(request) {
          seen.push(request)
          return create(ListDocumentsResponseSchema, { documents: [summary('doc-1', 'Report'), summary('doc-2')] })
        },
      })
    })
    expect(wrapper.get('[data-testid="drive-table"]').text()).toContain('INTAKE')
    expect(wrapper.get('[data-testid="drive-table"]').text()).toContain('docs/acct-a')
    const table = wrapper.get('[data-testid="document-table"]')
    expect(table.text()).toContain('Report')
    expect(wrapper.get('[data-testid="doc-row-doc-2"]').text()).toContain('(no title)')
    expect(seen[0]).toMatchObject({ accountId: 'acct-a', pageSize: 50, pageToken: '' })
    expect(seen[0].filter).toBeUndefined()
    wrapper.unmount()
  })

  it('sends the typed filter and sort, and pages forward and back with the issued tokens', async () => {
    const seen: ListDocumentsRequest[] = []
    const { wrapper } = await mountRepositoryView(RepositoryAccountView, '/repository/accounts/acct-a', (router) => {
      drives()(router)
      router.service(DocumentService, {
        listDocuments(request) {
          seen.push(request)
          const page = request.pageToken === '' ? 1 : Number(request.pageToken.slice(1))
          return create(ListDocumentsResponseSchema, {
            documents: [summary(`doc-p${page}`)],
            nextPageToken: page < 2 ? `p${page + 1}` : '',
          })
        },
      })
    })
    await wrapper.get('[data-testid="filter-panel"] button').trigger('click')
    await settle()
    await wrapper.get('[data-testid="filter-title"] input').setValue('Report')
    ;(wrapper.vm as unknown as { query: { sortField: DocumentSortField } }).query.sortField = DocumentSortField.TITLE
    await wrapper.get('[data-testid="apply-query"]').trigger('click')
    await settle()
    const filtered = seen[seen.length - 1]
    expect(filtered.filter?.title).toBe('Report')
    expect(filtered.filter?.author).toBeUndefined()
    expect(filtered.sort?.field).toBe(DocumentSortField.TITLE)

    await wrapper.get('[data-testid="page-next"]').trigger('click')
    await settle()
    expect(seen[seen.length - 1].pageToken).toBe('p2')
    expect(seen[seen.length - 1].filter?.title).toBe('Report')
    expect(wrapper.text()).toContain('doc-p2')
    expect(wrapper.get('[data-testid="page-next"]').attributes('disabled')).toBeDefined()
    await wrapper.get('[data-testid="page-previous"]').trigger('click')
    await settle()
    expect(seen[seen.length - 1].pageToken).toBe('')
    wrapper.unmount()
  })

  it('refuses a contradictory filter by name without calling the server', async () => {
    let calls = 0
    const { wrapper } = await mountRepositoryView(RepositoryAccountView, '/repository/accounts/acct-a', (router) => {
      drives()(router)
      router.service(DocumentService, {
        listDocuments() {
          calls++
          return create(ListDocumentsResponseSchema)
        },
      })
    })
    const before = calls
    const vm = wrapper.vm as unknown as { query: { filter: { metadata: { key: string; value: string }[] } } }
    vm.query.filter.metadata.push({ key: 'team', value: 'a' }, { key: 'team', value: 'b' })
    await wrapper.get('[data-testid="apply-query"]').trigger('click')
    await settle()
    expect(calls).toBe(before)
    expect(wrapper.get('[data-testid="input-error"]').text()).toContain('"team" is already required to equal "a"')
    wrapper.unmount()
  })

  it('shows each failing call with its code and message, independently', async () => {
    const { wrapper } = await mountRepositoryView(RepositoryAccountView, '/repository/accounts/acct-b', (router) => {
      router.service(DriveService, {
        listDrives() {
          throw new ConnectError('account acct-b is not bound to this caller', Code.PermissionDenied)
        },
      })
      router.service(DocumentService, {
        listDocuments() {
          throw new ConnectError('catalog unavailable', Code.Unavailable)
        },
      })
      router.service(ArchiveService, {
        listArchives() {
          throw new ConnectError('archive store unreachable', Code.Unavailable)
        },
      })
    })
    const failures = wrapper.findAll('[data-testid="rpc-failure"]').map((f) => f.text())
    expect(failures).toEqual([
      expect.stringContaining('PERMISSION_DENIED'),
      expect.stringContaining('archive store unreachable'),
      expect.stringContaining('UNAVAILABLE'),
    ])
    expect(failures[0]).toContain('account acct-b is not bound to this caller')
    expect(wrapper.find('[data-testid="no-documents"]').exists()).toBe(false)
    wrapper.unmount()
  })

  it('aborts an in-flight listing when the screen unmounts', async () => {
    let serverSignal: AbortSignal | null = null
    const { wrapper } = await mountRepositoryView(RepositoryAccountView, '/repository/accounts/acct-a', (router) => {
      drives()(router)
      router.service(DocumentService, {
        listDocuments(_request, context) {
          serverSignal = context.signal
          return new Promise(() => {})
        },
      })
    })
    expect(serverSignal!.aborted).toBe(false)
    wrapper.unmount()
    expect(serverSignal!.aborted).toBe(true)
  })

  it('opens a tombstoned row at its tombstone revision, which a current read refuses', async () => {
    const { wrapper, router } = await mountRepositoryView(RepositoryAccountView, '/repository/accounts/acct-a', (router) => {
      drives()(router)
      router.service(DocumentService, {
        listDocuments: () =>
          create(ListDocumentsResponseSchema, {
            documents: [
              create(DocumentSummarySchema, { address: testAddress({ docId: 'gone' }), revision: 4n, committedAt: at('2026-10-01T00:00:00Z'), tombstone: true }),
              summary('live'),
            ],
          }),
      })
    })
    await wrapper.get('[data-testid="doc-row-gone"]').trigger('click')
    await settle()
    expect(router.currentRoute.value.query).toMatchObject({ doc: 'gone', revision: '4' })
    await router.push('/repository/accounts/acct-a')
    await settle()
    await wrapper.get('[data-testid="doc-row-live"]').trigger('click')
    await settle()
    expect(router.currentRoute.value.query.revision).toBeUndefined()
    wrapper.unmount()
  })

  it('pages with the query the tokens were issued for, not with unsent form edits', async () => {
    const seen: ListDocumentsRequest[] = []
    const { wrapper } = await mountRepositoryView(RepositoryAccountView, '/repository/accounts/acct-a', (router) => {
      drives()(router)
      router.service(DocumentService, {
        listDocuments(request) {
          seen.push(request)
          return create(ListDocumentsResponseSchema, { documents: [summary('d')], nextPageToken: request.pageToken === '' ? 'p2' : '' })
        },
      })
    })
    const vm = wrapper.vm as unknown as { query: { includeTombstoned: boolean; filter: { text: { title: string } } } }
    vm.query.includeTombstoned = true
    vm.query.filter.text.title = 'unsent'
    await wrapper.get('[data-testid="page-next"]').trigger('click')
    await settle()
    const paged = seen[seen.length - 1]
    expect(paged.pageToken).toBe('p2')
    expect(paged.includeTombstoned).toBe(false)
    expect(paged.filter).toBeUndefined()
    await wrapper.get('[data-testid="page-previous"]').trigger('click')
    await settle()
    expect(seen[seen.length - 1].filter).toBeUndefined()
    wrapper.unmount()
  })

  it('clears Descending when the sort returns to call number order', async () => {
    const seen: ListDocumentsRequest[] = []
    const { wrapper } = await mountRepositoryView(RepositoryAccountView, '/repository/accounts/acct-a', (router) => {
      drives()(router)
      router.service(DocumentService, {
        listDocuments(request) {
          seen.push(request)
          return create(ListDocumentsResponseSchema)
        },
      })
    })
    const vm = wrapper.vm as unknown as { query: { sortField: DocumentSortField; descending: boolean } }
    vm.query.sortField = DocumentSortField.TITLE
    vm.query.descending = true
    await settle()
    vm.query.sortField = DocumentSortField.UNSPECIFIED
    await settle()
    expect(vm.query.descending).toBe(false)
    await wrapper.get('[data-testid="apply-query"]').trigger('click')
    await settle()
    expect(wrapper.find('[data-testid="input-error"]').exists()).toBe(false)
    expect(seen[seen.length - 1].sort).toBeUndefined()
    wrapper.unmount()
  })
})
