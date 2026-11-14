// @vitest-environment jsdom
import { clone, create, toBinary } from '@bufbuild/protobuf'
import { Code, ConnectError, createClient, createRouterTransport } from '@connectrpc/connect'
import { mount } from '@vue/test-utils'
import { afterEach, describe, expect, it } from 'vitest'
import { installDomStubs, vuetifyForTests } from '@/componentTestKit'
import {
  ArchiveMutationKind,
  ArchiveMutationReceiptSchema,
  ArchiveMutationRequestSchema,
  ArchiveMutationState,
  type ArchiveMutationReceipt,
  type ArchiveMutationRequest,
} from '@/gen/ai/protomolt/proto/repo/archive/v1/archive_mutation_pb'
import {
  ArchiveMutationResponseSchema,
  ArchiveMutationService,
  GetArchiveMutationResponseSchema,
} from '@/gen/ai/protomolt/proto/repo/archive/v1/archive_mutation_service_pb'
import { EntryAddressSchema } from '@/gen/ai/protomolt/proto/repo/archive/v1/entry_pb'
import {
  ARCHIVE_DELETE_RECEIPTS,
  createArchiveDeleteReceiptStore,
} from '@/repository/archiveDeleteReceipts'
import { REPOSITORY_CLIENTS, repositoryClients } from '@/repository/clients'
import { routerClients } from '@/repository/repositoryTestKit'
import { settle } from '@/repository/repositoryViewKit'
import type { CallScope } from '@/repository/useCallScope'
import DeleteArchiveEntryDialog from './DeleteArchiveEntryDialog.vue'

const address = create(EntryAddressSchema, { accountId: 'acct-a', archive: 'papers', entryId: 'e-1' })
const digest = '8f98947ff1ae6a17435d9e48c584364593181ff83f1667d3af525067b9a081fe'
function receipt(request: ArchiveMutationRequest) {
  return create(ArchiveMutationReceiptSchema, {
    operationId: request.operationId,
    address,
    kind: ArchiveMutationKind.DELETE_ENTRY,
    state: ArchiveMutationState.COMPLETED,
    entryDeleted: true,
    commandSha256: digest,
  })
}
const button = (name: string) => document.body.querySelector(`[data-testid="delete-archive-${name}"]`) as HTMLButtonElement
const text = () => document.body.textContent ?? ''
afterEach(() => {
  document.body.innerHTML = ''
})

describe('archive delete confirmation', () => {
  it('keeps identity through unavailable/absent lookups and retry, then displays the recorded receipt', async () => {
    installDomStubs()
    const sent: ArchiveMutationRequest[] = []
    let lookups = 0
    const wrapper = mount(DeleteArchiveEntryDialog, {
      props: { address, version: 3n },
      attachTo: document.body,
      global: {
        plugins: [vuetifyForTests()],
        provide: {
          [REPOSITORY_CLIENTS as symbol]: routerClients((r) =>
            r.service(ArchiveMutationService, {
              archiveMutation(request) {
                sent.push(request)
                if (sent.length === 1) throw new ConnectError('lost response', Code.Unavailable)
                return create(ArchiveMutationResponseSchema, { receipt: receipt(request) })
              },
              getArchiveMutation(request) {
                expect(request.operationId).toBe(sent[0].operationId)
                if (++lookups === 1) throw new ConnectError('not recorded', Code.NotFound)
                throw new ConnectError('cannot look up', Code.Unavailable)
              },
            }),
          ),
        },
      },
    })
    await settle()
    button('confirm').click()
    await settle()
    expect(text()).toContain('outcome is unknown')
    button('check').click()
    await settle()
    expect(text()).toContain('original request may still commit')
    button('check').click()
    await settle()
    expect(text()).toContain('does not settle the original delete')
    await wrapper.setProps({ address: create(EntryAddressSchema, { ...address, entryId: 'other' }), version: 5n })
    expect(button('address').textContent).toContain('e-1')
    expect(button('version').textContent).toBe('3')
    button('confirm').click()
    await settle()
    expect(sent).toHaveLength(2)
    expect(toBinary(ArchiveMutationRequestSchema, sent[1])).toEqual(toBinary(ArchiveMutationRequestSchema, sent[0]))
    expect(button('receipt').textContent).toContain(sent[0].operationId)
    button('close').click()
    expect(wrapper.emitted('deleted')?.length).toBe(1)
    wrapper.unmount()
  })

  it.each([Code.Aborted, Code.PermissionDenied])('does not resend a definitive refusal %s', async (code) => {
    installDomStubs()
    let count = 0
    const wrapper = mount(DeleteArchiveEntryDialog, {
      props: { address, version: 3n },
      attachTo: document.body,
      global: {
        plugins: [vuetifyForTests()],
        provide: {
          [REPOSITORY_CLIENTS as symbol]: routerClients((r) =>
            r.service(ArchiveMutationService, {
              archiveMutation() {
                count++
                throw new ConnectError('server refusal', code)
              },
            }),
          ),
        },
      },
    })
    await settle()
    button('confirm').click()
    await settle()
    expect(button('confirm').disabled).toBe(true)
    expect(count).toBe(1)
    expect(wrapper.emitted('deleted')).toBeUndefined()
    if (code === Code.Aborted) expect(text()).toContain('Reload it and review the new version')
    wrapper.unmount()
  })

  it('settles a lost response by receipt lookup without another mutation', async () => {
    installDomStubs()
    let sent: ArchiveMutationRequest
    let count = 0
    const wrapper = mount(DeleteArchiveEntryDialog, {
      props: { address, version: 3n },
      attachTo: document.body,
      global: {
        plugins: [vuetifyForTests()],
        provide: {
          [REPOSITORY_CLIENTS as symbol]: routerClients((r) =>
            r.service(ArchiveMutationService, {
              archiveMutation(request) {
                sent = clone(ArchiveMutationRequestSchema, request)
                count++
                throw new ConnectError('lost', Code.Unavailable)
              },
              getArchiveMutation() {
                return create(GetArchiveMutationResponseSchema, { receipt: receipt(sent) })
              },
            }),
          ),
        },
      },
    })
    await settle()
    button('confirm').click()
    await settle()
    button('check').click()
    await settle()
    expect(button('receipt').textContent).toContain('COMPLETED')
    expect(count).toBe(1)
    wrapper.unmount()
  })

  it('keeps and emits a COMPLETED receipt when the dialog unmounts after the response arrives (#871)', async () => {
    installDomStubs()
    const store = createArchiveDeleteReceiptStore()
    const deleted: ArchiveMutationReceipt[] = []
    let onResponseArrived = () => {}
    const transport = createRouterTransport(
      (r) =>
        r.service(ArchiveMutationService, {
          archiveMutation(request) {
            return create(ArchiveMutationResponseSchema, { receipt: receipt(request) })
          },
        }),
      {
        transport: {
          interceptors: [
            (next) => async (req) => {
              const res = await next(req)
              onResponseArrived()
              return res
            },
          ],
        },
      },
    )
    const wrapper = mount(DeleteArchiveEntryDialog, {
      props: { address, version: 3n, onDeleted: (arrived: ArchiveMutationReceipt) => deleted.push(arrived) },
      attachTo: document.body,
      global: {
        plugins: [vuetifyForTests()],
        provide: {
          [REPOSITORY_CLIENTS as symbol]: repositoryClients(transport),
          [ARCHIVE_DELETE_RECEIPTS as symbol]: store,
        },
      },
    })
    onResponseArrived = () => wrapper.unmount()
    await settle()
    button('confirm').click()
    await settle()

    expect(deleted).toHaveLength(1)
    expect(deleted[0].state).toBe(ArchiveMutationState.COMPLETED)
    expect(deleted[0].commandSha256).toBe(digest)
    expect(store.items).toHaveLength(1)
    expect(store.items[0].receipt.operationId).toBe(deleted[0].operationId)
    expect(store.items[0].dialogOpen).toBe(false)
  })

  it('keeps and shows a COMPLETED receipt when a newer scope.next() aborts the signal before assignment (#871)', async () => {
    installDomStubs()
    const store = createArchiveDeleteReceiptStore()
    const deleted: ArchiveMutationReceipt[] = []
    let onResponseArrived = () => {}
    const transport = createRouterTransport(
      (r) =>
        r.service(ArchiveMutationService, {
          archiveMutation(request) {
            return create(ArchiveMutationResponseSchema, { receipt: receipt(request) })
          },
        }),
      {
        transport: {
          interceptors: [
            (next) => async (req) => {
              const res = await next(req)
              onResponseArrived()
              return res
            },
          ],
        },
      },
    )
    const wrapper = mount(DeleteArchiveEntryDialog, {
      props: { address, version: 3n, onDeleted: (arrived: ArchiveMutationReceipt) => deleted.push(arrived) },
      attachTo: document.body,
      global: {
        plugins: [vuetifyForTests()],
        provide: {
          [REPOSITORY_CLIENTS as symbol]: repositoryClients(transport),
          [ARCHIVE_DELETE_RECEIPTS as symbol]: store,
        },
      },
    })
    onResponseArrived = () => {
      ;(wrapper.vm as unknown as { scope: CallScope }).scope.next()
    }
    await settle()
    button('confirm').click()
    await settle()

    expect(button('receipt').textContent).toContain('COMPLETED')
    expect(button('close').textContent).toBe('Done')
    expect(button('close').disabled).toBe(false)
    expect(store.items).toHaveLength(1)
    expect(store.items[0].dialogOpen).toBe(true)

    button('close').click()
    expect(wrapper.emitted('deleted')).toHaveLength(1)
    expect(deleted).toHaveLength(1)
    expect(store.items[0].dialogOpen).toBe(false)
    wrapper.unmount()
    expect(deleted).toHaveLength(1)
  })

  it('keeps a COMPLETED lookup receipt when the dialog unmounts before check() assigns it (#871)', async () => {
    installDomStubs()
    const store = createArchiveDeleteReceiptStore()
    const deleted: ArchiveMutationReceipt[] = []
    let sent!: ArchiveMutationRequest
    let onLookupArrived = () => {}
    const baseClient = createClient(
      ArchiveMutationService,
      createRouterTransport(
        (r) =>
          r.service(ArchiveMutationService, {
            archiveMutation(request) {
              sent = clone(ArchiveMutationRequestSchema, request)
              throw new ConnectError('lost response', Code.Unavailable)
            },
            getArchiveMutation() {
              return create(GetArchiveMutationResponseSchema, { receipt: receipt(sent) })
            },
          }),
        {
          transport: {
            interceptors: [
              (next) => async (req) => {
                const res = await next(req)
                if (req.method.name === 'GetArchiveMutation') onLookupArrived()
                return res
              },
            ],
          },
        },
      ),
    )
    const clients = routerClients(() => {})
    clients.archiveMutations = baseClient
    const wrapper = mount(DeleteArchiveEntryDialog, {
      props: { address, version: 3n, onDeleted: (arrived: ArchiveMutationReceipt) => deleted.push(arrived) },
      attachTo: document.body,
      global: {
        plugins: [vuetifyForTests()],
        provide: {
          [REPOSITORY_CLIENTS as symbol]: clients,
          [ARCHIVE_DELETE_RECEIPTS as symbol]: store,
        },
      },
    })
    await settle()
    button('confirm').click()
    await settle()
    onLookupArrived = () => wrapper.unmount()
    button('check').click()
    await settle()

    expect(deleted).toHaveLength(1)
    expect(deleted[0].operationId).toBe(sent.operationId)
    expect(store.items).toHaveLength(1)
    expect(store.items[0].dialogOpen).toBe(false)
  })

  it('cancels an incomplete call on unmount without emitting deleted or recording a receipt', async () => {
    installDomStubs()
    const store = createArchiveDeleteReceiptStore()
    const deleted: ArchiveMutationReceipt[] = []
    let release!: () => void
    const pending = new Promise<void>((resolve) => {
      release = resolve
    })
    let started!: () => void
    const entered = new Promise<void>((resolve) => {
      started = resolve
    })
    let serverSignal: AbortSignal | undefined
    const wrapper = mount(DeleteArchiveEntryDialog, {
      props: { address, version: 3n, onDeleted: (arrived: ArchiveMutationReceipt) => deleted.push(arrived) },
      attachTo: document.body,
      global: {
        plugins: [vuetifyForTests()],
        provide: {
          [ARCHIVE_DELETE_RECEIPTS as symbol]: store,
          [REPOSITORY_CLIENTS as symbol]: routerClients((r) =>
            r.service(ArchiveMutationService, {
              async archiveMutation(request, context) {
                serverSignal = context.signal
                started()
                await pending
                return create(ArchiveMutationResponseSchema, { receipt: receipt(request) })
              },
            }),
          ),
        },
      },
    })
    await settle()
    button('confirm').click()
    await entered
    wrapper.unmount()
    release()
    await settle()
    expect(serverSignal?.aborted).toBe(true)
    expect(deleted).toHaveLength(0)
    expect(store.items).toHaveLength(0)
  })
})
