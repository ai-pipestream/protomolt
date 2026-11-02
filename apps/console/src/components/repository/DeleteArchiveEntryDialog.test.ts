// @vitest-environment jsdom
import { clone, create, toBinary } from '@bufbuild/protobuf'
import { Code, ConnectError } from '@connectrpc/connect'
import { mount } from '@vue/test-utils'
import { afterEach, describe, expect, it } from 'vitest'
import { installDomStubs, vuetifyForTests } from '@/componentTestKit'
import { ArchiveMutationKind, ArchiveMutationReceiptSchema, ArchiveMutationRequestSchema, ArchiveMutationState, type ArchiveMutationRequest } from '@/gen/ai/protomolt/proto/repo/archive/v1/archive_mutation_pb'
import { ArchiveMutationResponseSchema, ArchiveMutationService, GetArchiveMutationResponseSchema } from '@/gen/ai/protomolt/proto/repo/archive/v1/archive_mutation_service_pb'
import { EntryAddressSchema } from '@/gen/ai/protomolt/proto/repo/archive/v1/entry_pb'
import { REPOSITORY_CLIENTS } from '@/repository/clients'
import { routerClients } from '@/repository/repositoryTestKit'
import { settle } from '@/repository/repositoryViewKit'
import DeleteArchiveEntryDialog from './DeleteArchiveEntryDialog.vue'

const address = create(EntryAddressSchema, { accountId: 'acct-a', archive: 'papers', entryId: 'e-1' })
const digest = '8f98947ff1ae6a17435d9e48c584364593181ff83f1667d3af525067b9a081fe'
function receipt(request: ArchiveMutationRequest) {
  return create(ArchiveMutationReceiptSchema, { operationId: request.operationId, address,
    kind: ArchiveMutationKind.DELETE_ENTRY, state: ArchiveMutationState.COMPLETED, entryDeleted: true, commandSha256: digest })
}
const button = (name: string) => document.body.querySelector(`[data-testid="delete-archive-${name}"]`) as HTMLButtonElement
const text = () => document.body.textContent ?? ''
afterEach(() => { document.body.innerHTML = '' })

describe('archive delete confirmation', () => {
  it('keeps identity through unavailable/absent lookups and retry, then displays the recorded receipt', async () => {
    installDomStubs()
    const sent: ArchiveMutationRequest[] = []
    let lookups = 0
    const wrapper = mount(DeleteArchiveEntryDialog, {
      props: { address, version: 3n }, attachTo: document.body,
      global: { plugins: [vuetifyForTests()], provide: { [REPOSITORY_CLIENTS as symbol]: routerClients(r => r.service(ArchiveMutationService, {
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
      })) } },
    })
    await settle()
    button('confirm').click(); await settle()
    expect(text()).toContain('outcome is unknown')
    button('check').click(); await settle()
    expect(text()).toContain('original request may still commit')
    button('check').click(); await settle()
    expect(text()).toContain('does not settle the original delete')
    await wrapper.setProps({ address: create(EntryAddressSchema, { ...address, entryId: 'other' }), version: 5n })
    expect(button('address').textContent).toContain('e-1')
    expect(button('version').textContent).toBe('3')
    button('confirm').click(); await settle()
    expect(sent).toHaveLength(2)
    expect(toBinary(ArchiveMutationRequestSchema, sent[1])).toEqual(toBinary(ArchiveMutationRequestSchema, sent[0]))
    expect(button('receipt').textContent).toContain(sent[0].operationId)
    button('close').click()
    expect(wrapper.emitted('deleted')?.length).toBe(1)
    wrapper.unmount()
  })

  it.each([Code.Aborted, Code.PermissionDenied])('does not resend a definitive refusal %s', async code => {
    installDomStubs()
    let count = 0
    const wrapper = mount(DeleteArchiveEntryDialog, {
      props: { address, version: 3n }, attachTo: document.body,
      global: { plugins: [vuetifyForTests()], provide: { [REPOSITORY_CLIENTS as symbol]: routerClients(r => r.service(ArchiveMutationService, {
        archiveMutation() { count++; throw new ConnectError('server refusal', code) },
      })) } },
    })
    await settle(); button('confirm').click(); await settle()
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
      props: { address, version: 3n }, attachTo: document.body,
      global: { plugins: [vuetifyForTests()], provide: { [REPOSITORY_CLIENTS as symbol]: routerClients(r => r.service(ArchiveMutationService, {
        archiveMutation(request) { sent = clone(ArchiveMutationRequestSchema, request); count++; throw new ConnectError('lost', Code.Unavailable) },
        getArchiveMutation() { return create(GetArchiveMutationResponseSchema, { receipt: receipt(sent) }) },
      })) } },
    })
    await settle(); button('confirm').click(); await settle(); button('check').click(); await settle()
    expect(button('receipt').textContent).toContain('COMPLETED')
    expect(count).toBe(1)
    wrapper.unmount()
  })
  it('aborts its call on unmount without turning a late answer into a successful UI event', async () => {
    installDomStubs()
    let release!: () => void
    const pending = new Promise<void>(resolve => { release = resolve })
    let started!: () => void
    const entered = new Promise<void>(resolve => { started = resolve })
    let serverSignal: AbortSignal | undefined
    const wrapper = mount(DeleteArchiveEntryDialog, {
      props: { address, version: 3n }, attachTo: document.body,
      global: { plugins: [vuetifyForTests()], provide: { [REPOSITORY_CLIENTS as symbol]: routerClients(r => r.service(ArchiveMutationService, {
        async archiveMutation(request, context) {
          serverSignal = context.signal
          started()
          await pending
          return create(ArchiveMutationResponseSchema, { receipt: receipt(request) })
        },
      })) } },
    })
    await settle(); button('confirm').click(); await entered
    wrapper.unmount(); release(); await settle()
    expect(serverSignal?.aborted).toBe(true)
    expect(wrapper.emitted('deleted')).toBeUndefined()
  })

})
