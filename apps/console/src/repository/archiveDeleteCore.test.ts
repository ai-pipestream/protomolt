import { clone, create, toBinary } from '@bufbuild/protobuf'
import { Code, ConnectError, createClient, createRouterTransport } from '@connectrpc/connect'
import { describe, expect, it } from 'vitest'
import { ArchiveMutationKind, ArchiveMutationReceiptSchema, ArchiveMutationRequestSchema, ArchiveMutationState, type ArchiveMutationRequest } from '@/gen/ai/protomolt/proto/repo/archive/v1/archive_mutation_pb'
import { ArchiveMutationResponseSchema, ArchiveMutationService, GetArchiveMutationResponseSchema } from '@/gen/ai/protomolt/proto/repo/archive/v1/archive_mutation_service_pb'
import { EntryAddressSchema } from '@/gen/ai/protomolt/proto/repo/archive/v1/entry_pb'
import { archiveDeleteDigest, deleteArchiveEntry, lookupArchiveDelete, planArchiveDelete, requireArchiveDeleteReceipt } from './archiveDeleteCore'

const address = create(EntryAddressSchema, { accountId: 'acct-a', archive: 'papers', entryId: 'e-1' })
const id = '83c1c0da-e1c2-42a4-a22e-132021d7cd5a'
const signal = () => new AbortController().signal
const plan = () => planArchiveDelete(address, 3n, id)
// Independent wire vector also asserted by ArchiveOperationKeysTest on the production Java codec.
const digest = '8f98947ff1ae6a17435d9e48c584364593181ff83f1667d3af525067b9a081fe'
const receipt = () => create(ArchiveMutationReceiptSchema, {
  operationId: id, address, kind: ArchiveMutationKind.DELETE_ENTRY, state: ArchiveMutationState.COMPLETED,
  entryDeleted: true, commandSha256: digest,
})

describe('archive entry delete', () => {
  it('pins the observed version and snapshots identity while matching the Java receipt digest', () => {
    const mutable = clone(EntryAddressSchema, address)
    const request = planArchiveDelete(mutable, 9007199254740993n, id)
    mutable.entryId = 'other'
    expect(request.mutation).toMatchObject({ case: 'deleteEntry', value: { expectedVersion: 9007199254740993n, address } })
    expect(archiveDeleteDigest(request)).toBe(digest)
    expect(() => planArchiveDelete(address, 0n, id)).toThrow('positive version')
  })
  it('resends identical wire bytes and operation ID after a lost answer', async () => {
    const sent: Uint8Array[] = []
    const client = createClient(ArchiveMutationService, createRouterTransport(r => r.service(ArchiveMutationService, {
      archiveMutation(request) {
        sent.push(toBinary(ArchiveMutationRequestSchema, request))
        if (sent.length === 1) throw new ConnectError('answer lost', Code.Unavailable)
        return create(ArchiveMutationResponseSchema, { receipt: receipt() })
      },
    })))
    const request = plan()
    expect((await deleteArchiveEntry(client, request, signal())).kind).toBe('unsettled')
    expect((await deleteArchiveEntry(client, request, signal())).kind).toBe('deleted')
    expect(sent[1]).toEqual(sent[0])
  })
  it.each([Code.Aborted, Code.PermissionDenied, Code.AlreadyExists, Code.InvalidArgument])('preserves refusal %s', async code => {
    const client = createClient(ArchiveMutationService, createRouterTransport(r => r.service(ArchiveMutationService, {
      archiveMutation() { throw new ConnectError('server refusal', code) },
    })))
    expect(await deleteArchiveEntry(client, plan(), signal())).toMatchObject({ kind: code === Code.Aborted ? 'stale' : 'refused', failure: { codeNumber: code, message: 'server refusal' } })
  })
  it('looks up the same account/operation and never mutates during a lookup', async () => {
    let mutations = 0
    let lookups = 0
    const client = createClient(ArchiveMutationService, createRouterTransport(r => r.service(ArchiveMutationService, {
      archiveMutation() { mutations++; throw new Error('not expected') },
      getArchiveMutation(request) {
        expect(request).toMatchObject({ accountId: 'acct-a', operationId: id })
        if (++lookups === 1) throw new ConnectError('not recorded yet', Code.NotFound)
        if (lookups === 2) throw new ConnectError('backend unavailable', Code.Unavailable)
        return create(GetArchiveMutationResponseSchema, { receipt: receipt() })
      },
    })))
    expect((await lookupArchiveDelete(client, plan(), signal())).kind).toBe('unknown')
    expect((await lookupArchiveDelete(client, plan(), signal())).kind).toBe('unavailable')
    expect((await lookupArchiveDelete(client, plan(), signal())).kind).toBe('deleted')
    expect(mutations).toBe(0)
  })
  it('does not trust a missing, incomplete or wrong-command receipt', () => {
    for (const invalid of [undefined, create(ArchiveMutationReceiptSchema, { ...receipt(), operationId: 'different' }),
      create(ArchiveMutationReceiptSchema, { ...receipt(), commandSha256: '0'.repeat(64) }),
      create(ArchiveMutationReceiptSchema, { ...receipt(), state: ArchiveMutationState.ADMITTED }),
      create(ArchiveMutationReceiptSchema, { ...receipt(), entryDeleted: false })]) {
      expect(() => requireArchiveDeleteReceipt(plan(), invalid)).toThrow()
    }
  })
  it('preserves cancellation as an unsettled outcome', async () => {
    const client = createClient(ArchiveMutationService, createRouterTransport(r => r.service(ArchiveMutationService, {
      archiveMutation() { throw new ConnectError('cancelled', Code.Canceled) },
    })))
    expect(await deleteArchiveEntry(client, plan(), signal())).toMatchObject({ kind: 'unsettled', failure: { codeNumber: Code.Canceled } })
  })
})
