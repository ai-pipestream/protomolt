import { create } from '@bufbuild/protobuf'
import { describe, expect, it } from 'vitest'
import {
  ArchiveMutationKind,
  ArchiveMutationReceiptSchema,
  ArchiveMutationState,
} from '@/gen/ai/protomolt/proto/repo/archive/v1/archive_mutation_pb'
import { EntryAddressSchema } from '@/gen/ai/protomolt/proto/repo/archive/v1/entry_pb'
import { createArchiveDeleteReceiptStore } from './archiveDeleteReceipts'

function makeReceipt(operationId: string) {
  return create(ArchiveMutationReceiptSchema, {
    operationId,
    address: create(EntryAddressSchema, { accountId: 'acct-a', archive: 'papers', entryId: `entry-${operationId}` }),
    kind: ArchiveMutationKind.DELETE_ENTRY,
    state: ArchiveMutationState.COMPLETED,
    entryDeleted: true,
    commandSha256: 'a'.repeat(64),
  })
}

describe('archiveDeleteReceipts store', () => {
  it('deduplicates repeated arrivals for the same operation ID, toggles dialog visibility, and dismisses', () => {
    const store = createArchiveDeleteReceiptStore()
    store.record(makeReceipt('op-1'), true)
    expect(store.items).toHaveLength(1)
    expect(store.items[0].dialogOpen).toBe(true)
    expect(store.arrivals.value).toBe(1)

    store.record(makeReceipt('op-1'), false)
    expect(store.items).toHaveLength(1)
    expect(store.items[0].dialogOpen).toBe(false)
    expect(store.arrivals.value).toBe(1)

    store.setDialogOpen('op-1', true)
    expect(store.items[0].dialogOpen).toBe(true)

    store.dismiss('op-1')
    expect(store.items).toHaveLength(0)
  })

  it('bounds retained receipts by evicting the oldest closed entry', () => {
    const store = createArchiveDeleteReceiptStore()
    store.record(makeReceipt('open-op'), true)
    for (let i = 1; i <= 20; i++) {
      store.record(makeReceipt(`closed-${i}`), false)
    }
    expect(store.items).toHaveLength(20)
    expect(store.items[0].receipt.operationId).toBe('open-op')
    expect(store.items[1].receipt.operationId).toBe('closed-2')
    expect(store.items.at(-1)?.receipt.operationId).toBe('closed-20')
  })
})
