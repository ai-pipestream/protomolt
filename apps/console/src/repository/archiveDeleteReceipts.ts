/**
 * Retains verified archive entry deletion receipts across route changes so a
 * receipt that arrives as or after its confirmation dialog unmounts is still
 * shown to the operator until dismissed.
 */
import { computed, inject, provide, reactive, ref, type InjectionKey, type Ref } from 'vue'
import type { ArchiveMutationReceipt } from '@/gen/ai/protomolt/proto/repo/archive/v1/archive_mutation_pb'

const MAX_RETAINED_ARCHIVE_DELETE_RECEIPTS = 20

export interface RetainedArchiveDeleteReceipt {
  readonly receipt: ArchiveMutationReceipt
  dialogOpen: boolean
}

export interface ArchiveDeleteReceiptStore {
  readonly items: readonly RetainedArchiveDeleteReceipt[]
  readonly arrivals: Readonly<Ref<number>>
  readonly latestArrival: Readonly<Ref<ArchiveMutationReceipt | null>>
  record(receipt: ArchiveMutationReceipt, dialogOpen: boolean): void
  setDialogOpen(operationId: string, dialogOpen: boolean): void
  dismiss(operationId: string): void
}

export function createArchiveDeleteReceiptStore(): ArchiveDeleteReceiptStore {
  const items = reactive<RetainedArchiveDeleteReceipt[]>([])
  const arrivals = ref(0)
  const latestArrival = ref<ArchiveMutationReceipt | null>(null)

  return {
    items,
    arrivals,
    latestArrival,
    record(receipt: ArchiveMutationReceipt, dialogOpen: boolean) {
      const existing = items.find((item) => item.receipt.operationId === receipt.operationId)
      if (existing) {
        existing.dialogOpen = dialogOpen
        return
      }
      if (items.length >= MAX_RETAINED_ARCHIVE_DELETE_RECEIPTS) {
        const closedIndex = items.findIndex((item) => !item.dialogOpen)
        items.splice(closedIndex >= 0 ? closedIndex : 0, 1)
      }
      items.push({ receipt, dialogOpen })
      latestArrival.value = receipt
      arrivals.value += 1
    },
    setDialogOpen(operationId: string, dialogOpen: boolean) {
      const existing = items.find((item) => item.receipt.operationId === operationId)
      if (existing) {
        existing.dialogOpen = dialogOpen
      }
    },
    dismiss(operationId: string) {
      const index = items.findIndex((item) => item.receipt.operationId === operationId)
      if (index >= 0) {
        items.splice(index, 1)
      }
    },
  }
}

export const ARCHIVE_DELETE_RECEIPTS: InjectionKey<ArchiveDeleteReceiptStore> = Symbol('archive-delete-receipts')
const ARCHIVE_RECEIPT_BANNER_HOSTED: InjectionKey<boolean> = Symbol('archive-receipt-banner-hosted')

/**
 * Returns the ancestor receipt store when mounted inside a console or repository
 * host, or a local isolated store in standalone component tests.
 */
export function useArchiveDeleteReceipts(): ArchiveDeleteReceiptStore {
  return inject(ARCHIVE_DELETE_RECEIPTS, null) ?? createArchiveDeleteReceiptStore()
}

/**
 * Installs a receipt store (if none is provided above) and claims banner
 * ownership for the outermost mounted host so nested views never duplicate it.
 */
export function useArchiveDeleteReceiptHost(): {
  receipts: ArchiveDeleteReceiptStore
  showBanner: Readonly<Ref<boolean>>
} {
  const existing = inject(ARCHIVE_DELETE_RECEIPTS, null)
  const receipts = existing ?? createArchiveDeleteReceiptStore()
  if (existing === null) {
    provide(ARCHIVE_DELETE_RECEIPTS, receipts)
  }
  const hostedAbove = inject(ARCHIVE_RECEIPT_BANNER_HOSTED, false)
  const ownsBanner = !hostedAbove
  if (ownsBanner) {
    provide(ARCHIVE_RECEIPT_BANNER_HOSTED, true)
  }
  const showBanner = computed(() => ownsBanner && receipts.items.some((item) => !item.dialogOpen))
  return { receipts, showBanner }
}
