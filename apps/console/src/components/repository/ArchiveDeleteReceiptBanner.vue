<template>
  <div v-if="visible.length > 0" data-testid="archive-delete-receipts">
    <VAlert
      v-for="item in visible"
      :key="item.receipt.operationId"
      type="success"
      variant="tonal"
      density="compact"
      closable
      class="mb-3"
      data-testid="archive-delete-receipt-banner"
      @click:close="receipts.dismiss(item.receipt.operationId)"
    >
      <div>
        The archive recorded the deletion of
        <span v-if="item.receipt.address" class="text-mono">{{ item.receipt.address.accountId }} / {{ item.receipt.address.archive }} / {{ item.receipt.address.entryId }}</span><span v-else>the entry</span>.
        Receipt
        <RouterLink
          v-if="item.receipt.address?.accountId && hasLookupRoute"
          :to="{ name: 'repository-lookup', query: { account: item.receipt.address.accountId, op: item.receipt.operationId } }"
          class="text-mono"
        >{{ item.receipt.operationId }}</RouterLink>
        <span v-else class="text-mono">{{ item.receipt.operationId }}</span>
        is COMPLETED.
      </div>
      <div class="text-caption text-mono">Command SHA-256 {{ item.receipt.commandSha256 }}</div>
    </VAlert>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { RouterLink, useRouter } from 'vue-router'
import { VAlert } from 'vuetify/components'
import { useArchiveDeleteReceipts } from '@/repository/archiveDeleteReceipts'

const receipts = useArchiveDeleteReceipts()
const router = useRouter()
const hasLookupRoute = computed(() => router.hasRoute('repository-lookup'))
const visible = computed(() => receipts.items.filter((item) => !item.dialogOpen))
</script>
