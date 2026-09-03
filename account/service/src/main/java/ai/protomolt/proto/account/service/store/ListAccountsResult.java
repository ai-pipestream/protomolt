package ai.protomolt.proto.account.service.store;

import java.util.List;

/**
 * One page of accounts plus the total across all pages (the list path's
 * continuation-token arithmetic needs both).
 *
 * @param rows the page (in stable created_at, account_id order)
 * @param totalCount total rows matching the filter across all pages
 */
public record ListAccountsResult(List<AccountRecord> rows, long totalCount) {
}
