package ai.protomolt.proto.repo.container.ledger;

import java.util.List;

/**
 * One page of a {@link DocumentLedger#list(ListDocumentsFilter)} result.
 *
 * @param rows       the page's rows, ordered by {@code (created_at, node_id)};
 *                   detached snapshots
 * @param totalCount total rows matching the filter across ALL pages — the
 *                   console needs it to render pagination without walking
 *                   every page
 */
public record ListDocumentsResult(List<DocumentRecord> rows, long totalCount) {
}
