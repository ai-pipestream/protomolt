package ai.protomolt.proto.repo.container.ledger;

/**
 * Filter for {@link DocumentLedger#list(ListDocumentsFilter)}.
 * <p>
 * All fields are optional conjuncts (ANDed when non-null); rows not in
 * {@link DocumentStatus#AVAILABLE} never match, whatever the filter — a
 * tombstoned row is logically deleted and listers must not re-discover it.
 * Pagination is a
 * plain {@code offset} — deliberately simple: the listing shapes this backs
 * (admin console, per-crawl/per-drive enumeration) are shallow scans, not
 * deep exports, so the stability cost of offset paging under concurrent
 * inserts is acceptable and documented. Rows are ordered by
 * {@code (created_at, node_id)} so pages are at least deterministic for a
 * stable dataset.
 *
 * @param driveName    restrict to rows stored under this drive (nullable)
 * @param connectorId  restrict to rows produced by this connector (nullable)
 * @param crawlId      restrict to rows of this crawl run (nullable)
 * @param accountId    restrict to this account (nullable — but almost every
 *                     real caller should scope by account)
 * @param limit        page size; values &lt;= 0 fall back to 100
 * @param offset       zero-based row offset into the filtered, ordered result
 */
public record ListDocumentsFilter(
        String driveName,
        String connectorId,
        String crawlId,
        String accountId,
        int limit,
        long offset) {

    /** No filtering, first page of 100. */
    public static ListDocumentsFilter all() {
        return new ListDocumentsFilter(null, null, null, null, 100, 0);
    }

    /** The effective page size ({@link #limit} with the fallback applied). */
    public int effectiveLimit() {
        return limit > 0 ? limit : 100;
    }
}
