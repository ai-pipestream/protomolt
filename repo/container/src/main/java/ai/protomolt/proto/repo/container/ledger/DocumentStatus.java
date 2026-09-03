package ai.protomolt.proto.repo.container.ledger;

/**
 * Storage lifecycle status of a {@link DocumentRecord} row.
 * <p>
 * Status transitions are deliberately decoupled from {@code updated_at}: a
 * status-only change (e.g. a tombstone to {@link #PENDING_PURGE}) must NOT
 * bump {@code updated_at}, because a reclaim/purge compares {@code updated_at}
 * against the reclaim's requested_at to void stale purges. Only an actual
 * body (re)write moves {@code updated_at}, and it does so explicitly.
 */
public final class DocumentStatus {

    /** Default: the body is in object storage and readable. */
    public static final String AVAILABLE = "AVAILABLE";

    /** Soft-deleted/tombstoned, awaiting object deletion by the background purger. */
    public static final String PENDING_PURGE = "PENDING_PURGE";

    /** Object deletion failed after the purger's maximum attempts. */
    public static final String PURGE_FAILED = "PURGE_FAILED";

    private DocumentStatus() {
    }
}
