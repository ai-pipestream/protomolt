package ai.protomolt.proto.repo.container.ledger;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/** Bounded discovery for a trusted recovery supervisor; discovered identities grant no authority. */
public final class ReaderHostDiscovery {
    public static final int MAX_PAGE_SIZE = 1000;
    public enum State { ACTIVE, FENCED, QUIESCED }
    public enum Quiescence { NONE, LOCAL_DRAIN, HOST_TERMINATION }
    public record Reader(UUID incarnation, UUID registrationNonce, State state, Quiescence quiescence) {}
    public record Page(List<Reader> readers, Optional<UUID> nextCursor) {
        public Page { readers = List.copyOf(readers); Objects.requireNonNull(nextCursor); }
    }
    private final Tx tx;

    /** Borrow a transactional view with the supervisor's SQL timeouts and pool limits. */
    public ReaderHostDiscovery(Tx tx) { this.tx = Objects.requireNonNull(tx); }

    /**
     * Requires the exact permanent termination receipt, even for an empty page.
     * Includes QUIESCED readers: their pin/session cleanup may still be pending.
     * No locks are held across subsequent per-reader cleanup. State is a snapshot;
     * each mutation must recheck its own durable preconditions.
     *
     * Cursor order is PostgreSQL UUID order, not Java UUID.compareTo. An empty page
     * ends one traversal, not recovery: restart from an empty cursor to retry failed
     * work and discover late failed-registration tombstones. Advance scheduling
     * past failed work only while retaining and reporting its failure.
     */
    public Page discover(UUID host, UUID terminationReceipt, Optional<UUID> after, int limit) {
        Objects.requireNonNull(host); Objects.requireNonNull(terminationReceipt); Objects.requireNonNull(after);
        if (limit < 1 || limit > MAX_PAGE_SIZE)
            throw new IllegalArgumentException("Reader discovery requires 1 to " + MAX_PAGE_SIZE + " results");
        return tx.inTransaction(em -> {
            var verified = em.createNativeQuery("""
                    SELECT t.receipt_id FROM repository_reader_host_terminations t
                    JOIN repository_reader_host_executions h ON h.execution=t.execution
                    WHERE h.execution=:host AND h.state='TERMINATED' AND t.receipt_id=:receipt
                      AND t.host_identity=h.host_identity AND t.boot_identity=h.boot_identity
                    """).setParameter("host", host).setParameter("receipt", terminationReceipt).getResultList();
            if (verified.size() != 1) throw new IllegalStateException("Exact verified host termination is required");
            var query = em.createNativeQuery("""
                    SELECT incarnation,registration_nonce,state,quiescence_source
                    FROM repository_reader_incarnations WHERE host_execution=:host
                    """ + (after.isPresent() ? " AND incarnation > :after" : "")
                    + " ORDER BY incarnation LIMIT :limit").setParameter("host", host).setParameter("limit", limit);
            after.ifPresent(cursor -> query.setParameter("after", cursor));
            List<Object[]> rows = query.getResultList();
            var readers = rows.stream().map(row -> new Reader((UUID) row[0], (UUID) row[1],
                    State.valueOf((String) row[2]), row[3] == null ? Quiescence.NONE : Quiescence.valueOf((String) row[3]))).toList();
            return new Page(readers, readers.isEmpty() ? Optional.empty() : Optional.of(readers.getLast().incarnation()));
        });
    }
}
