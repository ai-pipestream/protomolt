package ai.protomolt.proto.repo.container.ledger;

import java.util.Objects;
import java.util.UUID;

/** Fresh reader registration; failed constructors have never admitted provider work. */
public final class ReaderRegistration {
    private ReaderRegistration() {}

    public enum Cleanup { PENDING, QUIESCED, FOREIGN_IDENTITY }

    /**
     * Retains the exact failed registration identity for explicit cleanup retry.
     * The private nonce cannot be supplied for an existing/live reader. The cause
     * remains the original registration error; the initial cleanup error is suppressed.
     */
    public static final class Failure extends IllegalStateException {
        private final UUID incarnation;
        private final UUID nonce;
        private final UUID hostExecution;
        private volatile Cleanup cleanup = Cleanup.PENDING;

        private Failure(UUID incarnation, UUID nonce, UUID hostExecution, RuntimeException cause) {
            super("Reader registration failed before admission", cause);
            this.incarnation = incarnation;
            this.nonce = nonce;
            this.hostExecution = hostExecution;
        }

        public UUID incarnation() { return incarnation; }
        public Cleanup cleanup() { return cleanup; }

        /**
         * Retry using a live, appropriately bounded Tx for the same repository.
         * FOREIGN_IDENTITY is not quiescence: a different registration owns that
         * UUID and remains untouched. No return value makes registration succeed.
         * Failure or a lost cleanup acknowledgment leaves PENDING for another retry.
         */
        public synchronized Cleanup retryCleanup(Tx tx) {
            Objects.requireNonNull(tx);
            if (cleanup != Cleanup.PENDING) return cleanup;
            cleanup = tx.inTransaction(em -> {
                // The unique-index conflict waits for an original in-flight INSERT
                // to resolve. A SELECT alone could miss an uncommitted registration.
                // On rollback, reserve the UUID as our permanent, closed tombstone.
                em.createNativeQuery("""
                        INSERT INTO repository_reader_incarnations(incarnation,state,registration_nonce,host_execution,quiescence_source,quiesced_at)
                        VALUES(:id,'QUIESCED',:nonce,CAST(:host AS uuid),'LOCAL_DRAIN',clock_timestamp()) ON CONFLICT(incarnation) DO NOTHING
                        """).setParameter("id", incarnation).setParameter("nonce", nonce).setParameter("host", hostExecution).executeUpdate();
                var stored = (Object[]) em.createNativeQuery("""
                        SELECT registration_nonce,host_execution FROM repository_reader_incarnations
                        WHERE incarnation=:id FOR UPDATE
                        """).setParameter("id", incarnation).getSingleResult();
                if (!nonce.equals(stored[0]) || !Objects.equals(hostExecution, stored[1])) return Cleanup.FOREIGN_IDENTITY;
                if (!Boolean.TRUE.equals(em.createNativeQuery("SELECT fence_repository_reader(:id)")
                        .setParameter("id", incarnation).getSingleResult()))
                    throw new IllegalStateException("Failed registration fence was not acknowledged");
                if (!Boolean.TRUE.equals(em.createNativeQuery("SELECT attest_local_reader_quiescence(:id)")
                        .setParameter("id", incarnation).getSingleResult()))
                    throw new IllegalStateException("Failed registration quiescence was not acknowledged");
                return Cleanup.QUIESCED;
            });
            return cleanup;
        }
    }

    /** Strictly fresh registration; never resume or reactivate a duplicate UUID. */
    public static void register(Tx tx, UUID incarnation) {
        registerWithBinding(tx, incarnation, null);
    }

    /** Bind a fresh reader to the host execution before any read can be admitted. */
    public static void register(Tx tx, UUID incarnation, UUID hostExecution) {
        registerWithBinding(tx, incarnation, Objects.requireNonNull(hostExecution));
    }

    private static void registerWithBinding(Tx tx, UUID incarnation, UUID hostExecution) {
        Objects.requireNonNull(tx); Objects.requireNonNull(incarnation);

        UUID nonce = UUID.randomUUID();
        try {
            tx.inTransaction(em -> {
                em.createNativeQuery("""
                        INSERT INTO repository_reader_incarnations(incarnation,state,registration_nonce,host_execution)
                        VALUES(:id,'ACTIVE',:nonce,CAST(:host AS uuid))
                        """).setParameter("id", incarnation).setParameter("nonce", nonce).setParameter("host", hostExecution).executeUpdate();
            });
        } catch (RuntimeException original) {
            var failure = new Failure(incarnation, nonce, hostExecution, original);
            try { failure.retryCleanup(tx); }
            catch (RuntimeException cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }
}
