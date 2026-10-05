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
        private volatile Cleanup cleanup = Cleanup.PENDING;

        private Failure(UUID incarnation, UUID nonce, RuntimeException cause) {
            super("Reader registration failed before admission", cause);
            this.incarnation = incarnation;
            this.nonce = nonce;
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
                        INSERT INTO repository_reader_incarnations(incarnation,state,registration_nonce)
                        VALUES(:id,'ACTIVE',:nonce) ON CONFLICT(incarnation) DO NOTHING
                        """).setParameter("id", incarnation).setParameter("nonce", nonce).executeUpdate();
                var stored = (UUID) em.createNativeQuery("""
                        SELECT registration_nonce FROM repository_reader_incarnations
                        WHERE incarnation=:id FOR UPDATE
                        """).setParameter("id", incarnation).getSingleResult();
                if (!nonce.equals(stored)) return Cleanup.FOREIGN_IDENTITY;
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
        Objects.requireNonNull(tx); Objects.requireNonNull(incarnation);
        UUID nonce = UUID.randomUUID();
        try {
            tx.inTransaction(em -> {
                em.createNativeQuery("""
                        INSERT INTO repository_reader_incarnations(incarnation,state,registration_nonce)
                        VALUES(:id,'ACTIVE',:nonce)
                        """).setParameter("id", incarnation).setParameter("nonce", nonce).executeUpdate();
            });
        } catch (RuntimeException original) {
            var failure = new Failure(incarnation, nonce, original);
            try { failure.retryCleanup(tx); }
            catch (RuntimeException cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }
}
