package ai.protomolt.proto.repo.container.ledger;

import java.util.Objects;
import java.util.UUID;

/** Exact per-reader recovery authorization under a persisted verified host termination. */
public final class ReaderExternalQuiescence {
    private final Tx tx;

    public ReaderExternalQuiescence(Tx tx) { this.tx = Objects.requireNonNull(tx); }

    /**
     * Rechecks the durable SQL receipt; supplied UUIDs are identities, not capabilities.
     * Returns the original quiescence receipt on exact replay. This operation does
     * not release pins; use the existing bounded recovery operations afterward.
     */
    public UUID quiesce(UUID reader, UUID registrationNonce, UUID hostExecution, UUID terminationReceipt) {
        Objects.requireNonNull(reader); Objects.requireNonNull(registrationNonce);
        Objects.requireNonNull(hostExecution); Objects.requireNonNull(terminationReceipt);
        return tx.inTransaction(em -> (UUID) em.createNativeQuery(
                "SELECT quiesce_repository_reader_from_host(:reader,:nonce,:host,:receipt)")
                .setParameter("reader", reader).setParameter("nonce", registrationNonce)
                .setParameter("host", hostExecution).setParameter("receipt", terminationReceipt).getSingleResult());
    }
}
