package ai.protomolt.proto.repo.container.ledger;

import java.util.Objects;
import java.util.UUID;

/** Internal host lifecycle. Host identity is supplied by trusted host configuration, not a read request. */
public final class ReaderHostExecutions {
    private ReaderHostExecutions() {}

    /**
     * Register a fresh execution. Duplicate UUIDs fail, including after an uncertain
     * commit; they cannot be adopted. The caller retains the UUID to fence a failed
     * startup, and uses a fresh execution UUID for any replacement process.
     */
    public static void register(Tx tx, UUID execution, String hostIdentity, String bootIdentity) {
        Objects.requireNonNull(tx); Objects.requireNonNull(execution);
        requireIdentity(hostIdentity); requireIdentity(bootIdentity);
        tx.inTransaction(em -> {
            em.createNativeQuery("""
                    INSERT INTO repository_reader_host_executions(execution,host_identity,boot_identity,state)
                    VALUES(:id,:host,:boot,'ACTIVE')
                    """).setParameter("id", execution).setParameter("host", hostIdentity)
                    .setParameter("boot", bootIdentity).executeUpdate();
        });
    }

    /** Stop new registrations and reads. Existing reads still need independent proof of completion. */
    public static void fence(Tx tx, UUID execution) {
        Objects.requireNonNull(tx); Objects.requireNonNull(execution);
        tx.inTransaction(em -> {
            if (!Boolean.TRUE.equals(em.createNativeQuery("SELECT fence_repository_reader_host(:id)")
                    .setParameter("id", execution).getSingleResult()))
                throw new IllegalStateException("Host execution fence was not acknowledged");
        });
    }

    private static void requireIdentity(String value) {
        Objects.requireNonNull(value);
        if (value.isBlank() || value.codePointCount(0, value.length()) > 512)
            throw new IllegalArgumentException("Host and boot identities must contain 1 to 512 characters");
    }
}
