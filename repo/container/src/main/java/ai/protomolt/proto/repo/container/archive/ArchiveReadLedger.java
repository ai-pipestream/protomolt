package ai.protomolt.proto.repo.container.archive;

import ai.protomolt.proto.repo.container.ledger.Tx;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * Durable read lifetimes after caller authorization. Each ledger belongs to one
 * fresh reader incarnation; never reuse that identity after a process restart.
 * Pins have no expiry. Crash recovery requires proof that the owning incarnation
 * and all its provider I/O have stopped; no automatic recovery API is enabled.
 */
public final class ArchiveReadLedger {
    private final Tx tx;
    private final UUID incarnation;

    public ArchiveReadLedger(Tx tx, UUID incarnation) {
        this.tx = Objects.requireNonNull(tx);
        this.incarnation = Objects.requireNonNull(incarnation);
    }

    /** Acquires before returning coordinates. Provider I/O must occur after commit. */
    public Optional<Pin> acquire(UUID entry, long version, UUID object) {
        Objects.requireNonNull(entry);
        Objects.requireNonNull(object);
        if (version <= 0) throw new IllegalArgumentException("A retained version is required");
        return tx.inTransaction(em -> {
            UUID id = UUID.randomUUID();
            var rows = em.createNativeQuery("""
                    SELECT * FROM acquire_archive_read_pin(:pin,:reader,:entry,:version,:object)
                    """).setParameter("pin", id).setParameter("reader", incarnation)
                    .setParameter("entry", entry).setParameter("version", version).setParameter("object", object).getResultList();
            if (rows.isEmpty()) return Optional.empty();
            if (rows.size() != 1) throw new IllegalStateException("Archive read admission returned multiple identities");
            return Optional.of(new Pin(id, ArchiveObjectLedger.decodeReadable((Object[]) rows.getFirst())));
        });
    }

    /**
     * Internal lifetime handle, not authorization. Close only after actual provider
     * completion, including when cancellation is ignored. A failed close is visible
     * and leaves the pin available for retry; it never implies cleanup permission.
     */
    public final class Pin implements AutoCloseable {
        private final UUID id;
        private final ArchiveObjectLedger.Readable readable;
        private boolean closed;

        private Pin(UUID id, ArchiveObjectLedger.Readable readable) {
            this.id = id;
            this.readable = readable;
        }

        public ArchiveObjectLedger.Readable readable() { return readable; }

        @Override public synchronized void close() {
            if (closed) return;
            tx.inTransaction(em -> {
                boolean released = (Boolean) em.createNativeQuery("SELECT release_archive_read_pin(:pin,:reader,:object)")
                        .setParameter("pin", id).setParameter("reader", incarnation)
                        .setParameter("object", readable.binding().objectId()).getSingleResult();
                if (!released) throw new IllegalStateException("Archive read release was not acknowledged");
            });
            closed = true;
        }
    }
}
