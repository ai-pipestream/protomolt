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
            // All lifecycle paths lock the source owner before common retention.
            if (em.createNativeQuery("SELECT object_id FROM archive_object_uploads WHERE object_id=:id FOR UPDATE")
                    .setParameter("id", object).getResultList().isEmpty()) return Optional.empty();
            boolean retiring = (Boolean) em.createNativeQuery(
                    "SELECT retiring FROM repository_object_retention WHERE object_id=:id FOR UPDATE")
                    .setParameter("id", object).getSingleResult();
            if (retiring) return Optional.empty();
            var readable = ArchiveObjectLedger.readable(em, entry, version, object);
            if (readable.isEmpty()) return Optional.empty();
            UUID id = UUID.randomUUID();
            em.createNativeQuery("""
                    INSERT INTO archive_read_pins(pin_id,reader_incarnation,object_id,entry_uuid,version)
                    VALUES(:pin,:reader,:object,:entry,:version)
                    """).setParameter("pin", id).setParameter("reader", incarnation)
                    .setParameter("object", object).setParameter("entry", entry).setParameter("version", version).executeUpdate();
            return Optional.of(new Pin(id, readable.orElseThrow()));
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
                // Lock order must precede DELETE's tuple lock as well as its trigger.
                em.createNativeQuery("SELECT object_id FROM archive_object_uploads WHERE object_id=:id FOR UPDATE")
                        .setParameter("id", readable.binding().objectId()).getSingleResult();
                em.createNativeQuery("SELECT object_id FROM repository_object_retention WHERE object_id=:id FOR UPDATE")
                        .setParameter("id", readable.binding().objectId()).getSingleResult();
                int deleted = em.createNativeQuery("DELETE FROM archive_read_pins WHERE pin_id=:pin AND reader_incarnation=:reader")
                        .setParameter("pin", id).setParameter("reader", incarnation).executeUpdate();
                // An earlier close may have committed despite a lost acknowledgement.
                // Absence is idempotent; another incarnation owning this ID is not.
                if (deleted != 1 && !em.createNativeQuery("SELECT pin_id FROM archive_read_pins WHERE pin_id=:pin")
                        .setParameter("pin", id).getResultList().isEmpty())
                    throw new IllegalStateException("Archive read pin belongs to another incarnation");
            });
            closed = true;
        }
    }
}
