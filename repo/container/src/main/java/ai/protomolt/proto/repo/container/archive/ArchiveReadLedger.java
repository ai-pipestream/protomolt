package ai.protomolt.proto.repo.container.archive;

import ai.protomolt.proto.repo.container.ledger.Tx;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * Durable read lifetimes after caller authorization. Each ledger belongs to one
 * fresh reader incarnation; never reuse that identity after a process restart.
 * Pins have no expiry. Crash recovery requires proof that the owning incarnation
 * and all its provider I/O have stopped. This ledger attests only local shutdown.
 * External recovery requires a separately persisted verified host termination
 * and exact per-reader quiescence receipt.
 */
public final class ArchiveReadLedger {
    private final Tx tx;
    private final UUID incarnation;
    private boolean fenced;
    private boolean quiesced;
    private final Object lifetime = new Object();
    private boolean admissionClosed;
    private int activeLifetimes;

    /** Registers a fresh identity before any admission; duplicate identities fail, never reactivate. */
    public ArchiveReadLedger(Tx tx, UUID incarnation) {
        this(tx, incarnation, Optional.empty());
    }

    /** Register under an existing ACTIVE host execution; no instance escapes on failure. */
    public ArchiveReadLedger(Tx tx, UUID incarnation, UUID hostExecution) {
        this(tx, incarnation, Optional.of(hostExecution));
    }

    private ArchiveReadLedger(Tx tx, UUID incarnation, Optional<UUID> hostExecution) {

        this.tx = Objects.requireNonNull(tx);
        this.incarnation = Objects.requireNonNull(incarnation);
        if (hostExecution.isPresent())
            ai.protomolt.proto.repo.container.ledger.ReaderRegistration.register(tx, incarnation, hostExecution.get());
        else ai.protomolt.proto.repo.container.ledger.ReaderRegistration.register(tx, incarnation);
    }

    /** Permanently stops new pin admission. Does not prove that existing reads stopped. */
    public synchronized void fence() {
        if (fenced) return;
        synchronized (lifetime) { admissionClosed = true; }
        tx.inTransaction(em -> {
            if (!Boolean.TRUE.equals(em.createNativeQuery("SELECT fence_repository_reader(:id)")
                    .setParameter("id", incarnation).getSingleResult()))
                throw new IllegalStateException("Reader fence was not acknowledged");
        });
        fenced = true;
    }

    /**
     * Attest only this uniquely registered owner's completed lifetimes. Closing a
     * pin asserts actual provider completion, even if its SQL release then fails.
     * This is local evidence, never a claim about another process or UUID.
     */
    public synchronized void attestLocalQuiescence() {
        if (quiesced) return;
        synchronized (lifetime) {
            if (!fenced || !admissionClosed || activeLifetimes != 0)
                throw new IllegalStateException("Reader must be fenced with all local lifetimes completed");
        }
        tx.inTransaction(em -> {
            if (!Boolean.TRUE.equals(em.createNativeQuery("SELECT attest_local_reader_quiescence(:id)")
                    .setParameter("id", incarnation).getSingleResult()))
                throw new IllegalStateException("Reader quiescence was not acknowledged");
        });
        quiesced = true;
    }

    /** Acquires before returning coordinates. Provider I/O must occur after commit. */
    public Optional<Pin> acquire(UUID entry, long version, UUID object) {
        Objects.requireNonNull(entry);
        Objects.requireNonNull(object);
        if (version <= 0) throw new IllegalArgumentException("A retained version is required");
        synchronized (lifetime) {
            if (admissionClosed) throw new IllegalStateException("Reader admission is closed");
            activeLifetimes++;
        }
        boolean handedOff = false;
        try {
            var result = tx.inTransaction(em -> {
                UUID id = UUID.randomUUID();
                var rows = em.createNativeQuery("""
                        SELECT * FROM acquire_archive_read_pin(:pin,:reader,:entry,:version,:object)
                        """).setParameter("pin", id).setParameter("reader", incarnation)
                        .setParameter("entry", entry).setParameter("version", version).setParameter("object", object).getResultList();
                if (rows.isEmpty()) return Optional.<Pin>empty();
                if (rows.size() != 1) throw new IllegalStateException("Archive read admission returned multiple identities");
                return Optional.of(new Pin(id, ArchiveObjectLedger.decodeReadable((Object[]) rows.getFirst())));
            });
            handedOff = result.isPresent();
            return result;
        } finally {
            if (!handedOff) completeLifetime();
        }
    }

    private void completeLifetime() {
        synchronized (lifetime) { activeLifetimes--; }
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
        private boolean lifetimeCompleted;

        private Pin(UUID id, ArchiveObjectLedger.Readable readable) {
            this.id = id;
            this.readable = readable;
        }

        public ArchiveObjectLedger.Readable readable() { return readable; }

        @Override public synchronized void close() {
            if (closed) return;
            try {
                tx.inTransaction(em -> {
                    boolean released = (Boolean) em.createNativeQuery("SELECT release_archive_read_pin(:pin,:reader,:object)")
                            .setParameter("pin", id).setParameter("reader", incarnation)
                            .setParameter("object", readable.binding().objectId()).getSingleResult();
                    if (!released) throw new IllegalStateException("Archive read release was not acknowledged");
                });
                closed = true;
            } finally {
                if (!lifetimeCompleted) {
                    lifetimeCompleted = true;
                    completeLifetime();
                }
            }
        }
    }
}
