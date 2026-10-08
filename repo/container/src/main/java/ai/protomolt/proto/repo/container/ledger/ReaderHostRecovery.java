package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.container.archive.ArchiveReadRecovery;
import ai.protomolt.proto.repo.spi.RepositoryOperationControl;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/** One bounded step for a trusted supervisor recovering an already-terminated execution. */
public final class ReaderHostRecovery {
    public static final int MAX_PAGE_WORK = 1000;

    public record Recovered(UUID reader, int archivePins, int documentPins, int assessmentSessions) {
        /** These three families only; capture drains and preparation roots need separate authority. */
        public boolean readerResourcesDrained() { return archivePins == 0 && documentPins == 0 && assessmentSessions == 0; }
    }
    public record Failed(UUID reader, RuntimeException failure) {
        public Failed { Objects.requireNonNull(reader); Objects.requireNonNull(failure); }
    }
    public record Page(List<Recovered> recovered, List<Failed> failed, Optional<UUID> nextCursor) {
        public Page {
            recovered = List.copyOf(recovered); failed = List.copyOf(failed); Objects.requireNonNull(nextCursor);
        }
    }
    /** Carries scheduling progress without converting any failed cleanup into a successful result. */
    public static final class PageFailure extends IllegalStateException {
        private final Page page;
        private PageFailure(Page page) {
            super("Reader host recovery page failed; completed releases remain committed", page.failed().getFirst().failure());
            this.page = page;
            page.failed().stream().skip(1).forEach(failure -> addSuppressed(failure.failure()));
        }
        public Page page() { return page; }
    }

    private final Tx tx;
    public ReaderHostRecovery(Tx tx) { this.tx = Objects.requireNonNull(tx); }

    /**
     * Requires a persisted termination receipt; it never infers death or installs
     * a verifier. Reader identity and state are checked again before cleanup.
     * Local quiescence is preserved; a race with local drain is reported for retry.
     *
     * A shared per-reader resource limit covers archive pins, document pins and
     * assessment sessions. readerLimit * resourceLimit must not exceed 1000.
     * Cancellation is checked between readers and before reporting a result; it
     * does not roll back earlier releases. SQL waits need the supervisor's bounded
     * Tx/pool configuration. No SQL transaction spans readers or provider work.
     *
     * Failed readers do not prevent attempts on other readers in the page. Any
     * failure throws PageFailure with progress. Its cursor is scheduling only;
     * wrap to the beginning to revisit failures and late registration tombstones.
     * An empty discovery page ends a traversal, never establishes host recovery.
     */
    public Page recoverPage(UUID host, UUID terminationReceipt, Optional<UUID> after,
            int readerLimit, int resourceLimit, RepositoryOperationControl control) {
        Objects.requireNonNull(host); Objects.requireNonNull(terminationReceipt);
        Objects.requireNonNull(after); Objects.requireNonNull(control);
        if (readerLimit < 1 || resourceLimit < 1 || (long) readerLimit * resourceLimit > MAX_PAGE_WORK)
            throw new IllegalArgumentException("Recovery page requires positive limits with product at most " + MAX_PAGE_WORK);
        control.check();
        var discovered = new ReaderHostDiscovery(tx).discover(host, terminationReceipt, after, readerLimit);
        var recovered = new ArrayList<Recovered>();
        var failed = new ArrayList<Failed>();
        for (var reader : discovered.readers()) {
            check(control, failed);
            try {
                recoverQuiescence(host, terminationReceipt, reader);
                int archive = new ArchiveReadRecovery(tx).recoverReaderBatch(reader.incarnation(), resourceLimit);
                var documents = archive == resourceLimit ? new DocumentReadRecovery.Batch(0, 0)
                        : new DocumentReadRecovery(tx).recoverResourcesBatch(reader.incarnation(), resourceLimit - archive);
                recovered.add(new Recovered(reader.incarnation(), archive, documents.documentPins(), documents.assessmentSessions()));
            } catch (RuntimeException failure) {
                failed.add(new Failed(reader.incarnation(), failure));
            }
        }
        check(control, failed);
        var page = new Page(recovered, failed, discovered.nextCursor());
        if (!failed.isEmpty()) throw new PageFailure(page);
        return page;
    }

    private void recoverQuiescence(UUID host, UUID receipt, ReaderHostDiscovery.Reader reader) {
        String state = tx.inTransaction(em -> {
            var rows = em.createNativeQuery("""
                    SELECT state FROM repository_reader_incarnations
                    WHERE incarnation=:reader AND registration_nonce=:nonce AND host_execution=:host
                    """).setParameter("reader", reader.incarnation()).setParameter("nonce", reader.registrationNonce())
                    .setParameter("host", host).getResultList();
            if (rows.size() != 1) throw new IllegalStateException("Reader recovery registration mismatch");
            return (String) rows.getFirst();
        });
        if ("QUIESCED".equals(state)) return; // Permanent; preserve the established provenance.
        if (!"ACTIVE".equals(state) && !"FENCED".equals(state))
            throw new IllegalStateException("Reader recovery state is unsupported: " + state);
        new ReaderExternalQuiescence(tx).quiesce(reader.incarnation(), reader.registrationNonce(), host, receipt);
    }

    private static void check(RepositoryOperationControl control, List<Failed> failed) {
        try { control.check(); }
        catch (RuntimeException stopped) {
            for (var failure : failed) if (failure.failure() != stopped) stopped.addSuppressed(failure.failure());
            throw stopped;
        }
    }
}
