package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.BackendIdentity;
import ai.protomolt.proto.repo.blob.spi.OpenedBlobStore;
import ai.protomolt.proto.repo.codec.PartObject;
import ai.protomolt.proto.repo.v1.NodeAddress;
import jakarta.persistence.EntityManager;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * Stages an authorized plan and atomically publishes its verified parts.
 * Authorization, schema admission and backend qualification belong to the host.
 * The store and database are borrowed: close this writer and await idle before
 * releasing either. A timeout means those resources must remain open.
 */
public final class DocumentAttemptWriter implements AutoCloseable {
    /** An attempt may require recovery or commit-outcome reconciliation; never retry blindly. */
    public static final class WriteFailure extends RuntimeException {
        private final UUID attemptId;
        private final String phase;

        private WriteFailure(UUID attemptId, String phase, Throwable cause) {
            super("Document attempt " + attemptId + " failed during " + phase + "; reconcile its outcome", cause);
            this.attemptId = attemptId;
            this.phase = phase;
        }

        public UUID attemptId() { return attemptId; }
        public String phase() { return phase; }
    }

    private static final int CAPACITY = 32;
    private final Tx tx;
    private final DriveLedger drives;
    private final String generation;
    private final BackendIdentity identity;
    private final DocumentPartStager stager;
    private final DocumentLedger documents;
    private final Semaphore operations = new Semaphore(CAPACITY);
    private final Object lifecycle = new Object();
    private volatile boolean closed;

    public DocumentAttemptWriter(Tx tx, DriveLedger drives, String generation,
            BackendIdentity identity, OpenedBlobStore opened) {
        this.tx = Objects.requireNonNull(tx);
        this.drives = Objects.requireNonNull(drives);
        this.generation = Objects.requireNonNull(generation);
        this.identity = Objects.requireNonNull(identity);
        this.documents = new DocumentLedger(tx);
        this.stager = new DocumentPartStager(tx, generation, identity, opened);
    }

    /**
     * The candidate factory must be pure, perform no provider I/O, and use the supplied verified identities.
     * The publication callback runs inside the SQL transaction: only transactional
     * raw-reference/outbox work using the supplied EntityManager belongs there,
     * never remote I/O or an independently committed transaction. The cancellation
     * check must be thread-safe and nonblocking; staging workers may call it
     * concurrently. No cancellation check follows a successful commit.
     */
    public DocumentRecord write(DocumentPartAttemptLedger.Plan plan, NodeAddress address, DriveRecord drive,
            List<PartObject> payloads, Duration lease, Map<String, String> metadata,
            Function<List<DocumentPublicationLedger.Part>, DocumentRecord> candidateFactory,
            Runnable check, BiConsumer<EntityManager, DocumentRecord> published) {
        return write(plan, address, drive, payloads, lease, metadata, List.of(), candidateFactory, check, published);
    }

    /** Every plan source requires a snapshot sampled from its authorized row before reading source bytes. */
    public DocumentRecord write(DocumentPartAttemptLedger.Plan plan, NodeAddress address, DriveRecord drive,
            List<PartObject> payloads, Duration lease, Map<String, String> metadata, List<DocumentSourceSnapshot> sourceSnapshots,
            Function<List<DocumentPublicationLedger.Part>, DocumentRecord> candidateFactory,
            Runnable check, BiConsumer<EntityManager, DocumentRecord> published) {
        Objects.requireNonNull(plan);
        Objects.requireNonNull(address);
        Objects.requireNonNull(candidateFactory);
        Objects.requireNonNull(check);
        Objects.requireNonNull(published);
        var sources = DocumentSourceSnapshot.matching(plan.sources(), sourceSnapshots);
        var target = new DocumentPublicationTarget(drives, drive, generation, identity);
        synchronized (lifecycle) {
            if (closed) throw new IllegalStateException("Document writer is closed");
            if (!operations.tryAcquire()) throw new IllegalStateException("Concurrent document write capacity exhausted");
        }
        Runnable active = () -> {
            check.run();
            if (closed || Thread.currentThread().isInterrupted())
                throw new CancellationException("Document writer stopped during write");
        };
        try {
            active.run();
            tx.inTransaction(em -> {
                for (var source : sources) source.requireCurrent(em);
                DocumentSourceSnapshot.lockDrives(em, target, sources);
                target.requirePlan(em, plan, address);
            });
            active.run();
            String phase = "staging";
            try {
                var staged = stager.stage(plan, payloads, lease, metadata, active);
                phase = "candidate construction";
                active.run();
                var candidate = Objects.requireNonNull(candidateFactory.apply(staged.parts()), "candidate");
                if (!plan.location().nodeId().equals(candidate.nodeId)
                        || !address.equals(candidate.readManifest().getAddress()))
                    throw new IllegalArgumentException("Candidate differs from authorized address");
                phase = "publication";
                return documents.saveVerifiedAttempt(candidate, plan.sampledRevision() == 0 ? null : plan.sampledRevision(),
                        plan.sources(), staged.attempt().id(), staged.attempt().token(), target, sources, active, published);
            } catch (RuntimeException failure) {
                throw new WriteFailure(plan.attemptId(), phase, failure);
            }
        } finally {
            operations.release();
        }
    }

    @Override public void close() {
        synchronized (lifecycle) { closed = true; }
        stager.close();
    }

    /** Includes candidate construction and publication, not just active provider calls. */
    public boolean awaitIdle(Duration timeout) throws InterruptedException {
        if (!closed) throw new IllegalStateException("Close the writer before awaiting idle");
        if (timeout.isNegative()) throw new IllegalArgumentException("Idle timeout must not be negative");
        long budget = timeout.toNanos();
        long started = System.nanoTime();
        if (!operations.tryAcquire(CAPACITY, budget, TimeUnit.NANOSECONDS)) return false;
        operations.release(CAPACITY);
        return stager.awaitIdle(Duration.ofNanos(Math.max(0, budget - (System.nanoTime() - started))));
    }
}
