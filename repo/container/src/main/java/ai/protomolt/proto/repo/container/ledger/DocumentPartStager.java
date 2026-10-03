package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.BackendIdentity;
import ai.protomolt.proto.repo.blob.spi.BlobCapability;
import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.blob.spi.OpenedBlobStore;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import ai.protomolt.proto.repo.codec.PartObject;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Internal staging only: no authorization, publication or reclamation. The caller
 * supplies an authorized complete plan and a host-qualified retained backend.
 * This remains package-private until recovery and the public writer are qualified.
 */
final class DocumentPartStager implements AutoCloseable {
    record Staged(DocumentPartAttemptLedger.Attempt attempt, List<DocumentPublicationLedger.Part> parts) {
        Staged { parts = List.copyOf(parts); }
    }

    static final class StageFailure extends RuntimeException {
        private final UUID attemptId;
        StageFailure(UUID attemptId, String phase, Throwable cause) {
            super("Document attempt " + attemptId + " failed during " + phase + "; storage outcome requires recovery", cause);
            this.attemptId = attemptId;
        }
        UUID attemptId() { return attemptId; }
    }

    private final DocumentPartAttemptLedger attempts;
    private final String generation;
    private final BlobStore store;
    private final java.util.concurrent.ScheduledExecutorService renewer;
    private final java.util.concurrent.Semaphore slots = new java.util.concurrent.Semaphore(32);
    private final java.util.concurrent.atomic.AtomicBoolean closed = new java.util.concurrent.atomic.AtomicBoolean();
    private final Object lifecycle = new Object();
    private final long maxBufferedBytes;
    private final java.util.concurrent.atomic.AtomicLong bufferedBytes = new java.util.concurrent.atomic.AtomicLong();

    /** The opened backend is borrowed. Capability flags do not qualify external retention policies. */
    DocumentPartStager(Tx tx, String generation, BackendIdentity identity, OpenedBlobStore opened) {
        this(tx, generation, identity, opened, 256L * 1024 * 1024);
    }

    /** The byte budget covers private payload copies across all concurrent stages. */
    DocumentPartStager(Tx tx, String generation, BackendIdentity identity, OpenedBlobStore opened, long maxBufferedBytes) {
        if (maxBufferedBytes <= 0) throw new IllegalArgumentException("Staging byte budget must be positive");
        this.maxBufferedBytes = maxBufferedBytes;
        Objects.requireNonNull(opened, "opened");
        if (!opened.capabilities().containsAll(Set.of(BlobCapability.NON_EXPIRING_WRITES, BlobCapability.PHYSICAL_RECLAMATION)))
            throw new IllegalArgumentException("Document staging requires non-expiring writes and exact physical reclamation");
        var profile = new ManagedBackendLedger(tx).find(generation)
                .orElseThrow(() -> new IllegalArgumentException("Document backend generation is not registered"));
        if (!profile.identity().equals(identity))
            throw new IllegalArgumentException("Document backend identity differs from retained profile");
        this.generation = generation;
        this.attempts = new DocumentPartAttemptLedger(tx);
        this.store = opened.store();
        this.renewer = Executors.newScheduledThreadPool(2, Thread.ofVirtual().name("document-part-lease", 0).factory());
    }

    Staged stage(DocumentPartAttemptLedger.Plan plan, List<PartObject> payloads, Duration lease, Map<String, String> metadata) {
        checkInterrupted();
        synchronized (lifecycle) {
            if (closed.get()) throw new IllegalStateException("Document stager is closed");
            if (!slots.tryAcquire()) throw new IllegalStateException("Concurrent document staging capacity exhausted");
        }
        long reserved = 0;
        try {
            if (payloads.size() != plan.objects().size()) throw new IllegalArgumentException("Payload count differs from plan");
            var stable = List.copyOf(payloads);
            long size = 0;
            for (var part : stable) size = Math.addExact(size, part.bytes().length);
            while (true) {
                long current = bufferedBytes.get();
                if (size > maxBufferedBytes - current) throw new IllegalStateException("Staging byte capacity exhausted");
                if (bufferedBytes.compareAndSet(current, current + size)) { reserved = size; break; }
            }
            return stageReserved(plan, stable, lease, metadata);
        } finally {
            bufferedBytes.addAndGet(-reserved);
            slots.release();
        }
    }

    private Staged stageReserved(DocumentPartAttemptLedger.Plan plan, List<PartObject> payloads, Duration lease, Map<String, String> metadata) {
        Objects.requireNonNull(plan, "plan");
        if (!generation.equals(plan.location().backendGeneration()))
            throw new IllegalArgumentException("Plan belongs to another backend generation");
        if (payloads.size() != plan.objects().size()) throw new IllegalArgumentException("Payload count differs from plan");
        var bodies = new ArrayList<byte[]>(payloads.size());
        for (int i = 0; i < payloads.size(); i++) {
            var part = payloads.get(i);
            var expected = plan.objects().get(i);
            byte[] body = part.bytes().clone();
            String measured = DocumentPartCodec.sha256Hex(body);
            if (part.part() != expected.part() || !part.subKey().equals(expected.subKey())
                    || body.length != expected.size() || !measured.equals(expected.sha256()) || !measured.equals(part.sha256()))
                throw new IllegalArgumentException("Payload differs from ordered document plan");
            bodies.add(body);
        }
        var attributes = Map.copyOf(metadata);
        checkInterrupted();
        DocumentPartAttemptLedger.Attempt attempt;
        try { attempt = attempts.begin(plan, lease); }
        catch (RuntimeException failure) { throw new StageFailure(plan.attemptId(), "admission", failure); }
        var renewalFailure = new AtomicReference<Throwable>();
        java.util.concurrent.ScheduledFuture<?> heartbeat = null;
        String phase = "lease renewal setup";
        try {
            long interval = Math.max(1, lease.toMillis() / 3);
            var owner = attempt;
            heartbeat = renewer.scheduleWithFixedDelay(() -> {
                if (renewalFailure.get() != null) return;
                try { attempts.renew(owner.id(), owner.token(), lease); }
                catch (Throwable failure) { renewalFailure.compareAndSet(null, failure); }
            }, interval, interval, TimeUnit.MILLISECONDS);
            var verified = new ArrayList<DocumentPublicationLedger.Part>();
            for (int i = 0; i < plan.objects().size(); i++) {
                var expected = plan.objects().get(i);
                checkActive(renewalFailure);
                phase = "lease check";
                attempts.renew(attempt.id(), attempt.token(), lease);
                phase = "PUT";
                var put = store.put(new BlobStore.PutSpec(plan.location().namespace(), expected.objectKey(),
                        expected.contentType(), attributes, expected.sha256()), bodies.get(i));
                checkActive(renewalFailure);
                if (put == null) throw new IllegalStateException("Provider did not return a PUT receipt");
                // The same live token must survive every provider call. Never reacquire an expired lease.
                attempts.renew(attempt.id(), attempt.token(), lease);
                phase = "read-back verification";
                var actual = store.get(plan.location().namespace(), expected.objectKey(), put.versionId());
                checkActive(renewalFailure);
                if (actual == null || actual.data() == null || actual.data().length != expected.size()
                        || !DocumentPartCodec.sha256Hex(actual.data()).equals(expected.sha256())
                        || !Objects.equals(expected.contentType(), actual.contentType())
                        || !Objects.equals(put.versionId(), actual.versionId()) || !Objects.equals(put.eTag(), actual.eTag()))
                    throw new IllegalStateException("Read-back differs from planned bytes or PUT identity");
                phase = "verification record";
                attempt = attempts.verify(attempt.id(), attempt.token(), expected.objectKey(), actual.data().length,
                        DocumentPartCodec.sha256Hex(actual.data()), actual.versionId(), actual.eTag());
                verified.add(new DocumentPublicationLedger.Part(expected.part(), expected.subKey(), expected.objectKey(),
                        expected.size(), expected.sha256(), actual.versionId(), actual.eTag()));
            }
            checkActive(renewalFailure);
            phase = "final lease check";
            attempt = attempts.renew(attempt.id(), attempt.token(), lease);
            checkActive(renewalFailure);
            if (!attempt.state().equals("VERIFIED")) throw new IllegalStateException("Document attempt is not fully verified");
            return new Staged(attempt, verified);
        } catch (RuntimeException failure) {
            var reported = new StageFailure(attempt.id(), phase, failure);
            var renewal = renewalFailure.get();
            if (renewal != null && renewal != failure) reported.addSuppressed(renewal);
            throw reported;
        } finally {
            // Cancellation does not prove a database/provider call stopped; its durable token remains authoritative.
            if (heartbeat != null) heartbeat.cancel(true);
        }
    }

    private static void checkInterrupted() {
        if (Thread.currentThread().isInterrupted()) throw new java.util.concurrent.CancellationException("Document staging interrupted");
    }

    private void checkActive(AtomicReference<Throwable> renewalFailure) {
        checkInterrupted();
        if (closed.get()) throw new java.util.concurrent.CancellationException("Document stager closed during staging");
        var failure = renewalFailure.get();
        if (failure instanceof Error error) throw error;
        if (failure instanceof RuntimeException runtime) throw runtime;
        if (failure != null) throw new IllegalStateException("Document lease renewal failed", failure);
    }

    @Override public void close() {
        synchronized (lifecycle) { closed.set(true); }
        renewer.shutdownNow();
    }

    /**
     * After close, wait for provider calls and renewal tasks to finish before
     * closing the borrowed store or database. False means resources are still in use.
     */
    boolean awaitIdle(Duration timeout) throws InterruptedException {
        if (!closed.get()) throw new IllegalStateException("Close the stager before awaiting idle");
        if (timeout.isNegative()) throw new IllegalArgumentException("Idle timeout must not be negative");
        long budget = timeout.toNanos();
        long started = System.nanoTime();
        if (!slots.tryAcquire(32, budget, TimeUnit.NANOSECONDS)) return false;
        slots.release(32);
        return renewer.awaitTermination(Math.max(0, budget - (System.nanoTime() - started)), TimeUnit.NANOSECONDS);
    }
}
