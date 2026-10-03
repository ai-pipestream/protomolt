package ai.protomolt.proto.repo.engine;

import ai.protomolt.proto.repo.blob.spi.ObjectReclaimer;
import ai.protomolt.proto.repo.container.ledger.ManagedBackendLedger;
import ai.protomolt.proto.repo.container.ledger.RawObjectLedger;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/** Durable cleanup of unreferenced managed bytes; provider I/O runs outside SQL transactions. */
public final class RawObjectRecovery {
    /** Resolves the exact historical profile; never substitute the current backend. Host owns lifetimes. */
    @FunctionalInterface
    public interface BackendResolver {
        ObjectReclaimer resolve(String generation, ManagedBackendLedger.Profile profile);
    }

    public enum Outcome { SKIPPED, RECLAIMED, RETRY_REQUIRED, SUPERSEDED }

    private final RawObjectLedger raw;
    private final ManagedBackendLedger backends;
    private final BackendResolver resolver;

    public RawObjectRecovery(RawObjectLedger raw, ManagedBackendLedger backends, BackendResolver resolver) {
        this.raw = Objects.requireNonNull(raw);
        this.backends = Objects.requireNonNull(backends);
        this.resolver = Objects.requireNonNull(resolver);
    }

    /**
     * Claims an eligible record and reclaims its original coordinates. A false
     * provider result remains retryable. Exceptions are recorded with a bounded
     * diagnostic code and propagated with their cause, never interpreted as success.
     */
    public Outcome recover(UUID rawId, Instant inactiveBefore) {
        if (Thread.currentThread().isInterrupted())
            throw new IllegalStateException("Raw recovery interrupted before claiming work");
        var claimed = raw.claimCleanup(rawId, inactiveBefore);
        if (claimed.isEmpty()) return Outcome.SKIPPED;
        var attempt = claimed.get();
        boolean absent;
        try {
            var profile = backends.find(attempt.backendIdentity).orElseThrow(() ->
                    new IllegalStateException("Original managed backend profile is unavailable"));
            var reclaimer = Objects.requireNonNull(resolver.resolve(attempt.backendIdentity, profile),
                    "Original managed backend reclaimer is unavailable");
            absent = reclaimer.reclaim(attempt.bucket, attempt.objectKey);
        } catch (RuntimeException failure) {
            try { raw.cleanupFailed(rawId, attempt.cleanupToken, "BACKEND_RECLAMATION_FAILED"); }
            catch (RuntimeException recordingFailure) { failure.addSuppressed(recordingFailure); }
            throw new IllegalStateException("Managed raw reclamation failed; retry remains required", failure);
        }
        boolean current = absent ? raw.cleanupSucceeded(rawId, attempt.cleanupToken)
                : raw.cleanupFailed(rawId, attempt.cleanupToken, "RECLAMATION_INCOMPLETE");
        if (!current) return Outcome.SUPERSEDED;
        return absent ? Outcome.RECLAIMED : Outcome.RETRY_REQUIRED;
    }
}
