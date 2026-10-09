package ai.protomolt.proto.repo.engine;

import ai.protomolt.proto.repo.blob.spi.ObjectReclaimer;
import ai.protomolt.proto.repo.container.archive.ArchiveCleanupLedger;
import ai.protomolt.proto.repo.container.ledger.ManagedBackendLedger;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/** Reclaims only admitted, unreferenced archive objects at their original location. */
public final class ArchiveObjectRecovery {
    @FunctionalInterface
    public interface BackendResolver {
        /** Resolve the exact persisted profile. The host owns the returned client. */
        ObjectReclaimer resolve(String generation, ManagedBackendLedger.Profile profile);
    }
    public enum Outcome { SKIPPED, RECLAIMED, RETRY_REQUIRED, SUPERSEDED }
    private final ArchiveCleanupLedger cleanup;
    private final ManagedBackendLedger profiles;
    private final BackendResolver resolver;

    public ArchiveObjectRecovery(ArchiveCleanupLedger cleanup, ManagedBackendLedger profiles, BackendResolver resolver) {
        this.cleanup = Objects.requireNonNull(cleanup);
        this.profiles = Objects.requireNonNull(profiles);
        this.resolver = Objects.requireNonNull(resolver);
    }

    public Outcome recover(UUID object, Instant inactiveBefore) {
        return recover(object, inactiveBefore, inactiveBefore);
    }

    public Outcome recover(UUID object, Instant inactiveBefore, Instant abandonedBefore) {
        if (Thread.currentThread().isInterrupted())
            throw new IllegalStateException("Archive recovery interrupted before claiming work");
        var claimed = cleanup.claim(object, inactiveBefore, abandonedBefore);
        if (claimed.isEmpty()) return Outcome.SKIPPED;
        var claim = claimed.get();
        var location = claim.binding().location();
        boolean absent;
        try {
            var profile = profiles.find(location.backendGeneration()).orElseThrow(() ->
                    new IllegalStateException("Original archive backend profile is unavailable"));
            if (!profile.storageRealm().equals(claim.binding().storageRealm()))
                throw new IllegalStateException("Original archive backend realm differs from its binding");
            var reclaimer = Objects.requireNonNull(resolver.resolve(location.backendGeneration(), profile),
                    "Original archive backend reclaimer is unavailable");
            absent = reclaimer.reclaim(location.bucket(), location.objectKey());
        } catch (RuntimeException failure) {
            try { cleanup.failed(object, claim.token(), "BACKEND_RECLAMATION_FAILED"); }
            catch (RuntimeException recording) { failure.addSuppressed(recording); }
            throw new IllegalStateException("Archive object reclamation failed; retry remains required", failure);
        }
        boolean current = absent ? cleanup.succeeded(object, claim.token())
                : cleanup.failed(object, claim.token(), "RECLAMATION_INCOMPLETE");
        if (!current) return Outcome.SUPERSEDED;
        return absent ? Outcome.RECLAIMED : Outcome.RETRY_REQUIRED;
    }
}
