package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.ObjectReclaimer;
import java.time.Duration;
import java.util.Objects;
import java.util.UUID;

/** Internal exact-key recovery. No current-drive fallback and no published-history pruning. */
final class DocumentAttemptRecovery {
    @FunctionalInterface interface BackendResolver {
        /** Nonblocking lookup of a borrowed original backend; the host owns its lifetime. */
        ObjectReclaimer resolve(String generation, ManagedBackendLedger.Profile profile);
    }
    enum Outcome { NOT_CLAIMED, ABSENT, RETRY, LOST_CLAIM }
    record Result(UUID attemptId, Outcome outcome, Throwable failure) {}

    private final DocumentAttemptCleanupLedger cleanup;
    private final BackendResolver backends;
    DocumentAttemptRecovery(DocumentAttemptCleanupLedger cleanup, BackendResolver backends) {
        this.cleanup = Objects.requireNonNull(cleanup);
        this.backends = Objects.requireNonNull(backends);
    }

    Result recover(UUID id, Duration lease) {
        var admitted = cleanup.claim(id, lease);
        if (admitted.isEmpty()) return new Result(id, Outcome.NOT_CLAIMED, null);
        var claim = admitted.orElseThrow();
        try {
            var reclaimer = backends.resolve(claim.generation(), claim.profile());
            if (reclaimer == null) throw new IllegalStateException("Original document cleanup backend is unavailable");
            boolean absent = true;
            for (String key : claim.keys()) {
                if (Thread.currentThread().isInterrupted()) throw new java.util.concurrent.CancellationException("Document cleanup interrupted");
                if (!cleanup.renew(claim, lease)) return new Result(id, Outcome.LOST_CLAIM, null);
                // Every call uses an immutable exact admitted key, including unverified PUT outcomes.
                boolean observed = reclaimer.reclaim(claim.namespace(), key);
                absent = observed && absent;
            }
            boolean recorded = cleanup.finish(claim, absent, absent ? null : "Physical absence was not confirmed");
            return new Result(id, !recorded ? Outcome.LOST_CLAIM : absent ? Outcome.ABSENT : Outcome.RETRY, null);
        } catch (RuntimeException failure) {
            boolean recorded;
            try {
                // Persist only a bounded diagnostic class, not provider messages that may contain credentials.
                recorded = cleanup.finish(claim, false, "Cleanup failed: " + failure.getClass().getSimpleName());
            } catch (RuntimeException recordingFailure) {
                if (recordingFailure != failure) recordingFailure.addSuppressed(failure);
                throw recordingFailure;
            }
            return new Result(id, recorded ? Outcome.RETRY : Outcome.LOST_CLAIM, failure);
        }
    }
}
