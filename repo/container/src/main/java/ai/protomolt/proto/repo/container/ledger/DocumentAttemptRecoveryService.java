package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.BlobCapability;
import ai.protomolt.proto.repo.blob.spi.ObjectReclaimer;
import ai.protomolt.proto.repo.blob.spi.OpenedBlobStore;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/** Bounded reconciliation of never-published attempts on one explicitly configured backend generation. */
public final class DocumentAttemptRecoveryService {
    public enum Outcome { NOT_CLAIMED, ABSENT, RETRY, LOST_CLAIM }
    /** ABSENT is an observation; the durable tombstone remains eligible for later reconciliation. */
    public record Result(UUID attemptId, Outcome outcome, Throwable failure) {}

    private final String generation;
    private final DocumentAttemptCleanupLedger cleanup;
    private final DocumentAttemptRecovery recovery;

    /**
     * Borrows the qualified backing handle. The host must retain it and the database
     * until every pass returns. The effective reclaimer may additionally evict a cache;
     * it must address this same backing handle, never a current-drive substitute.
     */
    public DocumentAttemptRecoveryService(Tx tx, String generation, ManagedBackendLedger.Profile profile,
            OpenedBlobStore backing, ObjectReclaimer effectiveReclaimer) {
        this.generation = Objects.requireNonNull(generation);
        Objects.requireNonNull(backing);
        Objects.requireNonNull(effectiveReclaimer);
        if (!backing.capabilities().containsAll(Set.of(BlobCapability.NON_EXPIRING_WRITES, BlobCapability.PHYSICAL_RECLAMATION)))
            throw new IllegalArgumentException("Document recovery requires non-expiring storage and physical reclamation");
        backing.reclaimer(); // Reject an already closed handle before installing the borrowed composition.
        if (!new ManagedBackendLedger(tx).find(generation).filter(Objects.requireNonNull(profile)::equals).isPresent())
            throw new IllegalArgumentException("Original document backend profile is not bound");
        cleanup = new DocumentAttemptCleanupLedger(tx);
        recovery = new DocumentAttemptRecovery(cleanup, (original, retained) -> {
            if (!generation.equals(original) || !profile.equals(retained))
                throw new IllegalStateException("Original document backend is not configured on this host");
            backing.reclaimer(); // Do not use the effective port after its backing lifetime closes.
            return effectiveReclaimer;
        });
    }

    /** Scans only this generation before applying the limit; reports original failures without hiding them. */
    public List<Result> reconcilePass(Duration recheckDelay, Duration claimLease, int limit) {
        Objects.requireNonNull(claimLease);
        if (claimLease.compareTo(Duration.ofSeconds(1)) < 0 || claimLease.compareTo(Duration.ofDays(1)) > 0)
            throw new IllegalArgumentException("Cleanup lease must be between one second and one day");
        var results = new ArrayList<Result>();
        for (UUID id : cleanup.candidates(recheckDelay, limit, generation)) {
            if (Thread.currentThread().isInterrupted()) break;
            try {
                var result = recovery.recover(id, claimLease);
                Outcome outcome = switch (result.outcome()) {
                    case NOT_CLAIMED -> Outcome.NOT_CLAIMED;
                    case ABSENT -> Outcome.ABSENT;
                    case RETRY -> Outcome.RETRY;
                    case LOST_CLAIM -> Outcome.LOST_CLAIM;
                };
                results.add(new Result(id, outcome, result.failure()));
            } catch (RuntimeException failure) {
                results.add(new Result(id, Outcome.RETRY, failure));
            }
        }
        return List.copyOf(results);
    }
}
