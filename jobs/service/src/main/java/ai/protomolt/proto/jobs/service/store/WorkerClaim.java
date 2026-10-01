package ai.protomolt.proto.jobs.service.store;

import java.util.Objects;
import java.util.UUID;

/** Immutable identity of one worker attempt, captured when the job is claimed. */
public record WorkerClaim(UUID jobId, String leaseOwner, int attempt) {
    public WorkerClaim {
        Objects.requireNonNull(jobId, "jobId");
        if (leaseOwner == null || leaseOwner.isBlank()) {
            throw new IllegalArgumentException("claim owner is required");
        }
        if (attempt < 1) throw new IllegalArgumentException("claim attempt must be positive");
    }

    /** Snapshot the returned claim row; never reload a newer claim to authorize an old attempt. */
    public static WorkerClaim from(WorkflowRunRecord claimed) {
        Objects.requireNonNull(claimed, "claimed");
        return new WorkerClaim(claimed.jobId, claimed.leaseOwner, claimed.attempt);
    }
}
