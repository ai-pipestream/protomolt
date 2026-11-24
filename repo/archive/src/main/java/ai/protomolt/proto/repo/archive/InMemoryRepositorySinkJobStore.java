package ai.protomolt.proto.repo.archive;

import ai.protomolt.proto.repo.sink.v1.RepositorySinkJob;
import ai.protomolt.proto.repo.sink.v1.RepositorySinkJobState;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe in-memory {@link RepositorySinkJobStore} enforcing the exact conditional lease
 * fencing and cooperative cancellation semantics required of shared durable stores.
 */
public final class InMemoryRepositorySinkJobStore implements RepositorySinkJobStore {
    private record Key(String accountId, String operationId) {}

    private final ConcurrentHashMap<Key, StoredJob> jobs = new ConcurrentHashMap<>();

    /** Creates an empty in-memory job store. */
    public InMemoryRepositorySinkJobStore() {}

    @Override
    public Optional<StoredJob> find(String accountId, String operationId) {
        Objects.requireNonNull(accountId, "accountId");
        Objects.requireNonNull(operationId, "operationId");
        return Optional.ofNullable(jobs.get(new Key(accountId, operationId)));
    }

    @Override
    public StoredJob insertOrGet(StoredJob candidate) {
        Objects.requireNonNull(candidate, "candidate");
        Key key = new Key(candidate.job().getAccountId(), candidate.job().getOperationId());
        StoredJob existing = jobs.putIfAbsent(key, candidate);
        return existing != null ? existing : candidate;
    }

    @Override
    public Optional<LeaseRenewal> renewLease(
            String accountId,
            String operationId,
            String ownerId,
            long leaseEpoch,
            Instant newExpiresAt) {
        Objects.requireNonNull(accountId, "accountId");
        Objects.requireNonNull(operationId, "operationId");
        Objects.requireNonNull(ownerId, "ownerId");
        Objects.requireNonNull(newExpiresAt, "newExpiresAt");

        LeaseRenewal[] out = new LeaseRenewal[1];
        jobs.computeIfPresent(new Key(accountId, operationId), (k, current) -> {
            if (!current.lease().matches(ownerId, leaseEpoch)
                    || !isActiveExecutionState(current.job().getState())) {
                return current;
            }
            JobLease renewed = new JobLease(ownerId, leaseEpoch, newExpiresAt);
            out[0] = new LeaseRenewal(renewed, current.cancelRequested(), current.cancelReason());
            return new StoredJob(
                    current.job(),
                    current.request(),
                    current.pendingProvenance(),
                    renewed,
                    current.cancelRequested(),
                    current.cancelReason());
        });
        return Optional.ofNullable(out[0]);
    }

    @Override
    public PreCommitDecision prepareCommit(StoredJob preCommit, String ownerId, long leaseEpoch) {
        Objects.requireNonNull(preCommit, "preCommit");
        Objects.requireNonNull(ownerId, "ownerId");
        Key key = new Key(preCommit.job().getAccountId(), preCommit.job().getOperationId());

        PreCommitDecision[] out = new PreCommitDecision[1];
        jobs.compute(key, (k, current) -> {
            if (current == null) {
                out[0] = new PreCommitDecision(PreCommitStatus.LEASE_LOST, preCommit);
                return null;
            }
            if (!current.lease().matches(ownerId, leaseEpoch)) {
                out[0] = new PreCommitDecision(PreCommitStatus.LEASE_LOST, current);
                return current;
            }
            if (current.job().getState()
                    != RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_RUNNING) {
                out[0] = new PreCommitDecision(PreCommitStatus.LEASE_LOST, current);
                return current;
            }
            if (current.cancelRequested()) {
                out[0] = new PreCommitDecision(PreCommitStatus.CANCEL_REQUESTED, current);
                return current;
            }
            StoredJob next = new StoredJob(
                    preCommit.job(),
                    preCommit.request(),
                    preCommit.pendingProvenance(),
                    preCommit.lease(),
                    false,
                    "");
            out[0] = new PreCommitDecision(PreCommitStatus.READY, next);
            return next;
        });
        return out[0];
    }

    @Override
    public boolean transitionOwned(StoredJob updated, String ownerId, long leaseEpoch) {
        Objects.requireNonNull(updated, "updated");
        Objects.requireNonNull(ownerId, "ownerId");
        Key key = new Key(updated.job().getAccountId(), updated.job().getOperationId());

        boolean[] applied = {false};
        jobs.computeIfPresent(key, (k, current) -> {
            if (!current.lease().matches(ownerId, leaseEpoch)
                    || isTerminalState(current.job().getState())) {
                return current;
            }
            applied[0] = true;
            return new StoredJob(
                    updated.job(),
                    updated.request(),
                    updated.pendingProvenance(),
                    updated.lease(),
                    current.cancelRequested(),
                    current.cancelReason());
        });
        return applied[0];
    }

    @Override
    public boolean reconcileTerminalCommit(StoredJob expected, RepositorySinkJob committed) {
        Objects.requireNonNull(expected, "expected");
        Objects.requireNonNull(committed, "committed");
        if (committed.getState() != RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_SUCCEEDED) {
            throw new IllegalArgumentException("Reconciliation requires a committed success");
        }
        boolean[] applied = {false};
        String ownerId = expected.lease().ownerId();
        long leaseEpoch = expected.lease().leaseEpoch();
        jobs.computeIfPresent(new Key(expected.job().getAccountId(), expected.job().getOperationId()),
                (key, current) -> {
                    RepositorySinkJobState state = current.job().getState();
                    if (!current.lease().matches(ownerId, leaseEpoch)
                            || state != expected.job().getState()
                            || (state != RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_FAILED
                                    && state != RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_CANCELLED)) {
                        return current;
                    }
                    applied[0] = true;
                    return current.withTransition(
                            committed, committed.getProvenance(), current.lease().leaseExpiresAt());
                });
        return applied[0];
    }

    @Override
    public Optional<StoredJob> requestCancel(String accountId, String operationId, String reason) {
        Objects.requireNonNull(accountId, "accountId");
        Objects.requireNonNull(operationId, "operationId");
        String normalizedReason = reason == null ? "" : reason.trim();

        Key key = new Key(accountId, operationId);
        StoredJob result = jobs.computeIfPresent(key, (k, current) -> {
            if (!isActiveExecutionState(current.job().getState())) {
                return current;
            }
            String effectiveReason = current.cancelReason().isEmpty()
                    ? normalizedReason
                    : current.cancelReason();
            return new StoredJob(
                    current.job(),
                    current.request(),
                    current.pendingProvenance(),
                    current.lease(),
                    true,
                    effectiveReason);
        });
        return Optional.ofNullable(result);
    }

    @Override
    public Optional<StoredJob> tryClaimExpiredOrUnresolved(
            String accountId,
            String operationId,
            long expectedLeaseEpoch,
            String newOwnerId,
            Instant now,
            Instant newExpiresAt) {
        Objects.requireNonNull(accountId, "accountId");
        Objects.requireNonNull(operationId, "operationId");
        Objects.requireNonNull(newOwnerId, "newOwnerId");
        Objects.requireNonNull(now, "now");
        Objects.requireNonNull(newExpiresAt, "newExpiresAt");

        StoredJob[] claimed = new StoredJob[1];
        jobs.computeIfPresent(new Key(accountId, operationId), (k, current) -> {
            RepositorySinkJobState state = current.job().getState();
            if (current.lease().leaseEpoch() != expectedLeaseEpoch
                    || current.lease().isActiveAt(now)
                    || !isClaimableUnsettledState(state)) {
                return current;
            }
            JobLease nextLease = new JobLease(newOwnerId, expectedLeaseEpoch + 1L, newExpiresAt);
            StoredJob next = new StoredJob(
                    current.job(),
                    current.request(),
                    current.pendingProvenance(),
                    nextLease,
                    current.cancelRequested(),
                    current.cancelReason());
            claimed[0] = next;
            return next;
        });
        return Optional.ofNullable(claimed[0]);
    }

    @Override
    public Optional<StoredJob> tryClaimRetryableFailure(
            String accountId,
            String operationId,
            long expectedLeaseEpoch,
            RepositorySinkJob requeuedJob,
            String newOwnerId,
            Instant newExpiresAt) {
        Objects.requireNonNull(accountId, "accountId");
        Objects.requireNonNull(operationId, "operationId");
        Objects.requireNonNull(requeuedJob, "requeuedJob");
        Objects.requireNonNull(newOwnerId, "newOwnerId");
        Objects.requireNonNull(newExpiresAt, "newExpiresAt");

        StoredJob[] claimed = new StoredJob[1];
        jobs.computeIfPresent(new Key(accountId, operationId), (k, current) -> {
            if (current.lease().leaseEpoch() != expectedLeaseEpoch
                    || current.job().getState()
                            != RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_FAILED) {
                return current;
            }
            JobLease nextLease = new JobLease(newOwnerId, expectedLeaseEpoch + 1L, newExpiresAt);
            StoredJob next = new StoredJob(
                    requeuedJob,
                    current.request(),
                    null,
                    nextLease,
                    false,
                    "");
            claimed[0] = next;
            return next;
        });
        return Optional.ofNullable(claimed[0]);
    }

    private static boolean isActiveExecutionState(RepositorySinkJobState state) {
        return state == RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_QUEUED
                || state == RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_RUNNING;
    }

    private static boolean isClaimableUnsettledState(RepositorySinkJobState state) {
        return isActiveExecutionState(state)
                || state == RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_UNRESOLVED;
    }

    private static boolean isTerminalState(RepositorySinkJobState state) {
        return state == RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_SUCCEEDED
                || state == RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_FAILED
                || state == RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_CANCELLED;
    }
}
