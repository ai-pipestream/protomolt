package ai.protomolt.proto.repo.archive;

import ai.protomolt.proto.repo.sink.v1.PublishRepositorySinkRequest;
import ai.protomolt.proto.repo.sink.v1.RenditionProvenance;
import ai.protomolt.proto.repo.sink.v1.RepositorySinkJob;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * Persistence contract for durable {@link RepositorySinkJob} records keyed by
 * {@code (account_id, operation_id)} with monotonic lease fencing and cooperative cross-instance
 * cancellation.
 *
 * <p>Implementations must execute short, bounded reads and conditional state transitions without
 * holding database transactions open across source reads, renderer execution, or streamed uploads.
 * Unconditional overwrites are intentionally absent so a stale worker or a peer host polling an
 * active job can never clobber a live lease owner or a settled terminal receipt.
 */
public interface RepositorySinkJobStore {

    /**
     * Durable ownership lease and monotonic fencing token for a sink job record.
     *
     * @param ownerId unique identifier of the {@link RepositorySink} instance holding the lease
     * @param leaseEpoch strictly positive, monotonically increasing fencing epoch
     * @param leaseExpiresAt instant after which another instance may fence and reconcile an
     *     unsettled job
     */
    record JobLease(String ownerId, long leaseEpoch, Instant leaseExpiresAt) {
        /** Validates lease invariants. */
        public JobLease {
            Objects.requireNonNull(ownerId, "ownerId");
            if (ownerId.isBlank()) {
                throw new IllegalArgumentException("ownerId must not be blank");
            }
            if (leaseEpoch < 1L) {
                throw new IllegalArgumentException("leaseEpoch must be >= 1");
            }
            Objects.requireNonNull(leaseExpiresAt, "leaseExpiresAt");
        }

        /**
         * Returns {@code true} when this lease has not yet expired at {@code now}.
         *
         * @param now current reference instant
         * @return {@code true} if {@code leaseExpiresAt.isAfter(now)}
         */
        public boolean isActiveAt(Instant now) {
            return leaseExpiresAt.isAfter(Objects.requireNonNull(now, "now"));
        }

        /**
         * Returns {@code true} when this lease matches {@code (expectedOwnerId, expectedEpoch)}.
         *
         * @param expectedOwnerId expected owner instance identifier
         * @param expectedEpoch expected monotonic lease epoch
         * @return {@code true} when both owner and epoch match
         */
        public boolean matches(String expectedOwnerId, long expectedEpoch) {
            return ownerId.equals(expectedOwnerId) && leaseEpoch == expectedEpoch;
        }
    }

    /**
     * Outcome of a background or pre-commit lease renewal by the current owner.
     *
     * @param lease renewed lease with updated expiration
     * @param cancelRequested {@code true} if any instance has durably requested cancellation
     * @param cancelReason caller-supplied cancellation reason when {@code cancelRequested} is true
     */
    record LeaseRenewal(JobLease lease, boolean cancelRequested, String cancelReason) {
        /** Normalizes nullable reason strings. */
        public LeaseRenewal {
            Objects.requireNonNull(lease, "lease");
            cancelReason = cancelReason == null ? "" : cancelReason;
        }
    }

    /** Verdict of the atomic pre-commit lease and cancellation fence. */
    enum PreCommitStatus {
        /** Lease is still held by the caller and no cancellation has been requested. */
        READY,
        /** Lease is still held by the caller, and durable cancellation was requested. */
        CANCEL_REQUESTED,
        /** Lease was fenced by another instance or the job already reached a terminal state. */
        LEASE_LOST
    }

    /**
     * Result of attempting to persist pre-commit provenance and verify durable lease authority
     * immediately before committing bytes to the repository.
     *
     * @param status pre-commit fence verdict
     * @param stored current stored job record corresponding to {@code status}
     */
    record PreCommitDecision(PreCommitStatus status, StoredJob stored) {
        /** Validates required fields. */
        public PreCommitDecision {
            Objects.requireNonNull(status, "status");
            Objects.requireNonNull(stored, "stored");
        }
    }

    /**
     * Durable job record bundling the public {@link RepositorySinkJob} state, the admitted
     * {@link PublishRepositorySinkRequest}, optional pre-commit {@link RenditionProvenance}, the
     * current {@link JobLease} fencing token, and durable cooperative cancellation state.
     *
     * @param job current job state and receipt
     * @param request admitted publication request with frozen source revision
     * @param pendingProvenance provenance prepared immediately prior to repository commit, or null
     * @param lease current ownership lease and fencing epoch
     * @param cancelRequested {@code true} when cooperative cancellation has been requested
     * @param cancelReason caller-supplied cancellation reason when {@code cancelRequested} is true
     */
    record StoredJob(
            RepositorySinkJob job,
            PublishRepositorySinkRequest request,
            RenditionProvenance pendingProvenance,
            JobLease lease,
            boolean cancelRequested,
            String cancelReason) {
        private static final JobLease UNOWNED_LEASE = new JobLease("unowned", 1L, Instant.EPOCH);

        /** Validates required record fields. */
        public StoredJob {
            Objects.requireNonNull(job, "job");
            Objects.requireNonNull(request, "request");
            Objects.requireNonNull(lease, "lease");
            cancelReason = cancelReason == null ? "" : cancelReason;
        }

        /**
         * Convenience constructor creating a record with a default expired unowned lease.
         *
         * @param job current job state and receipt
         * @param request admitted publication request
         * @param pendingProvenance optional pre-commit provenance
         */
        public StoredJob(
                RepositorySinkJob job,
                PublishRepositorySinkRequest request,
                RenditionProvenance pendingProvenance) {
            this(job, request, pendingProvenance, UNOWNED_LEASE, false, "");
        }

        /**
         * Produces a copy preserving the current owner, lease epoch, and cancellation flags while
         * updating the job snapshot, pending provenance, and lease expiration.
         *
         * @param nextJob updated job snapshot
         * @param nextPendingProvenance updated pre-commit provenance, or null
         * @param nextExpiresAt updated lease expiration instant
         * @return updated record carrying the same {@code (ownerId, leaseEpoch)}
         */
        public StoredJob withTransition(
                RepositorySinkJob nextJob,
                RenditionProvenance nextPendingProvenance,
                Instant nextExpiresAt) {
            return new StoredJob(
                    nextJob,
                    request,
                    nextPendingProvenance,
                    new JobLease(lease.ownerId(), lease.leaseEpoch(), nextExpiresAt),
                    cancelRequested,
                    cancelReason);
        }
    }

    /**
     * Looks up a persisted sink job by owning account and operation ID.
     *
     * @param accountId owning account identifier
     * @param operationId sink operation identifier
     * @return the persisted job record when present
     */
    Optional<StoredJob> find(String accountId, String operationId);

    /**
     * Atomically inserts {@code candidate} if no row exists for
     * {@code (candidate.job.account_id, candidate.job.operation_id)}, or returns the existing row
     * unchanged when one is already recorded.
     *
     * @param candidate newly admitted job record carrying its initial {@link JobLease}
     * @return {@code candidate} when newly inserted, or the pre-existing record for the same key
     */
    StoredJob insertOrGet(StoredJob candidate);

    /**
     * Atomically extends {@code lease_expires_at} for an active {@code QUEUED} or {@code RUNNING}
     * job iff the row is still owned by {@code (ownerId, leaseEpoch)}.
     *
     * @param accountId owning account identifier
     * @param operationId sink operation identifier
     * @param ownerId expected lease owner identifier
     * @param leaseEpoch expected monotonic lease epoch
     * @param newExpiresAt extended lease expiration instant
     * @return renewed lease plus durable cancellation state, or empty if fenced or terminal
     */
    Optional<LeaseRenewal> renewLease(
            String accountId,
            String operationId,
            String ownerId,
            long leaseEpoch,
            Instant newExpiresAt);

    /**
     * Atomically persists pre-commit job metadata and {@code pendingProvenance} immediately prior
     * to repository commit iff {@code (ownerId, leaseEpoch)} still owns the {@code RUNNING} row and
     * {@code cancelRequested} is {@code false}.
     *
     * @param preCommit pre-commit job record with updated lease expiration
     * @param ownerId expected lease owner identifier
     * @param leaseEpoch expected monotonic lease epoch
     * @return pre-commit decision indicating {@code READY}, {@code CANCEL_REQUESTED}, or
     *     {@code LEASE_LOST}
     */
    PreCommitDecision prepareCommit(StoredJob preCommit, String ownerId, long leaseEpoch);

    /**
     * Conditionally persists a state transition iff the row is still owned by
     * {@code (ownerId, leaseEpoch)} and has not yet settled into a terminal state
     * ({@code SUCCEEDED}, {@code FAILED}, or {@code CANCELLED}).
     *
     * @param updated updated job record
     * @param ownerId expected lease owner identifier
     * @param leaseEpoch expected monotonic lease epoch
     * @return {@code true} if the conditional update succeeded, {@code false} if fenced or terminal
     */
    boolean transitionOwned(StoredJob updated, String ownerId, long leaseEpoch);

    /**
     * Durably marks {@code cancel_requested = true} on a {@code QUEUED} or {@code RUNNING} job
     * without overwriting {@code state} or stealing an active owner's lease.
     *
     * @param accountId owning account identifier
     * @param operationId sink operation identifier
     * @param reason human-readable cancellation reason
     * @return the current stored job record after recording the request, or empty if absent
     */
    Optional<StoredJob> requestCancel(String accountId, String operationId, String reason);

    /**
     * Atomically fences and claims ownership ({@code lease_epoch = expectedLeaseEpoch + 1}) of an
     * unsettled ({@code QUEUED}, {@code RUNNING}, or {@code UNRESOLVED}) job whose lease has
     * expired ({@code lease_expires_at <= now}).
     *
     * @param accountId owning account identifier
     * @param operationId sink operation identifier
     * @param expectedLeaseEpoch lease epoch observed on the expired row
     * @param newOwnerId claiming instance identifier
     * @param now current reference instant
     * @param newExpiresAt expiration instant for the newly claimed lease
     * @return the newly fenced {@link StoredJob} when won, or empty if another instance claimed it
     *     or the lease is still active
     */
    Optional<StoredJob> tryClaimExpiredOrUnresolved(
            String accountId,
            String operationId,
            long expectedLeaseEpoch,
            String newOwnerId,
            Instant now,
            Instant newExpiresAt);

    /**
     * Atomically claims a retry lease ({@code lease_epoch = expectedLeaseEpoch + 1}) on a
     * {@code FAILED} job, resetting {@code state} to {@code QUEUED} and clearing any prior
     * cancellation flag.
     *
     * @param accountId owning account identifier
     * @param operationId sink operation identifier
     * @param expectedLeaseEpoch lease epoch observed on the failed row
     * @param requeuedJob job snapshot reset to {@code QUEUED}
     * @param newOwnerId claiming instance identifier
     * @param newExpiresAt expiration instant for the retry lease
     * @return the newly fenced {@link StoredJob} when won, or empty if another caller raced
     */
    Optional<StoredJob> tryClaimRetryableFailure(
            String accountId,
            String operationId,
            long expectedLeaseEpoch,
            RepositorySinkJob requeuedJob,
            String newOwnerId,
            Instant newExpiresAt);
}
