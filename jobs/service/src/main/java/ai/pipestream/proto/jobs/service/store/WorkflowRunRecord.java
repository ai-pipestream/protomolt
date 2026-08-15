package ai.pipestream.proto.jobs.service.store;

import java.time.Instant;
import java.util.UUID;

/**
 * One {@code workflow_run} row, detached. Public fields, no behavior — same
 * convention as the account module's store record. The JSONB columns ride as
 * their raw JSON text ({@code workflowDefinition}, {@code input},
 * {@code checkpoints}, {@code result}); Jackson work happens at the edges
 * (the worker, the actions), not in the row carrier.
 */
public final class WorkflowRunRecord {

    /** Claimable once {@code run_after} passes. */
    public static final String STATUS_QUEUED = "QUEUED";
    /** A worker holds the lease and is executing a segment. */
    public static final String STATUS_RUNNING = "RUNNING";
    /** Parked on an external-completion step, awaiting complete-step. */
    public static final String STATUS_WAITING = "WAITING";
    /** Terminal: the workflow ran to completion. */
    public static final String STATUS_COMPLETED = "COMPLETED";
    /** Terminal: a validation verdict or a non-retryable error. */
    public static final String STATUS_FAILED = "FAILED";
    /** Terminal: retries exhausted — the dead-letter state. */
    public static final String STATUS_DEAD = "DEAD";

    /** Every status the CHECK constraint (and list-jobs) accepts. */
    public static final java.util.Set<String> STATUSES = java.util.Set.of(
            STATUS_QUEUED, STATUS_RUNNING, STATUS_WAITING,
            STATUS_COMPLETED, STATUS_FAILED, STATUS_DEAD);

    /** The client-generated job uuid (the submit idempotency key). */
    public UUID jobId;
    /** The stored workflow name, or the inline workflow's declared name. */
    public String workflowName;
    /** The raw workflow JSON envelope, snapshotted at submit. */
    public String workflowDefinition;
    /** The workflow input, proto3 JSON. */
    public String input;
    /** Future repo-service claim-check for the input; null now. */
    public String inputRef;
    /** QUEUED / RUNNING / WAITING / COMPLETED / FAILED / DEAD. */
    public String status = STATUS_QUEUED;
    /** 1-based execution counter; claim increments it. */
    public int attempt;
    /** Retry ceiling; at attempt == maxAttempts a retryable failure lands DEAD. */
    public int maxAttempts = 3;
    /** Claim eligibility; backoff sets it into the future. */
    public Instant runAfter;
    /** The external step the job is parked on; set only while WAITING. */
    public String outstandingStep;
    /** The ordered checkpoint array (raw JSON text). */
    public String checkpoints = "[]";
    /** The workflow's composed output (raw JSON text); set on COMPLETED. */
    public String result;
    /** Future repo-service claim-check for the result; null now. */
    public String resultRef;
    /** The one-line completion summary. */
    public String verdict;
    /** The verbatim last error; set on FAILED and DEAD. */
    public String error;
    /** The worker holding the lease; null unless RUNNING. */
    public String leaseOwner;
    /** Lease expiry; the sweeper requeues RUNNING jobs past it. */
    public Instant leaseUntil;
    /** When the row was inserted. */
    public Instant createdAt;
    /** When the row last changed. */
    public Instant updatedAt;
    /** When the job reached a terminal state. */
    public Instant completedAt;
}
