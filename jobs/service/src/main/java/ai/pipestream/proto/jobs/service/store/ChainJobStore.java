package ai.pipestream.proto.jobs.service.store;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * The chain-jobs persistence SPI: the worker, the relay, and the verbs see
 * only this. Every method is one unit of work and every commit point writes
 * its outbox event IN THE SAME TRANSACTION as the job mutation it describes
 * — an event can never drift from the state change it announces.
 * <p>
 * Everything fails loud: SQL failures propagate as
 * {@link ChainJobStoreException}, never swallowed, never defaulted.
 */
public interface ChainJobStore {

    /**
     * The outcome of {@link #insert}: the row as stored plus whether this
     * call created it.
     *
     * @param job the stored row (the pre-existing one on a conflict)
     * @param created true when this call inserted the row (and its event);
     *        false on an idempotent resubmit, where neither row nor event
     *        was written
     */
    record InsertOutcome(ChainJobRecord job, boolean created) {
    }

    /**
     * Insert a new job and its first outbox event (ACCEPTED, or FAILED for a
     * broker-native submit of an unknown chain) in one transaction.
     * Idempotent on {@code job_id}: a conflict writes nothing and returns
     * the existing row with {@code created == false}.
     *
     * @param job the job row to insert
     * @param event the outbox event for the insert commit point
     * @return the stored row and whether this call created it
     */
    InsertOutcome insert(ChainJobRecord job, ChainJobEventRecord event);

    /**
     * Load one job by id.
     *
     * @param jobId the job id
     * @return the row, or empty
     */
    Optional<ChainJobRecord> get(UUID jobId);

    /**
     * Page jobs, newest first. Null filters match everything.
     *
     * @param statusOrNull restrict to this status, or null
     * @param chainNameOrNull restrict to this chain, or null
     * @param limit page size
     * @param offset rows to skip
     * @return the page (possibly empty)
     */
    List<ChainJobRecord> list(String statusOrNull, String chainNameOrNull, int limit, long offset);

    /**
     * Claim the oldest eligible QUEUED job ({@code run_after <= now()}) for
     * {@code workerId}: one atomic {@code UPDATE ... FOR UPDATE SKIP LOCKED}
     * that flips it RUNNING, stamps the lease, and increments the attempt
     * counter. Concurrent workers never claim the same row.
     *
     * @param workerId the claiming worker's identity (the lease owner)
     * @param leaseDuration how long the claim holds before the sweeper may
     *        requeue the job
     * @return the claimed row (attempt already incremented), or empty
     */
    Optional<ChainJobRecord> claim(String workerId, Duration leaseDuration);

    /**
     * Crash recovery: flip RUNNING jobs whose lease expired back to QUEUED
     * (lease cleared). The persisted checkpoints make the resume safe.
     *
     * @return how many jobs were requeued
     */
    int requeueExpiredLeases();

    /**
     * Persist the checkpoint array after a step landed, and the
     * STEP_CHECKPOINT event, in one transaction.
     *
     * @param jobId the job
     * @param checkpointsJson the FULL checkpoint array so far (raw JSON)
     * @param stepEvent the STEP_CHECKPOINT outbox event
     */
    void saveCheckpoint(UUID jobId, String checkpointsJson, ChainJobEventRecord stepEvent);

    /**
     * Park the job on an external-completion step: status WAITING,
     * outstanding_step set, lease cleared, checkpoints replaced, and the
     * WAITING event — one transaction.
     *
     * @param jobId the job
     * @param stepName the external step the job is parked on
     * @param checkpointsJson the full checkpoint array so far (raw JSON)
     * @param event the WAITING outbox event
     */
    void markWaiting(UUID jobId, String stepName, String checkpointsJson,
                     ChainJobEventRecord event);

    /**
     * Terminate the job successfully: status COMPLETED, the result document
     * and verdict, lease and outstanding step cleared, completed_at stamped,
     * and the COMPLETED event — one transaction.
     *
     * @param jobId the job
     * @param resultJson the chain's composed output (raw proto3 JSON)
     * @param verdict the one-line completion summary
     * @param event the COMPLETED outbox event
     */
    void markCompleted(UUID jobId, String resultJson, String verdict,
                       ChainJobEventRecord event);

    /**
     * Terminate the job as failed: a validation verdict or a non-retryable
     * error. The error is stored verbatim (the court's review queue consumes
     * these), completed_at stamped, and the FAILED event — one transaction.
     *
     * @param jobId the job
     * @param error the verbatim failure detail
     * @param event the FAILED outbox event
     */
    void markFailed(UUID jobId, String error, ChainJobEventRecord event);

    /**
     * Dead-letter the job: retries exhausted. The last error is stored
     * verbatim and the DEAD event written — one transaction. Operator
     * territory: nothing re-enqueues a DEAD job.
     *
     * @param jobId the job
     * @param error the verbatim last error
     * @param event the DEAD outbox event
     */
    void markDead(UUID jobId, String error, ChainJobEventRecord event);

    /**
     * Back off a failed attempt: status QUEUED with {@code run_after} pushed
     * {@code delay} into the future, lease cleared. No event — a requeue is
     * not a lifecycle commit point.
     *
     * @param jobId the job
     * @param delay how long until the job is claimable again
     */
    void requeue(UUID jobId, Duration delay);

    /**
     * Accept the response for a parked external step, atomically gated on
     * the job's state: only a WAITING job whose outstanding_step is exactly
     * {@code stepName} takes the checkpoint (appended), requeues, and writes
     * the STEP_CHECKPOINT event — one transaction. A step already present in
     * the checkpoints is an idempotent redelivery; anything else is a wrong
     * state. See {@link ParkedCompletion}.
     *
     * @param jobId the job
     * @param stepName the step being completed
     * @param checkpointEntryJson the checkpoint entry
     *        ({@code {"name", "skipped": false, "response"}}) as raw JSON
     * @param stepEvent the STEP_CHECKPOINT outbox event
     * @return the gate's verdict
     */
    ParkedCompletion completeParkedStep(UUID jobId, String stepName,
                                        String checkpointEntryJson,
                                        ChainJobEventRecord stepEvent);

    /**
     * Claim up to {@code limit} PENDING outbox rows, oldest first
     * ({@code FOR UPDATE SKIP LOCKED}). The claim transaction commits before
     * the relay publishes, so this is at-least-once claiming, not exclusive
     * ownership (a competing relay may republish; consumers dedupe on
     * {@code event_id}).
     *
     * @param limit the batch size
     * @return the claimed rows, possibly empty
     */
    List<ChainJobEventRecord> pollPendingEvents(int limit);

    /**
     * Mark a claimed outbox row PUBLISHED after the broker ack. Conditional
     * on PENDING: a row settled by a competing relay is not re-settled.
     *
     * @param eventId the outbox row id
     * @return true when this call moved the row
     */
    boolean markEventPublished(UUID eventId);

    /**
     * Record a relay failure: attempts + 1 and the error detail, landing the
     * row FAILED (the DLQ) at the attempts ceiling.
     *
     * @param event the row whose publication failed
     * @param error the failure detail (truncated to the column's cap)
     * @return the updated row, or empty when it was no longer PENDING
     */
    Optional<ChainJobEventRecord> markEventFailed(ChainJobEventRecord event, String error);
}
