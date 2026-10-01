package ai.protomolt.proto.jobs.service.store;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * The test-double {@link WorkflowRunStore}: the full state machine in memory
 * (same transitions, same gates, same event appends) so worker and action
 * unit tests run without Docker. The real claim's SKIP LOCKED is a simple
 * monitor here; the semantics — one claim per row, attempt incremented on
 * claim, lease expiry making a job re-claimable only via the sweep — match.
 * <p>
 * {@link #failNextCheckpoint} simulates a store hiccup inside the worker's
 * checkpoint observer: the next {@link #saveCheckpoint} throws, which the
 * worker must treat as a retryable WORKFLOW-kind failure (requeue), never a
 * settled failure.
 */
public final class InMemoryWorkflowRunStore implements WorkflowRunStore {

    private static final ObjectMapper JSON = new ObjectMapper();

    private final Map<UUID, WorkflowRunRecord> jobs = new LinkedHashMap<>();
    private final List<WorkflowRunEventRecord> events = new CopyOnWriteArrayList<>();

    /** When true, the next saveCheckpoint throws once (a store hiccup). */
    public volatile boolean failNextCheckpoint;
    /** Test hook: replace a live claim at this 1-based saveCheckpoint call, then reject it. */
    public volatile int stealClaimAtCheckpoint;
    private int checkpointWrites;

    @Override
    public synchronized InsertOutcome insert(WorkflowRunRecord job, WorkflowRunEventRecord event) {
        WorkflowRunRecord existing = jobs.get(job.jobId);
        if (existing != null) {
            return new InsertOutcome(existing, false,
                    !WorkflowRunStore.sameSubmission(existing, job));
        }
        Instant now = Instant.now();
        job.createdAt = now;
        job.updatedAt = now;
        if (job.runAfter == null) {
            job.runAfter = now;
        }
        jobs.put(job.jobId, job);
        events.add(event);
        return new InsertOutcome(job, true, false);
    }

    @Override
    public synchronized Optional<WorkflowRunRecord> get(UUID jobId) {
        return Optional.ofNullable(jobs.get(jobId));
    }

    @Override
    public synchronized List<WorkflowRunRecord> list(String statusOrNull, String workflowNameOrNull,
            int limit, long offset) {
        return jobs.values().stream()
                .filter(job -> statusOrNull == null || statusOrNull.equals(job.status))
                .filter(job -> workflowNameOrNull == null || workflowNameOrNull.equals(job.workflowName))
                .sorted(Comparator.comparing((WorkflowRunRecord job) -> job.createdAt).reversed())
                .skip(offset)
                .limit(limit)
                .toList();
    }

    @Override
    public synchronized Optional<WorkflowRunRecord> claim(String workerId, Duration leaseDuration) {
        Instant now = Instant.now();
        return jobs.values().stream()
                .filter(job -> WorkflowRunRecord.STATUS_QUEUED.equals(job.status))
                .filter(job -> !job.runAfter.isAfter(now))
                .min(Comparator.comparing(job -> job.createdAt))
                .map(job -> {
                    job.status = WorkflowRunRecord.STATUS_RUNNING;
                    job.leaseOwner = workerId;
                    job.leaseUntil = now.plus(leaseDuration);
                    job.attempt = job.attempt + 1;
                    job.updatedAt = now;
                    return job;
                });
    }

    @Override
    public synchronized int requeueExpiredLeases() {
        Instant now = Instant.now();
        int requeued = 0;
        for (WorkflowRunRecord job : jobs.values()) {
            if (WorkflowRunRecord.STATUS_RUNNING.equals(job.status)
                    && job.leaseUntil != null && job.leaseUntil.isBefore(now)) {
                job.status = WorkflowRunRecord.STATUS_QUEUED;
                job.leaseOwner = null;
                job.leaseUntil = null;
                job.updatedAt = now;
                requeued++;
            }
        }
        return requeued;
    }

    @Override
    public synchronized void saveCheckpoint(WorkerClaim claim, String checkpointsJson,
            WorkflowRunEventRecord stepEvent) {
        WorkflowRunRecord job = requireClaim(claim);
        checkpointWrites++;
        if (stealClaimAtCheckpoint == checkpointWrites) {
            stealClaimAtCheckpoint = 0;
            job.leaseUntil = Instant.now().minusMillis(1);
            requeueExpiredLeases();
            claim("replacement-worker", Duration.ofMinutes(1)).orElseThrow();
            throw new ClaimLostException(claim);
        }
        if (failNextCheckpoint) {
            failNextCheckpoint = false;
            throw WorkflowRunStoreException.wrap("simulated store hiccup",
                    new java.sql.SQLException("connection reset"));
        }
        job.checkpoints = checkpointsJson;
        job.updatedAt = Instant.now();
        events.add(stepEvent);
    }

    @Override
    public synchronized void markWaiting(WorkerClaim claim, String stepName, String checkpointsJson,
            WorkflowRunEventRecord event) {
        WorkflowRunRecord job = requireClaim(claim);
        job.status = WorkflowRunRecord.STATUS_WAITING;
        job.outstandingStep = stepName;
        job.checkpoints = checkpointsJson;
        job.leaseOwner = null;
        job.leaseUntil = null;
        job.updatedAt = Instant.now();
        events.add(event);
    }

    @Override
    public synchronized void markCompleted(WorkerClaim claim, String resultJson, String verdict,
            WorkflowRunEventRecord event) {
        WorkflowRunRecord job = requireClaim(claim);
        job.status = WorkflowRunRecord.STATUS_COMPLETED;
        job.result = resultJson;
        job.verdict = verdict;
        job.leaseOwner = null;
        job.leaseUntil = null;
        job.outstandingStep = null;
        job.completedAt = Instant.now();
        job.updatedAt = job.completedAt;
        events.add(event);
    }

    @Override
    public synchronized void markFailed(WorkerClaim claim, String error, WorkflowRunEventRecord event) {
        markTerminal(claim, WorkflowRunRecord.STATUS_FAILED, error, event);
    }

    @Override
    public synchronized void markDead(WorkerClaim claim, String error, WorkflowRunEventRecord event) {
        markTerminal(claim, WorkflowRunRecord.STATUS_DEAD, error, event);
    }

    private void markTerminal(WorkerClaim claim, String status, String error,
            WorkflowRunEventRecord event) {
        WorkflowRunRecord job = requireClaim(claim);
        job.status = status;
        job.error = error;
        job.leaseOwner = null;
        job.leaseUntil = null;
        job.outstandingStep = null;
        job.completedAt = Instant.now();
        job.updatedAt = job.completedAt;
        events.add(event);
    }

    @Override
    public synchronized void requeue(WorkerClaim claim, Duration delay) {
        WorkflowRunRecord job = requireClaim(claim);
        job.status = WorkflowRunRecord.STATUS_QUEUED;
        job.runAfter = Instant.now().plus(delay);
        job.leaseOwner = null;
        job.leaseUntil = null;
        job.updatedAt = Instant.now();
    }

    @Override
    public synchronized ParkedCompletion completeParkedStep(UUID jobId, String stepName,
            String checkpointEntryJson, WorkflowRunEventRecord stepEvent, String validationError) {
        WorkflowRunRecord job = require(jobId);
        try {
            ArrayNode checkpoints = (ArrayNode) JSON.readTree(
                    job.checkpoints == null ? "[]" : job.checkpoints);
            for (JsonNode entry : checkpoints) {
                if (stepName.equals(entry.path("name").asText())) {
                    return entry.path("response").equals(JSON.readTree(checkpointEntryJson).path("response"))
                            ? new ParkedCompletion.AlreadyDone(job.status)
                            : new ParkedCompletion.Conflict(job.status);
                }
            }
            if (!WorkflowRunRecord.STATUS_WAITING.equals(job.status)
                    || !stepName.equals(job.outstandingStep)) {
                return new ParkedCompletion.WrongState(job.status, job.outstandingStep);
            }
            if (validationError != null) {
                job.status = WorkflowRunRecord.STATUS_FAILED;
                job.error = validationError;
                job.leaseOwner = null;
                job.leaseUntil = null;
                job.outstandingStep = null;
                job.completedAt = Instant.now();
                job.updatedAt = job.completedAt;
                events.add(stepEvent);
                return new ParkedCompletion.Rejected(validationError);
            }
            checkpoints.add(JSON.readTree(checkpointEntryJson));
            job.checkpoints = checkpoints.toString();
            job.status = WorkflowRunRecord.STATUS_QUEUED;
            job.outstandingStep = null;
            job.runAfter = Instant.now();
            job.updatedAt = job.runAfter;
            events.add(stepEvent);
            return new ParkedCompletion.Completed();
        } catch (Exception e) {
            throw WorkflowRunStoreException.wrap("complete-parked-step failed", e);
        }
    }

    @Override
    public List<WorkflowRunEventRecord> pollPendingEvents(int limit) {
        return events.stream()
                .filter(event -> WorkflowRunEventRecord.STATUS_PENDING.equals(event.status))
                .sorted(Comparator.comparing(event -> event.createdAt))
                .limit(limit)
                .toList();
    }

    @Override
    public boolean markEventPublished(UUID eventId) {
        for (WorkflowRunEventRecord event : events) {
            if (event.eventId.equals(eventId)
                    && WorkflowRunEventRecord.STATUS_PENDING.equals(event.status)) {
                event.status = WorkflowRunEventRecord.STATUS_PUBLISHED;
                event.publishedAt = Instant.now();
                return true;
            }
        }
        return false;
    }

    @Override
    public Optional<WorkflowRunEventRecord> markEventFailed(WorkflowRunEventRecord event,
            String error) {
        for (WorkflowRunEventRecord stored : events) {
            if (stored.eventId.equals(event.eventId)
                    && WorkflowRunEventRecord.STATUS_PENDING.equals(stored.status)) {
                stored.attempts = stored.attempts + 1;
                stored.lastError = error;
                if (stored.attempts >= WorkflowRunEventRecord.MAX_ATTEMPTS) {
                    stored.status = WorkflowRunEventRecord.STATUS_FAILED;
                }
                return Optional.of(stored);
            }
        }
        return Optional.empty();
    }

    /** All outbox rows, in insertion order (test introspection). */
    public List<WorkflowRunEventRecord> events() {
        return List.copyOf(events);
    }

    private WorkflowRunRecord require(UUID jobId) {
        WorkflowRunRecord job = jobs.get(jobId);
        if (job == null) {
            throw WorkflowRunStoreException.notFound(jobId);
        }
        return job;
    }

    private WorkflowRunRecord requireClaim(WorkerClaim claim) {
        WorkflowRunRecord job = require(claim.jobId());
        if (!WorkflowRunRecord.STATUS_RUNNING.equals(job.status)
                || !claim.leaseOwner().equals(job.leaseOwner)
                || claim.attempt() != job.attempt
                || job.leaseUntil == null || !job.leaseUntil.isAfter(Instant.now())) {
            throw new ClaimLostException(claim);
        }
        return job;
    }
}
