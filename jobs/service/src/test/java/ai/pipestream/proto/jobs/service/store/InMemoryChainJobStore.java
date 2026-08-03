package ai.pipestream.proto.jobs.service.store;

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
 * The test-double {@link ChainJobStore}: the full state machine in memory
 * (same transitions, same gates, same event appends) so worker and action
 * unit tests run without Docker. The real claim's SKIP LOCKED is a simple
 * monitor here; the semantics — one claim per row, attempt incremented on
 * claim, lease expiry making a job re-claimable only via the sweep — match.
 * <p>
 * {@link #failNextCheckpoint} simulates a store hiccup inside the worker's
 * checkpoint observer: the next {@link #saveCheckpoint} throws, which the
 * worker must treat as a retryable CHAIN-kind failure (requeue), never a
 * settled failure.
 */
public final class InMemoryChainJobStore implements ChainJobStore {

    private static final ObjectMapper JSON = new ObjectMapper();

    private final Map<UUID, ChainJobRecord> jobs = new LinkedHashMap<>();
    private final List<ChainJobEventRecord> events = new CopyOnWriteArrayList<>();

    /** When true, the next saveCheckpoint throws once (a store hiccup). */
    public volatile boolean failNextCheckpoint;

    @Override
    public synchronized InsertOutcome insert(ChainJobRecord job, ChainJobEventRecord event) {
        ChainJobRecord existing = jobs.get(job.jobId);
        if (existing != null) {
            return new InsertOutcome(existing, false);
        }
        Instant now = Instant.now();
        job.createdAt = now;
        job.updatedAt = now;
        if (job.runAfter == null) {
            job.runAfter = now;
        }
        jobs.put(job.jobId, job);
        events.add(event);
        return new InsertOutcome(job, true);
    }

    @Override
    public synchronized Optional<ChainJobRecord> get(UUID jobId) {
        return Optional.ofNullable(jobs.get(jobId));
    }

    @Override
    public synchronized List<ChainJobRecord> list(String statusOrNull, String chainNameOrNull,
            int limit, long offset) {
        return jobs.values().stream()
                .filter(job -> statusOrNull == null || statusOrNull.equals(job.status))
                .filter(job -> chainNameOrNull == null || chainNameOrNull.equals(job.chainName))
                .sorted(Comparator.comparing((ChainJobRecord job) -> job.createdAt).reversed())
                .skip(offset)
                .limit(limit)
                .toList();
    }

    @Override
    public synchronized Optional<ChainJobRecord> claim(String workerId, Duration leaseDuration) {
        Instant now = Instant.now();
        return jobs.values().stream()
                .filter(job -> ChainJobRecord.STATUS_QUEUED.equals(job.status))
                .filter(job -> !job.runAfter.isAfter(now))
                .min(Comparator.comparing(job -> job.createdAt))
                .map(job -> {
                    job.status = ChainJobRecord.STATUS_RUNNING;
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
        for (ChainJobRecord job : jobs.values()) {
            if (ChainJobRecord.STATUS_RUNNING.equals(job.status)
                    && job.leaseUntil != null && job.leaseUntil.isBefore(now)) {
                job.status = ChainJobRecord.STATUS_QUEUED;
                job.leaseOwner = null;
                job.leaseUntil = null;
                job.updatedAt = now;
                requeued++;
            }
        }
        return requeued;
    }

    @Override
    public synchronized void saveCheckpoint(UUID jobId, String checkpointsJson,
            ChainJobEventRecord stepEvent) {
        if (failNextCheckpoint) {
            failNextCheckpoint = false;
            throw ChainJobStoreException.wrap("simulated store hiccup",
                    new java.sql.SQLException("connection reset"));
        }
        ChainJobRecord job = require(jobId);
        job.checkpoints = checkpointsJson;
        job.updatedAt = Instant.now();
        events.add(stepEvent);
    }

    @Override
    public synchronized void markWaiting(UUID jobId, String stepName, String checkpointsJson,
            ChainJobEventRecord event) {
        ChainJobRecord job = require(jobId);
        job.status = ChainJobRecord.STATUS_WAITING;
        job.outstandingStep = stepName;
        job.checkpoints = checkpointsJson;
        job.leaseOwner = null;
        job.leaseUntil = null;
        job.updatedAt = Instant.now();
        events.add(event);
    }

    @Override
    public synchronized void markCompleted(UUID jobId, String resultJson, String verdict,
            ChainJobEventRecord event) {
        ChainJobRecord job = require(jobId);
        job.status = ChainJobRecord.STATUS_COMPLETED;
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
    public synchronized void markFailed(UUID jobId, String error, ChainJobEventRecord event) {
        markTerminal(jobId, ChainJobRecord.STATUS_FAILED, error, event);
    }

    @Override
    public synchronized void markDead(UUID jobId, String error, ChainJobEventRecord event) {
        markTerminal(jobId, ChainJobRecord.STATUS_DEAD, error, event);
    }

    private void markTerminal(UUID jobId, String status, String error,
            ChainJobEventRecord event) {
        ChainJobRecord job = require(jobId);
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
    public synchronized void requeue(UUID jobId, Duration delay) {
        ChainJobRecord job = require(jobId);
        job.status = ChainJobRecord.STATUS_QUEUED;
        job.runAfter = Instant.now().plus(delay);
        job.leaseOwner = null;
        job.leaseUntil = null;
        job.updatedAt = Instant.now();
    }

    @Override
    public synchronized ParkedCompletion completeParkedStep(UUID jobId, String stepName,
            String checkpointEntryJson, ChainJobEventRecord stepEvent) {
        ChainJobRecord job = require(jobId);
        try {
            ArrayNode checkpoints = (ArrayNode) JSON.readTree(
                    job.checkpoints == null ? "[]" : job.checkpoints);
            for (JsonNode entry : checkpoints) {
                if (stepName.equals(entry.path("name").asText())) {
                    return new ParkedCompletion.AlreadyDone(job.status);
                }
            }
            if (!ChainJobRecord.STATUS_WAITING.equals(job.status)
                    || !stepName.equals(job.outstandingStep)) {
                return new ParkedCompletion.WrongState(job.status, job.outstandingStep);
            }
            checkpoints.add(JSON.readTree(checkpointEntryJson));
            job.checkpoints = checkpoints.toString();
            job.status = ChainJobRecord.STATUS_QUEUED;
            job.outstandingStep = null;
            job.runAfter = Instant.now();
            job.updatedAt = job.runAfter;
            events.add(stepEvent);
            return new ParkedCompletion.Completed();
        } catch (Exception e) {
            throw ChainJobStoreException.wrap("complete-parked-step failed", e);
        }
    }

    @Override
    public List<ChainJobEventRecord> pollPendingEvents(int limit) {
        return events.stream()
                .filter(event -> ChainJobEventRecord.STATUS_PENDING.equals(event.status))
                .sorted(Comparator.comparing(event -> event.createdAt))
                .limit(limit)
                .toList();
    }

    @Override
    public boolean markEventPublished(UUID eventId) {
        for (ChainJobEventRecord event : events) {
            if (event.eventId.equals(eventId)
                    && ChainJobEventRecord.STATUS_PENDING.equals(event.status)) {
                event.status = ChainJobEventRecord.STATUS_PUBLISHED;
                event.publishedAt = Instant.now();
                return true;
            }
        }
        return false;
    }

    @Override
    public Optional<ChainJobEventRecord> markEventFailed(ChainJobEventRecord event,
            String error) {
        for (ChainJobEventRecord stored : events) {
            if (stored.eventId.equals(event.eventId)
                    && ChainJobEventRecord.STATUS_PENDING.equals(stored.status)) {
                stored.attempts = stored.attempts + 1;
                stored.lastError = error;
                if (stored.attempts >= ChainJobEventRecord.MAX_ATTEMPTS) {
                    stored.status = ChainJobEventRecord.STATUS_FAILED;
                }
                return Optional.of(stored);
            }
        }
        return Optional.empty();
    }

    /** All outbox rows, in insertion order (test introspection). */
    public List<ChainJobEventRecord> events() {
        return List.copyOf(events);
    }

    private ChainJobRecord require(UUID jobId) {
        ChainJobRecord job = jobs.get(jobId);
        if (job == null) {
            throw ChainJobStoreException.notFound(jobId);
        }
        return job;
    }
}
