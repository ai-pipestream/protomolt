package ai.pipestream.proto.jobs.service.store;

import ai.pipestream.proto.jobs.service.events.WorkflowRunEventFactory;
import ai.pipestream.proto.jobs.v1.WorkflowRunEvent;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The Postgres default {@link WorkflowRunStore} against a real testcontainers
 * PostgreSQL 17 (Flyway-migrated schema): idempotent insert, the atomic
 * SKIP LOCKED claim (including under concurrency), run_after gating, the
 * lease sweep, the checkpoint/park/complete transactions with their outbox
 * events, the complete-parked-step state machine, and the outbox drain with
 * its DLQ. No mocks — the schema, the SQL, and the JSONB round-trip are all
 * exercised as deployed.
 */
@Testcontainers(disabledWithoutDocker = true)
class JdbcWorkflowRunStoreIT {

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17-alpine");

    static WorkflowRunDatabase database;
    static JdbcWorkflowRunStore store;

    @BeforeAll
    static void boot() {
        database = new WorkflowRunDatabase(new WorkflowRunStoreConfig(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
        store = new JdbcWorkflowRunStore(database);
    }

    @AfterAll
    static void tearDown() {
        database.close();
    }

    @BeforeEach
    void clean() {
        database.inTransaction(c -> {
            try {
                c.createStatement().execute("DELETE FROM workflow_run_events_outbox");
                c.createStatement().execute("DELETE FROM workflow_run");
            } catch (java.sql.SQLException e) {
                throw new RuntimeException(e);
            }
            return null;
        });
    }

    private static WorkflowRunRecord newJob(String workflowName) {
        WorkflowRunRecord record = new WorkflowRunRecord();
        record.jobId = UUID.randomUUID();
        record.workflowName = workflowName;
        record.workflowDefinition = "{\"name\": \"" + workflowName + "\"}";
        record.input = "{\"text\": \"hi\"}";
        record.status = WorkflowRunRecord.STATUS_QUEUED;
        record.maxAttempts = 3;
        record.runAfter = Instant.now();
        return record;
    }

    private WorkflowRunRecord insert(String workflowName) {
        WorkflowRunRecord record = newJob(workflowName);
        WorkflowRunStore.InsertOutcome outcome =
                store.insert(record, WorkflowRunEventFactory.accepted(record));
        assertThat(outcome.created()).isTrue();
        return outcome.job();
    }

    @Test
    void insertIsIdempotentAndOutboxesTheAcceptedEvent() throws Exception {
        WorkflowRunRecord record = newJob("embed-text");
        WorkflowRunStore.InsertOutcome first =
                store.insert(record, WorkflowRunEventFactory.accepted(record));
        assertThat(first.created()).isTrue();
        assertThat(first.job().status).isEqualTo(WorkflowRunRecord.STATUS_QUEUED);
        assertThat(first.job().createdAt).isNotNull();
        assertThat(first.job().checkpoints).isEqualTo("[]");

        // The resubmit writes nothing and returns the existing row.
        WorkflowRunStore.InsertOutcome second =
                store.insert(record, WorkflowRunEventFactory.accepted(record));
        assertThat(second.created()).isFalse();
        assertThat(second.job().jobId).isEqualTo(record.jobId);

        List<WorkflowRunEventRecord> pending = store.pollPendingEvents(10);
        assertThat(pending).hasSize(1);
        WorkflowRunEventRecord outbox = pending.get(0);
        assertThat(outbox.eventType).isEqualTo(WorkflowRunEventRecord.TYPE_ACCEPTED);
        assertThat(outbox.kafkaKey).isEqualTo(record.jobId.toString());
        WorkflowRunEvent event = WorkflowRunEvent.parseFrom(outbox.payload);
        assertThat(event.getJobId()).isEqualTo(record.jobId.toString());
        assertThat(event.getEventId()).isEqualTo(outbox.eventId.toString());
        assertThat(event.getType()).isEqualTo(WorkflowRunEvent.Type.TYPE_ACCEPTED);
        assertThat(event.getWorkflowName()).isEqualTo("embed-text");
    }

    @Test
    void claimFlipsTheOldestEligibleJobAndIncrementsTheAttempt() {
        WorkflowRunRecord first = insert("workflow-a");
        WorkflowRunRecord second = insert("workflow-b");

        Optional<WorkflowRunRecord> claimed = store.claim("worker-1", Duration.ofMinutes(1));
        assertThat(claimed).isPresent();
        assertThat(claimed.get().jobId).isEqualTo(first.jobId);
        assertThat(claimed.get().status).isEqualTo(WorkflowRunRecord.STATUS_RUNNING);
        assertThat(claimed.get().leaseOwner).isEqualTo("worker-1");
        assertThat(claimed.get().leaseUntil).isAfter(Instant.now());
        assertThat(claimed.get().attempt).isEqualTo(1);

        Optional<WorkflowRunRecord> next = store.claim("worker-2", Duration.ofMinutes(1));
        assertThat(next).isPresent();
        assertThat(next.get().jobId).isEqualTo(second.jobId);

        // Nothing left: both rows are RUNNING.
        assertThat(store.claim("worker-3", Duration.ofMinutes(1))).isEmpty();
    }

    @Test
    void claimRespectsRunAfter() {
        WorkflowRunRecord job = insert("embed-text");
        store.requeue(job.jobId, Duration.ofMinutes(5));

        assertThat(store.claim("worker-1", Duration.ofMinutes(1))).isEmpty();
        WorkflowRunRecord row = store.get(job.jobId).orElseThrow();
        assertThat(row.status).isEqualTo(WorkflowRunRecord.STATUS_QUEUED);
        assertThat(row.runAfter).isAfter(Instant.now());
    }

    @Test
    void theSweeperRequeuesExpiredLeasesWithTheirAttemptsPreserved()
            throws Exception {
        WorkflowRunRecord job = insert("embed-text");
        store.claim("worker-1", Duration.ofMillis(1));
        Thread.sleep(50);

        assertThat(store.requeueExpiredLeases()).isEqualTo(1);
        WorkflowRunRecord swept = store.get(job.jobId).orElseThrow();
        assertThat(swept.status).isEqualTo(WorkflowRunRecord.STATUS_QUEUED);
        assertThat(swept.leaseOwner).isNull();
        assertThat(swept.attempt).isEqualTo(1);

        // Re-claimable, with the attempt counter continuing where it left off.
        Optional<WorkflowRunRecord> reclaimed = store.claim("worker-2", Duration.ofMinutes(1));
        assertThat(reclaimed).isPresent();
        assertThat(reclaimed.get().attempt).isEqualTo(2);

        // A live lease is not swept.
        assertThat(store.requeueExpiredLeases()).isZero();
    }

    @Test
    void concurrentClaimsNeverTakeTheSameRow() throws Exception {
        for (int i = 0; i < 3; i++) {
            insert("workflow-" + i);
        }
        ConcurrentLinkedQueue<UUID> claimed = new ConcurrentLinkedQueue<>();
        CountDownLatch start = new CountDownLatch(1);
        Runnable drainer = () -> {
            try {
                start.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            Optional<WorkflowRunRecord> job;
            while ((job = store.claim("worker-" + Thread.currentThread().getId(),
                    Duration.ofMinutes(1))).isPresent()) {
                claimed.add(job.get().jobId);
            }
        };
        Thread a = Thread.ofVirtual().start(drainer);
        Thread b = Thread.ofVirtual().start(drainer);
        start.countDown();
        a.join(TimeUnit.SECONDS.toMillis(30));
        b.join(TimeUnit.SECONDS.toMillis(30));

        assertThat(claimed).hasSize(3).doesNotHaveDuplicates();
    }

    @Test
    void saveCheckpointIsAtomicWithItsStepEvent() throws Exception {
        WorkflowRunRecord job = insert("embed-text");
        WorkflowRunRecord claimed = store.claim("worker-1", Duration.ofMinutes(1)).orElseThrow();
        String checkpoints = "[{\"name\": \"tokenize\", \"skipped\": false,"
                + " \"response\": {\"ids\": [\"104\"]}}]";
        store.saveCheckpoint(job.jobId, checkpoints,
                WorkflowRunEventFactory.stepCheckpoint(claimed, "tokenize"));

        WorkflowRunRecord row = store.get(job.jobId).orElseThrow();
        assertThat(row.checkpoints).contains("tokenize");

        List<WorkflowRunEventRecord> pending = store.pollPendingEvents(10);
        assertThat(pending).hasSize(2);
        WorkflowRunEventRecord stepEvent = pending.get(1);
        assertThat(stepEvent.eventType).isEqualTo(WorkflowRunEventRecord.TYPE_STEP_CHECKPOINT);
        WorkflowRunEvent event = WorkflowRunEvent.parseFrom(stepEvent.payload);
        assertThat(event.getStep()).isEqualTo("tokenize");
        assertThat(event.getAttempt()).isEqualTo(1);
    }

    @Test
    void theTerminalTransitionsCarryTheirPayloads() throws Exception {
        WorkflowRunRecord completed = insert("workflow-done");
        store.claim("worker-1", Duration.ofMinutes(1));
        store.markCompleted(completed.jobId, "{\"ok\": true}", "2 steps, output t.T",
                WorkflowRunEventFactory.completed(completed, "2 steps, output t.T"));
        WorkflowRunRecord doneRow = store.get(completed.jobId).orElseThrow();
        assertThat(doneRow.status).isEqualTo(WorkflowRunRecord.STATUS_COMPLETED);
        assertThat(doneRow.verdict).isEqualTo("2 steps, output t.T");
        assertThat(doneRow.result).contains("\"ok\": true");
        assertThat(doneRow.completedAt).isNotNull();
        assertThat(doneRow.leaseOwner).isNull();

        WorkflowRunRecord failed = insert("workflow-fail");
        store.claim("worker-1", Duration.ofMinutes(1));
        store.markFailed(failed.jobId, "VALIDATION: nope",
                WorkflowRunEventFactory.failed(failed, "embed", "VALIDATION: nope"));
        WorkflowRunRecord failedRow = store.get(failed.jobId).orElseThrow();
        assertThat(failedRow.status).isEqualTo(WorkflowRunRecord.STATUS_FAILED);
        assertThat(failedRow.error).isEqualTo("VALIDATION: nope");
        assertThat(failedRow.completedAt).isNotNull();

        WorkflowRunRecord dead = insert("workflow-dead");
        store.claim("worker-1", Duration.ofMinutes(1));
        store.markDead(dead.jobId, "GRPC: UNAVAILABLE",
                WorkflowRunEventFactory.dead(dead, "GRPC: UNAVAILABLE"));
        WorkflowRunRecord deadRow = store.get(dead.jobId).orElseThrow();
        assertThat(deadRow.status).isEqualTo(WorkflowRunRecord.STATUS_DEAD);
        assertThat(deadRow.error).isEqualTo("GRPC: UNAVAILABLE");

        // Terminal rows are out of every claim.
        assertThat(store.claim("worker-2", Duration.ofMinutes(1))).isEmpty();

        // And every transition outboxed its event.
        assertThat(store.pollPendingEvents(10).stream().map(e -> e.eventType))
                .containsExactlyInAnyOrder(
                        WorkflowRunEventRecord.TYPE_ACCEPTED,
                        WorkflowRunEventRecord.TYPE_ACCEPTED,
                        WorkflowRunEventRecord.TYPE_ACCEPTED,
                        WorkflowRunEventRecord.TYPE_COMPLETED,
                        WorkflowRunEventRecord.TYPE_FAILED,
                        WorkflowRunEventRecord.TYPE_DEAD);
    }

    @Test
    void markWaitingParksAndCompleteParkedStepGatesTheResume() {
        WorkflowRunRecord job = insert("embed-text");
        store.claim("worker-1", Duration.ofMinutes(1));
        String prefix = "[{\"name\": \"tokenize\", \"skipped\": false,"
                + " \"response\": {\"ids\": [\"104\"]}}]";
        store.markWaiting(job.jobId, "review", prefix,
                WorkflowRunEventFactory.waiting(job, "review"));
        WorkflowRunRecord parked = store.get(job.jobId).orElseThrow();
        assertThat(parked.status).isEqualTo(WorkflowRunRecord.STATUS_WAITING);
        assertThat(parked.outstandingStep).isEqualTo("review");
        assertThat(parked.leaseOwner).isNull();
        // A parked job is not claimable.
        assertThat(store.claim("worker-2", Duration.ofMinutes(1))).isEmpty();

        // The wrong step is refused with the state.
        ParkedCompletion wrong = store.completeParkedStep(job.jobId, "embed",
                "{\"name\": \"embed\", \"skipped\": false, \"response\": {}}",
                WorkflowRunEventFactory.stepCheckpoint(parked, "embed"));
        assertThat(wrong).isInstanceOf(ParkedCompletion.WrongState.class);
        assertThat(((ParkedCompletion.WrongState) wrong).currentStatus()).isEqualTo("WAITING");
        assertThat(((ParkedCompletion.WrongState) wrong).outstandingStep()).isEqualTo("review");

        // The right step appends and requeues.
        ParkedCompletion accepted = store.completeParkedStep(job.jobId, "review",
                "{\"name\": \"review\", \"skipped\": false, \"response\": {\"notes\": \"ok\"}}",
                WorkflowRunEventFactory.stepCheckpoint(parked, "review"));
        assertThat(accepted).isInstanceOf(ParkedCompletion.Completed.class);
        WorkflowRunRecord resumed = store.get(job.jobId).orElseThrow();
        assertThat(resumed.status).isEqualTo(WorkflowRunRecord.STATUS_QUEUED);
        assertThat(resumed.outstandingStep).isNull();
        assertThat(resumed.checkpoints).contains("review");

        // The redelivery is idempotent.
        ParkedCompletion again = store.completeParkedStep(job.jobId, "review",
                "{\"name\": \"review\", \"skipped\": false, \"response\": {\"notes\": \"ok\"}}",
                WorkflowRunEventFactory.stepCheckpoint(parked, "review"));
        assertThat(again).isInstanceOf(ParkedCompletion.AlreadyDone.class);
        assertThat(((ParkedCompletion.AlreadyDone) again).currentStatus()).isEqualTo("QUEUED");
        assertThat(store.get(job.jobId).orElseThrow().checkpoints)
                .isEqualTo(resumed.checkpoints);
    }

    @Test
    void theOutboxDrainSettlesPublishedAndDeadLettersAtTheAttemptsCeiling() {
        insert("workflow-a");
        insert("workflow-b");

        List<WorkflowRunEventRecord> batch = store.pollPendingEvents(10);
        assertThat(batch).hasSize(2);
        assertThat(batch.get(0).createdAt).isBeforeOrEqualTo(batch.get(1).createdAt);

        // Publish settles PENDING → PUBLISHED, conditionally.
        assertThat(store.markEventPublished(batch.get(0).eventId)).isTrue();
        assertThat(store.markEventPublished(batch.get(0).eventId)).isFalse();

        // Failures bump attempts; at the ceiling the row lands FAILED (DLQ)
        // and leaves the drain.
        WorkflowRunEventRecord doomed = batch.get(1);
        for (int i = 0; i < WorkflowRunEventRecord.MAX_ATTEMPTS; i++) {
            Optional<WorkflowRunEventRecord> updated = store.markEventFailed(doomed, "broker down");
            assertThat(updated).isPresent();
            doomed = updated.get();
        }
        assertThat(doomed.status).isEqualTo(WorkflowRunEventRecord.STATUS_FAILED);
        assertThat(doomed.attempts).isEqualTo(WorkflowRunEventRecord.MAX_ATTEMPTS);
        assertThat(doomed.lastError).isEqualTo("broker down");
        assertThat(store.pollPendingEvents(10)).isEmpty();
    }

    @Test
    void listPagesNewestFirstWithFilters() {
        insert("workflow-a");
        insert("workflow-a");
        insert("workflow-b");

        assertThat(store.list(null, null, 10, 0)).hasSize(3);
        assertThat(store.list("QUEUED", null, 10, 0)).hasSize(3);
        assertThat(store.list(null, "workflow-a", 10, 0)).hasSize(2);
        assertThat(store.list("COMPLETED", null, 10, 0)).isEmpty();
        assertThat(store.list(null, null, 2, 0)).hasSize(2);
        assertThat(store.list(null, null, 2, 2)).hasSize(1);
    }
}
