package ai.protomolt.proto.jobs.service.store;

import ai.protomolt.proto.jobs.service.events.WorkflowRunEventFactory;
import ai.protomolt.proto.jobs.v1.WorkflowRunEvent;
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
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

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
    void insertComparesSubmissionIdentityAfterJobIdConflict() {
        WorkflowRunRecord first = newJob("embed-text");
        first.workflowDefinition = "{\"name\":\"embed-text\",\"version\":1}";
        first.input = "{\"text\":\"hi\",\"count\":1}";
        store.insert(first, WorkflowRunEventFactory.accepted(first));

        WorkflowRunRecord same = newJob("embed-text");
        same.jobId = first.jobId;
        same.workflowDefinition = "{\"version\":1,\"name\":\"embed-text\"}";
        same.input = "{\"count\":1,\"text\":\"hi\"}";
        same.maxAttempts = 99;
        assertThat(store.insert(same, WorkflowRunEventFactory.accepted(same)).conflict()).isFalse();

        WorkflowRunRecord changed = newJob("embed-text");
        changed.jobId = first.jobId;
        changed.input = "{\"text\":\"different\"}";
        assertThat(store.insert(changed, WorkflowRunEventFactory.accepted(changed)).conflict())
                .isTrue();
        assertThat(WorkflowRunStore.sameSubmission(store.get(first.jobId).orElseThrow(), first))
                .isTrue();
        assertThat(store.pollPendingEvents(10)).hasSize(1);
    }

    @Test
    void concurrentDifferentSubmissionsHaveOneWinnerAndOneConflict() throws Exception {
        UUID id = UUID.randomUUID();
        WorkflowRunRecord a = newJob("embed-text");
        a.jobId = id;
        WorkflowRunRecord b = newJob("embed-text");
        b.jobId = id;
        b.input = "{\"text\":\"other\"}";
        CountDownLatch start = new CountDownLatch(1);
        ConcurrentLinkedQueue<WorkflowRunStore.InsertOutcome> results = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<Throwable> failures = new ConcurrentLinkedQueue<>();
        Thread one = Thread.ofVirtual().start(() -> insertAfter(start, a, results, failures));
        Thread two = Thread.ofVirtual().start(() -> insertAfter(start, b, results, failures));
        start.countDown();
        one.join(TimeUnit.SECONDS.toMillis(10));
        two.join(TimeUnit.SECONDS.toMillis(10));
        assertThat(one.isAlive() || two.isAlive()).isFalse();
        assertThat(failures).isEmpty();
        assertThat(results).hasSize(2);
        assertThat(results.stream().filter(WorkflowRunStore.InsertOutcome::created)).hasSize(1);
        assertThat(results.stream().filter(WorkflowRunStore.InsertOutcome::conflict)).hasSize(1);
        assertThat(store.pollPendingEvents(10)).hasSize(1);
    }

    private static void insertAfter(CountDownLatch start, WorkflowRunRecord record,
            ConcurrentLinkedQueue<WorkflowRunStore.InsertOutcome> results,
            ConcurrentLinkedQueue<Throwable> failures) {
        try {
            start.await();
            results.add(store.insert(record, WorkflowRunEventFactory.accepted(record)));
        } catch (Throwable failure) {
            failures.add(failure);
        }
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
        WorkerClaim claim = WorkerClaim.from(
                store.claim("worker-1", Duration.ofMinutes(1)).orElseThrow());
        store.requeue(claim, Duration.ofMinutes(5));

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
    void anExpiredLastAttemptBecomesDeadWithOneAtomicEventAndFencesItsWorker()
            throws Exception {
        WorkflowRunRecord job = newJob("exhausted-lease");
        job.maxAttempts = 1;
        store.insert(job, WorkflowRunEventFactory.accepted(job));
        WorkflowRunRecord claimed = store.claim("lost-worker", Duration.ofMinutes(1)).orElseThrow();
        expireLease(job.jobId);

        assertThat(store.requeueExpiredLeases()).isZero(); // no row was requeued
        WorkflowRunRecord dead = store.get(job.jobId).orElseThrow();
        assertThat(dead.status).isEqualTo(WorkflowRunRecord.STATUS_DEAD);
        assertThat(dead.attempt).isEqualTo(1);
        assertThat(dead.error).isEqualTo(
                "WORKFLOW: execution lease expired after attempt 1 of 1; retry budget exhausted");
        assertThat(dead.completedAt).isNotNull();
        assertThat(dead.updatedAt).isNotNull();
        assertThat(dead.leaseOwner).isNull();
        assertThat(dead.leaseUntil).isNull();

        var events = store.pollPendingEvents(10);
        assertThat(events).extracting(event -> event.eventType).containsExactly(
                WorkflowRunEventRecord.TYPE_ACCEPTED, WorkflowRunEventRecord.TYPE_DEAD);
        WorkflowRunEvent terminal = WorkflowRunEvent.parseFrom(events.get(1).payload);
        assertThat(terminal.getJobId()).isEqualTo(job.jobId.toString());
        assertThat(terminal.getAttempt()).isEqualTo(1);
        assertThat(terminal.getError()).isEqualTo(dead.error);
        assertThat(terminal.getType()).isEqualTo(WorkflowRunEvent.Type.TYPE_DEAD);

        assertThatThrownBy(() -> store.markCompleted(WorkerClaim.from(claimed), "{}", "late",
                WorkflowRunEventFactory.completed(claimed, "late")))
                .isInstanceOf(ClaimLostException.class);
        assertThat(store.requeueExpiredLeases()).isZero();
        assertThat(store.claim("replacement", Duration.ofMinutes(1))).isEmpty();
        assertThat(store.pollPendingEvents(10)).hasSize(2);
    }

    @Test
    void anEarlierExpiredAttemptRequeuesButTheNextExpiredAttemptExhaustsTheBudget()
            throws Exception {
        WorkflowRunRecord job = newJob("two-attempt-lease");
        job.maxAttempts = 2;
        store.insert(job, WorkflowRunEventFactory.accepted(job));
        store.claim("first", Duration.ofMinutes(1)).orElseThrow();
        expireLease(job.jobId);

        assertThat(store.requeueExpiredLeases()).isEqualTo(1);
        WorkflowRunRecord retry = store.get(job.jobId).orElseThrow();
        assertThat(retry.status).isEqualTo(WorkflowRunRecord.STATUS_QUEUED);
        assertThat(retry.attempt).isEqualTo(1);
        assertThat(store.pollPendingEvents(10)).hasSize(1);

        WorkflowRunRecord second = store.claim("second", Duration.ofMinutes(1)).orElseThrow();
        assertThat(second.attempt).isEqualTo(2);
        expireLease(job.jobId);
        assertThat(store.requeueExpiredLeases()).isZero();
        assertThat(store.get(job.jobId).orElseThrow().status).isEqualTo(WorkflowRunRecord.STATUS_DEAD);
        assertThat(store.requeueExpiredLeases()).isZero();
        assertThat(store.pollPendingEvents(10)).extracting(event -> event.eventType)
                .containsExactly(WorkflowRunEventRecord.TYPE_ACCEPTED,
                        WorkflowRunEventRecord.TYPE_DEAD);
    }

    @Test
    void concurrentSweepersCreateOnlyOneDeadEvent() throws Exception {
        WorkflowRunRecord job = newJob("concurrent-exhaustion");
        job.maxAttempts = 1;
        store.insert(job, WorkflowRunEventFactory.accepted(job));
        store.claim("lost-worker", Duration.ofMinutes(1)).orElseThrow();
        expireLease(job.jobId);

        CountDownLatch start = new CountDownLatch(1);
        ConcurrentLinkedQueue<Integer> requeued = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<Throwable> failures = new ConcurrentLinkedQueue<>();
        Runnable sweep = () -> {
            try {
                start.await();
                requeued.add(store.requeueExpiredLeases());
            } catch (Throwable failure) {
                failures.add(failure);
            }
        };
        Thread first = Thread.ofVirtual().start(sweep);
        Thread second = Thread.ofVirtual().start(sweep);
        start.countDown();
        first.join(TimeUnit.SECONDS.toMillis(10));
        second.join(TimeUnit.SECONDS.toMillis(10));

        assertThat(first.isAlive() || second.isAlive()).isFalse();
        assertThat(failures).isEmpty();
        assertThat(requeued).containsExactlyInAnyOrder(0, 0);
        assertThat(store.get(job.jobId).orElseThrow().status).isEqualTo(WorkflowRunRecord.STATUS_DEAD);
        assertThat(store.pollPendingEvents(10)).extracting(event -> event.eventType)
                .containsExactly(WorkflowRunEventRecord.TYPE_ACCEPTED,
                        WorkflowRunEventRecord.TYPE_DEAD);
    }

    @Test
    void aDeadEventInsertFailureRollsBackTheExpiredClaimSettlement() {
        WorkflowRunRecord job = newJob("atomic-exhaustion");
        job.maxAttempts = 1;
        store.insert(job, WorkflowRunEventFactory.accepted(job));
        store.claim("lost-worker", Duration.ofMinutes(1)).orElseThrow();
        expireLease(job.jobId);

        database.inTransaction(c -> {
            try (var statement = c.createStatement()) {
                statement.execute("""
                        CREATE FUNCTION reject_dead_event() RETURNS trigger LANGUAGE plpgsql AS $$
                        BEGIN
                          IF NEW.event_type = 'DEAD' THEN
                            RAISE EXCEPTION 'injected outbox failure';
                          END IF;
                          RETURN NEW;
                        END
                        $$""");
                statement.execute("CREATE TRIGGER reject_dead_event BEFORE INSERT "
                        + "ON workflow_run_events_outbox FOR EACH ROW "
                        + "EXECUTE FUNCTION reject_dead_event()");
            } catch (java.sql.SQLException e) {
                throw new RuntimeException(e);
            }
            return null;
        });
        try {
            assertThatThrownBy(() -> store.requeueExpiredLeases())
                    .isInstanceOf(WorkflowRunStoreException.class);
            WorkflowRunRecord unchanged = store.get(job.jobId).orElseThrow();
            assertThat(unchanged.status).isEqualTo(WorkflowRunRecord.STATUS_RUNNING);
            assertThat(unchanged.completedAt).isNull();
            assertThat(unchanged.leaseOwner).isEqualTo("lost-worker");
            assertThat(store.pollPendingEvents(10)).hasSize(1);
        } finally {
            database.inTransaction(c -> {
                try (var statement = c.createStatement()) {
                    statement.execute("DROP TRIGGER reject_dead_event ON workflow_run_events_outbox");
                    statement.execute("DROP FUNCTION reject_dead_event()");
                } catch (java.sql.SQLException e) {
                    throw new RuntimeException(e);
                }
                return null;
            });
        }
        assertThat(store.requeueExpiredLeases()).isZero();
        assertThat(store.get(job.jobId).orElseThrow().status).isEqualTo(WorkflowRunRecord.STATUS_DEAD);
        assertThat(store.pollPendingEvents(10)).extracting(event -> event.eventType)
                .containsExactly(WorkflowRunEventRecord.TYPE_ACCEPTED,
                        WorkflowRunEventRecord.TYPE_DEAD);
    }

    @Test
    void parkedContinuationMayCompleteAfterItsClaimCountExceedsTheRetryLimit() {
        WorkflowRunRecord job = newJob("parked-continuation");
        job.maxAttempts = 1;
        store.insert(job, WorkflowRunEventFactory.accepted(job));
        WorkflowRunRecord first = store.claim("first", Duration.ofMinutes(1)).orElseThrow();
        store.markWaiting(WorkerClaim.from(first), "review", "[]",
                WorkflowRunEventFactory.waiting(first, "review"));
        WorkflowRunRecord parked = store.get(job.jobId).orElseThrow();
        assertThat(store.completeParkedStep(job.jobId, "review",
                "{\"name\":\"review\",\"skipped\":false,\"response\":{}}",
                WorkflowRunEventFactory.stepCheckpoint(parked, "review")))
                .isInstanceOf(ParkedCompletion.Completed.class);

        WorkflowRunRecord continuation = store.claim("second", Duration.ofMinutes(1)).orElseThrow();
        assertThat(continuation.attempt).isEqualTo(2);
        assertThat(continuation.maxAttempts).isEqualTo(1);
        store.markCompleted(WorkerClaim.from(continuation), "{}", "resumed",
                WorkflowRunEventFactory.completed(continuation, "resumed"));
        assertThat(store.get(job.jobId).orElseThrow().status)
                .isEqualTo(WorkflowRunRecord.STATUS_COMPLETED);
        assertThat(store.pollPendingEvents(10)).extracting(event -> event.eventType)
                .doesNotContain(WorkflowRunEventRecord.TYPE_DEAD);
    }

    private static void expireLease(UUID jobId) {
        database.inTransaction(c -> {
            try (var statement = c.prepareStatement(
                    "UPDATE workflow_run SET lease_until = clock_timestamp() - interval '1 second'"
                            + " WHERE job_id = ?")) {
                statement.setObject(1, jobId);
                assertThat(statement.executeUpdate()).isEqualTo(1);
            } catch (java.sql.SQLException e) {
                throw new RuntimeException(e);
            }
            return null;
        });
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
            while ((job = store.claim("worker-" + Thread.currentThread().threadId(),
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
    void anExpiredAttemptCannotOverwriteItsReplacementCompletion() {
        WorkflowRunRecord job = insert("claim-recovery");
        var stale = store.claim("old-worker", Duration.ofMinutes(1)).orElseThrow();
        database.inTransaction(c -> {
            try (var statement = c.prepareStatement(
                    "UPDATE workflow_run SET lease_until = now() - interval '1 second' WHERE job_id = ?")) {
                statement.setObject(1, job.jobId);
                statement.executeUpdate();
            } catch (java.sql.SQLException e) {
                throw new RuntimeException(e);
            }
            return null;
        });
        assertThat(store.requeueExpiredLeases()).isEqualTo(1);
        var current = store.claim("new-worker", Duration.ofMinutes(1)).orElseThrow();
        assertThat(current.attempt).isEqualTo(stale.attempt + 1);
        WorkerClaim currentClaim = WorkerClaim.from(current);
        WorkerClaim staleClaim = WorkerClaim.from(stale);
        store.markCompleted(currentClaim, "{\"text\":\"current\"}", "current result",
                WorkflowRunEventFactory.completed(current, "current result"));
        int events = store.pollPendingEvents(100).size();
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> store.markCompleted(
                staleClaim, "{\"text\":\"stale\"}", "stale result",
                WorkflowRunEventFactory.completed(stale, "stale result")))
                .isInstanceOf(ClaimLostException.class);
        assertThat(store.get(job.jobId).orElseThrow().verdict).isEqualTo("current result");
        assertThat(store.pollPendingEvents(100)).hasSize(events);
    }

    @Test
    void everyWorkerMutationRejectsStaleOwnerAttemptAndExpiredLeaseWithoutWrites() {
        List<Consumer<WorkerClaim>> mutations = List.of(
                claim -> store.saveCheckpoint(claim, "[{\"name\":\"late\"}]",
                        WorkflowRunEventFactory.stepCheckpoint(
                                claimedRecord(claim), "late")),
                claim -> store.markWaiting(claim, "late", "[]",
                        WorkflowRunEventFactory.waiting(claimedRecord(claim), "late")),
                claim -> store.markCompleted(claim, "{\"late\":true}", "late",
                        WorkflowRunEventFactory.completed(claimedRecord(claim), "late")),
                claim -> store.markFailed(claim, "late failure",
                        WorkflowRunEventFactory.failed(claimedRecord(claim), "late", "late failure")),
                claim -> store.markDead(claim, "late dead",
                        WorkflowRunEventFactory.dead(claimedRecord(claim), "late dead")),
                claim -> store.requeue(claim, Duration.ofMinutes(1)));
        List<String> invalidations = List.of("owner", "attempt", "expired");

        for (String invalidation : invalidations) {
            for (int i = 0; i < mutations.size(); i++) {
                WorkflowRunRecord job = insert("fenced-" + invalidation + "-" + i);
                WorkflowRunRecord claimed = store.claim("owner-original", Duration.ofMinutes(2))
                        .orElseThrow();
                WorkerClaim claim = WorkerClaim.from(claimed);
                invalidate(claimed.jobId, invalidation);
                WorkflowRunRecord before = store.get(claimed.jobId).orElseThrow();
                int eventCount = store.pollPendingEvents(100).size();
                int mutationIndex = i;

                assertThatThrownBy(() -> mutations.get(mutationIndex).accept(claim))
                        .as("mutation %s must reject %s claim", mutationIndex, invalidation)
                        .isInstanceOf(ClaimLostException.class);

                WorkflowRunRecord after = store.get(job.jobId).orElseThrow();
                assertThat(after).usingRecursiveComparison().isEqualTo(before);
                assertThat(store.pollPendingEvents(100)).hasSize(eventCount);
            }
        }
    }

    private static WorkflowRunRecord claimedRecord(WorkerClaim claim) {
        return store.get(claim.jobId()).orElseThrow();
    }

    private void invalidate(UUID jobId, String kind) {
        String sql = switch (kind) {
            case "owner" -> "UPDATE workflow_run SET lease_owner = 'replacement-worker' "
                    + "WHERE job_id = ?";
            case "attempt" -> "UPDATE workflow_run SET attempt = attempt + 1 WHERE job_id = ?";
            case "expired" -> "UPDATE workflow_run SET lease_until = now() - interval '1 second' "
                    + "WHERE job_id = ?";
            default -> throw new IllegalArgumentException("unknown invalidation: " + kind);
        };
        database.inTransaction(c -> {
            try (var statement = c.prepareStatement(sql)) {
                statement.setObject(1, jobId);
                statement.executeUpdate();
            } catch (java.sql.SQLException e) {
                throw new RuntimeException(e);
            }
            return null;
        });
    }

    @Test
    void saveCheckpointIsAtomicWithItsStepEvent() throws Exception {
        WorkflowRunRecord job = insert("embed-text");
        WorkflowRunRecord claimed = store.claim("worker-1", Duration.ofMinutes(1)).orElseThrow();
        String checkpoints = "[{\"name\": \"tokenize\", \"skipped\": false,"
                + " \"response\": {\"ids\": [\"104\"]}}]";
        store.saveCheckpoint(WorkerClaim.from(claimed), checkpoints,
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
        WorkflowRunRecord completedClaimed = store.claim("worker-1", Duration.ofMinutes(1))
                .orElseThrow();
        store.markCompleted(WorkerClaim.from(completedClaimed), "{\"ok\": true}",
                "2 steps, output t.T",
                WorkflowRunEventFactory.completed(completedClaimed, "2 steps, output t.T"));
        WorkflowRunRecord doneRow = store.get(completed.jobId).orElseThrow();
        assertThat(doneRow.status).isEqualTo(WorkflowRunRecord.STATUS_COMPLETED);
        assertThat(doneRow.verdict).isEqualTo("2 steps, output t.T");
        assertThat(doneRow.result).contains("\"ok\": true");
        assertThat(doneRow.completedAt).isNotNull();
        assertThat(doneRow.leaseOwner).isNull();

        WorkflowRunRecord failed = insert("workflow-fail");
        WorkflowRunRecord failedClaimed = store.claim("worker-1", Duration.ofMinutes(1))
                .orElseThrow();
        store.markFailed(WorkerClaim.from(failedClaimed), "VALIDATION: nope",
                WorkflowRunEventFactory.failed(failedClaimed, "embed", "VALIDATION: nope"));
        WorkflowRunRecord failedRow = store.get(failed.jobId).orElseThrow();
        assertThat(failedRow.status).isEqualTo(WorkflowRunRecord.STATUS_FAILED);
        assertThat(failedRow.error).isEqualTo("VALIDATION: nope");
        assertThat(failedRow.completedAt).isNotNull();

        WorkflowRunRecord dead = insert("workflow-dead");
        WorkflowRunRecord deadClaimed = store.claim("worker-1", Duration.ofMinutes(1))
                .orElseThrow();
        store.markDead(WorkerClaim.from(deadClaimed), "GRPC: UNAVAILABLE",
                WorkflowRunEventFactory.dead(deadClaimed, "GRPC: UNAVAILABLE"));
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
        WorkflowRunRecord claimed = store.claim("worker-1", Duration.ofMinutes(1)).orElseThrow();
        String prefix = "[{\"name\": \"tokenize\", \"skipped\": false,"
                + " \"response\": {\"ids\": [\"104\"]}}]";
        store.markWaiting(WorkerClaim.from(claimed), "review", prefix,
                WorkflowRunEventFactory.waiting(claimed, "review"));
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
    void competingCompletionsCommitOnlyOneResponseAndEvent() throws Exception {
        WorkflowRunRecord job = parkedReview();
        var results = raceCompletion(job, false);
        assertThat(results.stream().filter(ParkedCompletion.Completed.class::isInstance)).hasSize(1);
        assertThat(results.stream().filter(ParkedCompletion.Conflict.class::isInstance)).hasSize(1);
        WorkflowRunRecord stored = store.get(job.jobId).orElseThrow();
        assertThat(stored.status).isEqualTo("QUEUED");
        assertThat(store.pollPendingEvents(100)).hasSize(3); // accepted, waiting, one checkpoint

        // A new store instance sees the same durable identity, including after settlement.
        WorkflowRunRecord completedClaim = store.claim("worker-1", Duration.ofMinutes(1))
                .orElseThrow();
        store.markCompleted(WorkerClaim.from(completedClaim), "{}", "accepted",
                WorkflowRunEventFactory.completed(completedClaim, "accepted"));
        var restarted = new JdbcWorkflowRunStore(database);
        String response = stored.checkpoints.contains("first") ? "first" : "second";
        assertThat(restarted.completeParkedStep(job.jobId, "review", completion(response),
                WorkflowRunEventFactory.stepCheckpoint(stored, "review")))
                .isInstanceOf(ParkedCompletion.AlreadyDone.class);
        assertThat(restarted.completeParkedStep(job.jobId, "review", completion("changed"),
                WorkflowRunEventFactory.stepCheckpoint(stored, "review")))
                .isInstanceOf(ParkedCompletion.Conflict.class);
        assertThat(store.get(job.jobId).orElseThrow().status).isEqualTo("COMPLETED");
        assertThat(store.pollPendingEvents(100)).hasSize(4);
    }

    @Test
    void validationRejectionAndSuccessfulCompletionHaveOneAtomicWinner() throws Exception {
        WorkflowRunRecord job = parkedReview();
        var results = raceCompletion(job, true);
        WorkflowRunRecord stored = store.get(job.jobId).orElseThrow();
        if (results.stream().anyMatch(ParkedCompletion.Completed.class::isInstance)) {
            assertThat(results.stream().filter(ParkedCompletion.Conflict.class::isInstance)).hasSize(1);
            assertThat(stored.status).isEqualTo("QUEUED");
            assertThat(stored.checkpoints).contains("first").doesNotContain("second");
            assertThat(stored.error).isNull();
        } else {
            assertThat(results.stream().filter(ParkedCompletion.Rejected.class::isInstance)).hasSize(1);
            assertThat(results.stream().filter(ParkedCompletion.WrongState.class::isInstance)).hasSize(1);
            assertThat(stored.status).isEqualTo("FAILED");
            assertThat(stored.checkpoints).isEqualTo("[]");
        }
        assertThat(store.pollPendingEvents(100)).hasSize(3);
    }

    @Test
    void aDelayedValidationRejectionCannotFailAnAlreadyCompletedStep() {
        WorkflowRunRecord job = parkedReview();
        assertThat(store.completeParkedStep(job.jobId, "review", completion("first"),
                WorkflowRunEventFactory.stepCheckpoint(job, "review")))
                .isInstanceOf(ParkedCompletion.Completed.class);
        assertThat(store.completeParkedStep(job.jobId, "review", completion("bad"),
                WorkflowRunEventFactory.failed(job, "review", "invalid"), "invalid"))
                .isInstanceOf(ParkedCompletion.Conflict.class);
        assertThat(store.get(job.jobId).orElseThrow().status).isEqualTo("QUEUED");
        assertThat(store.pollPendingEvents(100)).hasSize(3);
    }

    private WorkflowRunRecord parkedReview() {
        WorkflowRunRecord job = insert("review-workflow");
        WorkflowRunRecord claimed = store.claim("worker-1", Duration.ofMinutes(1)).orElseThrow();
        store.markWaiting(WorkerClaim.from(claimed), "review", "[]",
                WorkflowRunEventFactory.waiting(claimed, "review"));
        return store.get(job.jobId).orElseThrow();
    }

    private static String completion(String notes) {
        return "{\"name\":\"review\",\"skipped\":false,\"response\":{\"notes\":\"" + notes + "\"}}";
    }

    private List<ParkedCompletion> raceCompletion(WorkflowRunRecord job, boolean rejectSecond)
            throws Exception {
        CountDownLatch start = new CountDownLatch(1);
        try (var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var first = executor.submit(() -> {
                start.await();
                return store.completeParkedStep(job.jobId, "review", completion("first"),
                        WorkflowRunEventFactory.stepCheckpoint(job, "review"));
            });
            var second = executor.submit(() -> {
                start.await();
                return store.completeParkedStep(job.jobId, "review", completion("second"),
                        rejectSecond ? WorkflowRunEventFactory.failed(job, "review", "invalid")
                                : WorkflowRunEventFactory.stepCheckpoint(job, "review"),
                        rejectSecond ? "invalid" : null);
            });
            start.countDown();
            return List.of(first.get(10, TimeUnit.SECONDS), second.get(10, TimeUnit.SECONDS));
        }
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
