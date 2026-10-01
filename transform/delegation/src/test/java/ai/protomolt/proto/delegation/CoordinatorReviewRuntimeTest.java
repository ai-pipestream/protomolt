package ai.protomolt.proto.delegation;

import ai.protomolt.proto.delegation.v1.AgentDelegationServiceGrpc;
import ai.protomolt.proto.delegation.v1.CheckEvidence;
import ai.protomolt.proto.delegation.v1.CheckVerdict;
import ai.protomolt.proto.delegation.v1.CompletionCandidate;
import ai.protomolt.proto.delegation.v1.DelegateResponse;
import ai.protomolt.proto.delegation.v1.ReviewFailureCode;
import ai.protomolt.proto.delegation.v1.RetryCandidateReviewRequest;
import ai.protomolt.proto.delegation.v1.TaskSpec;
import ai.protomolt.proto.delegation.v1.Transcript;
import ai.protomolt.proto.delegation.v1.TranscriptEntry;
import ai.protomolt.proto.delegation.v1.WorkerCapability;
import ai.protomolt.proto.delegation.v1.WorkerHello;
import com.google.protobuf.Timestamp;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Runtime durability, deadline, recovery, and retry behavior for automatic candidate review. */
class CoordinatorReviewRuntimeTest {
    private static final String WORKER = "review-worker";
    private static final String TASK = DelegationFixtures.uuid("runtime-review-task");
    private static final Instant START = Instant.parse("2026-10-01T12:00:00Z");
    private static final Duration REVIEW_WINDOW = Duration.ofSeconds(30);

    @Test
    void candidateAndReviewStartCommitTogetherBeforeAnInfrastructureFailureIsRecorded()
            throws Exception {
        AtomicReference<Transcript> stored = new AtomicReference<>(Transcript.getDefaultInstance());
        AtomicInteger invocations = new AtomicInteger();
        try (Harness harness = new Harness(new SnapshotRepository(stored), new MutableClock(START),
                context -> {
                    invocations.incrementAndGet();
                    throw new IOException("private reviewer detail");
                })) {
            harness.start();
            harness.coordinator.offer(WORKER, TASK, DelegationFixtures.spec("build"),
                    Duration.ofMinutes(2));
            harness.releaseCandidate();

            TranscriptEntry started = harness.await(entry -> entry.hasCoordinatorFrame()
                    && entry.getCoordinatorFrame().hasReviewStarted());
            Transcript committed = stored.get();
            int startIndex = committed.getEntriesList().indexOf(started);
            assertThat(startIndex).isGreaterThan(0);
            assertThat(committed.getEntries(startIndex - 1).hasWorkerFrame()).isTrue();
            assertThat(committed.getEntries(startIndex - 1).getWorkerFrame().hasCompletion()).isTrue();
            assertThat(committed.getEntries(startIndex).getCoordinatorFrame().getSentAt())
                    .isEqualTo(committed.getEntries(startIndex).getCoordinatorFrame()
                            .getReviewStarted().getStartedAt());

            TranscriptEntry failed = harness.await(entry -> entry.hasCoordinatorFrame()
                    && entry.getCoordinatorFrame().hasReviewFailed());
            assertThat(failed.getCoordinatorFrame().getReviewFailed().getCode())
                    .isEqualTo(ReviewFailureCode.REVIEW_FAILURE_CODE_INFRASTRUCTURE);
            assertThat(harness.coordinator.state().clean()).isTrue();
            assertThat(harness.coordinator.state().tasks().get(TASK).review().status())
                    .isEqualTo(DelegationReducer.ReviewStatus.FAILED);
            assertThat(stored.get().toString()).doesNotContain("private reviewer detail");
            assertThat(invocations.get()).isEqualTo(1);
        }
    }

    @Test
    void unusableReviewerDecisionsBecomeInfrastructureFailures() throws Exception {
        for (CandidateReviewer reviewer : List.<CandidateReviewer>of(
                context -> null,
                context -> CandidateReviewer.ReviewDecision.accept(""))) {
            try (Harness harness = new Harness(new SnapshotRepository(new AtomicReference<>(
                    Transcript.getDefaultInstance())), new MutableClock(START), reviewer)) {
                harness.start();
                harness.coordinator.offer(WORKER, TASK, DelegationFixtures.spec("build"),
                        Duration.ofMinutes(2));
                harness.releaseCandidate();
                var failed = harness.await(entry -> entry.hasCoordinatorFrame()
                        && entry.getCoordinatorFrame().hasReviewFailed());
                assertThat(failed.getCoordinatorFrame().getReviewFailed().getCode())
                        .isEqualTo(ReviewFailureCode.REVIEW_FAILURE_CODE_INFRASTRUCTURE);
                assertThat(harness.coordinator.state().clean()).isTrue();
                assertThat(harness.coordinator.transcript().getEntriesList()).noneMatch(entry ->
                        entry.hasCoordinatorFrame() && entry.getCoordinatorFrame().hasAccepted());
            }
        }
    }

    @Test
    void pendingIsPersistedAsDeferredAndManualVerdictsBindOnlyAnOpenInvocation() throws Exception {
        AtomicReference<Transcript> stored = new AtomicReference<>(Transcript.getDefaultInstance());
        try (Harness harness = new Harness(new SnapshotRepository(stored), new MutableClock(START),
                context -> CandidateReviewer.ReviewDecision.pending())) {
            harness.start();
            harness.coordinator.offer(WORKER, TASK, DelegationFixtures.spec("build"),
                    Duration.ofMinutes(2));
            harness.releaseCandidate();
            TranscriptEntry deferred = harness.await(entry -> entry.hasCoordinatorFrame()
                    && entry.getCoordinatorFrame().hasReviewDeferred());
            assertThat(harness.coordinator.state().clean()).isTrue();
            assertThat(harness.coordinator.state().tasks().get(TASK).review().status())
                    .isEqualTo(DelegationReducer.ReviewStatus.DEFERRED);
            assertThat(harness.coordinator.eventsAfter(TASK, 0).stream().map(InProcessDelegationCoordinator.Event::entry))
                    .noneMatch(entry -> entry.hasCoordinatorFrame()
                            && entry.getCoordinatorFrame().hasReviewFailed());

            var deferredIdentity = deferred.getCoordinatorFrame().getReviewDeferred().getIdentity();
            assertThatThrownBy(() -> harness.coordinator.review(TASK, 1, 2,
                    CandidateReviewer.ReviewDecision.accept("verified")))
                    .isInstanceOf(IllegalStateException.class);
            harness.coordinator.review(TASK, 1, 1,
                    CandidateReviewer.ReviewDecision.accept("manual decision"));
            TranscriptEntry accepted = harness.coordinator.transcript().getEntriesList().stream()
                    .filter(entry -> entry.hasCoordinatorFrame()
                            && entry.getCoordinatorFrame().hasAccepted()).findFirst().orElseThrow();
            assertThat(accepted.getCoordinatorFrame().getAccepted().getReviewInvocationId()).isEmpty();
            assertThat(accepted.getCoordinatorFrame().getAccepted().getRevision()).isEqualTo(1);
            assertThat(deferredIdentity.getInvocationId()).isNotBlank();
        }
    }

    @Test
    void callbackAtDeadlineCannotAcceptAndAStaleCallbackCannotChangeManualOutcome() throws Exception {
        MutableClock clock = new MutableClock(START);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try (Harness harness = new Harness(new SnapshotRepository(new AtomicReference<>(
                Transcript.getDefaultInstance())), clock, context -> {
                    entered.countDown();
                    if (!release.await(20, TimeUnit.SECONDS)) throw new AssertionError("review was not released");
                    return CandidateReviewer.ReviewDecision.accept("late accept");
                })) {
            harness.start();
            harness.coordinator.offer(WORKER, TASK, DelegationFixtures.spec("build"),
                    Duration.ofMinutes(2));
            harness.releaseCandidate();
            TranscriptEntry started = harness.await(entry -> entry.hasCoordinatorFrame()
                    && entry.getCoordinatorFrame().hasReviewStarted());
            assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
            Timestamp deadline = started.getCoordinatorFrame().getReviewStarted().getDeadline();
            clock.set(Instant.ofEpochSecond(deadline.getSeconds(), deadline.getNanos()));
            release.countDown();
            TranscriptEntry failed = harness.await(entry -> entry.hasCoordinatorFrame()
                    && entry.getCoordinatorFrame().hasReviewFailed());
            assertThat(failed.getCoordinatorFrame().getReviewFailed().getCode())
                    .isEqualTo(ReviewFailureCode.REVIEW_FAILURE_CODE_DEADLINE);
            assertThat(harness.coordinator.transcript().getEntriesList()).noneMatch(entry ->
                    entry.hasCoordinatorFrame() && entry.getCoordinatorFrame().hasAccepted());
        } finally {
            release.countDown();
        }
    }

    @Test
    void manualVerdictEndsLiveInvocationAndLateAutomaticCallbackIsIgnored() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try (Harness harness = new Harness(new SnapshotRepository(new AtomicReference<>(
                Transcript.getDefaultInstance())), new MutableClock(START), context -> {
                    entered.countDown();
                    release.await();
                    return CandidateReviewer.ReviewDecision.revise("late revision", List.of("build"));
                })) {
            harness.start();
            harness.coordinator.offer(WORKER, TASK, DelegationFixtures.spec("build"),
                    Duration.ofMinutes(2));
            harness.releaseCandidate();
            TranscriptEntry started = harness.await(entry -> entry.hasCoordinatorFrame()
                    && entry.getCoordinatorFrame().hasReviewStarted());
            assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();

            harness.coordinator.review(TASK, 1, 1,
                    CandidateReviewer.ReviewDecision.accept("manual override"));
            release.countDown();
            TranscriptEntry accepted = harness.coordinator.transcript().getEntriesList().stream()
                    .filter(entry -> entry.hasCoordinatorFrame()
                            && entry.getCoordinatorFrame().hasAccepted()).findFirst().orElseThrow();
            assertThat(accepted.getCoordinatorFrame().getAccepted().getReviewInvocationId())
                    .isEqualTo(started.getCoordinatorFrame().getReviewStarted()
                            .getIdentity().getInvocationId());
            assertThat(harness.coordinator.transcript().getEntriesList()).noneMatch(entry ->
                    entry.hasCoordinatorFrame() && entry.getCoordinatorFrame().hasRevisionRequested());
            assertThat(harness.coordinator.state().clean()).isTrue();
        } finally {
            release.countDown();
        }
    }

    @Test
    void restartMarksCurrentStartedReviewInterruptedWithoutCallingReviewerAgain() throws Exception {
        AtomicReference<Transcript> stored = new AtomicReference<>(Transcript.getDefaultInstance());
        SnapshotRepository repository = new SnapshotRepository(stored);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger originalCalls = new AtomicInteger();
        Harness first = new Harness(repository, new MutableClock(START), context -> {
            originalCalls.incrementAndGet();
            entered.countDown();
            release.await();
            return CandidateReviewer.ReviewDecision.accept("late result");
        });
        first.start();
        first.coordinator.offer(WORKER, TASK, DelegationFixtures.spec("build"), Duration.ofMinutes(2));
        first.releaseCandidate();
        first.await(entry -> entry.hasCoordinatorFrame() && entry.getCoordinatorFrame().hasReviewStarted());
        assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
        first.close();

        AtomicInteger recoveredCalls = new AtomicInteger();
        try (InProcessDelegationCoordinator recovered = new InProcessDelegationCoordinator(
                AdmissionPolicy.allowAll(), context -> {
                    recoveredCalls.incrementAndGet();
                    return CandidateReviewer.ReviewDecision.accept("must not rerun");
                }, new MutableClock(START), repository, REVIEW_WINDOW, false, false)) {
            TranscriptEntry interrupted = stored.get().getEntriesList().stream()
                    .filter(entry -> entry.hasCoordinatorFrame()
                            && entry.getCoordinatorFrame().hasReviewFailed()).findFirst().orElseThrow();
            assertThat(interrupted.getCoordinatorFrame().getReviewFailed().getCode())
                    .isEqualTo(ReviewFailureCode.REVIEW_FAILURE_CODE_INTERRUPTED);
            assertThat(recovered.state().clean()).isTrue();
            assertThat(recoveredCalls.get()).isZero();
            assertThat(originalCalls.get()).isEqualTo(1);
        } finally {
            release.countDown();
        }
    }

    @Test
    void retryIsIdempotentAcrossTerminalStateAndChangedOrGlobalReuseConflicts() throws Exception {
        AtomicReference<Transcript> stored = new AtomicReference<>(Transcript.getDefaultInstance());
        AtomicInteger invocations = new AtomicInteger();
        try (Harness harness = new Harness(new SnapshotRepository(stored), new MutableClock(START),
                context -> {
                    if (invocations.incrementAndGet() == 1) throw new IOException("temporary failure");
                    return CandidateReviewer.ReviewDecision.accept("verified on retry");
                })) {
            harness.start();
            harness.coordinator.offer(WORKER, TASK, DelegationFixtures.spec("build"),
                    Duration.ofMinutes(2));
            harness.releaseCandidate();
            TranscriptEntry failed = harness.await(entry -> entry.hasCoordinatorFrame()
                    && entry.getCoordinatorFrame().hasReviewFailed());
            String previousInvocation = failed.getCoordinatorFrame().getReviewFailed()
                    .getIdentity().getInvocationId();
            RetryCandidateReviewRequest request = RetryCandidateReviewRequest.newBuilder()
                    .setTaskId(TASK).setAttempt(1).setRevision(1)
                    .setExpectedInvocationId(previousInvocation)
                    .setRetryId("11111111-1111-4111-8111-111111111111").build();
            var response = harness.coordinator.retryCandidateReview(request);
            TranscriptEntry accepted = harness.await(entry -> entry.hasCoordinatorFrame()
                    && entry.getCoordinatorFrame().hasAccepted());
            assertThat(accepted.getCoordinatorFrame().getAccepted().getReviewInvocationId())
                    .isEqualTo(response.getIdentity().getInvocationId());
            assertThat(harness.coordinator.retryCandidateReview(request)).isEqualTo(response);
            assertThat(invocations.get()).isEqualTo(2);

            assertThatThrownBy(() -> harness.coordinator.retryCandidateReview(request.toBuilder()
                    .setExpectedInvocationId("22222222-2222-4222-8222-222222222222").build()))
                    .isInstanceOf(RuntimeException.class);
            assertThat(invocations.get()).isEqualTo(2);
            assertThat(harness.coordinator.state().clean()).isTrue();
        }
    }

    @Test
    void candidateAndStartSaveFailureIsAllOrNothingAndUncertainCommitPoisonsStaleWriter()
            throws Exception {
        for (boolean commitThenFail : List.of(false, true)) {
            AtomicReference<Transcript> stored = new AtomicReference<>(Transcript.getDefaultInstance());
            AtomicInteger invocations = new AtomicInteger();
            SnapshotRepository repository = new SnapshotRepository(stored);
            try (Harness harness = new Harness(repository, new MutableClock(START), context -> {
                invocations.incrementAndGet();
                return CandidateReviewer.ReviewDecision.accept("not dispatched after uncertain save");
            })) {
                harness.start();
                harness.coordinator.offer(WORKER, TASK, DelegationFixtures.spec("build"),
                        Duration.ofMinutes(2));
                harness.await(entry -> entry.hasWorkerFrame() && entry.getWorkerFrame().hasAccept());
                repository.failNextCandidateSave(commitThenFail);
                harness.releaseCandidate();
                assertThat(repository.saveAttempted.await(10, TimeUnit.SECONDS)).isTrue();
                assertThat(repository.candidateAndStartPair()).isEqualTo(commitThenFail);
                assertThat(invocations.get()).isZero();
                if (commitThenFail) {
                    int writesAtFailure = repository.writes.get();
                    assertThatThrownBy(() -> harness.coordinator.cancel(TASK, "stale publication"))
                            .isInstanceOf(RuntimeException.class);
                    assertThat(repository.writes.get()).isEqualTo(writesAtFailure);
                    assertThatThrownBy(repository::load).isInstanceOf(RuntimeException.class);
                    Transcript committed = repository.rawCurrent();
                    assertThat(SnapshotRepository.containsCandidateAndStart(committed)).isTrue();
                } else {
                    assertThat(repository.rawCurrent().getEntriesList()).noneMatch(entry ->
                            entry.hasWorkerFrame() && entry.getWorkerFrame().hasCompletion());
                }
            }
        }
    }

    private static CompletionCandidate candidate(WorkerRunner.WorkerTask task) {
        CompletionCandidate.Builder candidate = CompletionCandidate.newBuilder()
                .setAttempt(task.offer().getAttempt()).setRevision(task.expectedRevision())
                .setSummary("candidate for runtime review")
                .addCommits(DelegationFixtures.commit("runtime-review"))
                .addArtifacts(DelegationFixtures.artifact("runtime-review-artifact"));
        task.offer().getSpec().getRequiredChecksList().forEach(check -> candidate.addEvidence(
                CheckEvidence.newBuilder().setCheckName(check.getName())
                        .setVerdict(CheckVerdict.CHECK_VERDICT_PASSED)
                        .setRanAt(Timestamp.newBuilder().setSeconds(START.getEpochSecond()))));
        return candidate.build();
    }

    private static final class Harness implements AutoCloseable {
        private final InMemoryTranscriptRepository inMemory = new InMemoryTranscriptRepository();
        private final TranscriptRepository repository;
        private final MutableClock clock;
        private final CandidateReviewer reviewer;
        private final CountDownLatch candidateGate = new CountDownLatch(1);
        private final InProcessDelegationCoordinator coordinator;
        private final String serverName = InProcessServerBuilder.generateName();
        private final Server server;
        private final ManagedChannel channel;
        private final DelegationWorker worker;

        Harness(TranscriptRepository repository, MutableClock clock, CandidateReviewer reviewer) throws Exception {
            this.repository = repository;
            this.clock = clock;
            this.reviewer = reviewer;
            coordinator = new InProcessDelegationCoordinator(AdmissionPolicy.allowAll(), reviewer,
                    clock, repository, REVIEW_WINDOW, false, false);
            server = InProcessServerBuilder.forName(serverName).addService(coordinator).build().start();
            channel = InProcessChannelBuilder.forName(serverName).build();
            WorkerHello hello = WorkerHello.newBuilder().setWorkerId(WORKER).setProtocolVersion(1)
                    .setProvider("runtime-test").setModel("scripted")
                    .addCapabilities(WorkerCapability.newBuilder().setName("java-build")).build();
            worker = new DelegationWorker(AgentDelegationServiceGrpc.newStub(channel),
                    new ScriptedWorkerRunner(hello, List.of((task, events) -> {
                        if (!candidateGate.await(5, TimeUnit.SECONDS)) {
                            throw new AssertionError("test did not release candidate gate");
                        }
                        return candidate(task);
                    })));
        }

        void start() throws Exception {
            worker.start();
            if (!worker.awaitAdmission(Duration.ofSeconds(10))) throw new AssertionError("worker admission timed out");
        }

        void releaseCandidate() { candidateGate.countDown(); }

        TranscriptEntry await(Predicate<TranscriptEntry> expected) throws Exception {
            long cursor = 0;
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (System.nanoTime() < until) {
                var event = coordinator.waitForEvent(TASK, cursor, Duration.ofSeconds(1));
                if (event.isPresent()) {
                    cursor = event.get().cursor();
                    if (expected.test(event.get().entry())) return event.get().entry();
                }
            }
            throw new AssertionError("timed out waiting for coordinator event; workerFailure="
                    + worker.streamFailure().map(Throwable::toString).orElse("none")
                    + "; transcript=" + coordinator.transcript()
                    + "; findings=" + coordinator.state().findings());
        }

        @Override public void close() throws Exception {
            candidateGate.countDown();
            worker.close();
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            coordinator.close();
            server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    private static final class SnapshotRepository implements TranscriptRepository {
        private final AtomicReference<Transcript> stored;
        private volatile boolean failCandidateSave;
        private volatile boolean commitThenFail;
        private final CountDownLatch saveAttempted = new CountDownLatch(1);
        private final AtomicInteger writes = new AtomicInteger();
        private volatile boolean poisoned;

        SnapshotRepository(AtomicReference<Transcript> stored) { this.stored = stored; }
        @Override public Optional<Transcript> load() {
            if (poisoned) throw new IllegalStateException("repository requires reconciliation");
            Transcript current = stored.get();
            return current.getEntriesCount() == 0 ? Optional.empty() : Optional.of(current);
        }
        @Override public synchronized void save(Transcript transcript) {
            writes.incrementAndGet();
            boolean pair = containsCandidateAndStart(transcript);
            if (pair && failCandidateSave) {
                failCandidateSave = false;
                if (commitThenFail) stored.set(transcript);
                poisoned = commitThenFail;
                saveAttempted.countDown();
                throw new IllegalStateException("injected uncertain transcript write");
            }
            stored.set(transcript);
        }
        void failNextCandidateSave(boolean commit) {
            commitThenFail = commit;
            failCandidateSave = true;
        }
        Transcript rawCurrent() { return stored.get(); }
        boolean candidateAndStartPair() { return containsCandidateAndStart(stored.get()); }
        private static boolean containsCandidateAndStart(Transcript transcript) {
            for (int i = 1; i < transcript.getEntriesCount(); i++) {
                if (transcript.getEntries(i - 1).hasWorkerFrame()
                        && transcript.getEntries(i - 1).getWorkerFrame().hasCompletion()
                        && transcript.getEntries(i).hasCoordinatorFrame()
                        && transcript.getEntries(i).getCoordinatorFrame().hasReviewStarted()) return true;
            }
            return false;
        }
    }

    private static final class MutableClock extends Clock {
        private final AtomicReference<Instant> instant;
        MutableClock(Instant initial) { instant = new AtomicReference<>(initial); }
        void set(Instant next) { instant.set(next); }
        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId zone) { return this; }
        @Override public Instant instant() { return instant.get(); }
    }
}
