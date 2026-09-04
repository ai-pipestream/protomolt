package ai.protomolt.proto.delegation;

import ai.protomolt.proto.delegation.v1.DelegateRequest;
import ai.protomolt.proto.delegation.v1.DelegateResponse;
import ai.protomolt.proto.delegation.v1.TaskMessage;
import ai.protomolt.proto.delegation.v1.TaskMessageKind;
import ai.protomolt.proto.delegation.v1.TaskSpec;
import ai.protomolt.proto.delegation.v1.Transcript;
import ai.protomolt.proto.delegation.v1.WorkerCapability;
import ai.protomolt.proto.delegation.v1.WorkerHello;
import com.google.protobuf.Timestamp;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static ai.protomolt.proto.delegation.DelegationFixtures.TASK;
import static ai.protomolt.proto.delegation.DelegationFixtures.WORKER;
import static ai.protomolt.proto.delegation.DelegationFixtures.spec;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TranscriptRepositoryCoordinatorTest {

    private static final Instant NOW = Instant.parse("2026-08-11T12:00:00Z");
    private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);

    @Test
    void constructorRestoresTranscriptEventsAndReducedTaskState() {
        TaskSpec taskSpec = spec("build");
        Transcript stored = new DelegationFixtures.TranscriptBuilder()
                .hello(WORKER)
                .admit(WORKER)
                .offer(TASK, WORKER, 1, taskSpec)
                .accept(TASK, WORKER, 1)
                .candidate(TASK, WORKER, 1, taskSpec)
                .accepted(TASK, WORKER, 1, "verified")
                .build();
        TranscriptRepository repository = repositoryContaining(stored);

        try (InProcessDelegationCoordinator restored = new InProcessDelegationCoordinator(
                AdmissionPolicy.allowAll(), CandidateReviewer.manual(), CLOCK, repository)) {
            assertThat(restored.transcript()).isEqualTo(stored);
            assertThat(restored.eventsAfter("", 0)).hasSize(stored.getEntriesCount());
            assertThat(restored.eventsAfter(TASK, 0)).hasSize(4);
            assertThat(restored.state().tasks().get(TASK).phase())
                    .isEqualTo(DelegationReducer.Phase.ACCEPTED);
        }
    }

    @Test
    void reconnectExpiresLeaseThatElapsedWhileCoordinatorWasOffline() {
        TaskSpec taskSpec = spec("build");
        Transcript stored = new DelegationFixtures.TranscriptBuilder()
                .hello(WORKER)
                .admit(WORKER)
                .offer(TASK, WORKER, 1, taskSpec)
                .accept(TASK, WORKER, 1)
                .build();
        InMemoryTranscriptRepository repository = new InMemoryTranscriptRepository();
        repository.save(stored);
        CapturingResponses responses = new CapturingResponses();

        try (InProcessDelegationCoordinator restored = new InProcessDelegationCoordinator(
                AdmissionPolicy.allowAll(), CandidateReviewer.manual(), CLOCK, repository)) {
            restored.delegate(responses).onNext(helloFrame(2, "restart-hello"));

            assertThat(responses.error).isNull();
            assertThat(responses.values).hasSize(2);
            assertThat(responses.values.get(0).hasAdmission()).isTrue();
            assertThat(responses.values.get(0).getSeq()).isEqualTo(2);
            assertThat(responses.values.get(1).hasExpired()).isTrue();
            assertThat(responses.values.get(1).getSeq()).isEqualTo(2);
            assertThat(restored.state().tasks().get(TASK).phase())
                    .isEqualTo(DelegationReducer.Phase.EXPIRED);
            assertThat(repository.load()).contains(restored.transcript());
        }
    }

    @Test
    void aCandidateAwaitingReviewDoesNotExpireWithItsLease() {
        InMemoryTranscriptRepository repository = new InMemoryTranscriptRepository();
        repository.save(candidateAwaitingReview());
        CapturingResponses responses = new CapturingResponses();

        try (InProcessDelegationCoordinator restored = new InProcessDelegationCoordinator(
                AdmissionPolicy.allowAll(), CandidateReviewer.manual(), CLOCK, repository)) {
            // The lease elapsed long before the restart, but the candidate is the
            // reviewer's to settle: neither the sweep nor the reconnect expires it.
            assertThat(restored.expireLeases(NOW)).isZero();
            restored.delegate(responses).onNext(helloFrame(2, "restart-hello"));
            assertThat(responses.error).isNull();
            assertThat(responses.values).hasSize(1);
            assertThat(responses.values.get(0).hasAdmission()).isTrue();
            assertThat(restored.state().tasks().get(TASK).phase())
                    .isEqualTo(DelegationReducer.Phase.CANDIDATE);

            restored.review(TASK, CandidateReviewer.ReviewDecision.accept(
                    "verified after the restart"));
            assertThat(responses.values).hasSize(2);
            assertThat(responses.values.get(1).hasAccepted()).isTrue();
            assertThat(responses.values.get(1).getSeq()).isEqualTo(2);
            assertThat(restored.state().tasks().get(TASK).phase())
                    .isEqualTo(DelegationReducer.Phase.ACCEPTED);
            assertThat(restored.state().clean()).isTrue();
        }
    }

    @Test
    void aVerdictForADisconnectedWorkerLandsOnTheTranscript() {
        InMemoryTranscriptRepository repository = new InMemoryTranscriptRepository();
        repository.save(candidateAwaitingReview());

        try (InProcessDelegationCoordinator restored = new InProcessDelegationCoordinator(
                AdmissionPolicy.allowAll(), CandidateReviewer.manual(), CLOCK, repository)) {
            assertThat(restored.workers().getFirst().connected()).isFalse();

            TaskMessage note = restored.sendMessage(WORKER, TASK,
                    TaskMessageKind.TASK_MESSAGE_KIND_NOTE, "reviewing the candidate now",
                    "", List.of());
            restored.review(TASK, CandidateReviewer.ReviewDecision.accept(
                    "verified while the worker was away"));

            List<InProcessDelegationCoordinator.Event> events = restored.eventsAfter(TASK, 0);
            assertThat(events).hasSize(5);
            assertThat(events.get(3).entry().getCoordinatorFrame().getTaskMessage())
                    .isEqualTo(note);
            DelegateResponse accepted = events.get(4).entry().getCoordinatorFrame();
            assertThat(accepted.hasAccepted()).isTrue();
            assertThat(accepted.getSeq()).isEqualTo(2);
            assertThat(restored.state().tasks().get(TASK).phase())
                    .isEqualTo(DelegationReducer.Phase.ACCEPTED);
            assertThat(repository.load()).contains(restored.transcript());

            // The worker's replacement stream continues the scopes past the recorded
            // verdict; it reads the verdict itself from the event feed.
            CapturingResponses responses = new CapturingResponses();
            restored.delegate(responses).onNext(helloFrame(2, "late-hello"));
            assertThat(responses.error).isNull();
            assertThat(responses.values).hasSize(1);
            assertThat(responses.values.get(0).hasAdmission()).isTrue();
            assertThat(restored.workers().getFirst().connected()).isTrue();
            assertThat(restored.state().clean()).isTrue();
        }
    }

    @Test
    void aRevisionRequestReArmsTheLease() {
        InMemoryTranscriptRepository repository = new InMemoryTranscriptRepository();
        repository.save(candidateAwaitingReview());

        try (InProcessDelegationCoordinator restored = new InProcessDelegationCoordinator(
                AdmissionPolicy.allowAll(), CandidateReviewer.manual(), CLOCK, repository)) {
            restored.review(TASK, CandidateReviewer.ReviewDecision.revise(
                    "the build check did not run", List.of("build")));

            List<InProcessDelegationCoordinator.Event> events = restored.eventsAfter(TASK, 0);
            assertThat(events).hasSize(5);
            assertThat(events.get(3).entry().getCoordinatorFrame().hasRevisionRequested())
                    .isTrue();
            DelegateResponse renewal = events.get(4).entry().getCoordinatorFrame();
            assertThat(renewal.hasRenewal()).isTrue();
            assertThat(renewal.getRenewal().getExpiresAt().getSeconds())
                    .isEqualTo(NOW.plusSeconds(300).getEpochSecond());
            assertThat(restored.state().tasks().get(TASK).phase())
                    .isEqualTo(DelegationReducer.Phase.LEASED);
            assertThat(restored.state().clean()).isTrue();

            // The worker has the whole lease again from the moment of the request.
            assertThat(restored.expireLeases(NOW.plusSeconds(299))).isZero();
            assertThat(restored.expireLeases(NOW.plusSeconds(300))).isEqualTo(1);
            assertThat(restored.state().tasks().get(TASK).phase())
                    .isEqualTo(DelegationReducer.Phase.EXPIRED);
        }
    }

    @Test
    void persistenceFailureDoesNotEmitOfferMutateTaskOrConsumeSequence() {
        FailingRepository repository = new FailingRepository();
        CapturingResponses responses = new CapturingResponses();
        try (InProcessDelegationCoordinator coordinator = new InProcessDelegationCoordinator(
                AdmissionPolicy.allowAll(), CandidateReviewer.manual(), CLOCK, repository)) {
            StreamObserver<DelegateRequest> worker = coordinator.delegate(responses);
            worker.onNext(helloFrame(1));
            assertThat(responses.values).hasSize(1);
            assertThat(repository.current.getEntriesCount()).isEqualTo(2);
            repository.failWrites = true;

            assertThatThrownBy(() -> coordinator.offer(WORKER, TASK, spec("build"),
                    Duration.ofMinutes(5)))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("repository unavailable");
            assertThat(coordinator.transcript().getEntriesCount()).isEqualTo(2);
            assertThat(coordinator.eventsAfter(TASK, 0)).isEmpty();
            assertThat(coordinator.state().tasks()).doesNotContainKey(TASK);
            assertThat(responses.values).hasSize(1);

            repository.failWrites = false;
            coordinator.offer(WORKER, TASK, spec("build"), Duration.ofMinutes(5));
            DelegateResponse offer = responses.values.getLast();
            assertThat(offer.hasOffer()).isTrue();
            assertThat(offer.getSeq()).isEqualTo(1);
        }
    }

    @Test
    void persistenceFailureDoesNotDeduplicateWorkerFrameThatWasNeverStored() {
        FailingRepository repository = new FailingRepository();
        repository.failWrites = true;
        try (InProcessDelegationCoordinator coordinator = new InProcessDelegationCoordinator(
                AdmissionPolicy.allowAll(), CandidateReviewer.manual(), CLOCK, repository)) {
            CapturingResponses firstResponses = new CapturingResponses();
            coordinator.delegate(firstResponses).onNext(helloFrame(1));
            assertThat(firstResponses.error).isNotNull();
            assertThat(coordinator.transcript().getEntriesCount()).isZero();

            repository.failWrites = false;
            CapturingResponses retryResponses = new CapturingResponses();
            coordinator.delegate(retryResponses).onNext(helloFrame(1));
            assertThat(retryResponses.error).isNull();
            assertThat(retryResponses.values).hasSize(1);
            assertThat(coordinator.transcript().getEntriesCount()).isEqualTo(2);
        }
    }

    @Test
    void invalidStoredTranscriptFailsBeforeCoordinatorStarts() {
        Transcript invalid = new DelegationFixtures.TranscriptBuilder()
                .admit(WORKER)
                .build();

        assertThatThrownBy(() -> new InProcessDelegationCoordinator(
                AdmissionPolicy.allowAll(), CandidateReviewer.manual(), CLOCK,
                repositoryContaining(invalid)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("stored transcript is invalid");
    }

    /** A worker whose 300-second lease elapsed in 2023 with its candidate still unreviewed. */
    private static Transcript candidateAwaitingReview() {
        TaskSpec taskSpec = spec("build");
        return new DelegationFixtures.TranscriptBuilder()
                .hello(WORKER)
                .admit(WORKER)
                .offer(TASK, WORKER, 1, taskSpec)
                .accept(TASK, WORKER, 1)
                .candidate(TASK, WORKER, 1, taskSpec)
                .build();
    }

    private static TranscriptRepository repositoryContaining(Transcript transcript) {
        return new TranscriptRepository() {
            @Override
            public Optional<Transcript> load() {
                return Optional.of(transcript);
            }

            @Override
            public void save(Transcript candidate) {
                throw new AssertionError("save was not expected");
            }
        };
    }

    private static DelegateRequest helloFrame(long sequence) {
        return helloFrame(sequence, "durability-hello");
    }

    private static DelegateRequest helloFrame(long sequence, String frameSeed) {
        return DelegateRequest.newBuilder()
                .setFrameId(UUID.nameUUIDFromBytes(frameSeed
                        .getBytes(java.nio.charset.StandardCharsets.UTF_8)).toString())
                .setSeq(sequence)
                .setSentAt(Timestamp.newBuilder().setSeconds(NOW.getEpochSecond()))
                .setHello(WorkerHello.newBuilder()
                        .setWorkerId(WORKER)
                        .setProtocolVersion(1)
                        .setProvider("scripted")
                        .setModel("deterministic")
                        .addCapabilities(WorkerCapability.newBuilder()
                                .setName("java-build")))
                .build();
    }

    private static final class FailingRepository implements TranscriptRepository {
        private Transcript current = Transcript.getDefaultInstance();
        private boolean failWrites;

        @Override
        public Optional<Transcript> load() {
            return current.getEntriesCount() == 0 ? Optional.empty() : Optional.of(current);
        }

        @Override
        public void save(Transcript transcript) {
            if (failWrites) {
                throw new IllegalStateException("repository unavailable");
            }
            current = transcript;
        }
    }

    private static final class CapturingResponses implements StreamObserver<DelegateResponse> {
        private final List<DelegateResponse> values = new ArrayList<>();
        private Throwable error;

        @Override
        public void onNext(DelegateResponse value) {
            values.add(value);
        }

        @Override
        public void onError(Throwable throwable) {
            error = throwable;
        }

        @Override
        public void onCompleted() {
        }
    }
}
