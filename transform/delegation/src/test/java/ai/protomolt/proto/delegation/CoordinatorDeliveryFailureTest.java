package ai.protomolt.proto.delegation;

import ai.protomolt.proto.delegation.v1.DelegateRequest;
import ai.protomolt.proto.delegation.v1.DelegateResponse;
import ai.protomolt.proto.delegation.v1.Transcript;
import ai.protomolt.proto.delegation.v1.WorkerCapability;
import ai.protomolt.proto.delegation.v1.WorkerHello;
import com.google.protobuf.Timestamp;
import io.grpc.stub.StreamObserver;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;

import static ai.protomolt.proto.delegation.DelegationFixtures.TASK;
import static ai.protomolt.proto.delegation.DelegationFixtures.WORKER;
import static ai.protomolt.proto.delegation.DelegationFixtures.spec;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** A failed worker stream must not roll back a persisted coordinator decision. */
class CoordinatorDeliveryFailureTest {

    private static final Instant NOW = Instant.parse("2026-08-11T12:00:00Z");
    private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);

    @Test
    void acceptedReviewStaysCommittedWhenTheWorkerObserverThrows() {
        Repository repository = new Repository(candidateAwaitingReview());
        FailingResponses responses = new FailingResponses();
        try (var coordinator = coordinator(repository)) {
            coordinator.delegate(responses).onNext(helloFrame());
            assertThat(responses.values).hasSize(1);

            coordinator.review(TASK, 1, 1, CandidateReviewer.ReviewDecision.accept("verified"));

            assertThat(coordinator.state().clean()).isTrue();
            assertThat(coordinator.state().tasks().get(TASK).phase())
                    .isEqualTo(DelegationReducer.Phase.ACCEPTED);
            assertThat(repository.current).isEqualTo(coordinator.transcript());
            assertThat(repository.current.getEntriesList().getLast()
                    .getCoordinatorFrame().hasAccepted()).isTrue();
            assertThat(coordinator.workers().getFirst().connected()).isFalse();
            assertThat(responses.values).hasSize(1);
            assertThatThrownBy(() -> coordinator.review(TASK, 1, 1,
                    CandidateReviewer.ReviewDecision.accept("duplicate")))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("no candidate");
        }
    }

    @Test
    void revisionAndRenewalStayCommittedAfterDeliveryFailure() {
        Repository repository = new Repository(candidateAwaitingReview());
        FailingResponses responses = new FailingResponses();
        try (var coordinator = coordinator(repository)) {
            coordinator.delegate(responses).onNext(helloFrame());

            coordinator.review(TASK, 1, 1, CandidateReviewer.ReviewDecision.revise(
                    "recheck the edge case", List.of("build")));

            assertThat(coordinator.state().clean()).isTrue();
            assertThat(coordinator.state().tasks().get(TASK).phase())
                    .isEqualTo(DelegationReducer.Phase.LEASED);
            assertThat(repository.current).isEqualTo(coordinator.transcript());
            assertThat(repository.current.getEntriesList().stream()
                    .filter(entry -> entry.getCoordinatorFrame().hasRevisionRequested())).hasSize(1);
            assertThat(repository.current.getEntriesList().stream()
                    .filter(entry -> entry.getCoordinatorFrame().hasRenewal())).hasSize(1);
            assertThat(coordinator.workers().getFirst().connected()).isFalse();
        }
    }

    @Test
    void cancellationStaysCommittedAfterDeliveryFailure() {
        Repository repository = new Repository(candidateAwaitingReview());
        FailingResponses responses = new FailingResponses();
        try (var coordinator = coordinator(repository)) {
            coordinator.delegate(responses).onNext(helloFrame());

            coordinator.cancel(TASK, "cancel this attempt");

            assertThat(coordinator.state().tasks().get(TASK).phase())
                    .isEqualTo(DelegationReducer.Phase.CANCELLED);
            assertThat(repository.current).isEqualTo(coordinator.transcript());
            assertThat(repository.current.getEntriesList().getLast()
                    .getCoordinatorFrame().hasCancellation()).isTrue();
            assertThat(coordinator.workers().getFirst().connected()).isFalse();
        }
    }

    @Test
    void failedPersistenceLeavesCandidateAndStreamUntouched() {
        Repository repository = new Repository(candidateAwaitingReview());
        FailingResponses responses = new FailingResponses();
        try (var coordinator = coordinator(repository)) {
            coordinator.delegate(responses).onNext(helloFrame());
            Transcript before = coordinator.transcript();
            repository.failWrites = true;

            assertThatThrownBy(() -> coordinator.review(TASK, 1, 1,
                    CandidateReviewer.ReviewDecision.accept("verified")))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("repository unavailable");
            assertThat(coordinator.transcript()).isEqualTo(before);
            assertThat(coordinator.state().tasks().get(TASK).phase())
                    .isEqualTo(DelegationReducer.Phase.CANDIDATE);
            assertThat(coordinator.workers().getFirst().connected()).isTrue();
            assertThat(responses.values).hasSize(1);
        }
    }

    private static InProcessDelegationCoordinator coordinator(Repository repository) {
        return new InProcessDelegationCoordinator(
                AdmissionPolicy.allowAll(), CandidateReviewer.manual(), CLOCK, repository);
    }

    private static Transcript candidateAwaitingReview() {
        var taskSpec = spec("build");
        return new DelegationFixtures.TranscriptBuilder()
                .hello(WORKER).admit(WORKER)
                .offer(TASK, WORKER, 1, taskSpec)
                .accept(TASK, WORKER, 1)
                .candidate(TASK, WORKER, 1, taskSpec)
                .build();
    }

    private static DelegateRequest helloFrame() {
        return DelegateRequest.newBuilder()
                .setFrameId(UUID.randomUUID().toString())
                .setSeq(2)
                .setSentAt(Timestamp.newBuilder().setSeconds(NOW.getEpochSecond()))
                .setHello(WorkerHello.newBuilder()
                        .setWorkerId(WORKER).setProtocolVersion(1)
                        .setProvider("scripted").setModel("deterministic")
                        .addCapabilities(WorkerCapability.newBuilder().setName("java-build")))
                .build();
    }

    private static final class Repository implements TranscriptRepository {
        private Transcript current;
        private boolean failWrites;

        Repository(Transcript current) { this.current = current; }

        @Override public Optional<Transcript> load() { return Optional.of(current); }

        @Override public void save(Transcript transcript) {
            if (failWrites) throw new IllegalStateException("repository unavailable");
            current = transcript;
        }
    }

    private static final class FailingResponses implements StreamObserver<DelegateResponse> {
        private final List<DelegateResponse> values = new ArrayList<>();

        @Override public void onNext(DelegateResponse value) {
            if (value.hasAccepted() || value.hasRevisionRequested()
                    || value.hasCancellation()) {
                throw new IllegalStateException("worker stream closed");
            }
            values.add(value);
        }

        @Override public void onError(Throwable error) { throw new AssertionError(error); }

        @Override public void onCompleted() { }
    }
}
