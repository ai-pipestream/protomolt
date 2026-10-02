package ai.protomolt.proto.delegation;

import ai.protomolt.proto.delegation.lifecycle.InMemoryTranscriptRepository;

import ai.protomolt.proto.delegation.lifecycle.DelegationReducer;

import ai.protomolt.proto.delegation.v1.DelegateRequest;
import ai.protomolt.proto.delegation.v1.DelegateResponse;
import ai.protomolt.proto.delegation.v1.TaskAccept;
import ai.protomolt.proto.delegation.v1.WorkerCapability;
import ai.protomolt.proto.delegation.v1.WorkerHello;
import com.google.protobuf.util.Timestamps;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

import static ai.protomolt.proto.delegation.DelegationFixtures.TASK;
import static ai.protomolt.proto.delegation.DelegationFixtures.WORKER;
import static ai.protomolt.proto.delegation.DelegationFixtures.spec;
import static org.assertj.core.api.Assertions.assertThat;

/** A superseded direct stream cannot publish frames after another hello takes its session. */
class DelegationSupersededStreamTest {
    private static final Instant NOW = Instant.parse("2026-10-01T12:00:00Z");

    @Test
    void oldStreamCannotAppendAValidNextSequenceFrameOrDisconnectReplacement() {
        try (var coordinator = new InProcessDelegationCoordinator(AdmissionPolicy.allowAll(),
                CandidateReviewer.manual(), Clock.fixed(NOW, ZoneOffset.UTC),
                new InMemoryTranscriptRepository())) {
            var oldResponses = new Responses();
            StreamObserver<DelegateRequest> old = coordinator.delegate(oldResponses);
            old.onNext(hello(1));
            assertThat(oldResponses.error).isNull();
            assertThat(oldResponses.values).singleElement().satisfies(response ->
                    assertThat(response.getAdmission().getAdmitted()).isTrue());
            coordinator.offer(WORKER, TASK, spec("stale-stream-fence"), Duration.ofMinutes(5));

            var replacementResponses = new Responses();
            StreamObserver<DelegateRequest> replacement = coordinator.delegate(replacementResponses);
            replacement.onNext(hello(2));
            assertThat(replacementResponses.error).isNull();
            assertThat(replacementResponses.values).singleElement().satisfies(response ->
                    assertThat(response.getAdmission().getAdmitted()).isTrue());
            int beforeStale = coordinator.transcript().getEntriesCount();
            assertThat(coordinator.workers()).singleElement().satisfies(worker ->
                    assertThat(worker.connected()).isTrue());

            old.onNext(accept());
            assertThat(Status.fromThrowable(oldResponses.error).getCode())
                    .isEqualTo(Status.Code.INVALID_ARGUMENT);
            assertThat(coordinator.transcript().getEntriesCount()).isEqualTo(beforeStale);
            assertThat(coordinator.state().tasks().get(TASK).phase())
                    .isEqualTo(DelegationReducer.Phase.OFFERED);
            assertThat(coordinator.workers()).singleElement().satisfies(worker ->
                    assertThat(worker.connected()).isTrue());

            DelegateRequest currentAccept = accept();
            replacement.onNext(currentAccept);
            assertThat(replacementResponses.error).isNull();
            assertThat(coordinator.transcript().getEntriesCount()).isEqualTo(beforeStale + 1);
            assertThat(coordinator.state().tasks().get(TASK).phase())
                    .isEqualTo(DelegationReducer.Phase.LEASED);
            assertThat(coordinator.state().clean()).isTrue();

            replacement.onNext(currentAccept); // Exact replay on the current stream remains harmless.
            assertThat(replacementResponses.error).isNull();
            assertThat(coordinator.transcript().getEntriesCount()).isEqualTo(beforeStale + 1);
        }
    }

    private static DelegateRequest hello(long sequence) {
        return DelegateRequest.newBuilder().setFrameId(UUID.randomUUID().toString())
                .setSeq(sequence).setSentAt(Timestamps.fromMillis(NOW.toEpochMilli()))
                .setHello(WorkerHello.newBuilder().setWorkerId(WORKER).setProtocolVersion(1)
                        .setProvider("stale-stream-test")
                        .addCapabilities(WorkerCapability.newBuilder().setName("structured-delegation")))
                .build();
    }

    private static DelegateRequest accept() {
        return DelegateRequest.newBuilder().setFrameId(UUID.randomUUID().toString())
                .setTaskId(TASK).setSeq(1).setSentAt(Timestamps.fromMillis(NOW.toEpochMilli()))
                .setAccept(TaskAccept.newBuilder().setAttempt(1)).build();
    }

    private static final class Responses implements StreamObserver<DelegateResponse> {
        private final List<DelegateResponse> values = new ArrayList<>();
        private Throwable error;
        @Override public void onNext(DelegateResponse value) { values.add(value); }
        @Override public void onError(Throwable failure) { error = failure; }
        @Override public void onCompleted() { }
    }
}
