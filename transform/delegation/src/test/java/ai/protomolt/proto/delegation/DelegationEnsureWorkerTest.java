package ai.protomolt.proto.delegation;

import ai.protomolt.proto.delegation.lifecycle.InMemoryTranscriptRepository;

import ai.protomolt.proto.delegation.v1.DelegateRequest;
import ai.protomolt.proto.delegation.v1.DelegateResponse;
import ai.protomolt.proto.delegation.v1.WorkerCapability;
import ai.protomolt.proto.delegation.v1.WorkerHello;
import com.google.protobuf.util.Timestamps;
import io.grpc.stub.StreamObserver;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

import static ai.protomolt.proto.delegation.DelegationFixtures.TASK;
import static ai.protomolt.proto.delegation.DelegationFixtures.WORKER;
import static ai.protomolt.proto.delegation.DelegationFixtures.spec;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DelegationEnsureWorkerTest {
    private static final Instant NOW = Instant.parse("2026-10-01T12:00:00Z");

    @Test
    void exactLostReplyAndConcurrentRetriesReuseOneBridgeSession() throws Exception {
        try (var coordinator = coordinator(new InMemoryTranscriptRepository());
             var bridge = new DelegationBridge(coordinator)) {
            WorkerHello hello = hello("scripted");
            var first = bridge.ensureWorker(hello);
            int afterFirst = coordinator.transcript().getEntriesCount();
            var replay = bridge.ensureWorker(hello);
            assertThat(replay).isEqualTo(first);
            assertThat(first.admitted()).isTrue();
            assertThat(coordinator.transcript().getEntriesCount()).isEqualTo(afterFirst);

            var ready = new CountDownLatch(2);
            var start = new CountDownLatch(1);
            try (var executor = Executors.newFixedThreadPool(2)) {
                var left = executor.submit(() -> {
                    ready.countDown();
                    assertThat(start.await(5, TimeUnit.SECONDS)).isTrue();
                    return bridge.ensureWorker(hello);
                });
                var right = executor.submit(() -> {
                    ready.countDown();
                    assertThat(start.await(5, TimeUnit.SECONDS)).isTrue();
                    return bridge.ensureWorker(hello);
                });
                assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
                start.countDown();
                assertThat(left.get(5, TimeUnit.SECONDS)).isEqualTo(first);
                assertThat(right.get(5, TimeUnit.SECONDS)).isEqualTo(first);
            }
            assertThat(coordinator.transcript().getEntriesCount()).isEqualTo(afterFirst);
            assertThatThrownBy(() -> bridge.ensureWorker(hello("changed")))
                    .isInstanceOf(WorkerRegistrationConflictException.class);
            assertThat(coordinator.transcript().getEntriesCount()).isEqualTo(afterFirst);
        }
    }

    @Test
    void connectedDirectSessionCannotBeAdoptedOrReplaced() {
        try (var coordinator = coordinator(new InMemoryTranscriptRepository());
             var bridge = new DelegationBridge(coordinator)) {
            var directResponses = new Responses();
            coordinator.delegate(directResponses).onNext(helloFrame(hello("scripted"), 1));
            assertThat(directResponses.error).isNull();
            int before = coordinator.transcript().getEntriesCount();
            assertThatThrownBy(() -> bridge.ensureWorker(hello("scripted")))
                    .isInstanceOf(WorkerRegistrationConflictException.class);
            assertThat(coordinator.transcript().getEntriesCount()).isEqualTo(before);
            assertThat(coordinator.workers()).singleElement().satisfies(worker ->
                    assertThat(worker.connected()).isTrue());
        }
    }

    @Test
    void supersededBridgeSessionIsNotReplayedEvenWithIdenticalHello() {
        try (var coordinator = coordinator(new InMemoryTranscriptRepository());
             var bridge = new DelegationBridge(coordinator)) {
            WorkerHello hello = hello("scripted");
            bridge.ensureWorker(hello);
            var directResponses = new Responses();
            coordinator.delegate(directResponses).onNext(helloFrame(hello, 2));
            assertThat(directResponses.error).isNull();
            int before = coordinator.transcript().getEntriesCount();
            assertThatThrownBy(() -> bridge.ensureWorker(hello))
                    .isInstanceOf(WorkerRegistrationConflictException.class);
            assertThat(coordinator.transcript().getEntriesCount()).isEqualTo(before);
            assertThat(coordinator.workers()).singleElement().satisfies(worker ->
                    assertThat(worker.connected()).isTrue());
        }
    }

    @Test
    void restoredCoordinatorEnsuresNewSessionAndResumesDurableCounters() {
        var repository = new InMemoryTranscriptRepository();
        try (var first = coordinator(repository); var bridge = new DelegationBridge(first)) {
            bridge.ensureWorker(hello("scripted"));
            bridge.offer(WORKER, TASK, spec("restart"), Duration.ofMinutes(5), null);
            bridge.accept(WORKER, TASK, 1);
            assertThat(bridge.progress(WORKER, TASK, 1, "first")).isEqualTo(1);
        }
        try (var restored = coordinator(repository); var bridge = new DelegationBridge(restored)) {
            var registration = bridge.ensureWorker(hello("scripted"));
            assertThat(registration.admitted()).isTrue();
            assertThat(bridge.progress(WORKER, TASK, 1, "after restart")).isEqualTo(2);
            assertThat(restored.state().clean()).isTrue();
        }
    }

    private static InProcessDelegationCoordinator coordinator(InMemoryTranscriptRepository repository) {
        return new InProcessDelegationCoordinator(AdmissionPolicy.allowAll(), CandidateReviewer.manual(),
                Clock.fixed(NOW, ZoneOffset.UTC), repository);
    }

    private static WorkerHello hello(String provider) {
        return WorkerHello.newBuilder().setWorkerId(WORKER).setProtocolVersion(1)
                .setProvider(provider)
                .addCapabilities(WorkerCapability.newBuilder().setName("structured-delegation"))
                .build();
    }

    private static DelegateRequest helloFrame(WorkerHello hello, long sequence) {
        return DelegateRequest.newBuilder().setFrameId(UUID.randomUUID().toString())
                .setSeq(sequence).setSentAt(Timestamps.fromMillis(NOW.toEpochMilli()))
                .setHello(hello).build();
    }

    private static final class Responses implements StreamObserver<DelegateResponse> {
        private final List<DelegateResponse> frames = new ArrayList<>();
        private Throwable error;
        @Override public void onNext(DelegateResponse frame) { frames.add(frame); }
        @Override public void onError(Throwable failure) { error = failure; }
        @Override public void onCompleted() { }
    }
}
