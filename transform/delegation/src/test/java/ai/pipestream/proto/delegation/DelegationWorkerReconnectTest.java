package ai.pipestream.proto.delegation;

import ai.pipestream.proto.delegation.v1.AgentDelegationServiceGrpc;
import ai.pipestream.proto.delegation.v1.CompletionCandidate;
import ai.pipestream.proto.delegation.v1.DelegateRequest;
import ai.pipestream.proto.delegation.v1.DelegateResponse;
import ai.pipestream.proto.delegation.v1.Lane;
import ai.pipestream.proto.delegation.v1.TaskMessageKind;
import ai.pipestream.proto.delegation.v1.Transcript;
import ai.pipestream.proto.delegation.v1.TranscriptEntry;
import ai.pipestream.proto.delegation.v1.WorkerCapability;
import ai.pipestream.proto.delegation.v1.WorkerHello;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static ai.pipestream.proto.delegation.DelegationFixtures.TASK;
import static ai.pipestream.proto.delegation.DelegationFixtures.WORKER;
import static ai.pipestream.proto.delegation.DelegationFixtures.commit;
import static ai.pipestream.proto.delegation.DelegationFixtures.evidence;
import static ai.pipestream.proto.delegation.DelegationFixtures.spec;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The worker-side adapter across stream termination. A worker whose stream died opens
 * a replacement stream with {@code start()}; its per-scope sequence counters carry
 * over, so the re-hello and every later frame continue the recorded transcript. A
 * frame consumed by the dead stream but never recorded is the one case the
 * replacement cannot hide: the reducer reports the gap loudly on the next frame.
 */
class DelegationWorkerReconnectTest {

    private static final Instant NOW = Instant.parse("2026-08-12T12:00:00Z");
    private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);

    @Test
    void reconnectAcrossACoordinatorRestartContinuesTheRecordedScopes() throws Exception {
        InMemoryTranscriptRepository repository = new InMemoryTranscriptRepository();
        AtomicReference<InProcessDelegationCoordinator> target =
                new AtomicReference<>(coordinator(repository));
        String name = InProcessServerBuilder.generateName();
        Server server = InProcessServerBuilder.forName(name).directExecutor()
                .addService(new ForwardingService(target)).build().start();
        ManagedChannel channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        CountDownLatch parked = new CountDownLatch(1);
        CountDownLatch finish = new CountDownLatch(1);
        DelegationWorker worker = new DelegationWorker(
                AgentDelegationServiceGrpc.newStub(channel),
                new ScriptedWorkerRunner(hello(), List.of((task, events) -> {
                    events.progress("mapped the envelope");
                    parked.countDown();
                    finish.await();
                    return candidate(task);
                })));
        try {
            worker.start();
            assertTrue(worker.awaitAdmission(Duration.ofSeconds(5)));
            target.get().offer(WORKER, TASK, spec("unit-tests"), Duration.ofMinutes(5));
            // The runner holds the lease and is parked mid-attempt.
            assertTrue(parked.await(5, TimeUnit.SECONDS));

            // The coordinator "restarts": the old instance closes (its streams
            // complete) and a restored instance takes over the same service.
            target.get().close();
            awaitStreamDown(worker);
            InProcessDelegationCoordinator restored = coordinator(repository);
            target.set(restored);

            // The replacement stream re-sends the hello; this instance's counters
            // carry over, so the admission is a continuation, not a rejection.
            worker.start();
            assertTrue(worker.awaitAdmission(Duration.ofSeconds(5)));
            assertTrue(worker.streamFailure().isEmpty());

            // The parked runner finishes on the replacement stream; its candidate
            // continues the attempt scope and lands in the restored coordinator.
            finish.countDown();
            awaitPhase(restored, DelegationReducer.Phase.CANDIDATE);

            assertTrue(restored.state().clean(), restored.state().findings().toString());
            List<Long> hellos = restored.transcript().getEntriesList().stream()
                    .filter(entry -> entry.getLane() == Lane.LANE_WORKER)
                    .map(TranscriptEntry::getWorkerFrame)
                    .filter(DelegateRequest::hasHello)
                    .map(DelegateRequest::getSeq)
                    .toList();
            assertEquals(List.of(1L, 2L), hellos);
            assertEquals(DelegationReducer.Phase.CANDIDATE,
                    restored.state().tasks().get(TASK).phase());
        } finally {
            worker.close();
            channel.shutdownNow();
            server.shutdownNow();
        }
    }

    @Test
    void aFrameLostWithTheDeadStreamSurfacesAsALoudGapAfterReconnect() throws Exception {
        FailingRepository repository = new FailingRepository();
        InProcessDelegationCoordinator coordinator = coordinator(repository);
        String name = InProcessServerBuilder.generateName();
        Server server = InProcessServerBuilder.forName(name).directExecutor()
                .addService(coordinator).build().start();
        ManagedChannel channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        CountDownLatch finish = new CountDownLatch(1);
        DelegationWorker worker = new DelegationWorker(
                AgentDelegationServiceGrpc.newStub(channel),
                new ScriptedWorkerRunner(hello(), List.of((task, events) -> {
                    finish.await();
                    return candidate(task);
                })));
        try {
            worker.start();
            assertTrue(worker.awaitAdmission(Duration.ofSeconds(5)));
            coordinator.offer(WORKER, TASK, spec("unit-tests"), Duration.ofMinutes(5));
            awaitPhase(coordinator, DelegationReducer.Phase.LEASED);

            // The repository outage kills the stream mid-session; the message's
            // sequence was consumed by the dead stream but never recorded.
            repository.failWrites = true;
            worker.sendMessage(TASK, TaskMessageKind.TASK_MESSAGE_KIND_QUESTION,
                    "is the lease long enough?", "", List.of());
            awaitStreamDown(worker);
            assertTrue(worker.streamFailure().isPresent());
            int entriesBefore = coordinator.transcript().getEntriesCount();

            // The replacement stream re-admits (the session scope is intact), but the
            // next message skips the sequence the lost frame consumed: the reducer
            // reports the gap and the stream fails loudly instead of hiding the loss.
            repository.failWrites = false;
            worker.start();
            assertTrue(worker.awaitAdmission(Duration.ofSeconds(5)));
            worker.sendMessage(TASK, TaskMessageKind.TASK_MESSAGE_KIND_QUESTION,
                    "asking again", "", List.of());
            awaitStreamDown(worker);
            assertTrue(worker.streamFailure().isPresent());
            assertEquals(entriesBefore + 2, coordinator.transcript().getEntriesCount());
        } finally {
            finish.countDown();
            worker.close();
            channel.shutdownNow();
            server.shutdownNow();
        }
    }

    @Test
    void startWhileTheStreamIsLiveStillFailsFast() throws Exception {
        InMemoryTranscriptRepository repository = new InMemoryTranscriptRepository();
        InProcessDelegationCoordinator coordinator = coordinator(repository);
        String name = InProcessServerBuilder.generateName();
        Server server = InProcessServerBuilder.forName(name).directExecutor()
                .addService(coordinator).build().start();
        ManagedChannel channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        DelegationWorker worker = new DelegationWorker(
                AgentDelegationServiceGrpc.newStub(channel),
                new ScriptedWorkerRunner(hello(), List.of()));
        try {
            worker.start();
            assertTrue(worker.awaitAdmission(Duration.ofSeconds(5)));
            assertThrows(IllegalStateException.class, worker::start);
        } finally {
            worker.close();
            channel.shutdownNow();
            server.shutdownNow();
        }
    }

    private static InProcessDelegationCoordinator coordinator(TranscriptRepository repository) {
        return new InProcessDelegationCoordinator(AdmissionPolicy.allowAll(),
                CandidateReviewer.manual(), CLOCK, repository);
    }

    private static WorkerHello hello() {
        return WorkerHello.newBuilder()
                .setWorkerId(WORKER)
                .setProtocolVersion(1)
                .setProvider("scripted")
                .setModel("deterministic")
                .addCapabilities(WorkerCapability.newBuilder().setName("java-build"))
                .build();
    }

    private static CompletionCandidate candidate(WorkerRunner.WorkerTask task) {
        return CompletionCandidate.newBuilder()
                .setAttempt(task.offer().getAttempt())
                .setRevision(task.expectedRevision())
                .setSummary("implemented and proven")
                .addEvidence(evidence("unit-tests"))
                .addCommits(commit("reconnect-output"))
                .build();
    }

    private static void awaitPhase(InProcessDelegationCoordinator coordinator,
                                   DelegationReducer.Phase phase) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            DelegationReducer.TaskState state = coordinator.state().tasks().get(TASK);
            if (state != null && state.phase() == phase) {
                return;
            }
            Thread.sleep(10);
        }
        throw new AssertionError("task did not reach phase " + phase);
    }

    private static void awaitStreamDown(DelegationWorker worker) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            if (!worker.streamOpen()) {
                return;
            }
            Thread.sleep(10);
        }
        throw new AssertionError("the worker stream did not terminate");
    }

    /** Routes every incoming stream to the coordinator instance currently mounted. */
    private static final class ForwardingService
            extends AgentDelegationServiceGrpc.AgentDelegationServiceImplBase {
        private final AtomicReference<InProcessDelegationCoordinator> target;

        private ForwardingService(AtomicReference<InProcessDelegationCoordinator> target) {
            this.target = target;
        }

        @Override
        public StreamObserver<DelegateRequest> delegate(
                StreamObserver<DelegateResponse> responseObserver) {
            return target.get().delegate(responseObserver);
        }
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
}
