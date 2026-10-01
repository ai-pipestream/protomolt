package ai.protomolt.proto.delegation;

import ai.protomolt.proto.delegation.v1.TaskOffer;
import ai.protomolt.proto.delegation.v1.TaskSpec;
import ai.protomolt.proto.delegation.v1.WorkerCapability;
import ai.protomolt.proto.delegation.v1.WorkerHello;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

import static ai.protomolt.proto.delegation.DelegationFixtures.TASK;
import static ai.protomolt.proto.delegation.DelegationFixtures.WORKER;
import static ai.protomolt.proto.delegation.DelegationFixtures.spec;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Create-once admission for browser-authored starts over the durable coordinator. */
class InitialOfferAdmissionTest {
    private static final String BINDING_A = "a".repeat(64);
    private static final String BINDING_B = "b".repeat(64);

    @Test
    void concurrentIdenticalCreatePublishesOneOfferAndBothCallersObserveIt() throws Exception {
        var repository = new InMemoryTranscriptRepository();
        try (var coordinator = coordinator(AdmissionPolicy.allowAll(), repository);
             var bridge = new DelegationBridge(coordinator)) {
            bridge.registerWorker(hello(WORKER));
            AtomicInteger creates = new AtomicInteger();
            CountDownLatch ready = new CountDownLatch(2);
            CountDownLatch go = new CountDownLatch(1);

            Callable<TaskOffer> create = () -> {
                ready.countDown();
                assertThat(go.await(5, TimeUnit.SECONDS)).isTrue();
                return offerOnce(coordinator, WORKER, TASK, BINDING_A, creates);
            };
            try (var executor = Executors.newFixedThreadPool(2)) {
                var first = executor.submit(create);
                var second = executor.submit(create);
                assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
                go.countDown();
                TaskOffer firstOffer = first.get(5, TimeUnit.SECONDS);
                TaskOffer secondOffer = second.get(5, TimeUnit.SECONDS);

                assertThat(firstOffer).isEqualTo(secondOffer);
                assertThat(firstOffer.getAttempt()).isEqualTo(1);
                assertThat(firstOffer.getStartBindingSha256()).isEqualTo(BINDING_A);
                assertThat(creates).hasValue(1);
                assertThat(coordinator.transcript().getEntriesList().stream()
                        .filter(entry -> entry.getCoordinatorFrame().hasOffer()
                                && entry.getCoordinatorFrame().getTaskId().equals(TASK))).hasSize(1);
                assertThat(coordinator.state().clean()).isTrue();
            }
        }
    }

    @Test
    void changedWorkerOrBindingAndGenericExistingTaskConflictWithoutAppending() {
        var repository = new InMemoryTranscriptRepository();
        try (var coordinator = coordinator(AdmissionPolicy.allowAll(), repository);
             var bridge = new DelegationBridge(coordinator)) {
            bridge.registerWorker(hello(WORKER));
            bridge.registerWorker(hello(DelegationFixtures.SECOND_WORKER));
            TaskOffer original = offerOnce(coordinator, WORKER, TASK, BINDING_A, new AtomicInteger());
            int before = coordinator.transcript().getEntriesCount();

            assertThatThrownBy(() -> offerOnce(coordinator, WORKER, TASK, BINDING_B, new AtomicInteger()))
                    .isInstanceOf(TaskStartConflictException.class);
            assertThatThrownBy(() -> offerOnce(coordinator, DelegationFixtures.SECOND_WORKER, TASK,
                    BINDING_A, new AtomicInteger()))
                    .isInstanceOf(TaskStartConflictException.class);
            assertThat(coordinator.transcript().getEntriesCount()).isEqualTo(before);
            assertThat(coordinator.transcript().getEntriesList().stream()
                    .filter(entry -> entry.getCoordinatorFrame().hasOffer()
                            && entry.getCoordinatorFrame().getTaskId().equals(TASK))).hasSize(1);
            assertThat(original.getAttempt()).isEqualTo(1);
        }

        String genericTask = DelegationFixtures.uuid("generic-existing-task");
        try (var coordinator = coordinator(AdmissionPolicy.allowAll(), new InMemoryTranscriptRepository());
             var bridge = new DelegationBridge(coordinator)) {
            bridge.registerWorker(hello(WORKER));
            bridge.offer(WORKER, genericTask, spec("generic"), java.time.Duration.ofMinutes(5), null);
            int before = coordinator.transcript().getEntriesCount();
            assertThatThrownBy(() -> offerOnce(coordinator, WORKER, genericTask, BINDING_A,
                    new AtomicInteger())).isInstanceOf(TaskStartConflictException.class);
            assertThat(coordinator.transcript().getEntriesCount()).isEqualTo(before);
        }
    }

    @Test
    void cancelledStartReplaysOriginalOfferRatherThanReassigningOrRenewing() {
        var repository = new InMemoryTranscriptRepository();
        try (var coordinator = coordinator(AdmissionPolicy.allowAll(), repository);
             var bridge = new DelegationBridge(coordinator)) {
            bridge.registerWorker(hello(WORKER));
            AtomicInteger creates = new AtomicInteger();
            TaskOffer original = offerOnce(coordinator, WORKER, TASK, BINDING_A, creates);
            coordinator.cancel(TASK, "operator cancelled");
            int afterCancel = coordinator.transcript().getEntriesCount();

            TaskOffer replayed = offerOnce(coordinator, WORKER, TASK, BINDING_A, creates);

            assertThat(replayed).isEqualTo(original);
            assertThat(replayed.getAttempt()).isEqualTo(1);
            assertThat(creates).hasValue(1);
            assertThat(coordinator.transcript().getEntriesCount()).isEqualTo(afterCancel);
            assertThat(coordinator.state().tasks().get(TASK).phase())
                    .isEqualTo(DelegationReducer.Phase.CANCELLED);
            assertThat(coordinator.state().clean()).isTrue();
        }
    }

    @Test
    void replayAfterTerminalAndReassignmentStillReturnsTheCommittedFirstOffer() {
        var repository = new InMemoryTranscriptRepository();
        try (var coordinator = coordinator(AdmissionPolicy.allowAll(), repository);
             var bridge = new DelegationBridge(coordinator)) {
            bridge.registerWorker(hello(WORKER));
            bridge.registerWorker(hello(DelegationFixtures.SECOND_WORKER));
            AtomicInteger creates = new AtomicInteger();
            TaskOffer original = offerOnce(coordinator, WORKER, TASK, BINDING_A, creates);
            bridge.reject(WORKER, TASK, 1, "worker cannot continue", true);
            TaskOffer reassigned = bridge.offer(DelegationFixtures.SECOND_WORKER, TASK, spec("retry"),
                    java.time.Duration.ofMinutes(5), null);
            bridge.accept(DelegationFixtures.SECOND_WORKER, TASK, 2);
            int afterReassignment = coordinator.transcript().getEntriesCount();

            TaskOffer replayed = offerOnce(coordinator, WORKER, TASK, BINDING_A, creates);

            assertThat(reassigned.getAttempt()).isEqualTo(2);
            assertThat(replayed).isEqualTo(original);
            assertThat(replayed.getAttempt()).isEqualTo(1);
            assertThat(creates).hasValue(1);
            assertThat(coordinator.transcript().getEntriesCount()).isEqualTo(afterReassignment);
            assertThat(coordinator.state().tasks().get(TASK).holder())
                    .isEqualTo(DelegationFixtures.SECOND_WORKER);
            assertThat(coordinator.state().tasks().get(TASK).attempt()).isEqualTo(2);
            assertThat(coordinator.state().clean()).isTrue();
        }
    }

    @Test
    void durableReplayAfterRestartNeedsNeitherConnectedWorkerNorCurrentAdmission() throws Exception {
        var repository = new InMemoryTranscriptRepository();
        TaskOffer original;
        AtomicInteger creates = new AtomicInteger();
        try (var coordinator = coordinator(AdmissionPolicy.allowAll(), repository);
             var bridge = new DelegationBridge(coordinator)) {
            bridge.registerWorker(hello(WORKER));
            original = offerOnce(coordinator, WORKER, TASK, BINDING_A, creates);
        }

        var reloadedRepository = new InMemoryTranscriptRepository();
        reloadedRepository.save(ai.protomolt.proto.delegation.v1.Transcript.parseFrom(
                repository.load().orElseThrow().toByteArray()));
        try (var restored = coordinator(hello -> AdmissionPolicy.Decision.reject("worker unavailable"), reloadedRepository)) {
            int before = restored.transcript().getEntriesCount();
            TaskOffer replayed = restored.offerOnce(WORKER, TASK,
                    existing -> existing.getStartBindingSha256().equals(BINDING_A),
                    () -> {
                        throw new AssertionError("exact durable replay must not create or check admission");
                    });

            assertThat(replayed).isEqualTo(original);
            assertThat(replayed.getAttempt()).isEqualTo(1);
            assertThat(restored.transcript().getEntriesCount()).isEqualTo(before);
            assertThat(creates).hasValue(1);
            assertThat(restored.state().clean()).isTrue();
        }
    }

    @Test
    void expiredOfferReplayDoesNotCreateANewAttemptOrExtendTheLease() {
        try (var coordinator = coordinator(AdmissionPolicy.allowAll(), new InMemoryTranscriptRepository());
             var bridge = new DelegationBridge(coordinator)) {
            bridge.registerWorker(hello(WORKER));
            var original = offerOnce(coordinator, WORKER, TASK, BINDING_A, new AtomicInteger());
            bridge.accept(WORKER, TASK, 1);
            assertThat(coordinator.expireLeases(java.time.Instant.ofEpochSecond(
                    original.getExpiresAt().getSeconds() + 1))).isEqualTo(1);
            int before = coordinator.transcript().getEntriesCount();
            assertThat(coordinator.offerOnce(WORKER, TASK,
                    offer -> BINDING_A.equals(offer.getStartBindingSha256()),
                    () -> { throw new AssertionError("expired replay created a new offer"); }))
                    .isEqualTo(original);
            assertThat(coordinator.transcript().getEntriesCount()).isEqualTo(before);
        }
    }

    @Test
    void ambiguousSaveFailsClosedAndRestorationRecoversTheCommittedOffer() {
        var stored = new InMemoryTranscriptRepository();
        var failOnce = new java.util.concurrent.atomic.AtomicBoolean(true);
        TranscriptRepository ambiguous = new TranscriptRepository() {
            @Override public java.util.Optional<ai.protomolt.proto.delegation.v1.Transcript> load() {
                return stored.load();
            }
            @Override public void save(ai.protomolt.proto.delegation.v1.Transcript transcript) {
                stored.save(transcript);
                if (transcript.getEntriesList().stream().anyMatch(entry ->
                        entry.getCoordinatorFrame().hasOffer()) && failOnce.getAndSet(false)) {
                    throw new IllegalStateException("save acknowledgement lost after commit");
                }
            }
        };
        try (var coordinator = coordinator(AdmissionPolicy.allowAll(), ambiguous);
             var bridge = new DelegationBridge(coordinator)) {
            bridge.registerWorker(hello(WORKER));
            assertThatThrownBy(() -> offerOnce(coordinator, WORKER, TASK, BINDING_A, new AtomicInteger()))
                    .isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> offerOnce(coordinator, WORKER, TASK, BINDING_A, new AtomicInteger()))
                    .isInstanceOf(IllegalStateException.class);
        }
        try (var restored = coordinator(AdmissionPolicy.allowAll(), stored)) {
            int before = restored.transcript().getEntriesCount();
            var original = restored.offerOnce(WORKER, TASK,
                    offer -> BINDING_A.equals(offer.getStartBindingSha256()),
                    () -> { throw new AssertionError("committed start was lost"); });
            assertThat(original.getAttempt()).isEqualTo(1);
            assertThat(restored.transcript().getEntriesCount()).isEqualTo(before);
            assertThat(restored.transcript().getEntriesList().stream()
                    .filter(entry -> entry.getCoordinatorFrame().hasOffer())).hasSize(1);
        }
    }

    @Test
    void unavailableWorkerAndMalformedBindingDoNotAppendOfferFrames() {
        var repository = new InMemoryTranscriptRepository();
        try (var coordinator = coordinator(hello -> AdmissionPolicy.Decision.reject("not admitted"), repository);
             var bridge = new DelegationBridge(coordinator)) {
            bridge.registerWorker(hello(WORKER));
            int afterRegistration = coordinator.transcript().getEntriesCount();
            AtomicInteger creates = new AtomicInteger();
            assertThatThrownBy(() -> offerOnce(coordinator, WORKER, TASK, BINDING_A, creates))
                    .isInstanceOf(RuntimeException.class);
            assertThat(creates).hasValue(0);
            assertThat(coordinator.transcript().getEntriesCount()).isEqualTo(afterRegistration);
            assertThat(coordinator.state().clean()).isTrue();
        }

        var validRepository = new InMemoryTranscriptRepository();
        try (var coordinator = coordinator(AdmissionPolicy.allowAll(), validRepository);
             var bridge = new DelegationBridge(coordinator)) {
            bridge.registerWorker(hello(WORKER));
            int before = coordinator.transcript().getEntriesCount();
            assertThatThrownBy(() -> coordinator.offerOnce(WORKER, TASK,
                    ignored -> false, () -> new InProcessDelegationCoordinator.InitialOffer(
                            spec("matcher-reject"), Duration.ofSeconds(300), BINDING_A)))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThat(coordinator.transcript().getEntriesCount()).isEqualTo(before);
            assertThat(coordinator.state().tasks()).doesNotContainKey(TASK);

            assertThatThrownBy(() -> coordinator.offerOnce(WORKER, TASK,
                    ignored -> false, () -> new InProcessDelegationCoordinator.InitialOffer(
                            spec("invalid-binding"), Duration.ofSeconds(300), "bad")))
                    .isInstanceOf(RuntimeException.class);
            assertThat(coordinator.transcript().getEntriesCount()).isEqualTo(before);
            assertThat(coordinator.state().tasks()).doesNotContainKey(TASK);
            assertThat(coordinator.state().clean()).isTrue();
        }
    }

    private static TaskOffer offerOnce(InProcessDelegationCoordinator coordinator,
            String worker, String task, String binding, AtomicInteger creates) {
        return coordinator.offerOnce(worker, task,
                committed -> committed.getStartBindingSha256().equals(binding), () -> {
                    creates.incrementAndGet();
                    return new InProcessDelegationCoordinator.InitialOffer(
                            spec("start-check"), Duration.ofSeconds(300), binding);
                });
    }

    private static WorkerHello hello(String workerId) {
        return WorkerHello.newBuilder().setWorkerId(workerId).setProtocolVersion(1).setProvider("fixture")
                .addCapabilities(WorkerCapability.newBuilder().setName("structured-delegation")).build();
    }

    private static InProcessDelegationCoordinator coordinator(AdmissionPolicy admission,
            TranscriptRepository repository) {
        return new InProcessDelegationCoordinator(admission, CandidateReviewer.manual(),
                java.time.Clock.systemUTC(), repository, false);
    }
}
