package ai.pipestream.proto.delegation;

import ai.pipestream.proto.delegation.v1.TaskMessageKind;
import ai.pipestream.proto.delegation.v1.Transcript;
import ai.pipestream.proto.delegation.v1.WorkerCapability;
import ai.pipestream.proto.delegation.v1.WorkerHello;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static ai.pipestream.proto.delegation.DelegationFixtures.spec;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Same-worker re-registration through the bridge. After a coordinator restart over a
 * durable transcript, and after a mid-session stream failure, the replacement stream
 * seeds its sequence counters from the recorded transcript and resumes every scope
 * instead of rewinding it: the reducer stays clean, no frame is duplicated, and a
 * frame the repository never stored is re-emitted under its original sequence.
 */
class DelegationStreamReplacementTest {

    private static final Instant NOW = Instant.parse("2026-08-12T12:00:00Z");
    private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);
    private static final String WORKER = "worker-kimi-edge";

    @Test
    void reRegistrationAfterRestartResumesScopesCursorsAndLifecycle() {
        InMemoryTranscriptRepository repository = new InMemoryTranscriptRepository();
        String taskId = UUID.randomUUID().toString();
        long cursorBeforeRestart;
        Transcript beforeRestart;
        try (InProcessDelegationCoordinator coordinator = coordinator(repository)) {
            DelegationBridge bridge = new DelegationBridge(coordinator);
            assertTrue(register(bridge).admitted());
            bridge.offer(WORKER, taskId, spec("unit-tests"), Duration.ofMinutes(5), null);
            bridge.accept(WORKER, taskId, 1);
            assertEquals(1, bridge.progress(WORKER, taskId, 1, "mapped the envelope"));
            assertEquals(1, bridge.checkpoint(WORKER, taskId, 1, "token-1", "halfway", null));
            bridge.sendWorkerMessage(WORKER, taskId, TaskMessageKind.TASK_MESSAGE_KIND_QUESTION,
                    "is the lease long enough?", "", List.of());
            cursorBeforeRestart = coordinator.eventsAfter("", 0).getLast().cursor();
            beforeRestart = coordinator.transcript();
            bridge.close();
        }

        try (InProcessDelegationCoordinator restored = coordinator(repository)) {
            DelegationBridge bridge = new DelegationBridge(restored);
            // Restart restoration: transcript, lifecycle phase, and cursors are intact.
            assertEquals(beforeRestart, restored.transcript());
            assertEquals(DelegationReducer.Phase.LEASED,
                    restored.state().tasks().get(taskId).phase());

            // The same worker re-registers: admitted, and the re-hello continues the
            // session scope instead of rewinding it.
            assertTrue(register(bridge).admitted());

            // Progress and checkpoints continue their attempt scope counters.
            assertEquals(2, bridge.progress(WORKER, taskId, 1, "wired the stream"));
            assertEquals(2, bridge.checkpoint(WORKER, taskId, 1, "token-2", "further", null));

            // A watcher resuming from the pre-restart cursor sees exactly the frames
            // after it: the re-hello, the admission, and the two updates, nothing else.
            List<InProcessDelegationCoordinator.Event> tail =
                    restored.eventsAfter("", cursorBeforeRestart);
            assertEquals(4, tail.size());
            assertEquals(cursorBeforeRestart + 1, tail.get(0).cursor());
            assertTrue(tail.get(0).entry().getWorkerFrame().hasHello());
            assertEquals(2, tail.get(0).entry().getWorkerFrame().getSeq());
            assertTrue(tail.get(1).entry().getCoordinatorFrame().hasAdmission());

            // The reducer accepts the whole recording: no rejection, no duplicate,
            // no lost frame.
            assertTrue(restored.state().clean(), restored.state().findings().toString());
            assertEquals(beforeRestart.getEntriesCount() + 4,
                    restored.transcript().getEntriesCount());
            assertEquals(DelegationReducer.Phase.LEASED,
                    restored.state().tasks().get(taskId).phase());
            bridge.close();
        }
    }

    @Test
    void reRegistrationAfterStreamFailureRecoversFromTheTranscript() {
        FailingRepository repository = new FailingRepository();
        String taskId = UUID.randomUUID().toString();
        try (InProcessDelegationCoordinator coordinator = coordinator(repository)) {
            DelegationBridge bridge = new DelegationBridge(coordinator);
            assertTrue(register(bridge).admitted());
            bridge.offer(WORKER, taskId, spec("unit-tests"), Duration.ofMinutes(5), null);
            bridge.accept(WORKER, taskId, 1);
            assertEquals(1, bridge.progress(WORKER, taskId, 1, "mapped the envelope"));
            int entriesBefore = coordinator.transcript().getEntriesCount();

            // The repository dies. A worker frame validates on the bridge and reaches
            // the coordinator, but the durable append fails: nothing is recorded,
            // nothing becomes visible, and the stream goes down.
            repository.failWrites = true;
            assertThrows(IllegalStateException.class,
                    () -> bridge.sendWorkerMessage(WORKER, taskId,
                            TaskMessageKind.TASK_MESSAGE_KIND_NOTE,
                            "never durable", "", List.of()));
            assertThrows(IllegalStateException.class,
                    () -> bridge.progress(WORKER, taskId, 1, "stream is down"));
            assertEquals(entriesBefore, coordinator.transcript().getEntriesCount());
            assertTrue(coordinator.eventsAfter(taskId, 0).stream()
                    .noneMatch(event -> event.entry().getWorkerFrame().hasTaskMessage()));

            // The repository heals. The replacement registration resumes from the
            // transcript: the lost message's sequence is legitimately free again,
            // because the frame was never recorded anywhere.
            repository.failWrites = false;
            assertTrue(register(bridge).admitted());
            bridge.sendWorkerMessage(WORKER, taskId, TaskMessageKind.TASK_MESSAGE_KIND_NOTE,
                    "durable this time", "", List.of());
            assertEquals(2, bridge.progress(WORKER, taskId, 1, "resumed cleanly"));

            assertTrue(coordinator.state().clean(), coordinator.state().findings().toString());
            assertEquals(entriesBefore + 4, coordinator.transcript().getEntriesCount());
            assertEquals(DelegationReducer.Phase.LEASED,
                    coordinator.state().tasks().get(taskId).phase());
            bridge.close();
        }
    }

    @Test
    void failedRegistrationDuringOutageLeavesNoPartialState() {
        FailingRepository repository = new FailingRepository();
        try (InProcessDelegationCoordinator coordinator = coordinator(repository)) {
            DelegationBridge bridge = new DelegationBridge(coordinator);
            repository.failWrites = true;

            // The hello cannot be persisted, so admission never happens and nothing
            // becomes visible: no transcript entry, no event, no cursor movement.
            assertThrows(IllegalStateException.class, () -> register(bridge));
            assertEquals(0, coordinator.transcript().getEntriesCount());
            assertTrue(coordinator.eventsAfter("", 0).isEmpty());
            assertTrue(coordinator.state().clean(), coordinator.state().findings().toString());

            // After the outage the same worker registers from scratch: the failed
            // attempt consumed no sequence, so the hello is seq 1.
            repository.failWrites = false;
            assertTrue(register(bridge).admitted());
            assertEquals(1, coordinator.eventsAfter("", 0).get(0)
                    .entry().getWorkerFrame().getSeq());
            assertTrue(coordinator.state().clean(), coordinator.state().findings().toString());
            bridge.close();
        }
    }

    private static InProcessDelegationCoordinator coordinator(TranscriptRepository repository) {
        return new InProcessDelegationCoordinator(AdmissionPolicy.allowAll(),
                CandidateReviewer.manual(), CLOCK, repository);
    }

    private static DelegationBridge.WorkerRegistration register(DelegationBridge bridge) {
        return bridge.registerWorker(WorkerHello.newBuilder()
                .setWorkerId(WORKER)
                .setProtocolVersion(1)
                .setProvider("kimi")
                .setModel("kimi-k2")
                .addCapabilities(WorkerCapability.newBuilder().setName("java-build"))
                .build());
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
