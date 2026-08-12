package ai.pipestream.proto.delegation;

import ai.pipestream.proto.delegation.v1.TaskMessage;
import ai.pipestream.proto.delegation.v1.TaskMessageKind;
import ai.pipestream.proto.delegation.v1.TaskOffer;
import ai.pipestream.proto.delegation.v1.WorkerCapability;
import ai.pipestream.proto.delegation.v1.WorkerHello;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The session-owning bridge over the in-process coordinator, without any transport. */
class DelegationBridgeTest {

    private InProcessDelegationCoordinator coordinator;
    private DelegationBridge bridge;

    @AfterEach
    void closeRuntime() {
        if (bridge != null) {
            bridge.close();
        }
        if (coordinator != null) {
            coordinator.close();
        }
    }

    @Test
    void registeredWorkerRunsTheWholeLifecycleThroughTheBridge() throws Exception {
        coordinator = new InProcessDelegationCoordinator();
        bridge = new DelegationBridge(coordinator);
        DelegationBridge.WorkerRegistration registration = bridge.registerWorker(hello());
        assertTrue(registration.admitted());
        assertFalse(registration.sessionId().isBlank());

        String taskId = UUID.randomUUID().toString();
        TaskOffer offer = bridge.offer("bridge-kimi", taskId,
                DelegationFixtures.spec("unit-tests"), Duration.ofSeconds(30), null);
        assertEquals(1, offer.getAttempt());

        bridge.accept("bridge-kimi", taskId, 1);
        assertEquals(1, bridge.progress("bridge-kimi", taskId, 1, "mapped the envelope"));
        assertEquals(2, bridge.progress("bridge-kimi", taskId, 1, "wired the stream"));
        assertEquals(1, bridge.checkpoint("bridge-kimi", taskId, 1, "halfway", "note", null));

        TaskMessage question = bridge.sendWorkerMessage("bridge-kimi", taskId,
                TaskMessageKind.TASK_MESSAGE_KIND_QUESTION, "is the lease long enough?",
                "", List.of());
        assertEquals("bridge-kimi", question.getSender());
        TaskMessage answer = bridge.sendCoordinatorMessage("bridge-kimi", taskId,
                TaskMessageKind.TASK_MESSAGE_KIND_ANSWER, "it renews on heartbeat",
                question.getMessageId(), List.of());
        assertEquals(question.getMessageId(), answer.getReplyTo());

        bridge.submitCandidate("bridge-kimi", taskId, candidate(1, 1));
        waitForPhase(taskId, DelegationReducer.Phase.CANDIDATE);
        bridge.review(taskId, CandidateReviewer.ReviewDecision.revise("prove the edge case",
                List.of("unit-tests")));
        bridge.submitCandidate("bridge-kimi", taskId, candidate(1, 2));
        waitForPhase(taskId, DelegationReducer.Phase.CANDIDATE);
        bridge.review(taskId, CandidateReviewer.ReviewDecision.accept("verified"));
        waitForPhase(taskId, DelegationReducer.Phase.ACCEPTED);

        assertTrue(coordinator.state().clean(), coordinator.state().findings().toString());
        DelegationReducer.TaskState state = coordinator.state().tasks().get(taskId);
        assertEquals(2, state.candidateRevision());
        assertEquals(2, state.lastProgressSeq());
        assertEquals(1, state.lastCheckpointSeq());
    }

    @Test
    void duplicateRegistrationFailsFast() {
        coordinator = new InProcessDelegationCoordinator();
        bridge = new DelegationBridge(coordinator);
        DelegationBridge.WorkerRegistration first = bridge.registerWorker(hello());
        assertTrue(first.admitted());

        // A replacement stream would rewind the transcript's sequence scopes, so the
        // bridge refuses it outright instead of failing mid-admission.
        IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> bridge.registerWorker(hello()));
        assertTrue(failure.getMessage().contains("already registered"));
        assertTrue(coordinator.state().clean(), coordinator.state().findings().toString());
    }

    @Test
    void unregisteredWorkerFailsFast() {
        coordinator = new InProcessDelegationCoordinator();
        bridge = new DelegationBridge(coordinator);

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> bridge.accept("nobody", UUID.randomUUID().toString(), 1));
        assertTrue(failure.getMessage().contains("not registered"));
    }

    @Test
    void aMalformedFrameFailsTheCallWithoutTearingTheStreamDown() throws Exception {
        coordinator = new InProcessDelegationCoordinator();
        bridge = new DelegationBridge(coordinator);
        bridge.registerWorker(hello());
        String taskId = UUID.randomUUID().toString();
        bridge.offer("bridge-kimi", taskId, DelegationFixtures.spec("unit-tests"),
                Duration.ofSeconds(30), null);
        bridge.accept("bridge-kimi", taskId, 1);

        // A blank progress message is structurally invalid; the call fails.
        assertThrows(IllegalArgumentException.class,
                () -> bridge.progress("bridge-kimi", taskId, 1, ""));

        // The stream survived: the next well-formed frame lands with the next sequence.
        assertEquals(1, bridge.progress("bridge-kimi", taskId, 1, "still alive"));
        waitForPhase(taskId, DelegationReducer.Phase.LEASED);
        assertTrue(coordinator.state().clean(), coordinator.state().findings().toString());
    }

    private void waitForPhase(String taskId, DelegationReducer.Phase phase)
            throws InterruptedException {
        long cursor = 0;
        java.time.Instant deadline = java.time.Instant.now().plusSeconds(5);
        while (java.time.Instant.now().isBefore(deadline)) {
            DelegationReducer.TaskState state = coordinator.state().tasks().get(taskId);
            if (state != null && state.phase() == phase) {
                return;
            }
            InProcessDelegationCoordinator.Event event = coordinator.waitForEvent(
                    taskId, cursor, Duration.ofMillis(200)).orElse(null);
            if (event != null) {
                cursor = event.cursor();
            }
        }
        throw new AssertionError("task did not reach phase " + phase);
    }

    private static WorkerHello hello() {
        return WorkerHello.newBuilder()
                .setWorkerId("bridge-kimi")
                .setProtocolVersion(1)
                .setProvider("scripted")
                .addCapabilities(WorkerCapability.newBuilder().setName("java-build"))
                .build();
    }

    private static ai.pipestream.proto.delegation.v1.CompletionCandidate candidate(
            int attempt, int revision) {
        return ai.pipestream.proto.delegation.v1.CompletionCandidate.newBuilder()
                .setAttempt(attempt)
                .setRevision(revision)
                .setSummary("implemented and proven")
                .addEvidence(DelegationFixtures.evidence("unit-tests"))
                .addCommits(DelegationFixtures.commit("bridge-output-" + revision))
                .build();
    }
}
