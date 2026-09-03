package ai.protomolt.proto.delegation;

import ai.protomolt.proto.delegation.v1.AgentDelegationServiceGrpc;
import ai.protomolt.proto.delegation.v1.Checkpoint;
import ai.protomolt.proto.delegation.v1.CompletionCandidate;
import ai.protomolt.proto.delegation.v1.Lane;
import ai.protomolt.proto.delegation.v1.TaskMessage;
import ai.protomolt.proto.delegation.v1.TaskMessageKind;
import ai.protomolt.proto.delegation.v1.Transcript;
import ai.protomolt.proto.delegation.v1.WorkerHello;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Task messages: non-transitioning structured exchange on top of the live lifecycle. */
class DelegationMessagingTest {

    private InProcessDelegationCoordinator coordinator;
    private DelegationWorker worker;
    private ManagedChannel channel;
    private Server server;

    @AfterEach
    void closeRuntime() throws Exception {
        if (worker != null) {
            worker.close();
        }
        if (channel != null) {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
        if (server != null) {
            server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
        if (coordinator != null) {
            coordinator.close();
        }
    }

    @Test
    void messagesFlowBothWaysWithoutMovingTheLifecycle() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        coordinator = new InProcessDelegationCoordinator();
        startServer(new ScriptedWorkerRunner(hello(), List.of((task, events) -> {
            events.checkpoint(Checkpoint.newBuilder()
                    .setResumeToken("mid-task")
                    .setNote("paused for guidance")
                    .build());
            assertTrue(release.await(5, TimeUnit.SECONDS));
            return candidate(task.offer().getAttempt(), task.expectedRevision());
        })));
        assertTrue(worker.awaitAdmission(Duration.ofSeconds(2)));
        String taskId = UUID.randomUUID().toString();
        coordinator.offer("messaging-kimi", taskId,
                DelegationFixtures.spec("unit-tests"), Duration.ofSeconds(30));

        // The lease is held while the worker runs; guidance and questions ride along.
        TaskMessage guidance = coordinator.sendMessage("messaging-kimi", taskId,
                TaskMessageKind.TASK_MESSAGE_KIND_GUIDANCE,
                "keep the change inside the reducer", "", List.of());
        TaskMessage question = worker.sendMessage(taskId,
                TaskMessageKind.TASK_MESSAGE_KIND_QUESTION,
                "may the mapper stay untouched?", "", List.of());
        TaskMessage answer = coordinator.sendMessage("messaging-kimi", taskId,
                TaskMessageKind.TASK_MESSAGE_KIND_ANSWER,
                "yes; answer in the transcript", question.getMessageId(), List.of());

        List<TaskMessage> received = worker.drainTaskMessages();
        assertEquals(List.of(guidance, answer), received);
        assertEquals(guidance.getMessageId(), received.get(0).getMessageId());
        assertEquals(question.getMessageId(), answer.getReplyTo());

        // The lifecycle never moved: the task is still leased to the worker.
        assertEquals(DelegationReducer.Phase.LEASED,
                coordinator.state().tasks().get(taskId).phase());
        assertTrue(coordinator.state().clean(), coordinator.state().findings().toString());

        release.countDown();
        waitForPhase(taskId, DelegationReducer.Phase.CANDIDATE);
        coordinator.review(taskId, CandidateReviewer.ReviewDecision.accept("verified"));
        waitForPhase(taskId, DelegationReducer.Phase.ACCEPTED);

        Transcript transcript = coordinator.transcript();
        assertTrue(transcript.getEntriesList().stream()
                .filter(entry -> entry.getLane() == Lane.LANE_WORKER)
                .anyMatch(entry -> entry.getWorkerFrame().hasTaskMessage()));
        assertTrue(coordinator.state().clean(), coordinator.state().findings().toString());

        // After acceptance the task is terminal: no message may follow on either lane.
        assertThrows(IllegalArgumentException.class,
                () -> coordinator.sendMessage("messaging-kimi", taskId,
                        TaskMessageKind.TASK_MESSAGE_KIND_NOTE, "late note", "", List.of()));
    }

    @Test
    void coordinatorCannotMessageAnUnknownTask() throws Exception {
        coordinator = new InProcessDelegationCoordinator();
        startServer(new ScriptedWorkerRunner(hello(), List.of()));
        assertTrue(worker.awaitAdmission(Duration.ofSeconds(2)));

        assertThrows(IllegalArgumentException.class,
                () -> coordinator.sendMessage("messaging-kimi", UUID.randomUUID().toString(),
                        TaskMessageKind.TASK_MESSAGE_KIND_NOTE, "no such task", "", List.of()));
    }

    @Test
    void reducerFlagsAForgedWorkerSender() {
        Transcript transcript = new DelegationFixtures.TranscriptBuilder()
                .hello(DelegationFixtures.WORKER)
                .admit(DelegationFixtures.WORKER)
                .offer(DelegationFixtures.TASK, DelegationFixtures.WORKER, 1,
                        DelegationFixtures.spec("unit-tests"))
                .accept(DelegationFixtures.TASK, DelegationFixtures.WORKER, 1)
                .workerMessage(DelegationFixtures.TASK, DelegationFixtures.WORKER,
                        "honest question")
                .workerMessage(DelegationFixtures.TASK, DelegationFixtures.WORKER,
                        DelegationFixtures.SECOND_WORKER, "forged sender")
                .build();

        DelegationReducer.Result result = new DelegationReducer().reduce(transcript);

        assertEquals(1, result.findings().size(), result.findings().toString());
        assertEquals("session", result.findings().get(0).kind());
    }

    @Test
    void reducerAcceptsHonestMessagesOnBothLanes() {
        Transcript transcript = new DelegationFixtures.TranscriptBuilder()
                .hello(DelegationFixtures.WORKER)
                .admit(DelegationFixtures.WORKER)
                .offer(DelegationFixtures.TASK, DelegationFixtures.WORKER, 1,
                        DelegationFixtures.spec("unit-tests"))
                .accept(DelegationFixtures.TASK, DelegationFixtures.WORKER, 1)
                .workerMessage(DelegationFixtures.TASK, DelegationFixtures.WORKER,
                        "what does clean mean here?")
                .coordinatorMessage(DelegationFixtures.TASK, DelegationFixtures.WORKER,
                        "no findings in the reduction")
                .candidate(DelegationFixtures.TASK, DelegationFixtures.WORKER, 1,
                        DelegationFixtures.spec("unit-tests"))
                .accepted(DelegationFixtures.TASK, DelegationFixtures.WORKER, 1, "verified")
                .build();

        DelegationReducer.Result result = new DelegationReducer().reduce(transcript);

        assertTrue(result.clean(), result.findings().toString());
        assertEquals(DelegationReducer.Phase.ACCEPTED,
                result.tasks().get(DelegationFixtures.TASK).phase());
    }

    @Test
    void messageStructureIsValidatedAgainstTheEnvelope() {
        TaskMessage.Builder message = TaskMessage.newBuilder()
                .setMessageId(UUID.randomUUID().toString())
                .setSender("messaging-kimi")
                .setRecipient(DelegationValidation.COORDINATOR)
                .setTaskId(UUID.randomUUID().toString())
                .setKind(TaskMessageKind.TASK_MESSAGE_KIND_QUESTION)
                .setText("is the lease still open?")
                .setSentAt(com.google.protobuf.util.Timestamps.fromMillis(1_700_000_000_000L));

        DelegationValidation.validate(message.build(), message.getTaskId());

        assertThrows(IllegalArgumentException.class, () -> DelegationValidation.validate(
                message.build(), UUID.randomUUID().toString()));
        assertThrows(IllegalArgumentException.class, () -> DelegationValidation.validate(
                message.clone().setText("").build(), message.getTaskId()));
        assertThrows(IllegalArgumentException.class, () -> DelegationValidation.validate(
                message.clone().setKind(TaskMessageKind.TASK_MESSAGE_KIND_UNSPECIFIED).build(),
                message.getTaskId()));
        assertThrows(IllegalArgumentException.class, () -> DelegationValidation.validate(
                message.clone().setRecipient(message.getSender()).build(),
                message.getTaskId()));
    }

    private void waitForPhase(String taskId, DelegationReducer.Phase phase)
            throws InterruptedException {
        long cursor = 0;
        java.time.Instant deadline = java.time.Instant.now().plusSeconds(5);
        while (java.time.Instant.now().isBefore(deadline)) {
            if (coordinator.state().tasks().get(taskId).phase() == phase) {
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

    private void startServer(WorkerRunner runner) throws IOException {
        String name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name)
                .directExecutor()
                .addService(coordinator)
                .build()
                .start();
        channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        worker = new DelegationWorker(
                AgentDelegationServiceGrpc.newStub(channel), runner);
        worker.start();
    }

    private static WorkerHello hello() {
        return WorkerHello.newBuilder()
                .setWorkerId("messaging-kimi")
                .setProtocolVersion(1)
                .setProvider("scripted")
                .setModel("deterministic")
                .build();
    }

    private static CompletionCandidate candidate(int attempt, int revision) {
        return CompletionCandidate.newBuilder()
                .setAttempt(attempt)
                .setRevision(revision)
                .setSummary("implemented and proven")
                .addEvidence(DelegationFixtures.evidence("unit-tests"))
                .addCommits(DelegationFixtures.commit("messaging-output"))
                .build();
    }
}
