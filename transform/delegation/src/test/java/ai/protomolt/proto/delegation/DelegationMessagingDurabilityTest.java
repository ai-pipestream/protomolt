package ai.protomolt.proto.delegation;

import ai.protomolt.proto.delegation.v1.DelegateRequest;
import ai.protomolt.proto.delegation.v1.DelegateResponse;
import ai.protomolt.proto.delegation.v1.Lane;
import ai.protomolt.proto.delegation.v1.TaskAccept;
import ai.protomolt.proto.delegation.v1.TaskMessage;
import ai.protomolt.proto.delegation.v1.TaskMessageKind;
import ai.protomolt.proto.delegation.v1.Transcript;
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

import static ai.protomolt.proto.delegation.DelegationFixtures.TASK;
import static ai.protomolt.proto.delegation.DelegationFixtures.WORKER;
import static ai.protomolt.proto.delegation.DelegationFixtures.spec;
import static ai.protomolt.proto.delegation.DelegationFixtures.uuid;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Task messages under the durable coordinator: restart restoration, attempt-0
 * sequencing across a reconnect, cursor continuity for watchers, and repository
 * failure atomicity.
 */
class DelegationMessagingDurabilityTest {

    private static final Instant NOW = Instant.parse("2026-08-11T12:00:00Z");
    private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);

    @Test
    void messagesSurviveACoordinatorRestartWithoutTouchingThePhase() {
        InMemoryTranscriptRepository repository = new InMemoryTranscriptRepository();
        Transcript beforeRestart;
        try (InProcessDelegationCoordinator coordinator = coordinator(repository)) {
            CapturingResponses responses = new CapturingResponses();
            StreamObserver<DelegateRequest> worker = coordinator.delegate(responses);
            worker.onNext(helloFrame(1, "restart-hello-1"));
            coordinator.offer(WORKER, TASK, spec("build"), Duration.ofMinutes(5));
            worker.onNext(acceptFrame(1));
            worker.onNext(workerMessageFrame(1, "restart-q1",
                    "is the lease long enough?"));
            coordinator.sendMessage(WORKER, TASK, TaskMessageKind.TASK_MESSAGE_KIND_ANSWER,
                    "it renews on heartbeat", "", List.of());
            assertEquals(DelegationReducer.Phase.LEASED,
                    coordinator.state().tasks().get(TASK).phase());
            beforeRestart = coordinator.transcript();
        }

        try (InProcessDelegationCoordinator restored = coordinator(repository)) {
            assertEquals(beforeRestart, restored.transcript());
            List<TranscriptEntryView> messages = taskMessages(restored.transcript());
            assertEquals(2, messages.size());
            assertEquals(WORKER, messages.get(0).sender());
            assertEquals(DelegationValidation.COORDINATOR, messages.get(1).sender());
            // Restoration replayed the messages without moving the lifecycle.
            assertEquals(DelegationReducer.Phase.LEASED,
                    restored.state().tasks().get(TASK).phase());
            assertTrue(restored.state().clean(),
                    restored.state().findings().toString());
        }
    }

    @Test
    void messageSequencingContinuesInTheAttemptZeroScopeAcrossARestart() {
        InMemoryTranscriptRepository repository = new InMemoryTranscriptRepository();
        try (InProcessDelegationCoordinator coordinator = coordinator(repository)) {
            StreamObserver<DelegateRequest> worker =
                    coordinator.delegate(new CapturingResponses());
            worker.onNext(helloFrame(1, "seq-hello-1"));
            coordinator.offer(WORKER, TASK, spec("build"), Duration.ofMinutes(5));
            worker.onNext(acceptFrame(1));
            worker.onNext(workerMessageFrame(1, "seq-q1", "first question"));
            coordinator.sendMessage(WORKER, TASK, TaskMessageKind.TASK_MESSAGE_KIND_ANSWER,
                    "first answer", "", List.of());
        }

        try (InProcessDelegationCoordinator restored = coordinator(repository)) {
            CapturingResponses responses = new CapturingResponses();
            StreamObserver<DelegateRequest> worker = restored.delegate(responses);
            worker.onNext(helloFrame(2, "seq-hello-2"));
            assertNull(responses.error);
            // The reconnect admission continues the session sequence.
            assertEquals(2, responses.values.get(0).getSeq());

            // A coordinator message continues the restored attempt-0 scope: the
            // pre-restart answer was seq 1 there.
            restored.sendMessage(WORKER, TASK, TaskMessageKind.TASK_MESSAGE_KIND_GUIDANCE,
                    "second note", "", List.of());
            DelegateResponse emitted = responses.values.getLast();
            assertTrue(emitted.hasTaskMessage());
            assertEquals(2, emitted.getSeq());

            // A worker message continues its lane's attempt-0 scope the same way.
            worker.onNext(workerMessageFrame(2, "seq-q2", "second question"));
            assertNull(responses.error);

            assertTrue(restored.state().clean(),
                    restored.state().findings().toString());
            assertEquals(DelegationReducer.Phase.LEASED,
                    restored.state().tasks().get(TASK).phase());
        }
    }

    @Test
    void watcherResumeAcrossARestartSeesExactlyTheFramesAfterTheCursor() {
        InMemoryTranscriptRepository repository = new InMemoryTranscriptRepository();
        List<InProcessDelegationCoordinator.Event> expected;
        long savedCursor;
        try (InProcessDelegationCoordinator coordinator = coordinator(repository)) {
            StreamObserver<DelegateRequest> worker =
                    coordinator.delegate(new CapturingResponses());
            worker.onNext(helloFrame(1, "watch-hello-1"));
            coordinator.offer(WORKER, TASK, spec("build"), Duration.ofMinutes(5));
            worker.onNext(acceptFrame(1));
            worker.onNext(workerMessageFrame(1, "watch-q1", "question one"));
            coordinator.sendMessage(WORKER, TASK, TaskMessageKind.TASK_MESSAGE_KIND_ANSWER,
                    "answer one", "", List.of());
            worker.onNext(workerMessageFrame(2, "watch-q2", "question two"));
            // The watcher disconnects after the offer: cursor 3 of 7 events.
            savedCursor = 3;
            expected = coordinator.eventsAfter("", savedCursor);
            assertEquals(4, expected.size());
        }

        try (InProcessDelegationCoordinator restored = coordinator(repository)) {
            List<InProcessDelegationCoordinator.Event> resumed =
                    restored.eventsAfter("", savedCursor);
            // Cursor is transcript position, stable across the restart: exactly the
            // frames after it come back, with the same cursors, none repeated.
            assertEquals(expected, resumed);
            assertEquals(savedCursor + 1, resumed.get(0).cursor());
            for (int i = 1; i < resumed.size(); i++) {
                assertEquals(resumed.get(i - 1).cursor() + 1, resumed.get(i).cursor());
            }
            assertEquals(2, resumed.stream()
                    .filter(event -> event.entry().getLane() == Lane.LANE_WORKER)
                    .filter(event -> event.entry().getWorkerFrame().hasTaskMessage())
                    .count());
        }
    }

    @Test
    void failedRepositoryAppendNeverPublishesAMessage() {
        FailingRepository repository = new FailingRepository();
        try (InProcessDelegationCoordinator coordinator = coordinator(repository)) {
            CapturingResponses responses = new CapturingResponses();
            StreamObserver<DelegateRequest> worker = coordinator.delegate(responses);
            worker.onNext(helloFrame(1, "atomic-hello-1"));
            coordinator.offer(WORKER, TASK, spec("build"), Duration.ofMinutes(5));
            worker.onNext(acceptFrame(1));
            long cursorBefore = coordinator.eventsAfter("", 0)
                    .getLast().cursor();

            repository.failWrites = true;
            assertThrows(IllegalStateException.class,
                    () -> coordinator.sendMessage(WORKER, TASK,
                            TaskMessageKind.TASK_MESSAGE_KIND_NOTE, "not durable", "",
                            List.of()));

            // Nothing became visible: no event, no transcript entry, no delivery, and
            // the sequence was not consumed.
            assertTrue(coordinator.eventsAfter("", cursorBefore).isEmpty());
            assertEquals(cursorBefore,
                    coordinator.eventsAfter("", 0).getLast().cursor());
            assertTrue(responses.values.stream()
                    .noneMatch(DelegateResponse::hasTaskMessage));

            repository.failWrites = false;
            coordinator.sendMessage(WORKER, TASK, TaskMessageKind.TASK_MESSAGE_KIND_NOTE,
                    "durable note", "", List.of());
            DelegateResponse emitted = responses.values.getLast();
            assertTrue(emitted.hasTaskMessage());
            assertEquals(1, emitted.getSeq());
            assertEquals(cursorBefore + 1,
                    coordinator.eventsAfter("", cursorBefore).get(0).cursor());
        }
    }

    @Test
    void failedRepositoryAppendNeverPublishesAWorkerMessage() {
        FailingRepository repository = new FailingRepository();
        try (InProcessDelegationCoordinator coordinator = coordinator(repository)) {
            CapturingResponses responses = new CapturingResponses();
            StreamObserver<DelegateRequest> worker = coordinator.delegate(responses);
            worker.onNext(helloFrame(1, "atomic-hello-2"));
            coordinator.offer(WORKER, TASK, spec("build"), Duration.ofMinutes(5));
            worker.onNext(acceptFrame(1));
            int entriesBefore = coordinator.transcript().getEntriesCount();

            repository.failWrites = true;
            worker.onNext(workerMessageFrame(1, "atomic-q1", "never stored"));

            assertNotNull(responses.error);
            assertEquals(entriesBefore, coordinator.transcript().getEntriesCount());
            assertTrue(coordinator.eventsAfter(TASK, 0).stream()
                    .noneMatch(event -> event.entry().getWorkerFrame().hasTaskMessage()));
        }
    }

    private static InProcessDelegationCoordinator coordinator(
            TranscriptRepository repository) {
        return new InProcessDelegationCoordinator(AdmissionPolicy.allowAll(),
                CandidateReviewer.manual(), CLOCK, repository);
    }

    private static List<TranscriptEntryView> taskMessages(Transcript transcript) {
        List<TranscriptEntryView> messages = new ArrayList<>();
        for (var entry : transcript.getEntriesList()) {
            TaskMessage message = entry.getLane() == Lane.LANE_WORKER
                    ? entry.getWorkerFrame().getTaskMessage()
                    : entry.getCoordinatorFrame().getTaskMessage();
            boolean isMessage = entry.getLane() == Lane.LANE_WORKER
                    ? entry.getWorkerFrame().hasTaskMessage()
                    : entry.getCoordinatorFrame().hasTaskMessage();
            if (isMessage) {
                messages.add(new TranscriptEntryView(message.getSender(),
                        message.getText()));
            }
        }
        return messages;
    }

    private record TranscriptEntryView(String sender, String text) {
    }

    private static DelegateRequest helloFrame(long sequence, String frameSeed) {
        return DelegateRequest.newBuilder()
                .setFrameId(uuid(frameSeed))
                .setSeq(sequence)
                .setSentAt(Timestamp.newBuilder().setSeconds(NOW.getEpochSecond()).build())
                .setHello(WorkerHello.newBuilder()
                        .setWorkerId(WORKER)
                        .setProtocolVersion(1)
                        .setProvider("scripted")
                        .setModel("deterministic"))
                .build();
    }

    private static DelegateRequest acceptFrame(long sequence) {
        return DelegateRequest.newBuilder()
                .setFrameId(uuid("accept-" + sequence))
                .setTaskId(TASK)
                .setSeq(sequence)
                .setSentAt(Timestamp.newBuilder().setSeconds(NOW.getEpochSecond()).build())
                .setAccept(TaskAccept.newBuilder().setAttempt(1))
                .build();
    }

    private static DelegateRequest workerMessageFrame(long sequence, String frameSeed,
                                                      String text) {
        return DelegateRequest.newBuilder()
                .setFrameId(uuid(frameSeed))
                .setTaskId(TASK)
                .setSeq(sequence)
                .setSentAt(Timestamp.newBuilder().setSeconds(NOW.getEpochSecond()).build())
                .setTaskMessage(TaskMessage.newBuilder()
                        .setMessageId(uuid(frameSeed + "-message"))
                        .setSender(WORKER)
                        .setRecipient(DelegationValidation.COORDINATOR)
                        .setTaskId(TASK)
                        .setKind(TaskMessageKind.TASK_MESSAGE_KIND_QUESTION)
                        .setText(text)
                        .setSentAt(Timestamp.newBuilder()
                                .setSeconds(NOW.getEpochSecond()).build()))
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
