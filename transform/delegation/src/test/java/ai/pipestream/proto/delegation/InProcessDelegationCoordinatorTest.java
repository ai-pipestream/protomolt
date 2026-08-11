package ai.pipestream.proto.delegation;

import ai.pipestream.proto.delegation.v1.AgentDelegationServiceGrpc;
import ai.pipestream.proto.delegation.v1.Checkpoint;
import ai.pipestream.proto.delegation.v1.CheckpointReference;
import ai.pipestream.proto.delegation.v1.DelegateResponse;
import ai.pipestream.proto.delegation.v1.Lane;
import ai.pipestream.proto.delegation.v1.TaskOffer;
import ai.pipestream.proto.delegation.v1.WorkerCapability;
import ai.pipestream.proto.delegation.v1.WorkerHello;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

class InProcessDelegationCoordinatorTest {

    @TempDir
    Path repository;

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
    void scriptedWorkerRevisesCandidateAfterExactCommitVerification() throws Exception {
        GrpcJavaTaskFixtures.GrpcJavaImplementationJob job =
                GrpcJavaTaskFixtures.job();
        initializeGitRepository();
        String incomplete = commit("incomplete implementation");

        write("src/main/java/example/Coordinator.java", """
                package example;
                public final class Coordinator {
                    public void waitForEvent() {}
                }
                """);
        write("src/test/java/example/CoordinatorTest.java", """
                package example;
                import org.junit.jupiter.api.Test;
                final class CoordinatorTest {
                    @Test void revisesFailedCandidate() {}
                }
                """);
        String complete = commit("complete implementation");

        coordinator = new InProcessDelegationCoordinator(
                AdmissionPolicy.allowAll(),
                new GitCommitClaimReviewer(repository, job));
        startServer(new ScriptedWorkerRunner(hello(), List.of(
                (task, events) -> {
                    events.progress("implemented the first candidate");
                    events.checkpoint(Checkpoint.newBuilder()
                            .setResumeToken("first-candidate")
                            .setNote("ready for review")
                            .build());
                    return GrpcJavaTaskFixtures.result(
                            task.offer().getAttempt(), task.expectedRevision(), incomplete);
                },
                (task, events) -> GrpcJavaTaskFixtures.result(
                        task.offer().getAttempt(), task.expectedRevision(), complete))));

        assertTrue(worker.awaitAdmission(Duration.ofSeconds(2)));
        String taskId = UUID.randomUUID().toString();
        coordinator.offer("scripted-kimi", taskId, job.spec(), Duration.ofSeconds(30));

        InProcessDelegationCoordinator.Event accepted = waitForAccepted(taskId);
        assertTrue(accepted.entry().getCoordinatorFrame().hasAccepted());
        assertEquals(2, accepted.entry().getCoordinatorFrame()
                .getAccepted().getRevision());

        List<DelegateResponse.PayloadCase> responses = coordinator.transcript()
                .getEntriesList().stream()
                .filter(entry -> entry.getLane() == Lane.LANE_COORDINATOR)
                .filter(entry -> entry.getCoordinatorFrame().getTaskId().equals(taskId))
                .map(entry -> entry.getCoordinatorFrame().getPayloadCase())
                .toList();
        assertTrue(responses.contains(DelegateResponse.PayloadCase.REVISION_REQUESTED));
        assertTrue(responses.contains(DelegateResponse.PayloadCase.ACCEPTED));
        assertTrue(coordinator.state().clean(), coordinator.state().findings().toString());
        assertEquals(DelegationReducer.Phase.ACCEPTED,
                coordinator.state().tasks().get(taskId).phase());
        assertFalse(worker.streamFailure().isPresent());
    }

    @Test
    void waitForEventBlocksUntilOfferArrives() throws Exception {
        coordinator = new InProcessDelegationCoordinator();
        startServer(new ScriptedWorkerRunner(hello(), List.of()));
        assertTrue(worker.awaitAdmission(Duration.ofSeconds(2)));
        long cursor = coordinator.transcript().getEntriesCount();
        String taskId = UUID.randomUUID().toString();

        Thread offer = Thread.ofVirtual().start(() -> coordinator.offer(
                "scripted-kimi", taskId, GrpcJavaTaskFixtures.job().spec(),
                Duration.ofSeconds(30)));
        InProcessDelegationCoordinator.Event event = coordinator.waitForEvent(
                taskId, cursor, Duration.ofSeconds(2)).orElseThrow();
        offer.join();

        assertEquals(taskId, event.taskId());
        assertTrue(event.entry().getCoordinatorFrame().hasOffer());
    }

    @Test
    void heartbeatRenewsLeaseBeforeCandidateAcceptance() throws Exception {
        coordinator = new InProcessDelegationCoordinator(
                AdmissionPolicy.allowAll(), CandidateReviewer.acceptAll());
        var job = GrpcJavaTaskFixtures.job();
        startServer(new ScriptedWorkerRunner(hello(), List.of((task, events) -> {
            events.heartbeat("still compiling");
            return GrpcJavaTaskFixtures.result(task.offer().getAttempt(),
                    task.expectedRevision(), "a".repeat(40));
        })));
        assertTrue(worker.awaitAdmission(Duration.ofSeconds(2)));
        String taskId = UUID.randomUUID().toString();

        coordinator.offer("scripted-kimi", taskId, job.spec(), Duration.ofSeconds(30));
        waitForAccepted(taskId);

        assertTrue(coordinator.transcript().getEntriesList().stream()
                .filter(entry -> entry.getLane() == Lane.LANE_COORDINATOR)
                .map(entry -> entry.getCoordinatorFrame())
                .anyMatch(DelegateResponse::hasRenewal));
        assertTrue(coordinator.state().clean(), coordinator.state().findings().toString());
    }

    @Test
    void elapsedLeaseStopsWorkerWithoutCancellationNotice() throws Exception {
        CountDownLatch running = new CountDownLatch(1);
        coordinator = new InProcessDelegationCoordinator();
        startServer(new ScriptedWorkerRunner(hello(), List.of((task, events) -> {
            running.countDown();
            while (!events.cancelled()) {
                Thread.sleep(10);
            }
            throw new InterruptedException("lease ended");
        })));
        assertTrue(worker.awaitAdmission(Duration.ofSeconds(2)));
        String taskId = UUID.randomUUID().toString();
        TaskOffer offer = coordinator.offer("scripted-kimi", taskId,
                GrpcJavaTaskFixtures.job().spec(), Duration.ofSeconds(30));
        assertTrue(running.await(2, TimeUnit.SECONDS));

        assertEquals(1, coordinator.expireLeases(
                Instant.ofEpochSecond(offer.getExpiresAt().getSeconds() + 1)));

        assertEquals(DelegationReducer.Phase.EXPIRED,
                coordinator.state().tasks().get(taskId).phase());
        assertTrue(coordinator.state().clean(), coordinator.state().findings().toString());
        assertFalse(coordinator.transcript().getEntriesList().stream()
                .filter(entry -> entry.getLane() == Lane.LANE_WORKER)
                .map(entry -> entry.getWorkerFrame())
                .anyMatch(frame -> frame.getTaskId().equals(taskId)
                        && frame.hasCancelled()));
    }

    @Test
    void failedAttemptCanResumeFromRecordedCheckpoint() throws Exception {
        coordinator = new InProcessDelegationCoordinator(
                AdmissionPolicy.allowAll(), CandidateReviewer.acceptAll());
        var job = GrpcJavaTaskFixtures.job();
        startServer(new ScriptedWorkerRunner(hello(), List.of(
                (task, events) -> {
                    events.checkpoint(Checkpoint.newBuilder()
                            .setResumeToken("compiled-sources")
                            .setNote("compilation completed")
                            .build());
                    throw new IllegalStateException("transient provider failure");
                },
                (task, events) -> {
                    assertTrue(task.offer().hasResumeFrom());
                    assertEquals("compiled-sources",
                            task.offer().getResumeFrom().getResumeToken());
                    return GrpcJavaTaskFixtures.result(task.offer().getAttempt(),
                            task.expectedRevision(), "b".repeat(40));
                })));
        assertTrue(worker.awaitAdmission(Duration.ofSeconds(2)));
        String taskId = UUID.randomUUID().toString();

        coordinator.offer("scripted-kimi", taskId, job.spec(), Duration.ofSeconds(30));
        waitForWorkerFailure(taskId);
        coordinator.offer("scripted-kimi", taskId, job.spec(), Duration.ofSeconds(30),
                CheckpointReference.newBuilder()
                        .setAttempt(1)
                        .setCheckpointSeq(1)
                        .setResumeToken("compiled-sources")
                        .build());
        InProcessDelegationCoordinator.Event accepted = waitForAccepted(taskId);

        assertEquals(2, accepted.entry().getCoordinatorFrame().getAccepted().getAttempt());
        assertTrue(coordinator.state().clean(), coordinator.state().findings().toString());
    }

    @Test
    void slowCandidateReviewDoesNotBlockCoordinatorControl() throws Exception {
        CountDownLatch reviewStarted = new CountDownLatch(1);
        CountDownLatch releaseReview = new CountDownLatch(1);
        coordinator = new InProcessDelegationCoordinator(
                AdmissionPolicy.allowAll(), context -> {
                    reviewStarted.countDown();
                    releaseReview.await();
                    return CandidateReviewer.ReviewDecision.accept("reviewed");
                });
        var job = GrpcJavaTaskFixtures.job();
        startServer(new ScriptedWorkerRunner(hello(), List.of((task, events) ->
                GrpcJavaTaskFixtures.result(task.offer().getAttempt(),
                        task.expectedRevision(), "c".repeat(40)))));
        assertTrue(worker.awaitAdmission(Duration.ofSeconds(2)));
        String taskId = UUID.randomUUID().toString();
        coordinator.offer("scripted-kimi", taskId, job.spec(), Duration.ofSeconds(30));
        assertTrue(reviewStarted.await(2, TimeUnit.SECONDS));

        try {
            assertTimeoutPreemptively(Duration.ofSeconds(1),
                    () -> coordinator.cancel(taskId, "superseded"));
        } finally {
            releaseReview.countDown();
        }

        assertEquals(DelegationReducer.Phase.CANCELLED,
                coordinator.state().tasks().get(taskId).phase());
        assertTrue(coordinator.state().clean(), coordinator.state().findings().toString());
    }

    @Test
    void rejectedWorkerCannotReceiveOffers() throws Exception {
        coordinator = new InProcessDelegationCoordinator(
                hello -> AdmissionPolicy.Decision.reject("capability is not allowed"),
                CandidateReviewer.manual());
        startServer(new ScriptedWorkerRunner(hello(), List.of()));

        assertFalse(worker.awaitAdmission(Duration.ofSeconds(2)));
        IllegalStateException failure = org.junit.jupiter.api.Assertions.assertThrows(
                IllegalStateException.class,
                () -> coordinator.offer("scripted-kimi", UUID.randomUUID().toString(),
                        GrpcJavaTaskFixtures.job().spec(), Duration.ofSeconds(30)));
        assertTrue(failure.getMessage().contains("not admitted"));
    }

    private InProcessDelegationCoordinator.Event waitForAccepted(String taskId)
            throws InterruptedException {
        long cursor = 0;
        Instant deadline = Instant.now().plusSeconds(5);
        while (Instant.now().isBefore(deadline)) {
            InProcessDelegationCoordinator.Event event = coordinator.waitForEvent(
                    taskId, cursor, Duration.ofSeconds(1)).orElse(null);
            if (event == null) {
                continue;
            }
            cursor = event.cursor();
            if (event.entry().getLane() == Lane.LANE_COORDINATOR
                    && event.entry().getCoordinatorFrame().hasAccepted()) {
                return event;
            }
        }
        throw new AssertionError("completion acceptance did not arrive");
    }

    private void waitForWorkerFailure(String taskId) throws InterruptedException {
        long cursor = 0;
        Instant deadline = Instant.now().plusSeconds(5);
        while (Instant.now().isBefore(deadline)) {
            InProcessDelegationCoordinator.Event event = coordinator.waitForEvent(
                    taskId, cursor, Duration.ofSeconds(1)).orElse(null);
            if (event == null) {
                continue;
            }
            cursor = event.cursor();
            if (event.entry().getLane() == Lane.LANE_WORKER
                    && event.entry().getWorkerFrame().hasFailed()) {
                return;
            }
        }
        throw new AssertionError("worker failure did not arrive");
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
                .setWorkerId("scripted-kimi")
                .setProtocolVersion(1)
                .setProvider("scripted")
                .setModel("deterministic")
                .addCapabilities(WorkerCapability.newBuilder()
                        .setName("java-build")
                        .setDescription("implements and tests gRPC Java services"))
                .build();
    }

    private void initializeGitRepository() throws Exception {
        git("init");
        git("config", "user.name", "ProtoMolt Test");
        git("config", "user.email", "protomolt-test@example.invalid");
        write("README.md", "fixture repository\n");
    }

    private String commit(String subject) throws Exception {
        git("add", ".");
        git("commit", "-m", subject);
        return git("rev-parse", "HEAD").trim();
    }

    private void write(String relative, String content) throws IOException {
        Path path = repository.resolve(relative);
        Files.createDirectories(path.getParent());
        Files.writeString(path, content);
    }

    private String git(String... arguments) throws Exception {
        String[] command = new String[arguments.length + 1];
        command[0] = "git";
        System.arraycopy(arguments, 0, command, 1, arguments.length);
        Process process = new ProcessBuilder(command)
                .directory(repository.toFile())
                .redirectErrorStream(true)
                .start();
        String output = new String(process.getInputStream().readAllBytes());
        assertEquals(0, process.waitFor(), output);
        return output;
    }
}
