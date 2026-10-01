package ai.protomolt.proto.workflow.authoring;

import ai.protomolt.proto.delegation.v1.CheckEvidence;
import ai.protomolt.proto.delegation.v1.CheckVerdict;
import ai.protomolt.proto.grpc.workflow.WorkflowValidation;
import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import ai.protomolt.proto.grpc.workflow.v1.ServiceDependency;
import ai.protomolt.proto.grpc.workflow.v1.StepCompletion;
import ai.protomolt.proto.grpc.workflow.v1.VersionedWorkflow;
import ai.protomolt.proto.grpc.workflow.v1.Workflow;
import ai.protomolt.proto.grpc.workflow.v1.WorkflowStep;
import ai.protomolt.proto.samples.starter.v1.WorkflowAcceptedCandidate;
import ai.protomolt.proto.samples.starter.v1.WorkflowAcceptanceFixture;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringDeliverable;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringLaunchAuthorization;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringLaunchRequest;
import ai.protomolt.proto.samples.starter.v1.WorkflowDeliverable;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import com.google.protobuf.UnknownFieldSet;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FileSystemWorkflowLaunchAuthorizationRepositoryTest {
    private static final String LAUNCH_ID = "0000000a-0000-4000-8000-000000000002";
    private static final String OTHER_ID = "0000000b-0000-4000-8000-000000000003";
    private static final String TASK_ID = "00000000-0000-4000-8000-000000000001";

    private static ArtifactReference ref(String hash, String media) {
        return ArtifactReference.newBuilder().setSha256(hash.repeat(64))
                .setMediaType(media).setSizeBytes(32).build();
    }

    private static WorkflowAuthoringLaunchAuthorization authorization(String launchId, String inputHash) {
        ArtifactReference payload = ref(inputHash, "application/x-protobuf");
        ArtifactReference fixture = ref("a", "application/x-protobuf");
        Workflow workflow = Workflow.newBuilder().setName("launch-workflow")
                .setInputType("example.v1.Input").setValidateContract(true)
                .addDependencies(ServiceDependency.newBuilder().setAlias("echo")
                        .setServiceProfile("example.v1.Echo").setEndpoint("local")
                        .setDescriptorFingerprint("f".repeat(64)))
                .addSteps(WorkflowStep.newBuilder().setName("call").setDependency("echo")
                        .setMethod("example.v1.Echo/Run")
                        .setCompletion(StepCompletion.STEP_COMPLETION_LIVE)
                        .setValidateResponse(true))
                .setDeadline(Duration.newBuilder().setSeconds(30)).build();
        WorkflowDeliverable deliverable = WorkflowDeliverable.newBuilder()
                .setWorkflow(workflow).setWorkflowArtifact(fixture).setDescriptors(fixture)
                .addFixtures(fixture)
                .addChecks(CheckEvidence.newBuilder().setCheckName("fixture")
                        .setVerdict(CheckVerdict.CHECK_VERDICT_PASSED)
                        .setRanAt(Timestamp.newBuilder().setSeconds(1)).addArtifacts(fixture))
                .setRunId("fixture-run").setReceipt(fixture).build();
        WorkflowAuthoringDeliverable authored = WorkflowAuthoringDeliverable.newBuilder()
                .setDeliverable(deliverable)
                .setExecutableSource(ref("b", "application/json"))
                .addAcceptanceFixtures(WorkflowAcceptanceFixture.newBuilder()
                        .setName("fixture").setInput(fixture).setExpectedOutput(fixture))
                .build();
        WorkflowAcceptedCandidate accepted = WorkflowAcceptedCandidate.newBuilder()
                .setTaskId(TASK_ID).setAttempt(1).setRevision(1)
                .setTaskSpecSha256("a".repeat(64)).setCandidateSha256("b".repeat(64))
                .setAcceptedEntrySha256("c".repeat(64)).build();
        WorkflowAuthoringLaunchRequest request = WorkflowAuthoringLaunchRequest.newBuilder()
                .setLaunchId(launchId).setAcceptance(accepted).setInput(payload).build();
        VersionedWorkflow promoted = VersionedWorkflow.newBuilder().setWorkflow(workflow)
                .setVersion("accepted-" + "d".repeat(64))
                .setWorkflowFingerprint(WorkflowValidation.fingerprint(workflow))
                .setCreatedAt(Timestamp.newBuilder().setSeconds(1)).build();
        return WorkflowAuthoringLaunchAuthorization.newBuilder().setRequest(request)
                .setPolicy(fixture).setAuthored(authored).setPromoted(promoted).build();
    }

    @Test void persistsAcrossInstancesAndReturnsExactRetry(@TempDir Path directory) throws Exception {
        var original = authorization(LAUNCH_ID, "d");
        var first = new FileSystemWorkflowLaunchAuthorizationRepository(directory);
        assertThat(first.createOrMatch(original)).isEqualTo(original);

        var reopened = new FileSystemWorkflowLaunchAuthorizationRepository(directory);
        assertThat(reopened.find(LAUNCH_ID)).contains(original);
        assertThat(reopened.createOrMatch(original)).isEqualTo(original);
        assertThatThrownBy(() -> reopened.createOrMatch(authorization(LAUNCH_ID, "e")))
                .isInstanceOf(IOException.class).hasMessageContaining("conflict");
        assertThat(first.find(LAUNCH_ID)).contains(original);
    }

    @Test void concurrentIdenticalWritersReturnOneRecord(@TempDir Path directory) throws Exception {
        var original = authorization(LAUNCH_ID, "d");
        var first = new FileSystemWorkflowLaunchAuthorizationRepository(directory);
        var second = new FileSystemWorkflowLaunchAuthorizationRepository(directory);
        CountDownLatch start = new CountDownLatch(1);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<WorkflowAuthoringLaunchAuthorization>> results = new ArrayList<>();
            for (int index = 0; index < 12; index++) {
                var writer = index % 2 == 0 ? first : second;
                results.add(executor.submit(() -> {
                    start.await();
                    return writer.createOrMatch(original);
                }));
            }
            start.countDown();
            for (var result : results) assertThat(result.get()).isEqualTo(original);
        }
        assertThat(first.find(LAUNCH_ID)).contains(original);
    }

    @Test void concurrentDifferentWritersHaveOneWinner(@TempDir Path directory) throws Exception {
        var first = new FileSystemWorkflowLaunchAuthorizationRepository(directory);
        var second = new FileSystemWorkflowLaunchAuthorizationRepository(directory);
        var left = authorization(LAUNCH_ID, "d");
        var right = authorization(LAUNCH_ID, "e");
        CountDownLatch start = new CountDownLatch(1);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            Future<Boolean> leftResult = executor.submit(() -> attempt(first, left, start));
            Future<Boolean> rightResult = executor.submit(() -> attempt(second, right, start));
            start.countDown();
            assertThat(List.of(leftResult.get(), rightResult.get())).containsExactlyInAnyOrder(true, false);
        }
        var winner = first.find(LAUNCH_ID).orElseThrow();
        assertThat(winner).isIn(left, right);
    }

    private static boolean attempt(WorkflowLaunchAuthorizationRepository repository,
                                   WorkflowAuthoringLaunchAuthorization authorization,
                                   CountDownLatch start) throws Exception {
        start.await();
        try {
            repository.createOrMatch(authorization);
            return true;
        } catch (IOException e) {
            assertThat(e).hasMessageContaining("conflict");
            return false;
        }
    }

    @Test void rejectsMalformedTamperedAndKeyMismatchedRecords(@TempDir Path directory)
            throws Exception {
        var repository = new FileSystemWorkflowLaunchAuthorizationRepository(directory);
        Path target = directory.resolve(LAUNCH_ID + ".pb");
        Files.write(target, new byte[] {1, 2, 3});
        assertThatThrownBy(() -> repository.find(LAUNCH_ID)).isInstanceOf(IOException.class);

        Files.write(target, WorkflowLaunchValidation.deterministicBytes(authorization(OTHER_ID, "d")));
        assertThatThrownBy(() -> repository.find(LAUNCH_ID)).isInstanceOf(IOException.class)
                .hasMessageContaining("identity");

        Files.write(target, new byte[4 * 1024 * 1024 + 1]);
        assertThatThrownBy(() -> repository.find(LAUNCH_ID)).isInstanceOf(IOException.class)
                .hasMessageContaining("exceeds");
    }

    @Test void rejectsInvalidIdsAndLeavesExistingContentUntouched(@TempDir Path directory)
            throws Exception {
        var repository = new FileSystemWorkflowLaunchAuthorizationRepository(directory);
        assertThatThrownBy(() -> repository.find("../escape"))
                .isInstanceOf(IllegalArgumentException.class);
        var original = authorization(LAUNCH_ID, "d");
        repository.createOrMatch(original);
        byte[] bytes = Files.readAllBytes(directory.resolve(LAUNCH_ID + ".pb"));
        assertThatThrownBy(() -> repository.createOrMatch(authorization(LAUNCH_ID, "e")))
                .isInstanceOf(IOException.class);
        assertThat(Files.readAllBytes(directory.resolve(LAUNCH_ID + ".pb"))).isEqualTo(bytes);
    }

    @Test void uppercaseUuidAliasUsesTheSameKey(@TempDir Path directory) throws Exception {
        var repository = new FileSystemWorkflowLaunchAuthorizationRepository(directory);
        var original = authorization(LAUNCH_ID, "d");
        repository.createOrMatch(original);
        String alias = LAUNCH_ID.toUpperCase(Locale.ROOT);

        assertThat(repository.find(alias)).contains(original);
        assertThatThrownBy(() -> repository.createOrMatch(authorization(alias, "d")))
                .isInstanceOf(IOException.class).hasMessageContaining("conflict");
        try (var files = Files.list(directory)) {
            assertThat(files.filter(path -> path.getFileName().toString().endsWith(".pb")).count())
                    .isEqualTo(1);
        }
    }

    @Test void rejectsNestedUnknownFieldsBeforePersistence(@TempDir Path directory)
            throws Exception {
        var repository = new FileSystemWorkflowLaunchAuthorizationRepository(directory);
        var original = authorization(LAUNCH_ID, "d");
        var unknown = UnknownFieldSet.newBuilder().addField(900,
                UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build();
        var alteredRequest = original.getRequest().toBuilder()
                .setAcceptance(original.getRequest().getAcceptance().toBuilder()
                        .setUnknownFields(unknown)).build();
        var altered = original.toBuilder().setRequest(alteredRequest).build();

        assertThatThrownBy(() -> repository.createOrMatch(altered))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("unknown");
        assertThat(repository.find(LAUNCH_ID)).isEmpty();
    }

    @Test void interruptedTemporaryFileIsNeverAnAuthorization(@TempDir Path directory)
            throws Exception {
        var repository = new FileSystemWorkflowLaunchAuthorizationRepository(directory);
        Files.write(directory.resolve(".workflow-launch-interrupted.tmp"),
                WorkflowLaunchValidation.deterministicBytes(authorization(LAUNCH_ID, "d")));
        assertThat(repository.find(LAUNCH_ID)).isEmpty();
    }
}
