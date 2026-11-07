package ai.protomolt.proto.schema.registry.git;

import ai.protomolt.proto.grpc.workflow.WorkflowVersionRepository;
import ai.protomolt.proto.grpc.workflow.WorkflowValidation;
import ai.protomolt.proto.grpc.workflow.v1.Workflow;
import ai.protomolt.proto.grpc.workflow.v1.WorkflowStep;
import ai.protomolt.proto.grpc.workflow.v1.ServiceDependency;
import ai.protomolt.proto.grpc.workflow.v1.StepCompletion;
import ai.protomolt.proto.grpc.workflow.v1.VersionedWorkflow;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Workflow promotion against the git-backed registry: versions are immutable, idempotent to
 * re-promote, recoverable by version across store instances, and invalid or corrupt content
 * is rejected rather than served.
 */
class WorkflowPromotionTest {

    private static final String FINGERPRINT = "a".repeat(64);

    private static VersionedWorkflow versioned(String version) {
        Workflow workflow = Workflow.newBuilder()
                .setName("analyze-document")
                .setInputType("example.v1.Document")
                .addDependencies(ServiceDependency.newBuilder()
                        .setAlias("nlp")
                        .setServiceProfile("tokenizer")
                        .setEndpoint("local")
                        .setDescriptorFingerprint(FINGERPRINT)
                        .build())
                .addSteps(WorkflowStep.newBuilder()
                        .setName("tokenize")
                        .setDependency("nlp")
                        .setMethod("example.v1.Tokenizer/Tokenize")
                        .setCompletion(StepCompletion.STEP_COMPLETION_LIVE)
                        .build())
                .setDeadline(Duration.newBuilder().setSeconds(30).build())
                .build();
        return VersionedWorkflow.newBuilder()
                .setWorkflow(workflow)
                .setVersion(version)
                .setWorkflowFingerprint(WorkflowValidation.fingerprint(workflow))
                .setCreatedAt(Timestamp.newBuilder().setSeconds(10).build())
                .build();
    }

    private static GitSchemaRegistryStore store(Path dir) {
        return GitSchemaRegistryStore.builder().repositoryDir(dir).build();
    }

    @Test
    void promoteFindAndListRoundTrip(@TempDir Path dir) throws Exception {
        try (GitSchemaRegistryStore git = store(dir)) {
            WorkflowVersionRepository repository = new RegistryWorkflowVersionRepository(git);
            VersionedWorkflow v1 = versioned("v1");
            VersionedWorkflow v2 = versioned("v2");

            repository.save(v1);
            repository.save(v2);

            assertThat(repository.find("analyze-document", "v1")).contains(v1);
            assertThat(repository.versions("analyze-document"))
                    .containsExactly(v1, v2);
            assertThat(git.workflowNames()).containsExactly("analyze-document");
            assertThat(repository.find("analyze-document", "v9")).isEmpty();
            assertThat(repository.versions("nobody")).isEmpty();
        }
    }

    @Test
    void rePromotionIsIdempotentButDifferentContentIsRefused(@TempDir Path dir) throws Exception {
        try (GitSchemaRegistryStore git = store(dir)) {
            WorkflowVersionRepository repository = new RegistryWorkflowVersionRepository(git);
            repository.save(versioned("v1"));
            repository.save(versioned("v1")); // identical bytes: a no-op

            assertThat(repository.versions("analyze-document")).hasSize(1);

            VersionedWorkflow altered = VersionedWorkflow.newBuilder(versioned("v1"))
                    .setCreatedAt(Timestamp.newBuilder().setSeconds(11).build())
                    .build();
            assertThatThrownBy(() -> repository.save(altered))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("immutable");
            assertThat(repository.find("analyze-document", "v1")).contains(versioned("v1"));
        }
    }

    @Test
    void invalidContentIsRejectedBeforeItTouchesTheRepository(@TempDir Path dir) throws Exception {
        try (GitSchemaRegistryStore git = store(dir)) {
            WorkflowVersionRepository repository = new RegistryWorkflowVersionRepository(git);
            VersionedWorkflow broken = VersionedWorkflow.newBuilder(versioned("v1"))
                    .setWorkflowFingerprint("b".repeat(64)) // no longer matches the content
                    .build();

            assertThatThrownBy(() -> repository.save(broken))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("fingerprint");
            assertThat(repository.find("analyze-document", "v1")).isEmpty();
            assertThat(git.workflowNames()).isEmpty();
        }
    }

    @Test
    void promotionSurvivesStoreRestart(@TempDir Path dir) throws Exception {
        VersionedWorkflow v1 = versioned("v1");
        try (GitSchemaRegistryStore git = store(dir)) {
            new RegistryWorkflowVersionRepository(git).save(v1);
        }

        try (GitSchemaRegistryStore reopened = store(dir)) {
            WorkflowVersionRepository repository = new RegistryWorkflowVersionRepository(reopened);
            assertThat(repository.find("analyze-document", "v1")).contains(v1);
            assertThat(repository.versions("analyze-document")).containsExactly(v1);
        }
    }

    @Test
    void corruptStoredBytesFailLoudly(@TempDir Path dir) throws Exception {
        try (GitSchemaRegistryStore git = store(dir)) {
            WorkflowVersionRepository repository = new RegistryWorkflowVersionRepository(git);
            repository.save(versioned("v1"));

            // The corruption must be committed to be authoritative: only HEAD is read.
            RegistryGitTestSupport.checkoutHead(dir);
            Path stored = dir.resolve("workflow-versions/analyze-document/v1.pb");
            Files.write(stored, new byte[]{1, 2, 3});
            RegistryGitTestSupport.commitWorkingTree(dir, "Commit corrupt promotion");

            assertThatThrownBy(() -> repository.find("analyze-document", "v1"))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("analyze-document");
        }
    }

    @Test
    void externallyCommittedPromotionIsReadable(@TempDir Path dir) throws Exception {
        try (GitSchemaRegistryStore git = store(dir)) {
            WorkflowVersionRepository repository = new RegistryWorkflowVersionRepository(git);
            // An outside actor promotes through its own porcelain commit: HEAD is what counts.
            RegistryGitTestSupport.checkoutHead(dir);
            Path stored = dir.resolve("workflow-versions/analyze-document/v1.pb");
            Files.createDirectories(stored.getParent());
            Files.write(stored, versioned("v1").toByteArray());
            RegistryGitTestSupport.commitWorkingTree(dir, "Promote pending workflow");

            assertThat(repository.find("analyze-document", "v1")).contains(versioned("v1"));
            assertThat(repository.versions("analyze-document")).containsExactly(versioned("v1"));
        }
    }

    @Test
    void workingTreeFilesDoNotBlockOrAlterPromotion(@TempDir Path dir) throws Exception {
        try (GitSchemaRegistryStore git = store(dir)) {
            WorkflowVersionRepository repository = new RegistryWorkflowVersionRepository(git);
            // A working-tree file at the version path is not consulted and never claimed:
            // promotion writes the committed object directly, whatever the worktree holds.
            Path pending = dir.resolve("workflow-versions/analyze-document/v1.pb");
            Files.createDirectories(pending.getParent());
            Files.write(pending, versioned("v1").toByteArray());

            repository.save(versioned("v1"));
            assertThat(repository.find("analyze-document", "v1")).contains(versioned("v1"));

            Files.write(pending, new byte[]{9, 9, 9});
            assertThat(repository.find("analyze-document", "v1")).contains(versioned("v1"));

            // The committed bytes stay immutable even with garbage under the worktree path.
            VersionedWorkflow altered = VersionedWorkflow.newBuilder(versioned("v1"))
                    .setCreatedAt(Timestamp.newBuilder().setSeconds(11).build())
                    .build();
            assertThatThrownBy(() -> repository.save(altered))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("immutable");
        }
    }

    @Test
    void oversizedCommittedPromotionIsRefusedBeforeDecoding(@TempDir Path dir) throws Exception {
        try (GitSchemaRegistryStore git = store(dir)) {
            WorkflowVersionRepository repository = new RegistryWorkflowVersionRepository(git);
            repository.save(versioned("v1"));
            RegistryGitTestSupport.checkoutHead(dir);
            Path stored = dir.resolve("workflow-versions/analyze-document/v1.pb");
            Files.write(stored, new byte[ai.protomolt.proto.grpc.workflow.WorkflowValidation.MAX_WORKFLOW_BYTES + 1]);
            RegistryGitTestSupport.commitWorkingTree(dir, "Oversize promotion");

            assertThatThrownBy(() -> repository.find("analyze-document", "v1"))
                    .isInstanceOf(IOException.class)
                    .hasStackTraceContaining("1048576-byte limit");
        }
    }

    @Test
    void identitiesStayPathSafe(@TempDir Path dir) throws Exception {
        try (GitSchemaRegistryStore git = store(dir)) {
            assertThatThrownBy(() -> git.workflow("../escape", "v1"))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> git.workflowVersions("not safe!"))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }
}
