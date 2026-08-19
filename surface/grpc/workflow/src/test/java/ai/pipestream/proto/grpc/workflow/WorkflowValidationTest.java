package ai.pipestream.proto.grpc.workflow;

import ai.pipestream.proto.grpc.workflow.v1.ArtifactReference;
import ai.pipestream.proto.grpc.workflow.v1.Workflow;
import ai.pipestream.proto.grpc.workflow.v1.RunEvidence;
import ai.pipestream.proto.grpc.workflow.v1.RunStatus;
import ai.pipestream.proto.grpc.workflow.v1.StepCompletion;
import ai.pipestream.proto.grpc.workflow.v1.VersionedWorkflow;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkflowValidationTest {

    @Test
    void acceptsWorkflowPromotionArtifactsAndRunEvidence() {
        WorkflowValidation.validate(TestWorkflows.workflow());
        WorkflowValidation.validate(TestWorkflows.versionedWorkflow());
        WorkflowValidation.validate(TestWorkflows.artifact("{}", true));
        WorkflowValidation.validate(TestWorkflows.evidence());
    }

    @Test
    void rejectsUnsafeIdentityMissingDependencyAndUnspecifiedCompletion() {
        assertThatThrownBy(() -> WorkflowValidation.validate(TestWorkflows.workflow().toBuilder()
                .setName("../escape").build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("slug");

        assertThatThrownBy(() -> WorkflowValidation.validate(TestWorkflows.workflow().toBuilder()
                .setSteps(0, TestWorkflows.workflow().getSteps(0).toBuilder()
                        .setDependency("missing").build()).build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not declared");

        assertThatThrownBy(() -> WorkflowValidation.validate(TestWorkflows.workflow().toBuilder()
                .setSteps(0, TestWorkflows.workflow().getSteps(0).toBuilder()
                        .setCompletion(StepCompletion.STEP_COMPLETION_UNSPECIFIED).build()).build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("completion");
    }

    @Test
    void namesAreSlugsWhileReferencesKeepTheWiderPathSafeFamily() {
        // Local names (workflow and step identities) follow the slug contract now.
        assertThatThrownBy(() -> WorkflowValidation.validate(TestWorkflows.workflow().toBuilder()
                .setName("Analyze-Document").build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("slug");

        // Reference names stay path-safe: the compiler uses service FQNs as aliases
        // and the structured sentinel carries a hyphen.
        Workflow fqnAliased = TestWorkflows.workflow().toBuilder()
                .clearDependencies()
                .addDependencies(TestWorkflows.dependency().toBuilder()
                        .setAlias("pipeline.test.Worker")
                        .setServiceProfile("pipeline.test.Worker").build())
                .setSteps(0, TestWorkflows.workflow().getSteps(0).toBuilder()
                        .setDependency("pipeline.test.Worker").build())
                .build();
        WorkflowValidation.validate(fqnAliased);

        assertThatThrownBy(() -> WorkflowValidation.validate(TestWorkflows.workflow().toBuilder()
                .clearDependencies()
                .addDependencies(TestWorkflows.dependency().toBuilder()
                        .setAlias("../escape").build())
                .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("path-safe reference");
    }

    @Test
    void rejectsChangedWorkflowContentUnderAnExistingFingerprint() {
        VersionedWorkflow promoted = TestWorkflows.versionedWorkflow();
        Workflow changed = promoted.getWorkflow().toBuilder().setDescription("changed").build();

        assertThatThrownBy(() -> WorkflowValidation.validate(promoted.toBuilder()
                .setWorkflow(changed).build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not match");
    }

    @Test
    void rejectsInvalidArtifactIdentityMediaTypeAndBounds() {
        ArtifactReference artifact = TestWorkflows.artifact("{}", true);
        WorkflowValidation.validate(artifact.toBuilder().setSizeBytes(0).build());
        assertThatThrownBy(() -> WorkflowValidation.validate(artifact.toBuilder()
                .setSha256("not-a-hash").build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("SHA-256");
        assertThatThrownBy(() -> WorkflowValidation.validate(artifact.toBuilder()
                .setMediaType("not a media type").build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("type/subtype");
        assertThatThrownBy(() -> WorkflowValidation.validate(artifact.toBuilder()
                .setSizeBytes(WorkflowValidation.MAX_ARTIFACT_BYTES + 1L).build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maximum");
        assertThatThrownBy(() -> WorkflowValidation.validate(artifact.toBuilder()
                .setSizeBytes(-1L).build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maximum");
    }

    @Test
    void rejectsTerminalRunWithoutCompletionAndReversedTimestamps() {
        RunEvidence evidence = TestWorkflows.evidence();
        assertThatThrownBy(() -> WorkflowValidation.validate(evidence.toBuilder()
                .clearCompletedAt().build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("completed_at must be present");
        assertThatThrownBy(() -> WorkflowValidation.validate(evidence.toBuilder()
                .setStartedAt(Timestamp.newBuilder().setSeconds(30).build()).build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not precede");
    }

    @Test
    void permitsRunningEvidenceWithoutCompletionButRejectsUnknownStatus() {
        WorkflowValidation.validate(TestWorkflows.evidence().toBuilder()
                .setStatus(RunStatus.RUN_STATUS_RUNNING)
                .clearCompletedAt()
                .build());

        assertThatThrownBy(() -> WorkflowValidation.validate(TestWorkflows.evidence().toBuilder()
                .setStatusValue(99).build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("status must be recognized");
    }
}
