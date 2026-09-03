package ai.protomolt.proto.grpc.workflow;

import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import ai.protomolt.proto.grpc.workflow.v1.Workflow;
import ai.protomolt.proto.grpc.workflow.v1.RunEvidence;
import ai.protomolt.proto.grpc.workflow.v1.RunStatus;
import ai.protomolt.proto.grpc.workflow.v1.StepCompletion;
import ai.protomolt.proto.grpc.workflow.v1.VersionedWorkflow;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.ValidationResult;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
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
                .hasMessageContaining("path-safe name");

        // The standalone dependency contract carries the same annotations for
        // its direct pipeline callers.
        assertThatThrownBy(() -> WorkflowValidation.validate(TestWorkflows.dependency()
                .toBuilder().setAlias("../escape").build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("path-safe name");
    }

    @Test
    void declaredIdentityRulesCarryTheNameFamiliesWithoutTheHandValidator() {
        // The contract annotations alone refuse bad identities: the divergence
        // class the hand validator used to own is pinned at the proto layer.
        assertThat(ruleIds(TestWorkflows.workflow().toBuilder()
                .setName("Analyze-Document").build()))
                .contains("string.slug");
        assertThat(ruleIds(TestWorkflows.dependency().toBuilder()
                .setAlias("../escape").build()))
                .contains("string.path_safe_name");
        assertThat(ruleIds(TestWorkflows.dependency().toBuilder()
                .setDescriptorFingerprint("not-a-hash").build()))
                .contains("string.sha256_hex");
        assertThat(ruleIds(TestWorkflows.dependency().toBuilder()
                .clearDescriptorFingerprint().build()))
                .contains("required");
        assertThat(ruleIds(TestWorkflows.artifact("{}", true).toBuilder()
                .setMediaType("not a media type").build()))
                .contains("string.mime_type");
        assertThat(ruleIds(TestWorkflows.versionedWorkflow().toBuilder()
                .setVersion("V1").build()))
                .contains("string.slug");
        // An FQN alias passes the reference family; the same value refuses as a slug.
        assertThat(ruleIds(TestWorkflows.dependency().toBuilder()
                .setAlias("pipeline.test.Worker").build()))
                .doesNotContain("string.path_safe_name");
        // A blank promoted version is allowed on run evidence; a bad one refuses.
        assertThat(ruleIds(TestWorkflows.evidence().toBuilder()
                .clearWorkflowVersion().build()))
                .isEmpty();
        assertThat(ruleIds(TestWorkflows.evidence().toBuilder()
                .setWorkflowVersion("Not-A-Slug").build()))
                .contains("string.slug");
    }

    private static List<String> ruleIds(Message message) {
        return ProtoValidator.forMessageType(message.getDescriptorForType())
                .validate(message).violations().stream()
                .map(ValidationResult.Violation::ruleId)
                .toList();
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
