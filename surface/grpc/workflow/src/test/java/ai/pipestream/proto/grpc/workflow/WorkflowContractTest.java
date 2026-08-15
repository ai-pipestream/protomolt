package ai.pipestream.proto.grpc.workflow;

import ai.pipestream.proto.grpc.workflow.v1.ArtifactReference;
import ai.pipestream.proto.grpc.workflow.v1.Workflow;
import ai.pipestream.proto.grpc.workflow.v1.RunEvidence;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class WorkflowContractTest {

    @Test
    void workflowAndEvidenceRoundTripWithoutEmbeddingArtifactsOrCredentials() throws Exception {
        Workflow workflow = TestWorkflows.workflow();
        RunEvidence evidence = TestWorkflows.evidence();

        assertThat(Workflow.parseFrom(workflow.toByteArray())).isEqualTo(workflow);
        assertThat(RunEvidence.parseFrom(evidence.toByteArray())).isEqualTo(evidence);
        assertThat(workflow.getDependencies(0).getDescriptorFingerprint()).hasSize(64);
        assertThat(Workflow.getDescriptor().getFields())
                .extracting(field -> field.getName())
                .doesNotContain("password", "secret", "credential", "private_key", "artifact_bytes");
        assertThat(RunEvidence.getDescriptor().getFields())
                .extracting(field -> field.getName())
                .doesNotContain("request_bytes", "response_bytes", "artifact_bytes");
        assertThat(ArtifactReference.getDescriptor().findFieldByName("content")).isNull();
    }

    @Test
    void workflowFingerprintChangesWithExecutableContent() {
        Workflow original = TestWorkflows.workflow();
        Workflow changed = original.toBuilder()
                .setSteps(0, original.getSteps(0).toBuilder()
                        .setMethod("example.v1.Tokenizer/TokenizeStream").build())
                .build();

        assertThat(WorkflowValidation.fingerprint(original))
                .isNotEqualTo(WorkflowValidation.fingerprint(changed));
    }
}
