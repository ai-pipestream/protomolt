package ai.protomolt.proto.grpc.workflow;

import ai.protomolt.proto.grpc.profile.ServiceProfileValidation;
import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import ai.protomolt.proto.grpc.workflow.v1.Workflow;
import ai.protomolt.proto.grpc.workflow.v1.WorkflowStep;
import ai.protomolt.proto.grpc.workflow.v1.RunEvidence;
import ai.protomolt.proto.grpc.workflow.v1.RunStatus;
import ai.protomolt.proto.grpc.workflow.v1.ServiceDependency;
import ai.protomolt.proto.grpc.workflow.v1.StepCompletion;
import ai.protomolt.proto.grpc.workflow.v1.StepEvidence;
import ai.protomolt.proto.grpc.workflow.v1.StepStatus;
import ai.protomolt.proto.grpc.workflow.v1.VersionedWorkflow;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;

final class TestWorkflows {

    static final String DESCRIPTOR_FINGERPRINT = ServiceProfileValidation.sha256(
            "descriptor".getBytes(java.nio.charset.StandardCharsets.UTF_8));

    private TestWorkflows() {
    }

    static Workflow workflow() {
        return Workflow.newBuilder()
                .setName("analyze-document")
                .setDescription("Tokenize a document through a registered service")
                .setInputType("example.v1.Document")
                .addDependencies(dependency())
                .addSteps(WorkflowStep.newBuilder()
                        .setName("tokenize")
                        .setDependency("nlp")
                        .setMethod("example.v1.Tokenizer/Tokenize")
                        .addRules("text=input.body")
                        .setValidateResponse(true)
                        .setCompletion(StepCompletion.STEP_COMPLETION_LIVE)
                        .build())
                .setDeadline(Duration.newBuilder().setSeconds(30).build())
                .build();
    }

    static ServiceDependency dependency() {
        return ServiceDependency.newBuilder()
                .setAlias("nlp")
                .setServiceProfile("tokenizer")
                .setEndpoint("local")
                .setDescriptorFingerprint(DESCRIPTOR_FINGERPRINT)
                .build();
    }

    static ArtifactReference artifact(String content, boolean redacted) {
        byte[] bytes = content.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        return ArtifactReference.newBuilder()
                .setSha256(ServiceProfileValidation.sha256(bytes))
                .setMediaType("application/json")
                .setSizeBytes(bytes.length)
                .setRedacted(redacted)
                .build();
    }

    static VersionedWorkflow versionedWorkflow() {
        Workflow workflow = workflow();
        return VersionedWorkflow.newBuilder()
                .setWorkflow(workflow)
                .setVersion("v1")
                .setWorkflowFingerprint(WorkflowValidation.fingerprint(workflow))
                .setCreatedAt(timestamp(10))
                .build();
    }

    static RunEvidence evidence() {
        return RunEvidence.newBuilder()
                .setRunId("run-001")
                .setWorkflowName(workflow().getName())
                .setWorkflowVersion("v1")
                .setWorkflowFingerprint(WorkflowValidation.fingerprint(workflow()))
                .setStatus(RunStatus.RUN_STATUS_SUCCEEDED)
                .setStartedAt(timestamp(20))
                .setCompletedAt(timestamp(21))
                .addDependencies(dependency())
                .setInputArtifact(artifact("{\"body\":\"hello\"}", true))
                .setOutputArtifact(artifact("{\"tokens\":[\"hello\"]}", true))
                .addSteps(StepEvidence.newBuilder()
                        .setStepName("tokenize")
                        .setMethod("example.v1.Tokenizer/Tokenize")
                        .setStatus(StepStatus.STEP_STATUS_SUCCEEDED)
                        .setStartedAt(timestamp(20))
                        .setCompletedAt(timestamp(21))
                        .setRequestArtifact(artifact("{\"text\":\"hello\"}", true))
                        .setResponseArtifact(artifact("{\"tokens\":[\"hello\"]}", true))
                        .setGrpcStatusCode(0)
                        .build())
                .build();
    }

    private static Timestamp timestamp(long seconds) {
        return Timestamp.newBuilder().setSeconds(seconds).build();
    }
}
