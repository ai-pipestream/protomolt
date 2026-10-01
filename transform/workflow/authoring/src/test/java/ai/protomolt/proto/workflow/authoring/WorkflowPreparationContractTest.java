package ai.protomolt.proto.workflow.authoring;

import ai.protomolt.proto.delegation.v1.CheckEvidence;
import ai.protomolt.proto.delegation.v1.CheckVerdict;
import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import ai.protomolt.proto.grpc.workflow.v1.ServiceDependency;
import ai.protomolt.proto.grpc.workflow.v1.StepCompletion;
import ai.protomolt.proto.grpc.workflow.v1.Workflow;
import ai.protomolt.proto.grpc.workflow.v1.WorkflowStep;
import ai.protomolt.proto.samples.starter.v1.WorkflowAcceptanceFixture;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringDeliverable;
import ai.protomolt.proto.samples.starter.v1.WorkflowDeliverable;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.workflow.authoring.v1.PrepareWorkflowCandidateRequest;
import ai.protomolt.proto.workflow.authoring.v1.PrepareWorkflowCandidateResponse;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowPreparationBinding;
import com.google.protobuf.ByteString;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Shape-level contract tests only; lifecycle and artifact semantics remain handler checks. */
class WorkflowPreparationContractTest {
    private static final String TASK_ID = "dcff5265-1466-46c0-bbe0-a9dc39da0e82";
    private static final String PREPARATION_ID = "00000000-0000-4000-8000-000000000002";
    private static final String HASH_A = "a".repeat(64);
    private static final String HASH_B = "b".repeat(64);

    @Test
    void requestValidatesGeneratedAndDynamicMessagesAndKeepsTaskSpelling() throws Exception {
        var validator = ProtoValidator.forMessageType(PrepareWorkflowCandidateRequest.getDescriptor());
        PrepareWorkflowCandidateRequest minimum = request(TASK_ID, 1, 1, PREPARATION_ID,
                ByteString.copyFrom("é".getBytes(StandardCharsets.UTF_8)));
        PrepareWorkflowCandidateRequest maximum = request(TASK_ID, 1024, 1024, PREPARATION_ID,
                ByteString.copyFrom(new byte[1_048_576]));

        assertValidBoth(validator, minimum);
        assertValidBoth(validator, maximum);
        assertValidBoth(validator, minimum.toBuilder()
                .setExecutableSourceJson(ByteString.copyFromUtf8("x")).build());
        assertValidBoth(validator, minimum.toBuilder()
                .setExecutableSourceJson(ByteString.copyFromUtf8("é".repeat(524_288))).build());
        assertInvalidBoth(validator, minimum.toBuilder()
                .setExecutableSourceJson(ByteString.copyFromUtf8("é".repeat(524_288) + "x")).build());
        assertThat(minimum.getExecutableSourceJson().size()).isEqualTo(2);
        assertThat(maximum.getExecutableSourceJson().size()).isEqualTo(1_048_576);

        // UUID shape accepts a UUID spelling; exact task-key equality is a handler binding check.
        String suppliedSpelling = TASK_ID.toUpperCase(java.util.Locale.ROOT);
        PrepareWorkflowCandidateRequest exact = request(suppliedSpelling, 3, 7, PREPARATION_ID,
                ByteString.copyFromUtf8("not JSON"));
        assertThat(validator.validate(exact).valid()).isTrue();
        DynamicMessage decoded = DynamicMessage.parseFrom(exact.getDescriptorForType(), exact.toByteArray());
        assertThat(decoded.getField(exact.getDescriptorForType().findFieldByName("task_id")))
                .isEqualTo(suppliedSpelling);
        // JSON syntax, UTF-8 policy, source identity and authorization are not inferred from shape validity.
    }

    @Test
    void requestRejectsMissingFieldsBadIdentifiersOutOfRangeCountersAndByteBounds() throws Exception {
        var validator = ProtoValidator.forMessageType(PrepareWorkflowCandidateRequest.getDescriptor());
        assertInvalidBoth(validator, PrepareWorkflowCandidateRequest.getDefaultInstance());
        assertInvalidBoth(validator, request("not-a-uuid", 1, 1, PREPARATION_ID,
                ByteString.copyFromUtf8("{}")));
        assertInvalidBoth(validator, request(TASK_ID, 1, 1,
                "00000000-0000-4000-8000-00000000000A", ByteString.copyFromUtf8("{}")));
        assertInvalidBoth(validator, request(TASK_ID, 0, 1, PREPARATION_ID, ByteString.copyFromUtf8("{}")));
        assertInvalidBoth(validator, request(TASK_ID, 1025, 1, PREPARATION_ID, ByteString.copyFromUtf8("{}")));
        assertInvalidBoth(validator, request(TASK_ID, 1, 0, PREPARATION_ID, ByteString.copyFromUtf8("{}")));
        assertInvalidBoth(validator, request(TASK_ID, 1, 1025, PREPARATION_ID, ByteString.copyFromUtf8("{}")));
        assertInvalidBoth(validator, request(TASK_ID, 1, 1, PREPARATION_ID, ByteString.EMPTY));
        assertInvalidBoth(validator, request(TASK_ID, 1, 1, PREPARATION_ID,
                ByteString.copyFrom(new byte[1_048_577])));
    }

    @Test
    void bindingValidatesIdentifiersCountersAndBothDigests() throws Exception {
        var validator = ProtoValidator.forMessageType(WorkflowPreparationBinding.getDescriptor());
        WorkflowPreparationBinding valid = binding(TASK_ID, 1, 1024, PREPARATION_ID, HASH_A, HASH_B);
        assertValidBoth(validator, valid);

        assertInvalidBoth(validator, WorkflowPreparationBinding.getDefaultInstance());
        assertInvalidBoth(validator, binding("bad", 1, 1, PREPARATION_ID, HASH_A, HASH_B));
        assertInvalidBoth(validator, binding(TASK_ID, 0, 1, PREPARATION_ID, HASH_A, HASH_B));
        assertInvalidBoth(validator, binding(TASK_ID, 1025, 1, PREPARATION_ID, HASH_A, HASH_B));
        assertInvalidBoth(validator, binding(TASK_ID, 1, 0, PREPARATION_ID, HASH_A, HASH_B));
        assertInvalidBoth(validator, binding(TASK_ID, 1, 1025, PREPARATION_ID, HASH_A, HASH_B));
        assertInvalidBoth(validator, binding(TASK_ID, 1, 1,
                "00000000-0000-4000-8000-00000000000A", HASH_A, HASH_B));
        assertInvalidBoth(validator, binding(TASK_ID, 1, 1, PREPARATION_ID, "A".repeat(64), HASH_B));
        assertInvalidBoth(validator, binding(TASK_ID, 1, 1, PREPARATION_ID, "short", HASH_B));
        assertInvalidBoth(validator, binding(TASK_ID, 1, 1, PREPARATION_ID, HASH_A, "short"));
    }

    @Test
    void responseRequiresNestedMessagesAndBindsSourceDigestWithCel() throws Exception {
        var validator = ProtoValidator.forMessageType(PrepareWorkflowCandidateResponse.getDescriptor());
        WorkflowAuthoringDeliverable authored = authored(HASH_B);
        PrepareWorkflowCandidateResponse valid = PrepareWorkflowCandidateResponse.newBuilder()
                .setBinding(binding(TASK_ID, 1, 2, PREPARATION_ID, HASH_A, HASH_B))
                .setAuthored(authored).build();
        assertValidBoth(validator, valid);

        assertInvalidBoth(validator, PrepareWorkflowCandidateResponse.getDefaultInstance());
        assertInvalidBoth(validator, valid.toBuilder().clearBinding().build());
        assertInvalidBoth(validator, valid.toBuilder().clearAuthored().build());
        PrepareWorkflowCandidateResponse wrongSource = valid.toBuilder()
                .setBinding(valid.getBinding().toBuilder().setSourceSha256(HASH_A)).build();
        assertThat(validator.validate(wrongSource).violations())
                .anySatisfy(violation -> assertThat(violation.ruleId()).isEqualTo("preparation-source-binding"));
        assertThat(validator.validate(DynamicMessage.parseFrom(wrongSource.getDescriptorForType(),
                wrongSource.toByteArray())).valid()).isFalse();
    }

    @Test
    void validRequestAndHashesDoNotProveJsonOrReferencedArtifactSemantics() throws Exception {
        byte[] invalidJson = "{ definitely not json".getBytes(StandardCharsets.UTF_8);
        String claimed = sha256(invalidJson);
        PrepareWorkflowCandidateRequest shaped = request(TASK_ID, 1, 1, PREPARATION_ID,
                ByteString.copyFrom(invalidJson));
        WorkflowPreparationBinding forged = binding(TASK_ID, 1, 1, PREPARATION_ID, HASH_A, claimed);

        assertThat(ProtoValidator.forMessageType(shaped.getDescriptorForType())
                .validate(shaped).valid()).isTrue();
        assertThat(ProtoValidator.forMessageType(forged.getDescriptorForType())
                .validate(forged).valid()).isTrue();
        // These bounded messages do not parse JSON, authenticate a caller, locate an offer,
        // or verify that the claimed offer/source digests name trusted artifacts.
    }

    private static PrepareWorkflowCandidateRequest request(String taskId, int attempt, int revision,
            String preparationId, ByteString source) {
        return PrepareWorkflowCandidateRequest.newBuilder().setTaskId(taskId).setAttempt(attempt)
                .setRevision(revision).setPreparationId(preparationId).setExecutableSourceJson(source).build();
    }

    private static WorkflowPreparationBinding binding(String taskId, int attempt, int revision,
            String preparationId, String offerHash, String sourceHash) {
        return WorkflowPreparationBinding.newBuilder().setTaskId(taskId).setAttempt(attempt)
                .setRevision(revision).setPreparationId(preparationId)
                .setOfferEntrySha256(offerHash).setSourceSha256(sourceHash).build();
    }

    static WorkflowAuthoringDeliverable authored(String sourceHash) {
        ArtifactReference evidence = artifact(HASH_A, "application/x-protobuf");
        Workflow workflow = Workflow.newBuilder().setName("prepared-workflow")
                .setInputType("example.v1.Input").setValidateContract(true)
                .addDependencies(ServiceDependency.newBuilder().setAlias("echo")
                        .setServiceProfile("example.v1.Echo").setEndpoint("local")
                        .setDescriptorFingerprint(HASH_A))
                .addSteps(WorkflowStep.newBuilder().setName("call").setDependency("echo")
                        .setMethod("example.v1.Echo/Run")
                        .setCompletion(StepCompletion.STEP_COMPLETION_LIVE))
                .setDeadline(Duration.newBuilder().setSeconds(30)).build();
        WorkflowDeliverable deliverable = WorkflowDeliverable.newBuilder().setWorkflow(workflow)
                .setWorkflowArtifact(evidence).setDescriptors(evidence).addFixtures(evidence)
                .addChecks(CheckEvidence.newBuilder().setCheckName("prepare")
                        .setVerdict(CheckVerdict.CHECK_VERDICT_PASSED)
                        .setRanAt(Timestamp.newBuilder().setSeconds(1)).addArtifacts(evidence))
                .setRunId("prepare-fixture-run").setReceipt(evidence).build();
        return WorkflowAuthoringDeliverable.newBuilder().setDeliverable(deliverable)
                .setExecutableSource(artifact(sourceHash, "application/json"))
                .addAcceptanceFixtures(WorkflowAcceptanceFixture.newBuilder()
                        .setName("default-fixture").setInput(evidence).setExpectedOutput(evidence)).build();
    }

    private static ArtifactReference artifact(String hash, String mediaType) {
        return ArtifactReference.newBuilder().setSha256(hash).setMediaType(mediaType).setSizeBytes(24).build();
    }

    private static String sha256(byte[] bytes) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }

    private static void assertValidBoth(ProtoValidator validator, com.google.protobuf.Message message)
            throws Exception {
        assertThat(validator.validate(message).valid())
                .as("generated violations: %s", validator.validate(message).violations()).isTrue();
        DynamicMessage dynamic = DynamicMessage.parseFrom(message.getDescriptorForType(), message.toByteArray());
        assertThat(validator.validate(dynamic).valid()).isTrue();
    }

    private static void assertInvalidBoth(ProtoValidator validator, com.google.protobuf.Message message)
            throws Exception {
        assertThat(validator.validate(message).valid()).isFalse();
        DynamicMessage dynamic = DynamicMessage.parseFrom(message.getDescriptorForType(), message.toByteArray());
        assertThat(validator.validate(dynamic).valid()).isFalse();
    }
}
