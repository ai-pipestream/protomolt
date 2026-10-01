package ai.protomolt.proto.workflow.authoring;

import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import ai.protomolt.proto.samples.starter.v1.WorkflowAcceptedCandidate;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.workflow.authoring.v1.GetWorkflowLaunchInputContractRequest;
import ai.protomolt.proto.workflow.authoring.v1.GetWorkflowLaunchInputContractResponse;
import ai.protomolt.proto.workflow.authoring.v1.PrepareWorkflowLaunchInputRequest;
import ai.protomolt.proto.workflow.authoring.v1.PrepareWorkflowLaunchInputResponse;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowLaunchInputServiceGrpc;
import com.google.protobuf.ByteString;
import com.google.protobuf.DynamicMessage;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Shape-level validation for launch input RPCs; acceptance and artifact custody remain handler checks. */
class WorkflowLaunchInputContractTest {
    private static final String TASK_ID = "dcff5265-1466-46c0-bbe0-a9dc39da0e82";
    private static final String HASH = "a".repeat(64);
    private static final String INPUT_TYPE = "example.v1.Input";
    private static final String PROTOBUF = "application/x-protobuf";

    @Test
    void requestsRequireAValidAcceptedCandidateAndValidateGeneratedAndDynamicMessages() throws Exception {
        var getValidator = ProtoValidator.forMessageType(GetWorkflowLaunchInputContractRequest.getDescriptor());
        var prepareValidator = ProtoValidator.forMessageType(PrepareWorkflowLaunchInputRequest.getDescriptor());
        WorkflowAcceptedCandidate accepted = acceptance();

        assertValidBoth(getValidator, GetWorkflowLaunchInputContractRequest.newBuilder()
                .setAcceptance(accepted).build());
        assertValidBoth(prepareValidator, PrepareWorkflowLaunchInputRequest.newBuilder()
                .setAcceptance(accepted).setInputJson(ByteString.copyFromUtf8("{}")).build());
        assertInvalidBoth(getValidator, GetWorkflowLaunchInputContractRequest.getDefaultInstance());
        assertInvalidBoth(prepareValidator, PrepareWorkflowLaunchInputRequest.getDefaultInstance());

        for (WorkflowAcceptedCandidate invalid : List.of(
                accepted.toBuilder().setTaskId("not-a-uuid").build(),
                accepted.toBuilder().setAttempt(0).build(),
                accepted.toBuilder().setAttempt(1025).build(),
                accepted.toBuilder().setRevision(0).build(),
                accepted.toBuilder().setRevision(1025).build(),
                accepted.toBuilder().setTaskSpecSha256("short").build(),
                accepted.toBuilder().setCandidateSha256("A".repeat(64)).build(),
                accepted.toBuilder().setAcceptedEntrySha256("").build())) {
            assertInvalidBoth(getValidator, GetWorkflowLaunchInputContractRequest.newBuilder()
                    .setAcceptance(invalid).build());
            assertInvalidBoth(prepareValidator, PrepareWorkflowLaunchInputRequest.newBuilder()
                    .setAcceptance(invalid).setInputJson(ByteString.copyFromUtf8("{}")).build());
        }
    }

    @Test
    void inputJsonHasByteBoundsButJsonParsingIsAHandlerObligation() throws Exception {
        var validator = ProtoValidator.forMessageType(PrepareWorkflowLaunchInputRequest.getDescriptor());
        byte[] oneByte = new byte[] {'x'};
        byte[] maximum = new byte[4_194_304];
        byte[] overMaximum = new byte[4_194_305];
        assertValidBoth(validator, prepare(ByteString.copyFrom(oneByte)));
        assertValidBoth(validator, prepare(ByteString.copyFrom(maximum)));
        assertInvalidBoth(validator, prepare(ByteString.EMPTY));
        assertInvalidBoth(validator, prepare(ByteString.copyFrom(overMaximum)));

        PrepareWorkflowLaunchInputRequest malformedJson = prepare(ByteString.copyFromUtf8("{ not-json"));
        assertThat(validator.validate(malformedJson).valid()).isTrue();
        assertThat(validator.validate(dynamic(malformedJson)).valid()).isTrue();
        // JSON syntax, strict UTF-8, descriptor-based parsing, and input-message validation are handler checks.
    }

    @Test
    void contractResponseChecksInputTypeDescriptorBytesAndDescriptorReferenceMetadata() throws Exception {
        var validator = ProtoValidator.forMessageType(GetWorkflowLaunchInputContractResponse.getDescriptor());
        GetWorkflowLaunchInputContractResponse valid = contractResponse(acceptance(), INPUT_TYPE,
                ByteString.copyFrom(new byte[] {0x0a, 0x01, 0x01}), descriptorRef(3, PROTOBUF, false));
        assertValidBoth(validator, valid);
        assertInvalidBoth(validator, GetWorkflowLaunchInputContractResponse.getDefaultInstance());
        assertInvalidBoth(validator, valid.toBuilder().clearAcceptance().build());
        assertInvalidBoth(validator, valid.toBuilder().clearInputType().build());
        // A message in the empty protobuf package has a single-component full name.
        assertValidBoth(validator, valid.toBuilder().setInputType("Input").build());
        assertInvalidBoth(validator, valid.toBuilder().setInputType("x".repeat(513)).build());
        assertInvalidBoth(validator, valid.toBuilder().setInputType("example.v1.1Input").build());
        assertInvalidBoth(validator, valid.toBuilder().clearDescriptors().build());
        assertInvalidBoth(validator, valid.toBuilder().clearDescriptorSet().build());
        assertValidBoth(validator, valid.toBuilder()
                .setDescriptorSet(ByteString.copyFrom(new byte[4_194_304]))
                .setDescriptors(descriptorRef(4_194_304, PROTOBUF, false)).build());
        assertInvalidBoth(validator, valid.toBuilder().setDescriptorSet(ByteString.copyFrom(new byte[4_194_305])).build());

        assertRule(valid.toBuilder().setDescriptors(descriptorRef(2, PROTOBUF, false)).build(),
                "launch-input-descriptor-metadata");
        assertRule(valid.toBuilder().setDescriptors(descriptorRef(3, "application/json", false)).build(),
                "launch-input-descriptor-metadata");
        assertRule(valid.toBuilder().setDescriptors(descriptorRef(3, PROTOBUF, true)).build(),
                "launch-input-descriptor-metadata");
    }

    @Test
    void preparedReferenceMustBeUnredactedProtobufAndBoundedButZeroByteMessageIsValid() throws Exception {
        var validator = ProtoValidator.forMessageType(PrepareWorkflowLaunchInputResponse.getDescriptor());
        PrepareWorkflowLaunchInputResponse valid = PrepareWorkflowLaunchInputResponse.newBuilder()
                .setAcceptance(acceptance()).setInput(artifact(HASH, PROTOBUF, 0, false)).build();
        assertValidBoth(validator, valid);
        assertValidBoth(validator, valid.toBuilder()
                .setInput(artifact(HASH, PROTOBUF, 4_194_304, false)).build());
        assertInvalidBoth(validator, PrepareWorkflowLaunchInputResponse.getDefaultInstance());
        assertInvalidBoth(validator, valid.toBuilder().clearAcceptance().build());
        assertInvalidBoth(validator, valid.toBuilder().clearInput().build());
        assertRule(valid.toBuilder().setInput(artifact(HASH, "application/json", 0, false)).build(),
                "prepared-launch-input-protobuf");
        assertRule(valid.toBuilder().setInput(artifact(HASH, PROTOBUF, 0, true)).build(),
                "prepared-launch-input-protobuf");
        assertRule(valid.toBuilder().setInput(artifact(HASH, PROTOBUF, 4_194_305, false)).build(),
                "prepared-launch-input-protobuf");
        assertInvalidBoth(validator, valid.toBuilder().setInput(artifact("bad-hash", PROTOBUF, 0, false)).build());
        assertInvalidBoth(validator, valid.toBuilder().setInput(artifact(HASH, "", 0, false)).build());
    }

    @Test
    void serviceDescriptorDeclaresOnlyTheTwoInputPreparationMethods() {
        var descriptor = WorkflowLaunchInputServiceGrpc.getServiceDescriptor();
        assertThat(descriptor.getName()).isEqualTo("ai.protomolt.proto.workflow.authoring.v1.WorkflowLaunchInputService");
        assertThat(descriptor.getMethods()).extracting(method -> method.getBareMethodName())
                .containsExactly("GetWorkflowLaunchInputContract", "PrepareWorkflowLaunchInput");
    }

    @Test
    void validSelectorAndHashShapesDoNotProveTrustedAcceptanceOrArtifactCustody() throws Exception {
        var validator = ProtoValidator.forMessageType(GetWorkflowLaunchInputContractRequest.getDescriptor());
        var forged = GetWorkflowLaunchInputContractRequest.newBuilder().setAcceptance(acceptance()).build();
        assertThat(validator.validate(forged).valid()).isTrue();
        assertThat(validator.validate(dynamic(forged)).valid()).isTrue();
        // The handler must compare this selector with trusted transcript state and rehash stored descriptor bytes.
    }

    private static WorkflowAcceptedCandidate acceptance() {
        return WorkflowAcceptedCandidate.newBuilder().setTaskId(TASK_ID).setAttempt(1).setRevision(1)
                .setTaskSpecSha256(HASH).setCandidateSha256("b".repeat(64))
                .setAcceptedEntrySha256("c".repeat(64)).build();
    }

    private static PrepareWorkflowLaunchInputRequest prepare(ByteString input) {
        return PrepareWorkflowLaunchInputRequest.newBuilder().setAcceptance(acceptance()).setInputJson(input).build();
    }

    private static GetWorkflowLaunchInputContractResponse contractResponse(WorkflowAcceptedCandidate accepted,
            String inputType, ByteString descriptors, ArtifactReference reference) {
        return GetWorkflowLaunchInputContractResponse.newBuilder().setAcceptance(accepted).setInputType(inputType)
                .setDescriptorSet(descriptors).setDescriptors(reference).build();
    }

    private static ArtifactReference descriptorRef(long size, String mediaType, boolean redacted) {
        return artifact(HASH, mediaType, size, redacted);
    }

    private static ArtifactReference artifact(String hash, String mediaType, long size, boolean redacted) {
        return ArtifactReference.newBuilder().setSha256(hash).setMediaType(mediaType)
                .setSizeBytes(size).setRedacted(redacted).build();
    }

    private static DynamicMessage dynamic(com.google.protobuf.Message message) throws Exception {
        return DynamicMessage.parseFrom(message.getDescriptorForType(), message.toByteArray());
    }

    private static void assertRule(com.google.protobuf.Message message, String ruleId) throws Exception {
        var validator = ProtoValidator.forMessageType(message.getDescriptorForType());
        assertThat(validator.validate(message).violations()).anySatisfy(
                violation -> assertThat(violation.ruleId()).isEqualTo(ruleId));
        assertThat(validator.validate(dynamic(message)).violations()).anySatisfy(
                violation -> assertThat(violation.ruleId()).isEqualTo(ruleId));
    }

    private static void assertValidBoth(ProtoValidator validator, com.google.protobuf.Message message)
            throws Exception {
        assertThat(validator.validate(message).valid())
                .as("generated violations: %s", validator.validate(message).violations()).isTrue();
        assertThat(validator.validate(dynamic(message)).valid()).isTrue();
    }

    private static void assertInvalidBoth(ProtoValidator validator, com.google.protobuf.Message message)
            throws Exception {
        assertThat(validator.validate(message).valid()).isFalse();
        assertThat(validator.validate(dynamic(message)).valid()).isFalse();
    }
}
