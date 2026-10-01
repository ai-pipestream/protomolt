package ai.protomolt.proto.workflow.authoring;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import ai.protomolt.proto.receipt.WorkRecords;
import ai.protomolt.proto.samples.starter.v1.WorkflowAcceptedCandidate;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.workflow.authoring.v1.GetWorkflowLaunchInputContractRequest;
import ai.protomolt.proto.workflow.authoring.v1.GetWorkflowLaunchInputContractResponse;
import ai.protomolt.proto.workflow.authoring.v1.PrepareWorkflowLaunchInputRequest;
import ai.protomolt.proto.workflow.authoring.v1.PrepareWorkflowLaunchInputResponse;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.ByteString;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.UnknownFieldSet;
import io.grpc.Status;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkflowLaunchInputActionsTest {
    private static final String TASK = "00000000-0000-4000-8000-000000000001";
    private static final String HASH_A = "a".repeat(64);
    private static final String HASH_B = "b".repeat(64);
    private static final String HASH_C = "c".repeat(64);
    private static final String GET = "get-workflow-launch-input-contract";
    private static final String PREPARE = "prepare-workflow-launch-input";
    private static final Caller LAUNCHER = Caller.scoped("launcher", Set.of(Scopes.WORKFLOW_LAUNCH));
    private final ActionContext context = ActionContext.create();

    @Test
    void launchScopeRunsBothActionsForGeneratedAndDynamicMessages() throws Exception {
        var operations = new FakeOperations();
        var catalog = catalog(operations);
        assertThat(catalog.get(GET).requiredScope()).isEqualTo(Scopes.WORKFLOW_LAUNCH);
        assertThat(catalog.get(GET).requestType()).isEqualTo(GetWorkflowLaunchInputContractRequest.getDescriptor());
        assertThat(catalog.get(GET).responseType()).isEqualTo(GetWorkflowLaunchInputContractResponse.getDescriptor());
        assertThat(catalog.get(PREPARE).requestType()).isEqualTo(PrepareWorkflowLaunchInputRequest.getDescriptor());
        assertThat(catalog.get(PREPARE).responseType()).isEqualTo(PrepareWorkflowLaunchInputResponse.getDescriptor());

        assertThat(catalog.execute(GET, getRequest(), LAUNCHER)).isEqualTo(operations.contractResponse);
        byte[] dynamicContract = executeDynamic(catalog, GET, getRequest());
        assertThat(dynamicContract).isEqualTo(operations.contractResponse.toByteArray());
        assertThat(catalog.execute(PREPARE, prepareRequest(), LAUNCHER)).isEqualTo(operations.prepareResponse);
        byte[] dynamicPrepared = executeDynamic(catalog, PREPARE, prepareRequest());
        assertThat(dynamicPrepared).isEqualTo(operations.prepareResponse.toByteArray());
        assertThat(operations.contractCalls).hasValue(2);
        assertThat(operations.prepareCalls).hasValue(2);

        ObjectNode jsonRequest = (ObjectNode) context.objectMapper().readTree(com.google.protobuf.util.JsonFormat.printer().print(getRequest()));
        ObjectNode jsonResponse = catalog.execute(GET, jsonRequest, LAUNCHER);
        assertThat(jsonResponse.path("inputType").asText()).isEqualTo("example.v1.Input");
    }

    @Test
    void unrelatedScopesAreDeniedBeforeRequestValidationOrBackendCalls() {
        var operations = new FakeOperations();
        var catalog = catalog(operations);
        for (String scope : Set.of(Scopes.WORKER_COORDINATE, Scopes.WORKFLOW_RUN, Scopes.WORKFLOW_AUTHOR)) {
            Caller caller = Caller.scoped("scoped", Set.of(scope));
            assertCode("permission-denied", () -> catalog.execute(GET,
                    GetWorkflowLaunchInputContractRequest.getDefaultInstance(), caller));
            assertCode("permission-denied", () -> catalog.execute(PREPARE,
                    PrepareWorkflowLaunchInputRequest.getDefaultInstance(), caller));
        }
        assertThat(operations.contractCalls).hasValue(0);
        assertThat(operations.prepareCalls).hasValue(0);
    }

    @Test
    void malformedAndUnknownRequestDataIsRejectedBeforeBackendCalls() {
        var operations = new FakeOperations();
        var catalog = catalog(operations);
        assertCode("invalid-input", () -> catalog.execute(GET,
                GetWorkflowLaunchInputContractRequest.getDefaultInstance(), LAUNCHER));
        assertCode("invalid-input", () -> catalog.execute(PREPARE,
                prepareRequest().toBuilder().setInputJson(ByteString.EMPTY).build(), LAUNCHER));
        var unknownAcceptance = acceptance().toBuilder().setUnknownFields(unknownFields()).build();
        assertCode("invalid-input", () -> catalog.execute(GET,
                GetWorkflowLaunchInputContractRequest.newBuilder().setAcceptance(unknownAcceptance).build(), LAUNCHER));
        var unknownRequest = getRequest().toBuilder().setUnknownFields(unknownFields()).build();
        assertCode("invalid-input", () -> catalog.execute(GET, unknownRequest, LAUNCHER));
        assertThat(operations.contractCalls).hasValue(0);
        assertThat(operations.prepareCalls).hasValue(0);
    }

    @Test
    void malformedNullAndMismatchedBackendResponsesAreRejected() {
        var operations = new FakeOperations();
        var catalog = catalog(operations);
        operations.contractResponse = null;
        assertCode("invalid-upstream-response", () -> catalog.execute(GET, getRequest(), LAUNCHER));

        operations.contractResponse = GetWorkflowLaunchInputContractResponse.getDefaultInstance();
        assertCode("invalid-upstream-response", () -> catalog.execute(GET, getRequest(), LAUNCHER));
        operations.contractResponse = contractResponse(acceptance().toBuilder().setTaskId(
                "00000000-0000-4000-8000-000000000099").build(), "example.v1.Input",
                ByteString.copyFromUtf8("descriptor"));
        assertCode("invalid-upstream-response", () -> catalog.execute(GET, getRequest(), LAUNCHER));
        operations.contractResponse = contractResponse(acceptance(), "example.v1.Input",
                ByteString.copyFromUtf8("descriptor"));
        operations.contractResponse = operations.contractResponse.toBuilder()
                .setDescriptors(artifact(HASH_B, "application/x-protobuf", 10, false)).build();
        assertCode("invalid-upstream-response", () -> catalog.execute(GET, getRequest(), LAUNCHER));

        operations.prepareResponse = null;
        assertCode("invalid-upstream-response", () -> catalog.execute(PREPARE, prepareRequest(), LAUNCHER));
        operations.prepareResponse = PrepareWorkflowLaunchInputResponse.getDefaultInstance();
        assertCode("invalid-upstream-response", () -> catalog.execute(PREPARE, prepareRequest(), LAUNCHER));
        operations.prepareResponse = PrepareWorkflowLaunchInputResponse.newBuilder()
                .setAcceptance(acceptance().toBuilder().setTaskId("00000000-0000-4000-8000-000000000099"))
                .setInput(artifact(HASH_B, "application/x-protobuf", 4, false)).build();
        assertCode("invalid-upstream-response", () -> catalog.execute(PREPARE, prepareRequest(), LAUNCHER));
    }

    @Test
    void generatedDescriptorResponseMayExceedFourMiBWhenWithinContractAndActionLimit() throws Exception {
        byte[] descriptorSet = new byte[4_194_304];
        ByteString descriptorBytes = ByteString.copyFrom(descriptorSet);
        var response = contractResponse(acceptance(), "example.v1.Input", descriptorBytes,
                artifact(WorkRecords.sha256Hex(descriptorSet), "application/x-protobuf", descriptorSet.length, false));
        assertThat(ProtoValidator.forMessageType(response.getDescriptorForType()).validate(response).valid()).isTrue();
        assertThat(response.getSerializedSize()).isGreaterThan(4 * 1024 * 1024).isLessThan(8 * 1024 * 1024);
        var operations = new FakeOperations();
        operations.contractResponse = response;
        assertThat(catalog(operations).execute(GET, getRequest(), LAUNCHER)).isEqualTo(response);
    }

    @Test
    void mapsBackendKindsToSanitizedStableActionErrors() {
        var operations = new FakeOperations();
        var catalog = catalog(operations);
        for (var mapping : java.util.Map.of(
                WorkflowLaunchInputException.Kind.INVALID_INPUT, "invalid-input",
                WorkflowLaunchInputException.Kind.INACTIVE, "workflow-authoring-rejected",
                WorkflowLaunchInputException.Kind.INCOMPATIBLE_ARTIFACT, "workflow-authoring-rejected",
                WorkflowLaunchInputException.Kind.CORRUPT_EVIDENCE, "invalid-upstream-response",
                WorkflowLaunchInputException.Kind.UNAVAILABLE, "workflow-authoring-unavailable",
                WorkflowLaunchInputException.Kind.DEADLINE, "workflow-authoring-deadline").entrySet()) {
            operations.failure = new WorkflowLaunchInputException(mapping.getKey(), "private backend detail",
                    new IOException("secret root cause"));
            assertCode(mapping.getValue(), () -> catalog.execute(GET, getRequest(), LAUNCHER));
        }
        operations.failure = null;
        operations.runtimeFailure = Status.UNAVAILABLE.asRuntimeException();
        assertCode("internal-error", () -> catalog.execute(GET, getRequest(), LAUNCHER));
    }

    private ActionCatalog catalog(FakeOperations operations) {
        return WorkflowLaunchInputActions.register(ActionCatalog.defaults(context), operations);
    }

    private static byte[] executeDynamic(ActionCatalog catalog, String action, com.google.protobuf.Message request)
            throws Exception {
        DynamicMessage dynamic = DynamicMessage.parseFrom(request.getDescriptorForType(), request.toByteArray());
        return catalog.execute(action, dynamic, LAUNCHER).toByteArray();
    }

    private static GetWorkflowLaunchInputContractRequest getRequest() {
        return GetWorkflowLaunchInputContractRequest.newBuilder().setAcceptance(acceptance()).build();
    }

    private static PrepareWorkflowLaunchInputRequest prepareRequest() {
        return PrepareWorkflowLaunchInputRequest.newBuilder().setAcceptance(acceptance())
                .setInputJson(ByteString.copyFromUtf8("{\"name\":\"valid\"}")).build();
    }

    private static WorkflowAcceptedCandidate acceptance() {
        return WorkflowAcceptedCandidate.newBuilder().setTaskId(TASK).setAttempt(1).setRevision(2)
                .setTaskSpecSha256(HASH_A).setCandidateSha256(HASH_B).setAcceptedEntrySha256(HASH_C).build();
    }

    private static GetWorkflowLaunchInputContractResponse contractResponse(WorkflowAcceptedCandidate candidate,
            String inputType, ByteString descriptorBytes) {
        return contractResponse(candidate, inputType, descriptorBytes,
                artifact(WorkRecords.sha256Hex(descriptorBytes.toByteArray()), "application/x-protobuf",
                        descriptorBytes.size(), false));
    }

    private static GetWorkflowLaunchInputContractResponse contractResponse(WorkflowAcceptedCandidate candidate,
            String inputType, ByteString descriptorBytes, ArtifactReference reference) {
        return GetWorkflowLaunchInputContractResponse.newBuilder().setAcceptance(candidate).setInputType(inputType)
                .setDescriptorSet(descriptorBytes).setDescriptors(reference).build();
    }

    private static ArtifactReference artifact(String hash, String mediaType, long size, boolean redacted) {
        return ArtifactReference.newBuilder().setSha256(hash).setMediaType(mediaType).setSizeBytes(size)
                .setRedacted(redacted).build();
    }

    private static UnknownFieldSet unknownFields() {
        return UnknownFieldSet.newBuilder().addField(99,
                UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build();
    }

    private static void assertCode(String code, org.assertj.core.api.ThrowableAssert.ThrowingCallable call) {
        assertThatThrownBy(call).isInstanceOfSatisfying(ActionException.class, failure -> {
            assertThat(failure.code()).isEqualTo(code);
            assertThat(failure.getMessage()).doesNotContain("private backend", "secret root cause");
        });
    }

    private static final class FakeOperations implements WorkflowLaunchInputOperations {
        private final AtomicInteger contractCalls = new AtomicInteger();
        private final AtomicInteger prepareCalls = new AtomicInteger();
        private GetWorkflowLaunchInputContractResponse contractResponse = contractResponse(acceptance(),
                "example.v1.Input", ByteString.copyFromUtf8("descriptor"));
        private PrepareWorkflowLaunchInputResponse prepareResponse = PrepareWorkflowLaunchInputResponse.newBuilder()
                .setAcceptance(acceptance())
                .setInput(artifact(HASH_C, "application/x-protobuf", 17, false)).build();
        private WorkflowLaunchInputException failure;
        private RuntimeException runtimeFailure;

        @Override public GetWorkflowLaunchInputContractResponse contract(GetWorkflowLaunchInputContractRequest request)
                throws WorkflowLaunchInputException {
            contractCalls.incrementAndGet();
            if (failure != null) throw failure;
            if (runtimeFailure != null) throw runtimeFailure;
            return contractResponse;
        }

        @Override public PrepareWorkflowLaunchInputResponse prepare(PrepareWorkflowLaunchInputRequest request)
                throws WorkflowLaunchInputException {
            prepareCalls.incrementAndGet();
            if (failure != null) throw failure;
            if (runtimeFailure != null) throw runtimeFailure;
            return prepareResponse;
        }
    }
}
