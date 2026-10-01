package ai.protomolt.proto.workflow.authoring;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import ai.protomolt.proto.samples.starter.v1.WorkflowAcceptedCandidate;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringLaunchRequest;
import ai.protomolt.proto.workflow.authoring.v1.GetWorkflowLaunchStatusRequest;
import ai.protomolt.proto.workflow.authoring.v1.GetWorkflowLaunchStatusResponse;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowLaunchJobState;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowLaunchJobStatus;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.UnknownFieldSet;
import io.grpc.Status;
import java.io.IOException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkflowLaunchStatusActionsTest {
    private static final String VERB = "get-workflow-launch-status";
    private static final String LAUNCH_ID = "00000000-0000-4000-8000-000000000001";
    private static final String TASK_ID = "00000000-0000-4000-8000-000000000002";
    private static final String HASH_A = "a".repeat(64);
    private static final String HASH_B = "b".repeat(64);
    private static final String HASH_C = "c".repeat(64);
    private static final Caller LAUNCHER = Caller.scoped("launcher", Set.of(Scopes.WORKFLOW_LAUNCH));
    private final ActionContext context = ActionContext.create();

    @Test
    void scopedActionSupportsGeneratedDynamicAndJsonAndBindsCanonicalLaunchUuid() throws Exception {
        var operations = new FakeOperations();
        var catalog = catalog(operations);
        var typed = request(LAUNCH_ID.toUpperCase(java.util.Locale.ROOT));
        operations.response = response(typed.getRequest().toBuilder()
                .setLaunchId(UUID.fromString(typed.getRequest().getLaunchId()).toString()).build(),
                status(LAUNCH_ID, WorkflowLaunchJobState.WORKFLOW_LAUNCH_JOB_STATE_COMPLETED));

        assertThat(catalog.get(VERB).requiredScope()).isEqualTo(Scopes.WORKFLOW_LAUNCH);
        assertThat(catalog.get(VERB).requestType()).isEqualTo(GetWorkflowLaunchStatusRequest.getDescriptor());
        assertThat(catalog.get(VERB).responseType()).isEqualTo(GetWorkflowLaunchStatusResponse.getDescriptor());
        assertThat(catalog.execute(VERB, typed, LAUNCHER)).isEqualTo(operations.response);
        assertThat(operations.calls).hasValue(1);

        DynamicMessage dynamic = DynamicMessage.parseFrom(typed.getDescriptorForType(), typed.toByteArray());
        assertThat(catalog.execute(VERB, dynamic, LAUNCHER).toByteArray()).isEqualTo(operations.response.toByteArray());

        ObjectNode jsonRequest = (ObjectNode) context.objectMapper().readTree(
                com.google.protobuf.util.JsonFormat.printer().print(typed));
        ObjectNode jsonResponse = catalog.execute(VERB, jsonRequest, LAUNCHER);
        assertThat(jsonResponse.path("request").path("launchId").asText()).isEqualTo(LAUNCH_ID);
        assertThat(operations.calls).hasValue(3);
    }

    @Test
    void unscopedAuthorAndRunCallersAreDeniedBeforeInvalidRequestOrBackend() {
        var operations = new FakeOperations();
        var catalog = catalog(operations);
        for (Caller caller : Set.of(
                Caller.scoped("default", Set.of()),
                Caller.scoped("author", Set.of(Scopes.WORKFLOW_AUTHOR)),
                Caller.scoped("runner", Set.of(Scopes.WORKFLOW_RUN)))) {
            assertCode("permission-denied", () -> catalog.execute(VERB,
                    GetWorkflowLaunchStatusRequest.getDefaultInstance(), caller));
        }
        assertThat(operations.calls).hasValue(0);
    }

    @Test
    void invalidOrUnknownRequestIsRejectedBeforeBackend() {
        var operations = new FakeOperations();
        var catalog = catalog(operations);
        assertCode("invalid-input", () -> catalog.execute(VERB,
                GetWorkflowLaunchStatusRequest.getDefaultInstance(), LAUNCHER));
        assertCode("invalid-input", () -> catalog.execute(VERB,
                request(LAUNCH_ID).toBuilder().setUnknownFields(unknown()).build(), LAUNCHER));
        var unknownLaunch = launchRequest(LAUNCH_ID).toBuilder().setUnknownFields(unknown()).build();
        assertCode("invalid-input", () -> catalog.execute(VERB,
                GetWorkflowLaunchStatusRequest.newBuilder().setRequest(unknownLaunch).build(), LAUNCHER));
        assertThat(operations.calls).hasValue(0);
    }

    @Test
    void malformedAndMismatchedSuccessfulResponsesAreRejected() {
        var operations = new FakeOperations();
        var catalog = catalog(operations);
        operations.response = null;
        assertCode("invalid-upstream-response", () -> catalog.execute(VERB, request(LAUNCH_ID), LAUNCHER));
        operations.response = GetWorkflowLaunchStatusResponse.getDefaultInstance();
        assertCode("invalid-upstream-response", () -> catalog.execute(VERB, request(LAUNCH_ID), LAUNCHER));
        operations.response = response(launchRequest(TASK_ID), null);
        assertCode("invalid-upstream-response", () -> catalog.execute(VERB, request(LAUNCH_ID), LAUNCHER));
        operations.response = response(launchRequest(LAUNCH_ID), status(TASK_ID,
                WorkflowLaunchJobState.WORKFLOW_LAUNCH_JOB_STATE_QUEUED));
        assertCode("invalid-upstream-response", () -> catalog.execute(VERB, request(LAUNCH_ID), LAUNCHER));
        operations.response = response(launchRequest(TASK_ID), status(LAUNCH_ID,
                WorkflowLaunchJobState.WORKFLOW_LAUNCH_JOB_STATE_QUEUED));
        assertCode("invalid-upstream-response", () -> catalog.execute(VERB, request(LAUNCH_ID), LAUNCHER));
    }

    @Test
    void mapsBackendFailuresToSanitizedActionCodes() {
        var operations = new FakeOperations();
        var catalog = catalog(operations);
        for (var mapping : java.util.Map.of(
                WorkflowLaunchStatusException.Kind.INVALID_INPUT, "invalid-input",
                WorkflowLaunchStatusException.Kind.CONFLICT, "workflow-launch-conflict",
                WorkflowLaunchStatusException.Kind.CORRUPT_EVIDENCE, "invalid-upstream-response",
                WorkflowLaunchStatusException.Kind.UNAVAILABLE, "workflow-authoring-unavailable",
                WorkflowLaunchStatusException.Kind.DEADLINE, "workflow-authoring-deadline").entrySet()) {
            operations.failure = new WorkflowLaunchStatusException(mapping.getKey(), "private store details",
                    new IOException("secret credential"));
            assertCode(mapping.getValue(), () -> catalog.execute(VERB, request(LAUNCH_ID), LAUNCHER));
        }
        operations.failure = null;
        operations.runtimeFailure = Status.UNAVAILABLE.asRuntimeException();
        assertCode("internal-error", () -> catalog.execute(VERB, request(LAUNCH_ID), LAUNCHER));
    }

    private ActionCatalog catalog(FakeOperations operations) {
        return WorkflowLaunchStatusActions.register(ActionCatalog.defaults(context), operations);
    }

    private static GetWorkflowLaunchStatusRequest request(String launchId) {
        return GetWorkflowLaunchStatusRequest.newBuilder().setRequest(launchRequest(launchId)).build();
    }

    private static WorkflowAuthoringLaunchRequest launchRequest(String launchId) {
        return WorkflowAuthoringLaunchRequest.newBuilder().setLaunchId(launchId)
                .setAcceptance(WorkflowAcceptedCandidate.newBuilder().setTaskId(TASK_ID).setAttempt(1).setRevision(1)
                        .setTaskSpecSha256(HASH_A).setCandidateSha256(HASH_B).setAcceptedEntrySha256(HASH_C))
                .setInput(ArtifactReference.newBuilder().setSha256(HASH_A)
                        .setMediaType("application/x-protobuf").setSizeBytes(1)).build();
    }

    private static GetWorkflowLaunchStatusResponse response(WorkflowAuthoringLaunchRequest request,
            WorkflowLaunchJobStatus job) {
        var builder = GetWorkflowLaunchStatusResponse.newBuilder().setRequest(request);
        if (job == null) builder.setNotAuthorized(true);
        else builder.setJob(job);
        return builder.build();
    }

    private static WorkflowLaunchJobStatus status(String id, WorkflowLaunchJobState state) {
        var builder = WorkflowLaunchJobStatus.newBuilder().setJobId(id).setState(state)
                .setAttempt(1).setMaxAttempts(3).setCreatedAt(com.google.protobuf.Timestamp.newBuilder().setSeconds(1))
                .setUpdatedAt(com.google.protobuf.Timestamp.newBuilder().setSeconds(2));
        if (state == WorkflowLaunchJobState.WORKFLOW_LAUNCH_JOB_STATE_COMPLETED
                || state == WorkflowLaunchJobState.WORKFLOW_LAUNCH_JOB_STATE_FAILED
                || state == WorkflowLaunchJobState.WORKFLOW_LAUNCH_JOB_STATE_DEAD) {
            builder.setCompletedAt(com.google.protobuf.Timestamp.newBuilder().setSeconds(2));
        }
        return builder.build();
    }

    private static UnknownFieldSet unknown() {
        return UnknownFieldSet.newBuilder().addField(99,
                UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build();
    }

    private static void assertCode(String code, org.assertj.core.api.ThrowableAssert.ThrowingCallable call) {
        assertThatThrownBy(call).isInstanceOfSatisfying(ActionException.class, failure -> {
            assertThat(failure.code()).isEqualTo(code);
            assertThat(failure.getMessage()).doesNotContain("private store details", "secret credential");
        });
    }

    private static final class FakeOperations implements WorkflowLaunchStatusOperations {
        private final AtomicInteger calls = new AtomicInteger();
        private GetWorkflowLaunchStatusResponse response = response(launchRequest(LAUNCH_ID),
                status(LAUNCH_ID, WorkflowLaunchJobState.WORKFLOW_LAUNCH_JOB_STATE_QUEUED));
        private WorkflowLaunchStatusException failure;
        private RuntimeException runtimeFailure;

        @Override public GetWorkflowLaunchStatusResponse get(GetWorkflowLaunchStatusRequest request)
                throws WorkflowLaunchStatusException {
            calls.incrementAndGet();
            if (failure != null) throw failure;
            if (runtimeFailure != null) throw runtimeFailure;
            return response;
        }
    }
}
