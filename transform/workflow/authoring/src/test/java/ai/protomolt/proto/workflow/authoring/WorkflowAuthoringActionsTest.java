package ai.protomolt.proto.workflow.authoring;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import ai.protomolt.proto.samples.starter.v1.WorkflowAcceptedCandidate;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringLaunchRequest;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringLaunchResult;
import ai.protomolt.proto.workflow.authoring.v1.GetAcceptedWorkflowRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.UnknownFieldSet;
import io.grpc.Status;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkflowAuthoringActionsTest {
    private static final String TASK_ID = "00000000-0000-4000-8000-000000000001";
    private static final String LAUNCH_ID = "00000000-0000-4000-8000-000000000002";
    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    void registersTypedDynamicAndJsonActionsAndAllowsOperator() throws Exception {
        var backend = new FakeOperations();
        var catalog = catalog(backend);
        var request = GetAcceptedWorkflowRequest.newBuilder().setTaskId(TASK_ID).build();

        assertThat(catalog.get("get-accepted-workflow").requestType())
                .isEqualTo(GetAcceptedWorkflowRequest.getDescriptor());
        assertThat(catalog.get("get-accepted-workflow").responseType())
                .isEqualTo(WorkflowAcceptedCandidate.getDescriptor());
        assertThat(catalog.get("launch-accepted-workflow").requestType())
                .isEqualTo(WorkflowAuthoringLaunchRequest.getDescriptor());
        assertThat(catalog.get("launch-accepted-workflow").responseType())
                .isEqualTo(WorkflowAuthoringLaunchResult.getDescriptor());

        assertThat(catalog.execute("get-accepted-workflow", request)).isEqualTo(backend.candidate);
        assertThat(catalog.execute("get-accepted-workflow",
                DynamicMessage.parseFrom(request.getDescriptorForType(), request.toByteArray())).toByteArray())
                .isEqualTo(backend.candidate.toByteArray());

        ObjectNode jsonRequest = JSON.createObjectNode().put("task_id", TASK_ID);
        ObjectNode jsonResult = catalog.execute("get-accepted-workflow", jsonRequest);
        assertThat(jsonResult.path("taskId").asText()).isEqualTo(TASK_ID);
        assertThat(jsonResult.path("candidateSha256").asText()).isEqualTo("b".repeat(64));

        var launchRequest = launchRequest();
        assertThat(catalog.execute("launch-accepted-workflow", launchRequest))
                .isEqualTo(backend.launchResult);
        var launchJson = catalog.execute("launch-accepted-workflow", (ObjectNode) JSON.readTree(
                com.google.protobuf.util.JsonFormat.printer().omittingInsignificantWhitespace()
                        .print(launchRequest)));
        assertThat(launchJson.path("jobId").asText()).isEqualTo(LAUNCH_ID);
        assertThat(backend.acceptedCalls.get()).isEqualTo(3);
        assertThat(backend.launchCalls.get()).isEqualTo(2);
    }

    @Test
    void refusesScopedCallersBeforeEitherBackendOperation() throws Exception {
        var backend = new FakeOperations();
        var catalog = catalog(backend);
        for (String scope : Set.of(Scopes.WORKER_COORDINATE, Scopes.WORKFLOW_RUN, Scopes.WORKFLOW_AUTHOR)) {
            Caller caller = Caller.scoped("scoped", Set.of(scope));
            assertThatThrownBy(() -> catalog.execute("get-accepted-workflow",
                    GetAcceptedWorkflowRequest.newBuilder().setTaskId(TASK_ID).build(), caller))
                    .isInstanceOfSatisfying(ActionException.class,
                            failure -> assertThat(failure.code()).isEqualTo("permission-denied"));
            assertThatThrownBy(() -> catalog.execute("launch-accepted-workflow", launchRequest(), caller))
                    .isInstanceOfSatisfying(ActionException.class,
                            failure -> assertThat(failure.code()).isEqualTo("permission-denied"));
            assertThatThrownBy(() -> catalog.execute("get-accepted-workflow",
                    JSON.createObjectNode().put("task_id", "not-even-valid"), caller))
                    .isInstanceOfSatisfying(ActionException.class,
                            failure -> assertThat(failure.code()).isEqualTo("permission-denied"));
        }
        assertThat(backend.acceptedCalls).hasValue(0);
        assertThat(backend.launchCalls).hasValue(0);

        assertThat(catalog.execute("get-accepted-workflow",
                GetAcceptedWorkflowRequest.newBuilder().setTaskId(TASK_ID).build(), Caller.operator()))
                .isEqualTo(backend.candidate);
    }

    @Test
    void explicitLaunchScopePermitsLookupAndLaunchThroughCatalog() throws Exception {
        var backend = new FakeOperations();
        var catalog = catalog(backend);
        Caller launcher = Caller.scoped("launcher", Set.of(Scopes.WORKFLOW_LAUNCH));
        assertThat(catalog.get("get-accepted-workflow").requiredScope()).isEqualTo(Scopes.WORKFLOW_LAUNCH);
        assertThat(catalog.get("launch-accepted-workflow").requiredScope()).isEqualTo(Scopes.WORKFLOW_LAUNCH);
        assertThat(catalog.execute("get-accepted-workflow",
                GetAcceptedWorkflowRequest.newBuilder().setTaskId(TASK_ID).build(), launcher))
                .isEqualTo(backend.candidate);
        assertThat(catalog.execute("launch-accepted-workflow", launchRequest(), launcher))
                .isEqualTo(backend.launchResult);
        assertThat(backend.acceptedCalls).hasValue(1);
        assertThat(backend.launchCalls).hasValue(1);
    }

    @Test
    void rejectsInvalidRequestsUnknownFieldsAndInvalidSuccessfulResponses() throws Exception {
        var backend = new FakeOperations();
        var catalog = catalog(backend);
        assertError("invalid-input", () -> catalog.execute("get-accepted-workflow",
                GetAcceptedWorkflowRequest.getDefaultInstance()));
        assertError("invalid-input", () -> catalog.execute("get-accepted-workflow",
                GetAcceptedWorkflowRequest.newBuilder().setTaskId(TASK_ID)
                        .setUnknownFields(UnknownFieldSet.newBuilder()
                                .addField(99, UnknownFieldSet.Field.newBuilder().addVarint(1).build())
                                .build()).build()));
        assertError("invalid-input", () -> catalog.execute("launch-accepted-workflow",
                WorkflowAuthoringLaunchRequest.getDefaultInstance()));
        assertThat(backend.acceptedCalls).hasValue(0);
        assertThat(backend.launchCalls).hasValue(0);

        // Shape validity cannot substitute for binding the reply to this request.
        backend.candidate = candidate().toBuilder().setTaskId(
                "10000000-0000-4000-8000-000000000001").build();
        assertError("invalid-upstream-response", () -> catalog.execute("get-accepted-workflow",
                GetAcceptedWorkflowRequest.newBuilder().setTaskId(TASK_ID).build()));
        backend.launchResult = backend.launchResult.toBuilder()
                .setJobId("10000000-0000-4000-8000-000000000001").build();
        assertError("invalid-upstream-response", () -> catalog.execute("launch-accepted-workflow", launchRequest()));

        backend.candidate = WorkflowAcceptedCandidate.getDefaultInstance();
        assertError("invalid-upstream-response", () -> catalog.execute("get-accepted-workflow",
                GetAcceptedWorkflowRequest.newBuilder().setTaskId(TASK_ID).build()));
        backend.launchResult = WorkflowAuthoringLaunchResult.getDefaultInstance();
        assertError("invalid-upstream-response", () -> catalog.execute("launch-accepted-workflow", launchRequest()));
    }

    @Test
    void mapsBackendFailuresToStableActionCodes() throws Exception {
        var backend = new FakeOperations();
        var catalog = catalog(backend);
        var lookup = GetAcceptedWorkflowRequest.newBuilder().setTaskId(TASK_ID).build();

        backend.failure = new WorkflowLaunchConflictException("launch UUID is already bound");
        assertError("workflow-launch-conflict", () -> catalog.execute("launch-accepted-workflow", launchRequest()));
        backend.failure = new IllegalArgumentException("input rejected by policy");
        assertError("workflow-authoring-rejected", () -> catalog.execute("launch-accepted-workflow", launchRequest()));
        backend.failure = new IOException("authorization store unavailable");
        assertError("workflow-authoring-storage-failed", () -> catalog.execute("launch-accepted-workflow", launchRequest()));
        backend.failure = Status.UNAVAILABLE.asRuntimeException();
        assertError("workflow-authoring-unavailable", () -> catalog.execute("get-accepted-workflow", lookup));
        backend.failure = Status.DEADLINE_EXCEEDED.asRuntimeException();
        assertError("workflow-authoring-deadline", () -> catalog.execute("get-accepted-workflow", lookup));
    }

    private static ActionCatalog catalog(FakeOperations operations) {
        return WorkflowAuthoringActions.register(ActionCatalog.defaults(ActionContext.create()), operations);
    }

    private static WorkflowAcceptedCandidate candidate() {
        return WorkflowAcceptedCandidate.newBuilder().setTaskId(TASK_ID).setAttempt(1).setRevision(1)
                .setTaskSpecSha256("a".repeat(64)).setCandidateSha256("b".repeat(64))
                .setAcceptedEntrySha256("c".repeat(64)).build();
    }

    private static WorkflowAuthoringLaunchRequest launchRequest() {
        return WorkflowAuthoringLaunchRequest.newBuilder().setLaunchId(LAUNCH_ID)
                .setAcceptance(candidate()).setInput(artifact("d", "application/x-protobuf")).build();
    }

    private static ArtifactReference artifact(String hash, String mediaType) {
        return ArtifactReference.newBuilder().setSha256(hash.repeat(64)).setMediaType(mediaType)
                .setSizeBytes(1).build();
    }

    private static void assertError(String code, ThrowingCall call) {
        assertThatThrownBy(call::run).isInstanceOfSatisfying(ActionException.class,
                failure -> assertThat(failure.code()).isEqualTo(code));
    }

    @FunctionalInterface private interface ThrowingCall { void run() throws Exception; }

    private static final class FakeOperations implements WorkflowAuthoringOperations {
        private final AtomicInteger acceptedCalls = new AtomicInteger();
        private final AtomicInteger launchCalls = new AtomicInteger();
        private WorkflowAcceptedCandidate candidate = candidate();
        private WorkflowAuthoringLaunchResult launchResult = WorkflowAuthoringLaunchResult.newBuilder()
                .setJobId(LAUNCH_ID).setAuthorization(artifact("e", "application/x-protobuf")).build();
        private Exception failure;

        @Override public WorkflowAcceptedCandidate acceptedCandidate(String taskId) throws Exception {
            acceptedCalls.incrementAndGet();
            if (failure != null) throw failure;
            return candidate;
        }

        @Override public WorkflowAuthoringLaunchResult launch(WorkflowAuthoringLaunchRequest request)
                throws Exception {
            launchCalls.incrementAndGet();
            if (failure != null) throw failure;
            return launchResult;
        }
    }
}
