package ai.protomolt.proto.workflow.authoring;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.authz.grpc.ApiTokenServerInterceptor;
import ai.protomolt.proto.grpc.service.CatalogBridge;
import ai.protomolt.proto.grpc.service.ProtoMoltGrpcService;
import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import ai.protomolt.proto.samples.starter.v1.WorkflowAcceptedCandidate;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringLaunchRequest;
import ai.protomolt.proto.workflow.authoring.v1.GetWorkflowLaunchStatusRequest;
import ai.protomolt.proto.workflow.authoring.v1.GetWorkflowLaunchStatusResponse;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowLaunchJobState;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowLaunchJobStatus;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowLaunchStatusServiceGrpc;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowLaunchStatusServiceOuterClass;
import com.google.protobuf.Timestamp;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.MetadataUtils;
import java.io.IOException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Generated-stub authorization and status mapping through a contributed catalog service. */
class WorkflowLaunchStatusGrpcTest {
    private static final String LAUNCH_ID = "00000000-0000-4000-8000-000000000001";
    private static final String TASK_ID = "00000000-0000-4000-8000-000000000002";
    private static final String HASH_A = "a".repeat(64);
    private static final String HASH_B = "b".repeat(64);
    private static final String HASH_C = "c".repeat(64);
    private static final String SECRET = "private-job-record-secret";
    private final FakeOperations operations = new FakeOperations();
    private Server server;
    private ManagedChannel channel;

    @BeforeEach
    void start() throws Exception {
        var catalog = WorkflowLaunchStatusActions.register(ActionCatalog.defaults(ActionContext.create()), operations);
        String name = InProcessServerBuilder.generateName();
        var service = WorkflowLaunchStatusServiceOuterClass.getDescriptor()
                .findServiceByName("WorkflowLaunchStatusService");
        server = InProcessServerBuilder.forName(name)
                .intercept(new ApiTokenServerInterceptor("operator-token", token -> switch (token) {
                    case "launch-token" -> Optional.of(Caller.scoped("launcher", Set.of(Scopes.WORKFLOW_LAUNCH)));
                    case "default-token" -> Optional.of(Caller.scoped("default", Set.of()));
                    case "author-token" -> Optional.of(Caller.scoped("author", Set.of(Scopes.WORKFLOW_AUTHOR)));
                    case "run-token" -> Optional.of(Caller.scoped("runner", Set.of(Scopes.WORKFLOW_RUN)));
                    default -> Optional.empty();
                }))
                .addService(ProtoMoltGrpcService.contributed(catalog, service)).build().start();
        channel = InProcessChannelBuilder.forName(name).build();
    }

    @AfterEach
    void stop() throws Exception {
        if (channel != null) channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        if (server != null) server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }

    @Test
    void launchScopeReadsProjectedStatusThroughGeneratedStub() {
        var response = stub("launch-token").getWorkflowLaunchStatus(request());
        assertThat(response).isEqualTo(operations.response);
        assertThat(operations.calls).hasValue(1);
    }

    @Test
    void defaultAuthorAndRunScopesAreDeniedBeforeBackendAndInvalidInput() {
        for (String token : Set.of("default-token", "author-token", "run-token")) {
            assertStatus(token, () -> stub(token).getWorkflowLaunchStatus(
                    GetWorkflowLaunchStatusRequest.getDefaultInstance()),
                    Status.Code.PERMISSION_DENIED, "permission-denied");
        }
        assertStatus("launch-token", () -> stub("launch-token").getWorkflowLaunchStatus(
                GetWorkflowLaunchStatusRequest.getDefaultInstance()), Status.Code.INVALID_ARGUMENT, "invalid-input");
        assertThat(operations.calls).hasValue(0);
    }

    @Test
    void corruptionConflictUnavailableAndDeadlineKeepTypedSanitizedStatuses() {
        operations.failure = new WorkflowLaunchStatusException(WorkflowLaunchStatusException.Kind.CORRUPT_EVIDENCE,
                SECRET, new IOException(SECRET));
        assertStatus("launch-token", () -> stub("launch-token").getWorkflowLaunchStatus(request()),
                Status.Code.DATA_LOSS, "invalid-upstream-response");

        operations.failure = new WorkflowLaunchStatusException(WorkflowLaunchStatusException.Kind.CONFLICT,
                SECRET, new IOException(SECRET));
        assertStatus("launch-token", () -> stub("launch-token").getWorkflowLaunchStatus(request()),
                Status.Code.ALREADY_EXISTS, "workflow-launch-conflict");

        operations.failure = new WorkflowLaunchStatusException(WorkflowLaunchStatusException.Kind.UNAVAILABLE,
                SECRET, new IOException(SECRET));
        assertStatus("launch-token", () -> stub("launch-token").getWorkflowLaunchStatus(request()),
                Status.Code.UNAVAILABLE, "workflow-authoring-unavailable");

        operations.failure = new WorkflowLaunchStatusException(WorkflowLaunchStatusException.Kind.DEADLINE,
                SECRET, new IOException(SECRET));
        assertStatus("launch-token", () -> stub("launch-token").getWorkflowLaunchStatus(request()),
                Status.Code.DEADLINE_EXCEEDED, "workflow-authoring-deadline");
    }

    @Test
    void malformedUpstreamResponseBecomesDataLoss() {
        operations.response = GetWorkflowLaunchStatusResponse.getDefaultInstance();
        assertStatus("launch-token", () -> stub("launch-token").getWorkflowLaunchStatus(request()),
                Status.Code.DATA_LOSS, "invalid-upstream-response");
    }

    private WorkflowLaunchStatusServiceGrpc.WorkflowLaunchStatusServiceBlockingStub stub(String token) {
        var headers = new Metadata();
        headers.put(Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER), token);
        return WorkflowLaunchStatusServiceGrpc.newBlockingStub(ClientInterceptors.intercept(channel,
                MetadataUtils.newAttachHeadersInterceptor(headers))).withDeadlineAfter(5, TimeUnit.SECONDS);
    }

    private static GetWorkflowLaunchStatusRequest request() {
        return GetWorkflowLaunchStatusRequest.newBuilder().setRequest(launchRequest()).build();
    }

    private static WorkflowAuthoringLaunchRequest launchRequest() {
        return WorkflowAuthoringLaunchRequest.newBuilder().setLaunchId(LAUNCH_ID)
                .setAcceptance(WorkflowAcceptedCandidate.newBuilder().setTaskId(TASK_ID).setAttempt(1).setRevision(1)
                        .setTaskSpecSha256(HASH_A).setCandidateSha256(HASH_B).setAcceptedEntrySha256(HASH_C))
                .setInput(ArtifactReference.newBuilder().setSha256(HASH_A)
                        .setMediaType("application/x-protobuf").setSizeBytes(1)).build();
    }

    private static GetWorkflowLaunchStatusResponse response() {
        return GetWorkflowLaunchStatusResponse.newBuilder().setRequest(launchRequest())
                .setJob(WorkflowLaunchJobStatus.newBuilder().setJobId(LAUNCH_ID)
                        .setState(WorkflowLaunchJobState.WORKFLOW_LAUNCH_JOB_STATE_COMPLETED)
                        .setAttempt(1).setMaxAttempts(3).setCreatedAt(Timestamp.newBuilder().setSeconds(1))
                        .setUpdatedAt(Timestamp.newBuilder().setSeconds(2))
                        .setCompletedAt(Timestamp.newBuilder().setSeconds(2))).build();
    }

    private void assertStatus(String token, org.assertj.core.api.ThrowableAssert.ThrowingCallable call,
            Status.Code expected, String errorCode) {
        assertThatThrownBy(call).isInstanceOfSatisfying(StatusRuntimeException.class, error -> {
            assertThat(error.getStatus().getCode()).isEqualTo(expected);
            assertThat(error.getTrailers().get(CatalogBridge.ERROR_CODE_KEY)).isEqualTo(errorCode);
            assertThat(error.getMessage()).doesNotContain(SECRET);
        });
    }

    private static final class FakeOperations implements WorkflowLaunchStatusOperations {
        private final AtomicInteger calls = new AtomicInteger();
        private GetWorkflowLaunchStatusResponse response = response();
        private WorkflowLaunchStatusException failure;

        @Override public GetWorkflowLaunchStatusResponse get(GetWorkflowLaunchStatusRequest request)
                throws WorkflowLaunchStatusException {
            calls.incrementAndGet();
            if (failure != null) throw failure;
            return response;
        }
    }
}
