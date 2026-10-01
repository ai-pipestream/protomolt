package ai.protomolt.proto.workflow.authoring;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.authz.grpc.ApiTokenServerInterceptor;
import ai.protomolt.proto.grpc.invoke.DynamicGrpcCalls;
import ai.protomolt.proto.grpc.service.CatalogBridge;
import ai.protomolt.proto.grpc.service.ProtoMoltGrpcService;
import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import ai.protomolt.proto.samples.starter.v1.WorkflowAcceptedCandidate;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringLaunchRequest;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringLaunchResult;
import ai.protomolt.proto.workflow.authoring.v1.GetAcceptedWorkflowRequest;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowAuthoringServiceOuterClass;
import com.google.protobuf.Message;
import com.google.protobuf.DynamicMessage;
import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
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

/** Wire-adapter qualification with a controlled backend, not workflow execution proof. */
class WorkflowAuthoringGrpcTest {
    private static final String ID = "b30d7d5c-9c65-467c-8c64-cbf5b7e18a1d";
    private static final String SECRET = "database-password-must-not-escape";
    private final AtomicInteger calls = new AtomicInteger();
    private volatile Exception failure;
    private volatile boolean invalidReply;
    private Server server;
    private ManagedChannel channel;

    private static WorkflowAcceptedCandidate accepted() {
        return WorkflowAcceptedCandidate.newBuilder().setTaskId(ID).setAttempt(1).setRevision(1)
                .setTaskSpecSha256("a".repeat(64)).setCandidateSha256("b".repeat(64))
                .setAcceptedEntrySha256("c".repeat(64)).build();
    }

    private static ArtifactReference artifact() {
        return ArtifactReference.newBuilder().setSha256("d".repeat(64))
                .setMediaType("application/x-protobuf").setSizeBytes(32).build();
    }

    @BeforeEach
    void start() throws Exception {
        var operations = new WorkflowAuthoringOperations() {
            public WorkflowAcceptedCandidate acceptedCandidate(String taskId) throws Exception {
                calls.incrementAndGet();
                if (failure != null) throw failure;
                return invalidReply ? WorkflowAcceptedCandidate.getDefaultInstance() : accepted();
            }
            public WorkflowAuthoringLaunchResult launch(WorkflowAuthoringLaunchRequest request)
                    throws Exception {
                calls.incrementAndGet();
                if (failure != null) throw failure;
                return WorkflowAuthoringLaunchResult.newBuilder().setJobId(request.getLaunchId())
                        .setAuthorization(artifact()).build();
            }
        };
        var catalog = WorkflowAuthoringActions.register(ActionCatalog.defaults(ActionContext.create()), operations);
        String name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name)
                .intercept(new ApiTokenServerInterceptor("operator-token", token ->
                        "worker-token".equals(token)
                                ? Optional.of(Caller.scoped("worker", Set.of(Scopes.WORKER_COORDINATE)))
                                : Optional.empty()))
                .addService(ProtoMoltGrpcService.contributed(catalog,
                        WorkflowAuthoringServiceOuterClass.getDescriptor()
                                .findServiceByName("WorkflowAuthoringService")))
                .build().start();
        channel = InProcessChannelBuilder.forName(name).build();
    }

    @AfterEach
    void stop() throws Exception {
        if (channel != null) channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        if (server != null) server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }

    private Message call(String rpc, Message request, String token) {
        var method = WorkflowAuthoringServiceOuterClass.getDescriptor()
                .findServiceByName("WorkflowAuthoringService").findMethodByName(rpc);
        var headers = new Metadata();
        headers.put(Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER), token);
        return DynamicGrpcCalls.call(channel, method, DynamicMessage.newBuilder(request.getDescriptorForType()).mergeFrom(request).build(),
                CallOptions.DEFAULT.withDeadlineAfter(5, TimeUnit.SECONDS), headers, 1).getFirst();
    }

    @Test
    void operatorCanUseBothReviewedContractsOverGrpc() throws Exception {
        var identity = WorkflowAcceptedCandidate.parseFrom(call("GetAcceptedWorkflow",
                GetAcceptedWorkflowRequest.newBuilder().setTaskId(ID).build(), "operator-token").toByteArray());
        assertThat(identity).isEqualTo(accepted());
        var request = WorkflowAuthoringLaunchRequest.newBuilder().setLaunchId(ID)
                .setAcceptance(identity).setInput(artifact()).build();
        var result = WorkflowAuthoringLaunchResult.parseFrom(
                call("LaunchAcceptedWorkflow", request, "operator-token").toByteArray());
        assertThat(result.getJobId()).isEqualTo(ID);
        assertThat(calls.get()).isEqualTo(2);
    }

    @Test
    void scopedCredentialCannotReadAcceptedIdentity() {
        assertStatus("GetAcceptedWorkflow", GetAcceptedWorkflowRequest.newBuilder().setTaskId(ID).build(),
                "worker-token", Status.Code.PERMISSION_DENIED, "permission-denied");
        assertThat(calls.get()).isZero();
    }

    @Test
    void inputAndSuccessfulResponseValidationSurviveTheWire() {
        assertStatus("GetAcceptedWorkflow", GetAcceptedWorkflowRequest.getDefaultInstance(),
                "operator-token", Status.Code.INVALID_ARGUMENT, "invalid-input");
        assertThat(calls.get()).isZero();
        invalidReply = true;
        assertStatus("GetAcceptedWorkflow", GetAcceptedWorkflowRequest.newBuilder().setTaskId(ID).build(),
                "operator-token", Status.Code.DATA_LOSS, "invalid-upstream-response");
    }

    @Test
    void infrastructureAndConflictFailuresStayTypedAndSanitized() {
        var request = WorkflowAuthoringLaunchRequest.newBuilder().setLaunchId(ID)
                .setAcceptance(accepted()).setInput(artifact()).build();
        failure = new WorkflowLaunchConflictException(SECRET);
        assertStatus("LaunchAcceptedWorkflow", request, "operator-token",
                Status.Code.ALREADY_EXISTS, "workflow-launch-conflict");
        failure = new IOException(SECRET);
        assertStatus("LaunchAcceptedWorkflow", request, "operator-token",
                Status.Code.INTERNAL, "workflow-authoring-storage-failed");
        failure = Status.UNAVAILABLE.withDescription(SECRET).asRuntimeException();
        assertStatus("LaunchAcceptedWorkflow", request, "operator-token",
                Status.Code.UNAVAILABLE, "workflow-authoring-unavailable");
    }

    private void assertStatus(String rpc, Message request, String token, Status.Code expected, String code) {
        assertThatThrownBy(() -> call(rpc, request, token))
                .isInstanceOfSatisfying(StatusRuntimeException.class, error -> {
                    assertThat(error.getStatus().getCode()).isEqualTo(expected);
                    assertThat(error.getTrailers().get(CatalogBridge.ERROR_CODE_KEY)).isEqualTo(code);
                    assertThat(error.getMessage()).doesNotContain(SECRET);
                });
    }
}
