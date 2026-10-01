package ai.protomolt.proto.workflow.authoring;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.authz.grpc.ApiTokenServerInterceptor;
import ai.protomolt.proto.grpc.service.CatalogBridge;
import ai.protomolt.proto.grpc.service.ProtoMoltGrpcService;
import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import ai.protomolt.proto.receipt.WorkRecords;
import ai.protomolt.proto.samples.starter.v1.WorkflowAcceptedCandidate;
import ai.protomolt.proto.workflow.authoring.v1.GetWorkflowLaunchInputContractRequest;
import ai.protomolt.proto.workflow.authoring.v1.GetWorkflowLaunchInputContractResponse;
import ai.protomolt.proto.workflow.authoring.v1.PrepareWorkflowLaunchInputRequest;
import ai.protomolt.proto.workflow.authoring.v1.PrepareWorkflowLaunchInputResponse;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowLaunchInputServiceGrpc;
import com.google.protobuf.ByteString;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.ManagedChannelBuilder;
import io.grpc.ServerBuilder;
import io.grpc.stub.MetadataUtils;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Generated-stub adapter tests; this does not mount a production RPC server. */
class WorkflowLaunchInputGrpcTest {
    private static final String TASK = "00000000-0000-4000-8000-000000000001";
    private static final String HASH_A = "a".repeat(64);
    private static final String HASH_B = "b".repeat(64);
    private static final String HASH_C = "c".repeat(64);
    private static final String SECRET = "private-input-store-credential";
    private final FakeOperations operations = new FakeOperations();
    private Server server;
    private ManagedChannel channel;

    @BeforeEach
    void start() throws Exception {
        var catalog = WorkflowLaunchInputActions.register(ActionCatalog.defaults(ActionContext.create()), operations);
        server = ServerBuilder.forPort(0)
                .intercept(new ApiTokenServerInterceptor("operator-token", token -> switch (token) {
                    case "launch-token" -> Optional.of(Caller.scoped("launcher", Set.of(Scopes.WORKFLOW_LAUNCH)));
                    case "worker-token" -> Optional.of(Caller.scoped("worker", Set.of(Scopes.WORKER_COORDINATE)));
                    case "author-token" -> Optional.of(Caller.scoped("author", Set.of(Scopes.WORKFLOW_AUTHOR)));
                    case "run-token" -> Optional.of(Caller.scoped("runner", Set.of(Scopes.WORKFLOW_RUN)));
                    default -> Optional.empty();
                }))
                .addService(ProtoMoltGrpcService.contributed(catalog,
                        ai.protomolt.proto.workflow.authoring.v1.WorkflowLaunchInputServiceOuterClass.getDescriptor().findServiceByName("WorkflowLaunchInputService")))
                .build().start();
        channel = ManagedChannelBuilder.forAddress("127.0.0.1", server.getPort())
                .usePlaintext().maxInboundMessageSize(8 * 1024 * 1024).build();
    }

    @AfterEach
    void stop() throws Exception {
        if (channel != null) channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        if (server != null) server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }

    @Test
    void explicitLaunchScopeUsesBothGeneratedRpcMethods() {
        var stub = stub("launch-token");
        assertThat(stub.getWorkflowLaunchInputContract(getRequest())).isEqualTo(operations.contractResponse);
        assertThat(stub.prepareWorkflowLaunchInput(prepareRequest())).isEqualTo(operations.prepareResponse);
        assertThat(operations.contractCalls).hasValue(1);
        assertThat(operations.prepareCalls).hasValue(1);
    }

    @Test
    void unrelatedScopesAndInvalidRequestsAreRejectedBeforeBackendCalls() {
        for (String token : Set.of("worker-token", "author-token", "run-token")) {
            assertStatus("GetWorkflowLaunchInputContract", () -> stub(token)
                    .getWorkflowLaunchInputContract(getRequest()), Status.Code.PERMISSION_DENIED, "permission-denied");
            assertStatus("PrepareWorkflowLaunchInput", () -> stub(token)
                    .prepareWorkflowLaunchInput(prepareRequest()), Status.Code.PERMISSION_DENIED, "permission-denied");
        }
        assertStatus("GetWorkflowLaunchInputContract", () -> stub("launch-token")
                .getWorkflowLaunchInputContract(GetWorkflowLaunchInputContractRequest.getDefaultInstance()),
                Status.Code.INVALID_ARGUMENT, "invalid-input");
        assertStatus("PrepareWorkflowLaunchInput", () -> stub("launch-token")
                .prepareWorkflowLaunchInput(PrepareWorkflowLaunchInputRequest.getDefaultInstance()),
                Status.Code.INVALID_ARGUMENT, "invalid-input");
        assertThat(operations.contractCalls).hasValue(0);
        assertThat(operations.prepareCalls).hasValue(0);
    }

    @Test
    void generatedClientAcceptsDescriptorResponseLargerThanFourMiBWithEightMiBInboundLimit() {
        byte[] descriptorSet = new byte[4_194_304];
        ByteString bytes = ByteString.copyFrom(descriptorSet);
        operations.contractResponse = GetWorkflowLaunchInputContractResponse.newBuilder()
                .setAcceptance(acceptance()).setInputType("example.v1.Input").setDescriptorSet(bytes)
                .setDescriptors(artifact(WorkRecords.sha256Hex(descriptorSet), "application/x-protobuf",
                        descriptorSet.length, false)).build();
        assertThat(operations.contractResponse.getSerializedSize()).isGreaterThan(4 * 1024 * 1024);
        var response = stub("launch-token").getWorkflowLaunchInputContract(getRequest());
        assertThat(response).isEqualTo(operations.contractResponse);
        assertThat(operations.contractCalls).hasValue(1);
    }

    @Test
    void corruptionAndDeadlineFailuresKeepTypedStatusesAndSanitizedDetails() {
        operations.failure = new WorkflowLaunchInputException(WorkflowLaunchInputException.Kind.CORRUPT_EVIDENCE,
                SECRET, new IllegalStateException(SECRET));
        assertStatus("GetWorkflowLaunchInputContract", () -> stub("launch-token")
                .getWorkflowLaunchInputContract(getRequest()), Status.Code.DATA_LOSS, "invalid-upstream-response");

        operations.failure = new WorkflowLaunchInputException(WorkflowLaunchInputException.Kind.DEADLINE,
                SECRET, new IllegalStateException(SECRET));
        assertStatus("PrepareWorkflowLaunchInput", () -> stub("launch-token")
                .prepareWorkflowLaunchInput(prepareRequest()), Status.Code.DEADLINE_EXCEEDED,
                "workflow-authoring-deadline");
    }

    private WorkflowLaunchInputServiceGrpc.WorkflowLaunchInputServiceBlockingStub stub(String token) {
        var headers = new Metadata();
        headers.put(Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER), token);
        return WorkflowLaunchInputServiceGrpc.newBlockingStub(ClientInterceptors.intercept(channel,
                MetadataUtils.newAttachHeadersInterceptor(headers))).withDeadlineAfter(5, TimeUnit.SECONDS);
    }

    private static GetWorkflowLaunchInputContractRequest getRequest() {
        return GetWorkflowLaunchInputContractRequest.newBuilder().setAcceptance(acceptance()).build();
    }

    private static PrepareWorkflowLaunchInputRequest prepareRequest() {
        return PrepareWorkflowLaunchInputRequest.newBuilder().setAcceptance(acceptance())
                .setInputJson(ByteString.copyFromUtf8("{\"name\":\"input\"}")).build();
    }

    private static WorkflowAcceptedCandidate acceptance() {
        return WorkflowAcceptedCandidate.newBuilder().setTaskId(TASK).setAttempt(1).setRevision(1)
                .setTaskSpecSha256(HASH_A).setCandidateSha256(HASH_B).setAcceptedEntrySha256(HASH_C).build();
    }

    private static GetWorkflowLaunchInputContractResponse contractResponse() {
        byte[] descriptorSet = new byte[] {0x0a, 0x01, 0x01};
        return GetWorkflowLaunchInputContractResponse.newBuilder().setAcceptance(acceptance())
                .setInputType("example.v1.Input").setDescriptorSet(ByteString.copyFrom(descriptorSet))
                .setDescriptors(artifact(WorkRecords.sha256Hex(descriptorSet), "application/x-protobuf",
                        descriptorSet.length, false)).build();
    }

    private static ArtifactReference artifact(String hash, String mediaType, long size, boolean redacted) {
        return ArtifactReference.newBuilder().setSha256(hash).setMediaType(mediaType).setSizeBytes(size)
                .setRedacted(redacted).build();
    }

    private void assertStatus(String rpc, org.assertj.core.api.ThrowableAssert.ThrowingCallable call,
            Status.Code expected, String errorCode) {
        assertThatThrownBy(call).isInstanceOfSatisfying(StatusRuntimeException.class, error -> {
            assertThat(error.getStatus().getCode()).as(rpc).isEqualTo(expected);
            assertThat(error.getTrailers().get(CatalogBridge.ERROR_CODE_KEY)).as(rpc).isEqualTo(errorCode);
            assertThat(error.getMessage()).as(rpc).doesNotContain(SECRET);
        });
    }

    private static final class FakeOperations implements WorkflowLaunchInputOperations {
        private final AtomicInteger contractCalls = new AtomicInteger();
        private final AtomicInteger prepareCalls = new AtomicInteger();
        private GetWorkflowLaunchInputContractResponse contractResponse = contractResponse();
        private PrepareWorkflowLaunchInputResponse prepareResponse = PrepareWorkflowLaunchInputResponse.newBuilder()
                .setAcceptance(acceptance())
                .setInput(artifact(HASH_C, "application/x-protobuf", 12, false)).build();
        private WorkflowLaunchInputException failure;

        @Override public GetWorkflowLaunchInputContractResponse contract(GetWorkflowLaunchInputContractRequest request)
                throws WorkflowLaunchInputException {
            contractCalls.incrementAndGet();
            if (failure != null) throw failure;
            return contractResponse;
        }

        @Override public PrepareWorkflowLaunchInputResponse prepare(PrepareWorkflowLaunchInputRequest request)
                throws WorkflowLaunchInputException {
            prepareCalls.incrementAndGet();
            if (failure != null) throw failure;
            return prepareResponse;
        }
    }
}
