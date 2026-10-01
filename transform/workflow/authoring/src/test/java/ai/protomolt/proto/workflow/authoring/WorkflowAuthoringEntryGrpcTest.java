package ai.protomolt.proto.workflow.authoring;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.authz.grpc.ApiTokenServerInterceptor;
import ai.protomolt.proto.delegation.DeliverableContracts;
import ai.protomolt.proto.delegation.v1.AcceptanceCheck;
import ai.protomolt.proto.delegation.v1.DeliverableContract;
import ai.protomolt.proto.delegation.v1.TaskOffer;
import ai.protomolt.proto.delegation.v1.TaskSpec;
import ai.protomolt.proto.grpc.service.CatalogBridge;
import ai.protomolt.proto.grpc.service.ProtoMoltGrpcService;
import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringDeliverable;
import ai.protomolt.proto.workflow.authoring.v1.GetWorkflowAuthoringTemplateRequest;
import ai.protomolt.proto.workflow.authoring.v1.GetWorkflowAuthoringTemplateResponse;
import ai.protomolt.proto.workflow.authoring.v1.StartWorkflowAuthoringRequest;
import ai.protomolt.proto.workflow.authoring.v1.StartWorkflowAuthoringResponse;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowAuthoringEntryServiceGrpc;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowAuthoringEntryServiceOuterClass;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowAuthoringTemplate;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Duration;
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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Generated gRPC callers exercise the mounted catalog service and its scoped boundary. */
class WorkflowAuthoringEntryGrpcTest {
    private static final String TASK_ID = "00000000-0000-4000-8000-0000000000a1";
    private static final String HASH = "a".repeat(64);
    private static final String OBJECTIVE = "Create and verify the bounded workflow deliverable.";
    private static final String SECRET = "private admission-store credential";
    private final FakeOperations operations = new FakeOperations();
    private Server server;
    private ManagedChannel channel;

    @BeforeEach
    void start() throws Exception {
        var catalog = WorkflowAuthoringEntryActions.register(
                ActionCatalog.defaults(ActionContext.create()), operations);
        var service = WorkflowAuthoringEntryServiceOuterClass.getDescriptor()
                .findServiceByName("WorkflowAuthoringEntryService");
        String name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name)
                .intercept(new ApiTokenServerInterceptor("operator-token", token -> switch (token) {
                    case "coordinator-token" -> Optional.of(Caller.scoped("coordinator",
                            Set.of(Scopes.WORKER_COORDINATE)));
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
    void workerCoordinateUsesBothGeneratedMethods() {
        assertThat(stub("coordinator-token").getWorkflowAuthoringTemplate(
                GetWorkflowAuthoringTemplateRequest.getDefaultInstance())).isEqualTo(operations.templateResponse);
        assertThat(stub("coordinator-token").startWorkflowAuthoring(request())).isEqualTo(operations.startResponse);
        assertThat(operations.templateCalls).hasValue(1);
        assertThat(operations.startCalls).hasValue(1);
    }

    @Test
    void defaultAuthorAndRunScopesAreDeniedBeforeInvalidRequestOrBackend() {
        for (String token : Set.of("default-token", "author-token", "run-token")) {
            assertStatus(token, () -> stub(token).startWorkflowAuthoring(
                    StartWorkflowAuthoringRequest.getDefaultInstance()),
                    Status.Code.PERMISSION_DENIED, "permission-denied");
            assertStatus(token, () -> stub(token).getWorkflowAuthoringTemplate(
                    GetWorkflowAuthoringTemplateRequest.getDefaultInstance()),
                    Status.Code.PERMISSION_DENIED, "permission-denied");
        }
        assertStatus("coordinator-token", () -> stub("coordinator-token").startWorkflowAuthoring(
                StartWorkflowAuthoringRequest.getDefaultInstance()), Status.Code.INVALID_ARGUMENT, "invalid-input");
        assertThat(operations.templateCalls).hasValue(0);
        assertThat(operations.startCalls).hasValue(0);
    }

    @Test
    void malformedSuccessfulTemplateAndStartResponsesBecomeDataLoss() {
        operations.templateResponse = GetWorkflowAuthoringTemplateResponse.getDefaultInstance();
        assertStatus("coordinator-token", () -> stub("coordinator-token").getWorkflowAuthoringTemplate(
                GetWorkflowAuthoringTemplateRequest.getDefaultInstance()),
                Status.Code.DATA_LOSS, "invalid-upstream-response");
        operations.templateResponse = templateResponse(template());
        operations.startResponse = StartWorkflowAuthoringResponse.getDefaultInstance();
        assertStatus("coordinator-token", () -> stub("coordinator-token").startWorkflowAuthoring(request()),
                Status.Code.DATA_LOSS, "invalid-upstream-response");
    }

    @Test
    void typedCoordinatorFailuresMapToStableGrpcCodesAndSanitizedTrailers() {
        operations.failure = new WorkflowAuthoringEntryException(
                WorkflowAuthoringEntryException.Kind.CONFLICT, SECRET, new IOException(SECRET));
        assertStatus("coordinator-token", () -> stub("coordinator-token").startWorkflowAuthoring(request()),
                Status.Code.ALREADY_EXISTS, "workflow-authoring-conflict");
        operations.failure = new WorkflowAuthoringEntryException(
                WorkflowAuthoringEntryException.Kind.INACTIVE, SECRET, new IOException(SECRET));
        assertStatus("coordinator-token", () -> stub("coordinator-token").startWorkflowAuthoring(request()),
                Status.Code.FAILED_PRECONDITION, "workflow-authoring-rejected");
        operations.failure = new WorkflowAuthoringEntryException(
                WorkflowAuthoringEntryException.Kind.CORRUPT_EVIDENCE, SECRET, new IOException(SECRET));
        assertStatus("coordinator-token", () -> stub("coordinator-token").startWorkflowAuthoring(request()),
                Status.Code.DATA_LOSS, "invalid-upstream-response");
        operations.failure = new WorkflowAuthoringEntryException(
                WorkflowAuthoringEntryException.Kind.UNAVAILABLE, SECRET, new IOException(SECRET));
        assertStatus("coordinator-token", () -> stub("coordinator-token").startWorkflowAuthoring(request()),
                Status.Code.UNAVAILABLE, "workflow-authoring-unavailable");
        operations.failure = new WorkflowAuthoringEntryException(
                WorkflowAuthoringEntryException.Kind.DEADLINE, SECRET, new IOException(SECRET));
        assertStatus("coordinator-token", () -> stub("coordinator-token").startWorkflowAuthoring(request()),
                Status.Code.DEADLINE_EXCEEDED, "workflow-authoring-deadline");
    }

    private WorkflowAuthoringEntryServiceGrpc.WorkflowAuthoringEntryServiceBlockingStub stub(String token) {
        var headers = new Metadata();
        headers.put(Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER), token);
        return WorkflowAuthoringEntryServiceGrpc.newBlockingStub(ClientInterceptors.intercept(channel,
                MetadataUtils.newAttachHeadersInterceptor(headers))).withDeadlineAfter(5, TimeUnit.SECONDS);
    }

    private void assertStatus(String token, org.assertj.core.api.ThrowableAssert.ThrowingCallable call,
            Status.Code expectedStatus, String expectedCode) {
        assertThatThrownBy(call).isInstanceOfSatisfying(StatusRuntimeException.class, failure -> {
            assertThat(failure.getStatus().getCode()).isEqualTo(expectedStatus);
            assertThat(failure.getTrailers().get(CatalogBridge.ERROR_CODE_KEY)).isEqualTo(expectedCode);
            assertThat(failure.getMessage()).doesNotContain(SECRET);
        });
    }

    private static WorkflowAuthoringTemplate template() {
        TaskSpec.Builder spec = TaskSpec.newBuilder().setObjective(OBJECTIVE)
                .addAllowedScope("transform/workflow/authoring")
                .addConstraints("No unbounded external writes.")
                .addContext(ArtifactReference.newBuilder().setSha256(HASH)
                        .setMediaType("application/x-protobuf").setSizeBytes(1))
                .setContract(DeliverableContract.newBuilder().setDescriptorSet(descriptorClosure())
                        .setTypeName(WorkflowAuthoringDeliverable.getDescriptor().getFullName()));
        WorkflowAuthoringReviewer.REQUIRED_CHECKS.forEach(name -> spec.addRequiredChecks(
                AcceptanceCheck.newBuilder().setName(name).setDescription(name)));
        TaskSpec rendered = DeliverableContracts.rendered(spec.build());
        return WorkflowAuthoringTemplate.newBuilder().setSpec(rendered).setLeaseSeconds(600).build();
    }

    private static ByteString descriptorClosure() {
        Map<String, FileDescriptor> files = new LinkedHashMap<>();
        collect(WorkflowAuthoringDeliverable.getDescriptor().getFile(), files);
        var set = FileDescriptorSet.newBuilder();
        files.values().forEach(file -> set.addFile(file.toProto()));
        return set.build().toByteString();
    }

    private static void collect(FileDescriptor file, Map<String, FileDescriptor> files) {
        if (files.containsKey(file.getName())) return;
        file.getDependencies().forEach(dependency -> collect(dependency, files));
        files.put(file.getName(), file);
    }

    private static GetWorkflowAuthoringTemplateResponse templateResponse(WorkflowAuthoringTemplate template) {
        return GetWorkflowAuthoringTemplateResponse.newBuilder().setTemplate(template)
                .setTemplateSha256(WorkflowAuthoringStartBinding.templateSha256(template)).build();
    }

    private static StartWorkflowAuthoringRequest request() {
        return StartWorkflowAuthoringRequest.newBuilder().setTaskId(TASK_ID).setWorkerId("author-worker")
                .setTemplateSha256(WorkflowAuthoringStartBinding.templateSha256(template()))
                .setObjective(OBJECTIVE).build();
    }

    private static StartWorkflowAuthoringResponse startResponse() {
        var request = request();
        var spec = template().getSpec();
        var lease = Duration.newBuilder().setSeconds(600).build();
        String binding = WorkflowAuthoringStartBinding.sha256(request, spec, lease);
        return StartWorkflowAuthoringResponse.newBuilder().setRequest(request)
                .setOffer(TaskOffer.newBuilder().setAttempt(1).setSpec(spec).setLeaseDuration(lease)
                        .setExpiresAt(Timestamp.newBuilder().setSeconds(1_800_000_000))
                        .setStartBindingSha256(binding)).build();
    }

    private static final class FakeOperations implements WorkflowAuthoringEntryOperations {
        private final AtomicInteger templateCalls = new AtomicInteger();
        private final AtomicInteger startCalls = new AtomicInteger();
        private GetWorkflowAuthoringTemplateResponse templateResponse =
                templateResponse(WorkflowAuthoringEntryGrpcTest.template());
        private StartWorkflowAuthoringResponse startResponse = startResponse();
        private WorkflowAuthoringEntryException failure;

        @Override public GetWorkflowAuthoringTemplateResponse template(GetWorkflowAuthoringTemplateRequest request) {
            templateCalls.incrementAndGet();
            if (failure != null) throw failure;
            return templateResponse;
        }

        @Override public StartWorkflowAuthoringResponse start(StartWorkflowAuthoringRequest request) {
            startCalls.incrementAndGet();
            if (failure != null) throw failure;
            return startResponse;
        }
    }
}
