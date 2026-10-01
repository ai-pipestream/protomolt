package ai.protomolt.proto.serve;

import ai.protomolt.proto.actions.*;
import ai.protomolt.proto.authz.CallerResolver;
import ai.protomolt.proto.delegation.*;
import ai.protomolt.proto.delegation.v1.*;
import ai.protomolt.proto.grpc.service.ProtoMoltGrpcServer;
import ai.protomolt.proto.grpc.workflow.FileSystemArtifactRepository;
import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import ai.protomolt.proto.http.openapi.ProtoOpenApiGenerator;
import ai.protomolt.proto.http.rest.ApiTokenRequirement;
import ai.protomolt.proto.http.rest.ForbiddenProtoRestException;
import ai.protomolt.proto.http.rest.ProtoRestGateway;
import ai.protomolt.proto.http.rest.ProtoRestMethodRegistry;
import ai.protomolt.proto.samples.starter.v1.WorkflowAcceptedCandidate;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringLaunchRequest;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringLaunchResult;
import ai.protomolt.proto.workflow.authoring.*;
import ai.protomolt.proto.workflow.authoring.v1.*;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;
import java.nio.file.Path;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkflowAuthorTaskMountTest {
    @TempDir Path temp;

    @Test void authorCredentialUsesOnlyScopedServiceAndCannotOfferReviewOrLaunch() throws Exception {
        var policy = ArtifactReference.newBuilder().setSha256("a".repeat(64))
                .setMediaType("application/x-protobuf").setSizeBytes(1).build();
        var artifacts = new FileSystemArtifactRepository(temp.resolve("artifacts"));
        var authoring = new WorkflowAuthoringMount.Prepared(policy, artifacts, null, null,
                null, null, null, 0, null, null);
        try (var coordinator = new InProcessDelegationCoordinator();
             var bridge = new DelegationBridge(coordinator)) {
            var catalog = ai.protomolt.proto.delegation.DelegationActions.register(
                    ActionCatalog.defaults(ActionContext.create()), bridge);
            WorkflowAuthorTaskActions.register(catalog, WorkflowAuthorTaskMount.operations(
                    bridge, new InMemoryTranscriptRepository(), authoring, Clock.systemUTC()));
            WorkflowAuthoringActions.register(catalog, new WorkflowAuthoringOperations() {
                @Override public WorkflowAcceptedCandidate acceptedCandidate(String taskId) {
                    throw new AssertionError("author cannot read accepted workflow");
                }
                @Override public WorkflowAuthoringLaunchResult launch(WorkflowAuthoringLaunchRequest request) {
                    throw new AssertionError("author cannot launch workflow");
                }
            });
            CallerResolver callers = token -> "author-token".equals(token)
                    ? Optional.of(Caller.scoped("author", Set.of(Scopes.WORKFLOW_AUTHOR)))
                    : Optional.empty();
            var delegationService = ai.protomolt.proto.delegation.v1.DelegationActions.getDescriptor()
                    .findServiceByName("DelegationService");
            var authoringService = GetAcceptedWorkflowRequest.getDescriptor().getFile()
                    .findServiceByName("WorkflowAuthoringService");
            var services = List.of(delegationService, WorkflowAuthorTaskMount.service(), authoringService);
            var rest = new ProtoRestMethodRegistry();
            ProtoMoltRestMount.register(rest, catalog, ApiTokenRequirement.apiKeyHeader("api_token"),
                    headers -> callers.resolve(headers.get("api_token")).orElse(null),
                    services, WorkflowAuthorTaskMount.bindings());
            Map<?, ?> openApi = new ProtoOpenApiGenerator().generate(rest);
            Map<?, ?> paths = (Map<?, ?>) openApi.get("paths");
            assertThat(paths.containsKey(
                    "/grpc-json/WorkflowAuthorTaskService/RegisterWorkflowAuthor")).isTrue();
            assertThat(paths.containsKey(
                    "/grpc-json/WorkflowAuthorTaskService/ReadWorkflowAuthorEvents")).isTrue();
            var gateway = new ProtoRestGateway(rest, ActionContext.create().transcoder(),
                    (requirement, headers, query) -> "author-token".equals(headers.get("api_token"))
                            ? Optional.empty() : Optional.of("Invalid API token"));
            assertThatThrownBy(() -> gateway.invoke("DelegationService", "RegisterWorker",
                    "{\"workerId\":\"author\"}", Map.of("api_token", "author-token"), Map.of()))
                    .isInstanceOf(ForbiddenProtoRestException.class);
            var disabledRest = new ProtoRestMethodRegistry();
            ProtoMoltRestMount.register(disabledRest, catalog,
                    ApiTokenRequirement.apiKeyHeader("api_token"),
                    headers -> callers.resolve(headers.get("api_token")).orElse(null),
                    List.of(delegationService, authoringService), WorkflowAuthorTaskMount.bindings());
            Map<?, ?> disabledPaths = (Map<?, ?>) new ProtoOpenApiGenerator()
                    .generate(disabledRest).get("paths");
            assertThat(disabledPaths.containsKey(
                    "/grpc-json/WorkflowAuthorTaskService/RegisterWorkflowAuthor")).isFalse();
            try (var server = ProtoMoltGrpcServer.start("127.0.0.1", 0, catalog,
                    "operator-token", callers, services,
                    WorkflowAuthorTaskMount.bindings())) {
                var channel = ManagedChannelBuilder.forAddress("127.0.0.1", server.port())
                        .usePlaintext().build();
                try {
                    Metadata headers = new Metadata();
                    headers.put(Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER),
                            "author-token");
                    var authenticated = io.grpc.ClientInterceptors.intercept(channel,
                            MetadataUtils.newAttachHeadersInterceptor(headers));
                    var task = WorkflowAuthorTaskServiceGrpc.newBlockingStub(authenticated)
                            .withDeadlineAfter(5, TimeUnit.SECONDS);
                    assertThat(task.registerWorkflowAuthor(RegisterWorkerRequest.newBuilder()
                            .setWorkerId("author").build()).getAdmitted()).isTrue();
                    int before = coordinator.transcript().getEntriesCount();
                    var delegation = DelegationServiceGrpc.newBlockingStub(authenticated)
                            .withDeadlineAfter(5, TimeUnit.SECONDS);
                    denied(() -> delegation.registerWorker(RegisterWorkerRequest.newBuilder()
                            .setWorkerId("author").build()));
                    denied(() -> delegation.offerTask(OfferTaskRequest.getDefaultInstance()));
                    denied(() -> delegation.reviewCandidate(ReviewCandidateRequest.getDefaultInstance()));
                    var launch = WorkflowAuthoringServiceGrpc.newBlockingStub(authenticated)
                            .withDeadlineAfter(5, TimeUnit.SECONDS);
                    denied(() -> launch.launchAcceptedWorkflow(
                            WorkflowAuthoringLaunchRequest.getDefaultInstance()));
                    assertThatThrownBy(() -> WorkflowAuthorTaskServiceGrpc.newBlockingStub(channel)
                            .withDeadlineAfter(5, TimeUnit.SECONDS)
                            .registerWorkflowAuthor(RegisterWorkerRequest.newBuilder()
                                    .setWorkerId("author").build()))
                            .isInstanceOfSatisfying(StatusRuntimeException.class,
                                    failure -> assertThat(failure.getStatus().getCode())
                                            .isEqualTo(Status.Code.UNAUTHENTICATED));
                    Metadata unknownHeaders = new Metadata();
                    unknownHeaders.put(Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER),
                            "unknown-token");
                    var unknown = io.grpc.ClientInterceptors.intercept(channel,
                            MetadataUtils.newAttachHeadersInterceptor(unknownHeaders));
                    assertThatThrownBy(() -> WorkflowAuthorTaskServiceGrpc.newBlockingStub(unknown)
                            .withDeadlineAfter(5, TimeUnit.SECONDS)
                            .registerWorkflowAuthor(RegisterWorkerRequest.newBuilder()
                                    .setWorkerId("author").build()))
                            .isInstanceOfSatisfying(StatusRuntimeException.class,
                                    failure -> assertThat(failure.getStatus().getCode())
                                            .isEqualTo(Status.Code.UNAUTHENTICATED));
                    assertThat(coordinator.transcript().getEntriesCount()).isEqualTo(before);
                } finally {
                    channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
                }
            }
        }
    }

    private static void denied(org.assertj.core.api.ThrowableAssert.ThrowingCallable call) {
        assertThatThrownBy(call).isInstanceOfSatisfying(StatusRuntimeException.class,
                failure -> assertThat(failure.getStatus().getCode())
                        .isEqualTo(Status.Code.PERMISSION_DENIED));
    }
}
