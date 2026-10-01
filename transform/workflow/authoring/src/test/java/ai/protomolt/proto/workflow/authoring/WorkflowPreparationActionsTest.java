package ai.protomolt.proto.workflow.authoring;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.workflow.authoring.v1.PrepareWorkflowCandidateRequest;
import ai.protomolt.proto.workflow.authoring.v1.PrepareWorkflowCandidateResponse;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowPreparationBinding;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowPreparationFailure;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowPreparationFailureReason;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowPreparationServiceOuterClass;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.JsonFormat;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkflowPreparationActionsTest {
    private static final String VERB = "prepare-workflow-candidate";
    private static final String TASK = "00000000-0000-4000-8000-000000000001";
    private static final String ID = "00000000-0000-4000-8000-000000000002";
    private static final Caller AUTHOR = Caller.scoped("author", Set.of(Scopes.WORKFLOW_AUTHOR));
    private final ActionContext context = ActionContext.create();

    @Test void scopedTypedAndJsonDispatchPreserveIdentityAndExposeMatchingContract() throws Exception {
        var count = new AtomicInteger();
        var catalog = catalog((request, caller) -> {
            assertThat(caller).isEqualTo(AUTHOR);
            assertThat(request).isEqualTo(request());
            count.incrementAndGet();
            throw new WorkflowPreparationException(WorkflowPreparationException.Kind.INACTIVE, "private backend", null);
        });
        var method = WorkflowPreparationServiceOuterClass.getDescriptor()
                .findServiceByName("WorkflowPreparationService").findMethodByName("PrepareWorkflowCandidate");
        assertThat(method.getInputType()).isEqualTo(catalog.get(VERB).requestType());
        assertThat(method.getOutputType()).isEqualTo(catalog.get(VERB).responseType());
        assertCode(() -> catalog.execute(VERB, request(), AUTHOR), "workflow-authoring-rejected");
        var json = (ObjectNode) context.objectMapper().readTree(JsonFormat.printer().print(request()));
        assertCode(() -> catalog.execute(VERB, json, AUTHOR), "workflow-authoring-rejected");
        assertThat(count).hasValue(2);
    }

    @Test void wrongScopeLegacyEntryAndMalformedRequestCannotInvokeBackend() {
        var count = new AtomicInteger();
        var catalog = catalog((request, caller) -> { count.incrementAndGet(); return null; });
        assertCode(() -> catalog.execute(VERB, request(), Caller.scoped("worker",
                Set.of(Scopes.WORKER_COORDINATE, Scopes.WORKFLOW_RUN))), "permission-denied");
        assertCode(() -> catalog.get(VERB).execute(request(), context), "permission-denied");
        assertCode(() -> catalog.execute(VERB, PrepareWorkflowCandidateRequest.getDefaultInstance(), AUTHOR),
                "invalid-input");
        assertThat(count).hasValue(0);
    }

    @Test void invalidSuccessfulOutputIsRefusedAndBackendMessagesAreSanitized() {
        var missing = catalog((request, caller) -> null);
        assertCode(() -> missing.execute(VERB, request(), AUTHOR), "invalid-upstream-response");
        var malformed = catalog((request, caller) -> PrepareWorkflowCandidateResponse.getDefaultInstance());
        assertCode(() -> malformed.execute(VERB, request(), AUTHOR), "invalid-upstream-response");
        var unexpected = catalog((request, caller) -> { throw new IllegalStateException("private backend"); });
        assertCode(() -> unexpected.execute(VERB, request(), AUTHOR), "internal-error");
    }

    @Test void terminalFailureCarriesValidatedBoundDetail() throws Exception {
        var detail = WorkflowPreparationFailure.newBuilder().setBinding(WorkflowPreparationBinding.newBuilder()
                .setTaskId(TASK).setAttempt(1).setRevision(1).setPreparationId(ID)
                .setOfferEntrySha256("a".repeat(64)).setSourceSha256(ai.protomolt.proto.receipt.WorkRecords.sha256Hex(
                        request().getExecutableSourceJson().toByteArray())))
                .setRunId("prepare-" + ID)
                .setReason(WorkflowPreparationFailureReason.WORKFLOW_PREPARATION_FAILURE_REASON_FIXTURE_REJECTED).build();
        var catalog = catalog((request, caller) -> {
            throw new WorkflowPreparationException(WorkflowPreparationException.Kind.TERMINAL_FAILED,
                    "private backend", null, detail);
        });
        assertThatThrownBy(() -> catalog.execute(VERB, request(), AUTHOR))
                .isInstanceOfSatisfying(ActionException.class, error -> {
                    assertThat(error.code()).isEqualTo("preparation-attempt-failed");
                    assertThat(error.getMessage()).doesNotContain("private backend");
                    var decoded = WorkflowPreparationFailure.newBuilder();
                    try { JsonFormat.parser().merge(error.details().orElseThrow().toString(), decoded); }
                    catch (Exception failure) { throw new AssertionError(failure); }
                    assertThat(decoded.build()).isEqualTo(detail);
                });
        var invalid = catalog((request, caller) -> {
            throw new WorkflowPreparationException(WorkflowPreparationException.Kind.TERMINAL_FAILED,
                    "private backend", null);
        });
        assertCode(() -> invalid.execute(VERB, request(), AUTHOR), "internal-error");
        var differentRun = catalog((request, caller) -> {
            throw new WorkflowPreparationException(WorkflowPreparationException.Kind.TERMINAL_FAILED,
                    "private backend", null, detail.toBuilder().setRunId("different-run").build());
        });
        assertCode(() -> differentRun.execute(VERB, request(), AUTHOR), "internal-error");
    }

    private ActionCatalog catalog(WorkflowPreparationOperations operation) {
        return WorkflowPreparationActions.register(ActionCatalog.defaults(context), operation);
    }

    @Test void contributedGrpcPreservesScopedIdentityAndConflictStatus() throws Exception {
        var catalog = catalog((request, caller) -> {
            assertThat(caller).isEqualTo(AUTHOR);
            throw new WorkflowPreparationException(WorkflowPreparationException.Kind.CONFLICT,
                    "private backend", null);
        });
        var service = WorkflowPreparationServiceOuterClass.getDescriptor()
                .findServiceByName("WorkflowPreparationService");
        String name = io.grpc.inprocess.InProcessServerBuilder.generateName();
        var server = io.grpc.inprocess.InProcessServerBuilder.forName(name)
                .intercept(new ai.protomolt.proto.authz.grpc.ApiTokenServerInterceptor("test-operator", token ->
                        "test-author".equals(token) ? java.util.Optional.of(AUTHOR) : java.util.Optional.empty()))
                .addService(ai.protomolt.proto.grpc.service.ProtoMoltGrpcService.contributed(catalog, service))
                .build().start();
        var channel = io.grpc.inprocess.InProcessChannelBuilder.forName(name).build();
        try {
            var headers = new io.grpc.Metadata();
            headers.put(io.grpc.Metadata.Key.of("api_token", io.grpc.Metadata.ASCII_STRING_MARSHALLER), "test-author");
            var method = service.findMethodByName("PrepareWorkflowCandidate");
            assertThatThrownBy(() -> ai.protomolt.proto.grpc.invoke.DynamicGrpcCalls.call(channel, method,
                    com.google.protobuf.DynamicMessage.parseFrom(method.getInputType(), request().toByteArray()),
                    io.grpc.CallOptions.DEFAULT.withDeadlineAfter(5, java.util.concurrent.TimeUnit.SECONDS), headers, 1))
                    .isInstanceOfSatisfying(io.grpc.StatusRuntimeException.class, error -> {
                        assertThat(error.getStatus().getCode()).isEqualTo(io.grpc.Status.Code.ALREADY_EXISTS);
                        assertThat(error.getStatus().getDescription()).doesNotContain("private backend");
                        assertThat(error.getTrailers().get(ai.protomolt.proto.grpc.service.CatalogBridge.ERROR_CODE_KEY))
                                .isEqualTo("workflow-preparation-conflict");
                    });
        } finally {
            channel.shutdownNow().awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS);
            server.shutdownNow().awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS);
        }
    }

    private static PrepareWorkflowCandidateRequest request() {
        return PrepareWorkflowCandidateRequest.newBuilder().setTaskId(TASK).setAttempt(1).setRevision(1)
                .setPreparationId(ID).setExecutableSourceJson(ByteString.copyFromUtf8("{}")).build();
    }

    private static void assertCode(org.assertj.core.api.ThrowableAssert.ThrowingCallable call, String code) {
        assertThatThrownBy(call).isInstanceOfSatisfying(ActionException.class, error -> {
            assertThat(error.code()).isEqualTo(code);
            assertThat(error.getMessage()).doesNotContain("private backend");
        });
    }
}
