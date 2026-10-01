package ai.protomolt.proto.workflow.authoring;

import ai.protomolt.proto.actions.*;
import ai.protomolt.proto.delegation.v1.*;
import ai.protomolt.proto.workflow.authoring.v1.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.util.JsonFormat;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkflowAuthorTaskActionsTest {
    private static final String TASK = "00000000-0000-4000-8000-000000000001";
    private static final Caller AUTHOR = Caller.scoped("author", Set.of(Scopes.WORKFLOW_AUTHOR));
    private final ActionContext context = ActionContext.create();

    @Test void typedAndJsonAcceptancePreserveIdentity() throws Exception {
        var calls = new AtomicInteger();
        var catalog = catalog(new Backend() {
            @Override public AcceptTaskResponse accept(AcceptTaskRequest request, Caller caller) {
                assertThat(caller).isEqualTo(AUTHOR);
                assertThat(request).isEqualTo(WorkflowAuthorTaskActionsTest.accept());
                calls.incrementAndGet();
                return accepted();
            }
        });
        assertThat(catalog.execute("accept-workflow-task", accept(), AUTHOR)).isEqualTo(accepted());
        var input = (ObjectNode) context.objectMapper().readTree(JsonFormat.printer().print(accept()));
        assertThat(catalog.execute("accept-workflow-task", input, AUTHOR).path("ok").asBoolean()).isTrue();
        assertThat(calls).hasValue(2);
    }

    @Test void wrongIdentityScopeLegacyAndInvalidInputCannotReachBackend() {
        var catalog = catalog(new Backend());
        code(() -> catalog.execute("accept-workflow-task", accept().toBuilder().setWorkerId("someone-else").build(), AUTHOR), "permission-denied");
        code(() -> catalog.execute("accept-workflow-task", accept(), Caller.operator()), "permission-denied");
        code(() -> catalog.execute("accept-workflow-task", accept(), Caller.scoped("author", Set.of(Scopes.WORKER_COORDINATE))), "permission-denied");
        code(() -> catalog.get("accept-workflow-task").execute(accept(), context), "permission-denied");
        code(() -> catalog.execute("accept-workflow-task", accept().toBuilder().setAttempt(0).build(), AUTHOR), "invalid-input");
    }

    @Test void jsonAnyResolvesAuthorContractAndInvalidPackedResultsStopBeforeBackend() throws Exception {
        var authored = WorkflowPreparationContractTest.authored("a".repeat(64));
        var candidate = CompletionCandidate.newBuilder().setAttempt(1).setRevision(1)
                .setSummary("Validated source and fixture evidence")
                .addAllEvidence(authored.getDeliverable().getChecksList())
                .addArtifacts(authored.getExecutableSource())
                .setResult(com.google.protobuf.Any.pack(authored)).build();
        var request = SubmitCandidateRequest.newBuilder().setWorkerId("author").setTaskId(TASK)
                .setCandidate(candidate).build();
        var calls = new AtomicInteger();
        var catalog = catalog(new Backend() {
            @Override public SubmitCandidateResponse submit(SubmitCandidateRequest value, Caller caller) {
                assertThat(value).isEqualTo(request);
                assertThat(caller).isEqualTo(AUTHOR);
                calls.incrementAndGet();
                return SubmitCandidateResponse.newBuilder().setOk(true).setTaskId(TASK)
                        .setAttempt(1).setRevision(1).build();
            }
        });
        catalog.execute("submit-workflow-candidate", request, AUTHOR);
        var registry = catalog.get("submit-workflow-candidate").typeRegistry(context.objectMapper().createObjectNode());
        var json = (ObjectNode) context.objectMapper().readTree(JsonFormat.printer()
                .usingTypeRegistry(registry).print(request));
        catalog.execute("submit-workflow-candidate", json, AUTHOR);
        var malformed = request.toBuilder().setCandidate(candidate.toBuilder().setResult(
                com.google.protobuf.Any.pack(authored.toBuilder().clearDeliverable().build()))).build();
        code(() -> catalog.execute("submit-workflow-candidate", malformed, AUTHOR), "invalid-input");
        var unknown = authored.toBuilder().setUnknownFields(com.google.protobuf.UnknownFieldSet.newBuilder()
                .addField(999, com.google.protobuf.UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build()).build();
        code(() -> catalog.execute("submit-workflow-candidate", request.toBuilder()
                .setCandidate(candidate.toBuilder().setResult(com.google.protobuf.Any.pack(unknown))).build(), AUTHOR), "invalid-input");
        assertThat(calls).hasValue(2);
    }

    @Test void outputBindingAndLegacyResponseGapsFailClosed() {
        var wrongAttempt = catalog(new Backend() {
            @Override public AcceptTaskResponse accept(AcceptTaskRequest request, Caller caller) {
                return accepted().toBuilder().setAttempt(2).build();
            }
        });
        code(() -> wrongAttempt.execute("accept-workflow-task", accept(), AUTHOR), "invalid-upstream-response");
        var badSession = catalog(new Backend() {
            @Override public RegisterWorkerResponse register(RegisterWorkerRequest request, Caller caller) {
                return RegisterWorkerResponse.newBuilder().setOk(true).setWorkerId("author")
                        .setAdmitted(true).setSessionId("not-a-uuid").build();
            }
        });
        code(() -> badSession.execute("register-workflow-author", RegisterWorkerRequest.newBuilder()
                .setWorkerId("author").build(), AUTHOR), "invalid-upstream-response");
        var badCursor = catalog(new Backend() {
            @Override public ReadWorkflowAuthorEventsResponse events(ReadWorkflowAuthorEventsRequest request, Caller caller) {
                return ReadWorkflowAuthorEventsResponse.newBuilder().setTaskId(TASK).setAttempt(1)
                        .setAfterCursor(6).setCursor(6).build();
            }
        });
        code(() -> badCursor.execute("read-workflow-author-events", ReadWorkflowAuthorEventsRequest.newBuilder()
                .setTaskId(TASK).setAttempt(1).setAfterCursor(5).setMaxEvents(1).build(), AUTHOR), "invalid-upstream-response");
    }

    @Test void actualGrpcRoutesIdentityAndConflictWithoutBackendDetails() throws Exception {
        var catalog = catalog(new Backend() {
            @Override public AcceptTaskResponse accept(AcceptTaskRequest request, Caller caller) throws WorkflowPreparationException {
                assertThat(caller).isEqualTo(AUTHOR);
                throw new WorkflowPreparationException(WorkflowPreparationException.Kind.CONFLICT, "private backend", null);
            }
        });
        var service = WorkflowAuthorTaskServiceOuterClass.getDescriptor().findServiceByName("WorkflowAuthorTaskService");
        String name = io.grpc.inprocess.InProcessServerBuilder.generateName();
        var server = io.grpc.inprocess.InProcessServerBuilder.forName(name)
                .intercept(new ai.protomolt.proto.authz.grpc.ApiTokenServerInterceptor("operator-token", token ->
                        "author-token".equals(token) ? java.util.Optional.of(AUTHOR) : java.util.Optional.empty()))
                .addService(ai.protomolt.proto.grpc.service.ProtoMoltGrpcService.contributed(catalog, service))
                .build().start();
        var channel = io.grpc.inprocess.InProcessChannelBuilder.forName(name).build();
        try {
            var headers = new io.grpc.Metadata();
            headers.put(io.grpc.Metadata.Key.of("api_token", io.grpc.Metadata.ASCII_STRING_MARSHALLER), "author-token");
            var stub = WorkflowAuthorTaskServiceGrpc.newBlockingStub(channel)
                    .withInterceptors(io.grpc.stub.MetadataUtils.newAttachHeadersInterceptor(headers));
            assertThatThrownBy(() -> stub.withDeadlineAfter(5, java.util.concurrent.TimeUnit.SECONDS).acceptWorkflowTask(accept()))
                    .isInstanceOfSatisfying(io.grpc.StatusRuntimeException.class, error -> {
                        assertThat(error.getStatus().getCode()).isEqualTo(io.grpc.Status.Code.ALREADY_EXISTS);
                        assertThat(error.getStatus().getDescription()).doesNotContain("private backend");
                        assertThat(error.getTrailers().get(ai.protomolt.proto.grpc.service.CatalogBridge.ERROR_CODE_KEY))
                                .isEqualTo("workflow-authoring-conflict");
                    });
        } finally {
            channel.shutdownNow().awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS);
            server.shutdownNow().awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS);
        }
    }

    @Test void eventResponseCannotHideAnotherTaskAttemptOrUnscopedMessage() throws Exception {
        var frame = DelegateRequest.newBuilder().setFrameId(TASK).setTaskId(TASK).setSeq(1)
                .setSentAt(com.google.protobuf.Timestamp.newBuilder().setSeconds(1))
                .setAccept(TaskAccept.newBuilder().setAttempt(1)).build();
        var entry = TranscriptEntry.newBuilder().setLane(Lane.LANE_WORKER).setWorkerId("author")
                .setWorkerFrame(frame).build();
        var request = ReadWorkflowAuthorEventsRequest.newBuilder().setTaskId(TASK).setAttempt(1)
                .setMaxEvents(1).build();
        eventCatalog(entry).execute("read-workflow-author-events", request, AUTHOR);
        var wrongAttempt = entry.toBuilder().setWorkerFrame(frame.toBuilder()
                .setAccept(TaskAccept.newBuilder().setAttempt(2))).build();
        code(() -> eventCatalog(wrongAttempt).execute("read-workflow-author-events", request, AUTHOR),
                "invalid-upstream-response");
        var wrongTask = entry.toBuilder().setWorkerFrame(frame.toBuilder()
                .setTaskId("00000000-0000-4000-8000-000000000002")).build();
        code(() -> eventCatalog(wrongTask).execute("read-workflow-author-events", request, AUTHOR),
                "invalid-upstream-response");
        var message = entry.toBuilder().setWorkerFrame(frame.toBuilder().clearAccept().setTaskMessage(
                TaskMessage.newBuilder().setMessageId(TASK).setTaskId(TASK).setSender("author")
                        .setRecipient("coordinator").setKind(TaskMessageKind.TASK_MESSAGE_KIND_NOTE)
                        .setText("Task note").setSentAt(com.google.protobuf.Timestamp.newBuilder().setSeconds(1)))).build();
        code(() -> eventCatalog(message).execute("read-workflow-author-events", request, AUTHOR),
                "invalid-upstream-response");
    }

    @Test void assignmentDiscoveryUsesAuthenticatedIdentityAndPreservesTypedDynamicAndJsonPages() throws Exception {
        var calls = new AtomicInteger();
        var expected = assignments();
        var catalog = catalog(new Backend() {
            @Override public ReadWorkflowAuthorAssignmentsResponse assignments(
                    ReadWorkflowAuthorAssignmentsRequest request, Caller caller) {
                assertThat(caller).isEqualTo(AUTHOR);
                assertThat(request).isEqualTo(assignmentRequest());
                calls.incrementAndGet();
                return expected;
            }
        });

        assertThat(catalog.get("read-workflow-author-assignments").requiredScope())
                .isEqualTo(Scopes.WORKFLOW_AUTHOR);
        assertThat(catalog.execute("read-workflow-author-assignments", assignmentRequest(), AUTHOR))
                .isEqualTo(expected);
        var dynamic = com.google.protobuf.DynamicMessage.parseFrom(
                assignmentRequest().getDescriptorForType(), assignmentRequest().toByteArray());
        assertThat(catalog.execute("read-workflow-author-assignments", dynamic, AUTHOR).toByteArray())
                .isEqualTo(expected.toByteArray());
        ObjectNode jsonRequest = (ObjectNode) context.objectMapper().readTree(
                JsonFormat.printer().print(assignmentRequest()));
        ObjectNode jsonResponse = catalog.execute("read-workflow-author-assignments", jsonRequest, AUTHOR);
        assertThat(jsonResponse.path("workerId").asText()).isEqualTo(AUTHOR.name());
        assertThat(jsonResponse.path("afterCursor").asLong()).isEqualTo(4);
        assertThat(jsonResponse.path("assignments").size()).isEqualTo(2);
        assertThat(calls).hasValue(3);
    }

    @Test void assignmentScopeAndInvalidRequestAreRejectedBeforeBackend() {
        var calls = new AtomicInteger();
        var catalog = catalog(new Backend() {
            @Override public ReadWorkflowAuthorAssignmentsResponse assignments(
                ReadWorkflowAuthorAssignmentsRequest request, Caller caller) {
                calls.incrementAndGet();
                return WorkflowAuthorTaskActionsTest.assignments();
            }
        });
        code(() -> catalog.execute("read-workflow-author-assignments", assignmentRequest(), Caller.scoped("reader", Set.of())),
                "permission-denied");
        code(() -> catalog.execute("read-workflow-author-assignments", assignmentRequest(),
                Caller.scoped("coordinator", Set.of(Scopes.WORKER_COORDINATE))), "permission-denied");
        code(() -> catalog.execute("read-workflow-author-assignments",
                assignmentRequest().toBuilder().setMaxAssignments(65).build(), AUTHOR), "invalid-input");
        code(() -> catalog.execute("read-workflow-author-assignments",
                assignmentRequest().toBuilder().setAfterCursor(-1).build(), AUTHOR), "invalid-input");
        assertThat(calls).hasValue(0);
    }

    @Test void assignmentBackendMustBindPrincipalRequestCursorCountAndOrderedContents() {
        // Use separate backends to keep every malformed-response case explicit.
        for (ReadWorkflowAuthorAssignmentsResponse invalid : java.util.List.of(
                assignments().toBuilder().setWorkerId("another-author").build(),
                assignments().toBuilder().setAfterCursor(3).build(),
                assignments().toBuilder().clearAssignments().addAssignments(assignment(5, TASK, 1, "a".repeat(64)))
                        .addAssignments(assignment(4, TASK, 2, "b".repeat(64))).build(),
                assignments().toBuilder().setCursor(10)
                        .addAssignments(assignment(10, TASK, 3, "c".repeat(64))).build(),
                assignments().toBuilder().setAssignments(0, assignment(5, "bad-id", 1, "A".repeat(64))).build())) {
            var fake = new Backend() {
                @Override public ReadWorkflowAuthorAssignmentsResponse assignments(
                        ReadWorkflowAuthorAssignmentsRequest request, Caller caller) { return invalid; }
            };
            code(() -> catalog(fake).execute("read-workflow-author-assignments", assignmentRequest(), AUTHOR),
                    "invalid-upstream-response");
        }
    }

    private static ReadWorkflowAuthorAssignmentsRequest assignmentRequest() {
        return ReadWorkflowAuthorAssignmentsRequest.newBuilder().setAfterCursor(4).setMaxAssignments(2).build();
    }

    private static WorkflowAuthorAssignment assignment(long cursor, String task, int attempt, String hash) {
        return WorkflowAuthorAssignment.newBuilder().setCursor(cursor).setTaskId(task)
                .setAttempt(attempt).setOfferEntrySha256(hash).build();
    }

    private static ReadWorkflowAuthorAssignmentsResponse assignments() {
        return ReadWorkflowAuthorAssignmentsResponse.newBuilder().setWorkerId(AUTHOR.name())
                .setAfterCursor(4).setCursor(9)
                .addAssignments(assignment(5, TASK, 1, "a".repeat(64)))
                .addAssignments(assignment(9, "00000000-0000-4000-8000-000000000002", 2, "b".repeat(64)))
                .build();
    }

    private ActionCatalog eventCatalog(TranscriptEntry entry) {
        return catalog(new Backend() {
            @Override public ReadWorkflowAuthorEventsResponse events(ReadWorkflowAuthorEventsRequest request, Caller caller) {
                return ReadWorkflowAuthorEventsResponse.newBuilder().setTaskId(TASK).setAttempt(1).setCursor(1)
                        .addEvents(ObservedEvent.newBuilder().setTaskId(TASK).setWorkerId("author")
                                .setCursor(1).setLane(Lane.LANE_WORKER).setEntry(entry)).build();
            }
        });
    }

    private ActionCatalog catalog(WorkflowAuthorTaskOperations operations) {
        return WorkflowAuthorTaskActions.register(ActionCatalog.defaults(context), operations);
    }
    private static AcceptTaskRequest accept() {
        return AcceptTaskRequest.newBuilder().setWorkerId("author").setTaskId(TASK).setAttempt(1).build();
    }
    private static AcceptTaskResponse accepted() {
        return AcceptTaskResponse.newBuilder().setOk(true).setTaskId(TASK).setAttempt(1).build();
    }
    private static void code(org.assertj.core.api.ThrowableAssert.ThrowingCallable action, String expected) {
        assertThatThrownBy(action).isInstanceOfSatisfying(ActionException.class,
                failure -> assertThat(failure.code()).isEqualTo(expected));
    }
    private static class Backend implements WorkflowAuthorTaskOperations {
        @Override public ReadWorkflowAuthorAssignmentsResponse assignments(ReadWorkflowAuthorAssignmentsRequest request, Caller caller) throws WorkflowPreparationException { throw new AssertionError("Unexpected backend call"); }
        @Override public RegisterWorkerResponse register(RegisterWorkerRequest request, Caller caller) throws WorkflowPreparationException { throw new AssertionError("Unexpected backend call"); }
        @Override public AcceptTaskResponse accept(AcceptTaskRequest request, Caller caller) throws WorkflowPreparationException { throw new AssertionError("Unexpected backend call"); }
        @Override public SubmitCandidateResponse submit(SubmitCandidateRequest request, Caller caller) throws WorkflowPreparationException { throw new AssertionError("Unexpected backend call"); }
        @Override public GetWorkflowAuthorContextResponse context(GetWorkflowAuthorContextRequest request, Caller caller) throws WorkflowPreparationException { throw new AssertionError("Unexpected backend call"); }
        @Override public ReadWorkflowAuthorEventsResponse events(ReadWorkflowAuthorEventsRequest request, Caller caller) throws WorkflowPreparationException { throw new AssertionError("Unexpected backend call"); }
    }
}
