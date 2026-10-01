package ai.protomolt.proto.workflow.authoring;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.delegation.v1.AcceptTaskRequest;
import ai.protomolt.proto.delegation.v1.AcceptTaskResponse;
import ai.protomolt.proto.delegation.v1.RegisterWorkerRequest;
import ai.protomolt.proto.delegation.v1.RegisterWorkerResponse;
import ai.protomolt.proto.delegation.v1.SubmitCandidateRequest;
import ai.protomolt.proto.delegation.v1.SubmitCandidateResponse;
import ai.protomolt.proto.workflow.authoring.v1.EnsureWorkflowAuthorRegistrationRequest;
import ai.protomolt.proto.workflow.authoring.v1.EnsureWorkflowAuthorRegistrationResponse;
import ai.protomolt.proto.workflow.authoring.v1.GetWorkflowAuthorContextRequest;
import ai.protomolt.proto.workflow.authoring.v1.GetWorkflowAuthorContextResponse;
import ai.protomolt.proto.workflow.authoring.v1.ReadWorkflowAuthorAssignmentsRequest;
import ai.protomolt.proto.workflow.authoring.v1.ReadWorkflowAuthorAssignmentsResponse;
import ai.protomolt.proto.workflow.authoring.v1.ReadWorkflowAuthorEventsRequest;
import ai.protomolt.proto.workflow.authoring.v1.ReadWorkflowAuthorEventsResponse;
import ai.protomolt.proto.validate.ProtoValidator;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.UnknownFieldSet;
import com.google.protobuf.util.JsonFormat;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkflowAuthorEnsureRegistrationActionsTest {
    private static final String WORKER = "sample-author";
    private static final String SESSION = "00000000-0000-4000-8000-000000000055";
    private static final Caller AUTHOR = Caller.scoped(WORKER, Set.of(Scopes.WORKFLOW_AUTHOR));
    private static final Caller OTHER_AUTHOR = Caller.scoped("another-author", Set.of(Scopes.WORKFLOW_AUTHOR));
    private final ActionContext context = ActionContext.create();

    @Test
    void wrapperContractsValidateGeneratedAndDynamicRequestsAndResponses() {
        var requestValidator = ProtoValidator.forMessageType(EnsureWorkflowAuthorRegistrationRequest.getDescriptor());
        var responseValidator = ProtoValidator.forMessageType(EnsureWorkflowAuthorRegistrationResponse.getDescriptor());
        assertValidBoth(requestValidator, request());
        assertInvalidBoth(requestValidator, EnsureWorkflowAuthorRegistrationRequest.getDefaultInstance());
        assertInvalidBoth(requestValidator, request().toBuilder().clearRegistration().build());
        assertValidBoth(responseValidator, admitted());
        assertInvalidBoth(responseValidator, EnsureWorkflowAuthorRegistrationResponse.getDefaultInstance());
        assertInvalidBoth(responseValidator, admitted().toBuilder().clearRegistration().build());
        assertInvalidBoth(responseValidator, admitted().toBuilder().setRegistration(
                RegisterWorkerResponse.newBuilder().setOk(false).setWorkerId(WORKER).setAdmitted(true)
                        .setSessionId(SESSION)).build());
    }

    @Test
    void ensureRegistrationUsesAuthenticatedBackendAndSupportsGeneratedDynamicAndJsonCalls() throws Exception {
        var calls = new AtomicInteger();
        var catalog = catalog(new Backend() {
            @Override public EnsureWorkflowAuthorRegistrationResponse ensureRegistration(
                    EnsureWorkflowAuthorRegistrationRequest request, Caller caller) {
                assertThat(request).isEqualTo(WorkflowAuthorEnsureRegistrationActionsTest.request());
                assertThat(caller).isEqualTo(AUTHOR);
                calls.incrementAndGet();
                return admitted();
            }
        });

        assertThat(catalog.get("ensure-workflow-author-registration").requiredScope())
                .isEqualTo(Scopes.WORKFLOW_AUTHOR);
        assertThat(catalog.execute("ensure-workflow-author-registration", request(), AUTHOR)).isEqualTo(admitted());
        DynamicMessage dynamic = DynamicMessage.newBuilder(request().getDescriptorForType())
                .mergeFrom(request()).build();
        assertThat(catalog.execute("ensure-workflow-author-registration", dynamic, AUTHOR).toByteArray())
                .isEqualTo(admitted().toByteArray());
        ObjectNode json = (ObjectNode) context.objectMapper().readTree(JsonFormat.printer().print(request()));
        ObjectNode jsonResponse = catalog.execute("ensure-workflow-author-registration", json, AUTHOR);
        assertThat(jsonResponse.path("registration").path("ok").asBoolean()).isTrue();
        assertThat(jsonResponse.path("registration").path("workerId").asText()).isEqualTo(WORKER);
        assertThat(jsonResponse.path("registration").path("admitted").asBoolean()).isTrue();
        assertThat(jsonResponse.path("registration").path("sessionId").asText()).isEqualTo(SESSION);
        assertThat(calls).hasValue(3);
    }

    @Test
    void foreignIdentityScopeMalformedAndUnknownRequestsAreRejectedBeforeBackend() {
        var calls = new AtomicInteger();
        var catalog = catalog(new Backend() {
            @Override public EnsureWorkflowAuthorRegistrationResponse ensureRegistration(
                    EnsureWorkflowAuthorRegistrationRequest request, Caller caller) {
                calls.incrementAndGet();
                return admitted();
            }
        });

        code(() -> catalog.execute("ensure-workflow-author-registration", request(),
                Caller.scoped(WORKER, Set.of())), "permission-denied");
        code(() -> catalog.execute("ensure-workflow-author-registration", request(), OTHER_AUTHOR), "permission-denied");
        code(() -> catalog.execute("ensure-workflow-author-registration",
                request().toBuilder().setRegistration(metadata().toBuilder().setWorkerId("another-author")).build(),
                AUTHOR), "permission-denied");
        code(() -> catalog.get("ensure-workflow-author-registration").execute(request(), context), "permission-denied");
        code(() -> catalog.execute("ensure-workflow-author-registration",
                request().toBuilder().setRegistration(metadata().toBuilder().setWorkerId("invalid worker")).build(),
                AUTHOR), "invalid-input");

        var tooManyCapabilities = metadata().toBuilder().clearCapabilities();
        for (int i = 0; i < 65; i++) {
            tooManyCapabilities.addCapabilities(ai.protomolt.proto.delegation.v1.WorkerCapability.newBuilder()
                    .setName("capability-" + i));
        }
        code(() -> catalog.execute("ensure-workflow-author-registration", request().toBuilder()
                .setRegistration(tooManyCapabilities).build(), AUTHOR), "invalid-input");

        var unknownNested = metadata().toBuilder().setUnknownFields(UnknownFieldSet.newBuilder()
                .addField(99, UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build()).build();
        code(() -> catalog.execute("ensure-workflow-author-registration", request().toBuilder()
                .setRegistration(unknownNested).build(), AUTHOR), "invalid-input");
        var unknownWrapper = request().toBuilder().setUnknownFields(UnknownFieldSet.newBuilder()
                .addField(99, UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build()).build();
        code(() -> catalog.execute("ensure-workflow-author-registration", unknownWrapper, AUTHOR), "invalid-input");
        assertThat(calls).hasValue(0);
    }

    @Test
    void responseMustEchoIdentityAndHaveSessionOnlyWhenAdmitted() {
        for (RegisterWorkerResponse invalid : java.util.List.of(
                admitted().getRegistration().toBuilder().setWorkerId("other").build(),
                admitted().getRegistration().toBuilder().setOk(false).build(),
                admitted().getRegistration().toBuilder().clearSessionId().build(),
                admitted().getRegistration().toBuilder().setSessionId("not-a-uuid").build(),
                admitted().getRegistration().toBuilder().setReason("inconsistent reason").build(),
                RegisterWorkerResponse.newBuilder().setOk(true).setWorkerId(WORKER).setAdmitted(false)
                        .setSessionId(SESSION).setReason("not admitted").build(),
                RegisterWorkerResponse.newBuilder().setOk(true).setWorkerId(WORKER).setAdmitted(false).build())) {
            var catalog = catalog(new Backend() {
                @Override public EnsureWorkflowAuthorRegistrationResponse ensureRegistration(
                        EnsureWorkflowAuthorRegistrationRequest request, Caller caller) {
                    return wrap(invalid);
                }
            });
            code(() -> catalog.execute("ensure-workflow-author-registration", request(), AUTHOR),
                    "invalid-upstream-response");
        }
    }

    @Test
    void deniedAdmissionMayReturnReasonButMustNotReturnSession() throws Exception {
        var denied = RegisterWorkerResponse.newBuilder().setOk(true).setWorkerId(WORKER)
                .setAdmitted(false).setReason("worker is not admitted").build();
        var response = EnsureWorkflowAuthorRegistrationResponse.newBuilder().setRegistration(denied).build();
        var catalog = catalog(new Backend() {
            @Override public EnsureWorkflowAuthorRegistrationResponse ensureRegistration(
                    EnsureWorkflowAuthorRegistrationRequest request, Caller caller) {
                return response;
            }
        });
        assertThat(catalog.execute("ensure-workflow-author-registration", request(), AUTHOR)).isEqualTo(response);
    }

    private ActionCatalog catalog(WorkflowAuthorTaskOperations operations) {
        return WorkflowAuthorTaskActions.register(ActionCatalog.defaults(context), operations);
    }

    private static EnsureWorkflowAuthorRegistrationRequest request() {
        return EnsureWorkflowAuthorRegistrationRequest.newBuilder().setRegistration(metadata()).build();
    }

    private static RegisterWorkerRequest metadata() {
        return RegisterWorkerRequest.newBuilder().setWorkerId(WORKER).setProvider("scripted")
                .setModel("authoring-worker").setModelVersion("1")
                .addCapabilities(ai.protomolt.proto.delegation.v1.WorkerCapability.newBuilder()
                        .setName("workflow-authoring")).build();
    }

    private static EnsureWorkflowAuthorRegistrationResponse admitted() {
        return EnsureWorkflowAuthorRegistrationResponse.newBuilder().setRegistration(
                RegisterWorkerResponse.newBuilder().setOk(true).setWorkerId(WORKER)
                        .setAdmitted(true).setSessionId(SESSION)).build();
    }

    private static EnsureWorkflowAuthorRegistrationResponse wrap(RegisterWorkerResponse registration) {
        return EnsureWorkflowAuthorRegistrationResponse.newBuilder().setRegistration(registration).build();
    }

    private static void assertValidBoth(ProtoValidator validator, Message message) {
        assertThat(validator.validate(message).valid()).isTrue();
        DynamicMessage dynamic = DynamicMessage.newBuilder(message.getDescriptorForType()).mergeFrom(message).build();
        assertThat(validator.validate(dynamic).valid()).isTrue();
    }

    private static void assertInvalidBoth(ProtoValidator validator, Message message) {
        assertThat(validator.validate(message).valid()).isFalse();
        DynamicMessage dynamic = DynamicMessage.newBuilder(message.getDescriptorForType()).mergeFrom(message).build();
        assertThat(validator.validate(dynamic).valid()).isFalse();
    }

    private static void code(org.assertj.core.api.ThrowableAssert.ThrowingCallable action, String expected) {
        assertThatThrownBy(action).isInstanceOfSatisfying(ActionException.class,
                failure -> assertThat(failure.code()).isEqualTo(expected));
    }

    private static class Backend implements WorkflowAuthorTaskOperations {
        @Override public EnsureWorkflowAuthorRegistrationResponse ensureRegistration(
                EnsureWorkflowAuthorRegistrationRequest request, Caller caller)
                throws WorkflowPreparationException { throw new AssertionError("Unexpected ensure-registration call"); }
        @Override public RegisterWorkerResponse register(RegisterWorkerRequest request, Caller caller)
                throws WorkflowPreparationException { throw new AssertionError("Unexpected register call"); }
        @Override public AcceptTaskResponse accept(AcceptTaskRequest request, Caller caller)
                throws WorkflowPreparationException { throw new AssertionError("Unexpected accept call"); }
        @Override public SubmitCandidateResponse submit(SubmitCandidateRequest request, Caller caller)
                throws WorkflowPreparationException { throw new AssertionError("Unexpected submit call"); }
        @Override public GetWorkflowAuthorContextResponse context(GetWorkflowAuthorContextRequest request, Caller caller)
                throws WorkflowPreparationException { throw new AssertionError("Unexpected context call"); }
        @Override public ReadWorkflowAuthorEventsResponse events(ReadWorkflowAuthorEventsRequest request, Caller caller)
                throws WorkflowPreparationException { throw new AssertionError("Unexpected events call"); }
        @Override public ReadWorkflowAuthorAssignmentsResponse assignments(
                ReadWorkflowAuthorAssignmentsRequest request, Caller caller)
                throws WorkflowPreparationException { throw new AssertionError("Unexpected assignments call"); }
    }
}
