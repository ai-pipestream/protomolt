package ai.protomolt.proto.workflow.authoring;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.delegation.contract.DeliverableContracts;
import ai.protomolt.proto.delegation.v1.AcceptanceCheck;
import ai.protomolt.proto.delegation.v1.DeliverableContract;
import ai.protomolt.proto.delegation.v1.TaskOffer;
import ai.protomolt.proto.delegation.v1.TaskSpec;
import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringDeliverable;
import ai.protomolt.proto.workflow.authoring.v1.GetWorkflowAuthoringTemplateRequest;
import ai.protomolt.proto.workflow.authoring.v1.GetWorkflowAuthoringTemplateResponse;
import ai.protomolt.proto.workflow.authoring.v1.StartWorkflowAuthoringRequest;
import ai.protomolt.proto.workflow.authoring.v1.StartWorkflowAuthoringResponse;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowAuthoringTemplate;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.ByteString;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Duration;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import com.google.protobuf.UnknownFieldSet;
import io.grpc.Status;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkflowAuthoringEntryActionsTest {
    private static final String GET = "get-workflow-authoring-template";
    private static final String START = "start-workflow-authoring";
    private static final String TASK_ID = "00000000-0000-4000-8000-0000000000a1";
    private static final String HASH = "a".repeat(64);
    private static final String OBJECTIVE = "Create and verify the bounded workflow deliverable.";
    private static final Caller COORDINATOR = Caller.scoped("coordinator", Set.of(Scopes.WORKER_COORDINATE));
    private final ActionContext context = ActionContext.create();

    @Test
    void workerCoordinateCanUseGeneratedDynamicAndJsonActions() throws Exception {
        var operations = new FakeOperations();
        var catalog = catalog(operations);
        operations.template = template();
        operations.startResponse = startResponse(startRequest());

        assertThat(catalog.get(GET).requiredScope()).isEqualTo(Scopes.WORKER_COORDINATE);
        assertThat(catalog.get(START).requiredScope()).isEqualTo(Scopes.WORKER_COORDINATE);
        assertThat(catalog.execute(GET, GetWorkflowAuthoringTemplateRequest.getDefaultInstance(), COORDINATOR))
                .isEqualTo(templateResponse(operations.template));
        assertThat(catalog.get(START).requestType()).isEqualTo(StartWorkflowAuthoringRequest.getDescriptor());
        assertThat(catalog.execute(START, startRequest(), COORDINATOR)).isEqualTo(operations.startResponse);

        var getDynamic = DynamicMessage.parseFrom(GetWorkflowAuthoringTemplateRequest.getDescriptor(), new byte[0]);
        assertThat(catalog.execute(GET, getDynamic, COORDINATOR).toByteArray())
                .isEqualTo(templateResponse(operations.template).toByteArray());
        ObjectNode getJson = (ObjectNode) context.objectMapper().readTree("{}");
        ObjectNode getJsonResponse = catalog.execute(GET, getJson, COORDINATOR);
        assertThat(getJsonResponse.path("templateSha256").asText())
                .isEqualTo(WorkflowAuthoringStartBinding.templateSha256(operations.template));

        DynamicMessage dynamic = DynamicMessage.parseFrom(startRequest().getDescriptorForType(),
                startRequest().toByteArray());
        assertThat(catalog.execute(START, dynamic, COORDINATOR).toByteArray())
                .isEqualTo(operations.startResponse.toByteArray());

        ObjectNode jsonRequest = (ObjectNode) context.objectMapper().readTree(
                com.google.protobuf.util.JsonFormat.printer().print(startRequest()));
        ObjectNode jsonResponse = catalog.execute(START, jsonRequest, COORDINATOR);
        assertThat(jsonResponse.path("offer").path("startBindingSha256").asText()).isEqualTo(operations.startResponse.getOffer().getStartBindingSha256());
        assertThat(operations.templateCalls).hasValue(3);
        assertThat(operations.startCalls).hasValue(3);
    }

    @Test
    void unrelatedScopesAreDeniedBeforeBackendInvocation() {
        var operations = new FakeOperations();
        var catalog = catalog(operations);
        for (Caller caller : Set.of(
                Caller.scoped("default", Set.of()),
                Caller.scoped("author", Set.of(Scopes.WORKFLOW_AUTHOR)),
                Caller.scoped("runner", Set.of(Scopes.WORKFLOW_RUN)))) {
            assertCode("permission-denied", () -> catalog.execute(START,
                    StartWorkflowAuthoringRequest.getDefaultInstance(), caller));
            assertCode("permission-denied", () -> catalog.execute(GET,
                    GetWorkflowAuthoringTemplateRequest.getDefaultInstance(), caller));
        }
        assertThat(operations.templateCalls).hasValue(0);
        assertThat(operations.startCalls).hasValue(0);
    }

    @Test
    void malformedAndUnknownRequestsAreRejectedBeforeBackend() {
        var operations = new FakeOperations();
        var catalog = catalog(operations);
        assertCode("invalid-input", () -> catalog.execute(START,
                StartWorkflowAuthoringRequest.getDefaultInstance(), COORDINATOR));
        assertCode("invalid-input", () -> catalog.execute(START,
                startRequest().toBuilder().setUnknownFields(unknown()).build(), COORDINATOR));
        assertCode("invalid-input", () -> catalog.execute(START,
                startRequest().toBuilder().setTaskId(TASK_ID.toUpperCase(java.util.Locale.ROOT)).build(), COORDINATOR));
        assertThat(operations.templateCalls).hasValue(0);
        assertThat(operations.startCalls).hasValue(0);
    }

    @Test
    void successfulUpstreamResponsesMustBeValidAndMatchTemplateAndStartBinding() {
        var operations = new FakeOperations();
        var catalog = catalog(operations);
        operations.template = template();
        operations.templateResponse = null;
        assertCode("invalid-upstream-response", () -> catalog.execute(GET,
                GetWorkflowAuthoringTemplateRequest.getDefaultInstance(), COORDINATOR));
        operations.templateResponse = GetWorkflowAuthoringTemplateResponse.getDefaultInstance();
        assertCode("invalid-upstream-response", () -> catalog.execute(GET,
                GetWorkflowAuthoringTemplateRequest.getDefaultInstance(), COORDINATOR));
        operations.templateResponse = templateResponse(operations.template)
                .toBuilder().setTemplateSha256("b".repeat(64)).build();
        assertCode("invalid-upstream-response", () -> catalog.execute(GET,
                GetWorkflowAuthoringTemplateRequest.getDefaultInstance(), COORDINATOR));
        WorkflowAuthoringTemplate forgedSchema = operations.template.toBuilder()
                .setSpec(operations.template.getSpec().toBuilder()
                        .setContract(operations.template.getSpec().getContract().toBuilder()
                                .setJsonSchema("{\"forged\":true}"))).build();
        operations.templateResponse = templateResponse(forgedSchema);
        assertCode("invalid-upstream-response", () -> catalog.execute(GET,
                GetWorkflowAuthoringTemplateRequest.getDefaultInstance(), COORDINATOR));
        WorkflowAuthoringTemplate malformedClosure = operations.template.toBuilder()
                .setSpec(operations.template.getSpec().toBuilder()
                        .setContract(operations.template.getSpec().getContract().toBuilder()
                                .setDescriptorSet(ByteString.copyFromUtf8("not a descriptor set")))).build();
        operations.templateResponse = templateResponse(malformedClosure);
        assertCode("invalid-upstream-response", () -> catalog.execute(GET,
                GetWorkflowAuthoringTemplateRequest.getDefaultInstance(), COORDINATOR));

        operations.startResponse = null;
        assertCode("invalid-upstream-response", () -> catalog.execute(START, startRequest(), COORDINATOR));
        operations.startResponse = StartWorkflowAuthoringResponse.getDefaultInstance();
        assertCode("invalid-upstream-response", () -> catalog.execute(START, startRequest(), COORDINATOR));
        operations.startResponse = startResponse(startRequest()).toBuilder()
                .setRequest(startRequest().toBuilder().setWorkerId("different-worker")).build();
        assertCode("invalid-upstream-response", () -> catalog.execute(START, startRequest(), COORDINATOR));
        operations.startResponse = startResponse(startRequest()).toBuilder()
                .setOffer(offer(template().getSpec(), HASH).toBuilder()
                        .setStartBindingSha256("c".repeat(64))).build();
        assertCode("invalid-upstream-response", () -> catalog.execute(START, startRequest(), COORDINATOR));
        operations.startResponse = startResponse(startRequest()).toBuilder().setUnknownFields(unknown()).build();
        assertCode("invalid-upstream-response", () -> catalog.execute(START, startRequest(), COORDINATOR));
    }

    @Test
    void typedFailuresMapToStableCodesWithoutLeakingBackendDetails() {
        var operations = new FakeOperations();
        var catalog = catalog(operations);
        for (var mapping : java.util.Map.of(
                WorkflowAuthoringEntryException.Kind.INVALID_INPUT, "invalid-input",
                WorkflowAuthoringEntryException.Kind.CONFLICT, "workflow-authoring-conflict",
                WorkflowAuthoringEntryException.Kind.INACTIVE, "workflow-authoring-rejected",
                WorkflowAuthoringEntryException.Kind.CORRUPT_EVIDENCE, "invalid-upstream-response",
                WorkflowAuthoringEntryException.Kind.UNAVAILABLE, "workflow-authoring-unavailable",
                WorkflowAuthoringEntryException.Kind.DEADLINE, "workflow-authoring-deadline").entrySet()) {
            operations.failure = new WorkflowAuthoringEntryException(mapping.getKey(), "private backend details",
                    new IOException("secret credential"));
            assertCode(mapping.getValue(), () -> catalog.execute(START, startRequest(), COORDINATOR));
        }
        operations.failure = null;
        operations.runtimeFailure = Status.UNAVAILABLE.asRuntimeException();
        assertCode("internal-error", () -> catalog.execute(START, startRequest(), COORDINATOR));
    }

    private ActionCatalog catalog(FakeOperations operations) {
        return WorkflowAuthoringEntryActions.register(ActionCatalog.defaults(context), operations);
    }

    private static WorkflowAuthoringTemplate template() {
        TaskSpec.Builder spec = TaskSpec.newBuilder().setObjective(OBJECTIVE)
                .addAllowedScope("transform/workflow/authoring")
                .addConstraints("No unbounded external writes.")
                .addContext(ArtifactReference.newBuilder().setSha256(HASH)
                        .setMediaType("application/x-protobuf").setSizeBytes(1))
                .setContract(DeliverableContract.newBuilder()
                        .setDescriptorSet(descriptorClosure())
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

    private static StartWorkflowAuthoringRequest startRequest() {
        return StartWorkflowAuthoringRequest.newBuilder().setTaskId(TASK_ID).setWorkerId("author-worker")
                .setTemplateSha256(WorkflowAuthoringStartBinding.templateSha256(template()))
                .setObjective(OBJECTIVE).build();
    }

    private static StartWorkflowAuthoringResponse startResponse(StartWorkflowAuthoringRequest request) {
        TaskSpec spec = template().getSpec();
        Duration lease = Duration.newBuilder().setSeconds(600).build();
        String binding = WorkflowAuthoringStartBinding.sha256(request, spec, lease);
        return StartWorkflowAuthoringResponse.newBuilder().setRequest(request)
                .setOffer(offer(spec, binding)).build();
    }

    private static TaskOffer offer(TaskSpec spec, String binding) {
        return TaskOffer.newBuilder().setAttempt(1).setSpec(spec)
                .setLeaseDuration(Duration.newBuilder().setSeconds(600))
                .setExpiresAt(Timestamp.newBuilder().setSeconds(1_800_000_000))
                .setStartBindingSha256(binding).build();
    }

    private static UnknownFieldSet unknown() {
        return UnknownFieldSet.newBuilder().addField(99,
                UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build();
    }

    private static void assertCode(String code, org.assertj.core.api.ThrowableAssert.ThrowingCallable call) {
        assertThatThrownBy(call).isInstanceOfSatisfying(ActionException.class, failure -> {
            assertThat(failure.code()).isEqualTo(code);
            assertThat(failure.getMessage()).doesNotContain("private backend details", "secret credential");
        });
    }

    private static final class FakeOperations implements WorkflowAuthoringEntryOperations {
        private final AtomicInteger templateCalls = new AtomicInteger();
        private final AtomicInteger startCalls = new AtomicInteger();
        private WorkflowAuthoringTemplate template = WorkflowAuthoringEntryActionsTest.template();
        private GetWorkflowAuthoringTemplateResponse templateResponse = templateResponse(template);
        private StartWorkflowAuthoringResponse startResponse = startResponse(startRequest());
        private WorkflowAuthoringEntryException failure;
        private RuntimeException runtimeFailure;

        @Override public GetWorkflowAuthoringTemplateResponse template(GetWorkflowAuthoringTemplateRequest request)
                throws WorkflowAuthoringEntryException {
            templateCalls.incrementAndGet();
            if (failure != null) throw failure;
            if (runtimeFailure != null) throw runtimeFailure;
            return templateResponse;
        }

        @Override public StartWorkflowAuthoringResponse start(StartWorkflowAuthoringRequest request)
                throws WorkflowAuthoringEntryException {
            startCalls.incrementAndGet();
            if (failure != null) throw failure;
            if (runtimeFailure != null) throw runtimeFailure;
            return startResponse;
        }
    }
}
