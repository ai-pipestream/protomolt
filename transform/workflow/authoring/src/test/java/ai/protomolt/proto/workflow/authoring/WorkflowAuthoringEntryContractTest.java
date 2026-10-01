package ai.protomolt.proto.workflow.authoring;

import ai.protomolt.proto.delegation.v1.AcceptanceCheck;
import ai.protomolt.proto.delegation.v1.DeliverableContract;
import ai.protomolt.proto.delegation.v1.TaskOffer;
import ai.protomolt.proto.delegation.v1.TaskSpec;
import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import ai.protomolt.proto.http.jsonschema.ProtoJsonSchemaGenerator;
import ai.protomolt.proto.http.openapi.ProtoOpenApiGenerator;
import ai.protomolt.proto.http.rest.ApiTokenRequirement;
import ai.protomolt.proto.http.rest.ProtoRestMethodRegistry;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.workflow.authoring.v1.GetWorkflowAuthoringTemplateRequest;
import ai.protomolt.proto.workflow.authoring.v1.GetWorkflowAuthoringTemplateResponse;
import ai.protomolt.proto.workflow.authoring.v1.StartWorkflowAuthoringRequest;
import ai.protomolt.proto.workflow.authoring.v1.StartWorkflowAuthoringResponse;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowAuthoringEntryServiceGrpc;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowAuthoringEntryServiceOuterClass;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowAuthoringTemplate;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Native message shape only; template trust, offer history, and admission are handler checks. */
class WorkflowAuthoringEntryContractTest {
    private static final String TASK_ID = "00000000-0000-4000-8000-0000000000a1";
    private static final String HASH = "a".repeat(64);
    private static final String OBJECTIVE = "Create and verify the bounded workflow deliverable.";

    @Test
    void templateRequiresAValidTaskSpecAndBoundsLease() throws Exception {
        var validator = ProtoValidator.forMessageType(WorkflowAuthoringTemplate.getDescriptor());
        WorkflowAuthoringTemplate valid = template();
        assertValidBoth(validator, valid);
        assertValidBoth(validator, valid.toBuilder().setLeaseSeconds(1).build());
        assertValidBoth(validator, valid.toBuilder().setLeaseSeconds(86_400).build());
        assertInvalidBoth(validator, WorkflowAuthoringTemplate.getDefaultInstance());
        assertInvalidBoth(validator, valid.toBuilder().clearSpec().build());
        assertInvalidBoth(validator, valid.toBuilder().setLeaseSeconds(0).build());
        assertInvalidBoth(validator, valid.toBuilder().setLeaseSeconds(86_401).build());
        assertInvalidBoth(ProtoValidator.forMessageType(TaskSpec.getDescriptor()),
                valid.getSpec().toBuilder().clearRequiredChecks().build());
        assertRule(valid.toBuilder().setSpec(valid.getSpec().toBuilder().clearContext()).build(),
                "authoring-template-contract");
        assertRule(valid.toBuilder().setSpec(valid.getSpec().toBuilder().clearRequiredChecks()).build(),
                "authoring-template-contract");
        assertRule(valid.toBuilder().setSpec(valid.getSpec().toBuilder().clearContract()).build(),
                "authoring-template-contract");

        // Empty start binding remains valid for generic and historical offers.
        assertValidBoth(ProtoValidator.forMessageType(TaskOffer.getDescriptor()), offer(1, OBJECTIVE)
                .toBuilder().clearStartBindingSha256().build());
        assertInvalidBoth(ProtoValidator.forMessageType(TaskOffer.getDescriptor()), offer(1, OBJECTIVE)
                .toBuilder().setStartBindingSha256("A".repeat(64)).build());
        assertInvalidBoth(ProtoValidator.forMessageType(TaskOffer.getDescriptor()), offer(1, OBJECTIVE)
                .toBuilder().setStartBindingSha256("short").build());
    }

    @Test
    void templateResponseRequiresLowercaseSha256ButHashAndDescriptorClosureAreRuntimeChecks() throws Exception {
        var validator = ProtoValidator.forMessageType(GetWorkflowAuthoringTemplateResponse.getDescriptor());
        GetWorkflowAuthoringTemplateResponse shaped = GetWorkflowAuthoringTemplateResponse.newBuilder()
                .setTemplate(template()).setTemplateSha256(HASH).build();
        assertValidBoth(validator, shaped);
        assertInvalidBoth(validator, GetWorkflowAuthoringTemplateResponse.getDefaultInstance());
        assertInvalidBoth(validator, shaped.toBuilder().clearTemplate().build());
        assertInvalidBoth(validator, shaped.toBuilder().clearTemplateSha256().build());
        assertInvalidBoth(validator, shaped.toBuilder().setTemplateSha256("A".repeat(64)).build());
        assertInvalidBoth(validator, shaped.toBuilder().setTemplateSha256("z".repeat(64)).build());

        // Any lowercase 64-hex value passes shape validation. The handler must hash deterministic
        // template bytes and verify descriptor closure, policy custody, and configured template state.
    }

    @Test
    void startRequestRequiresCanonicalUuidWorkerTemplateHashAndBoundedObjective() throws Exception {
        var validator = ProtoValidator.forMessageType(StartWorkflowAuthoringRequest.getDescriptor());
        StartWorkflowAuthoringRequest valid = request();
        assertValidBoth(validator, valid);
        assertValidBoth(validator, valid.toBuilder().setObjective("é".repeat(4096)).build());
        assertInvalidBoth(validator, valid.toBuilder().setObjective("").build());
        assertInvalidBoth(validator, StartWorkflowAuthoringRequest.getDefaultInstance());
        assertInvalidBoth(validator, valid.toBuilder().clearTaskId().build());
        assertInvalidBoth(validator, valid.toBuilder().setTaskId("not-a-uuid").build());
        assertInvalidBoth(validator, valid.toBuilder().setTaskId(
                "00000000-0000-4000-8000-0000000000A1").build());
        assertInvalidBoth(validator, valid.toBuilder().clearWorkerId().build());
        assertInvalidBoth(validator, valid.toBuilder().setWorkerId("bad worker").build());
        assertInvalidBoth(validator, valid.toBuilder().setWorkerId("w".repeat(129)).build());
        assertInvalidBoth(validator, valid.toBuilder().clearTemplateSha256().build());
        assertInvalidBoth(validator, valid.toBuilder().setTemplateSha256("B".repeat(64)).build());
        assertInvalidBoth(validator, valid.toBuilder().setTemplateSha256("short").build());
        assertInvalidBoth(validator, valid.toBuilder().setObjective("é".repeat(4097)).build());

        // A valid template digest shape does not establish that this is the host's current template;
        // the handler binds it to configured template bytes and atomically checks worker admission.
    }

    @Test
    void successfulStartMustReturnTheOriginalFirstOfferWithMatchingObjective() throws Exception {
        var validator = ProtoValidator.forMessageType(StartWorkflowAuthoringResponse.getDescriptor());
        StartWorkflowAuthoringRequest request = request();
        StartWorkflowAuthoringResponse valid = StartWorkflowAuthoringResponse.newBuilder()
                .setRequest(request).setOffer(offer(1, OBJECTIVE).toBuilder()
                        .setStartBindingSha256(HASH).build()).build();
        assertValidBoth(validator, valid);
        assertInvalidBoth(validator, StartWorkflowAuthoringResponse.getDefaultInstance());
        assertInvalidBoth(validator, valid.toBuilder().clearRequest().build());
        assertInvalidBoth(validator, valid.toBuilder().clearOffer().build());

        assertRule(valid.toBuilder().setOffer(offer(2, OBJECTIVE)).build(), "authoring-start-first-offer");
        assertRule(valid.toBuilder().setOffer(offer(1, OBJECTIVE).toBuilder()
                .setResumeFrom(ai.protomolt.proto.delegation.v1.CheckpointReference.newBuilder()
                        .setAttempt(1).setCheckpointSeq(1).setResumeToken("resume")).build()).build(),
                "authoring-start-first-offer");
        assertRule(valid.toBuilder().setOffer(offer(1, OBJECTIVE)).build(),
                "authoring-start-first-offer");
        assertRule(valid.toBuilder().setOffer(offer(1, "different objective")).build(),
                "authoring-start-objective");

        // The handler additionally proves this offer is the persisted original offer for the
        // task/worker/template intent and recomputes the domain-separated start binding digest;
        // native validation checks only digest shape, not equality with the original request.
    }

    @Test
    void generatedServiceDescriptorAndSchemasExposeTheContractAndRuntimeCelRules() throws Exception {
        var service = WorkflowAuthoringEntryServiceGrpc.getServiceDescriptor();
        assertThat(service.getName()).isEqualTo(
                "ai.protomolt.proto.workflow.authoring.v1.WorkflowAuthoringEntryService");
        assertThat(service.getMethods()).extracting(method -> method.getBareMethodName())
                .containsExactly("GetWorkflowAuthoringTemplate", "StartWorkflowAuthoring");
        assertThat(GetWorkflowAuthoringTemplateRequest.getDefaultInstance()).isNotNull();

        var json = new ObjectMapper();
        JsonNode templateSchema = json.valueToTree(ProtoJsonSchemaGenerator.create()
                .generateRooted(WorkflowAuthoringTemplate.getDescriptor()));
        assertThat(templateSchema.path("required").toString()).contains("spec");
        assertThat(templateSchema.path("x-protomolt-cel").toString())
                .contains("authoring-template-contract");
        JsonNode responseSchema = json.valueToTree(ProtoJsonSchemaGenerator.create()
                .generateRooted(StartWorkflowAuthoringResponse.getDescriptor()));
        assertThat(responseSchema.path("x-protomolt-cel").toString())
                .contains("authoring-start-first-offer", "authoring-start-objective");
        JsonNode nestedOfferSchema = json.valueToTree(ProtoJsonSchemaGenerator.create()
                .generateRooted(TaskOffer.getDescriptor()));
        // Resume absence is represented as runtime CEL, not an OpenAPI/JSON-schema rule.
        assertThat(nestedOfferSchema.path("x-protomolt-cel").toString()).isEmpty();

        var protobufService = WorkflowAuthoringEntryServiceOuterClass.getDescriptor()
                .findServiceByName("WorkflowAuthoringEntryService");
        var registry = new ProtoRestMethodRegistry();
        protobufService.getMethods().forEach(method -> registry.register(protobufService, method, unused -> {
            throw new AssertionError("OpenAPI generation must not invoke an implementation");
        }, ApiTokenRequirement.bearer()));
        JsonNode openApi = json.valueToTree(new ProtoOpenApiGenerator().generate(registry));
        assertThat(openApi.path("paths").has("/grpc-json/WorkflowAuthoringEntryService/StartWorkflowAuthoring"))
                .isTrue();
        assertThat(openApi.path("components").path("schemas").toString())
                .contains("authoring-template-contract", "authoring-start-first-offer", "authoring-start-objective");
    }

    private static WorkflowAuthoringTemplate template() {
        TaskSpec spec = TaskSpec.newBuilder().setObjective(OBJECTIVE)
                .addAllowedScope("transform/workflow/authoring")
                .addConstraints("Keep the workflow bounded and deterministic.")
                .addContext(ArtifactReference.newBuilder().setSha256(HASH)
                        .setMediaType("application/x-protobuf").setSizeBytes(1))
                .addRequiredChecks(AcceptanceCheck.newBuilder().setName("workflow-valid")
                        .setDescription("Compile and validate the workflow."))
                .setDeadline(Duration.newBuilder().setSeconds(300))
                .setContract(DeliverableContract.newBuilder()
                        .setDescriptorSet(ByteString.copyFromUtf8("descriptor"))
                        .setTypeName("example.v1.Workflow")).build();
        return WorkflowAuthoringTemplate.newBuilder().setSpec(spec).setLeaseSeconds(600).build();
    }

    private static StartWorkflowAuthoringRequest request() {
        return StartWorkflowAuthoringRequest.newBuilder().setTaskId(TASK_ID).setWorkerId("author-worker")
                .setTemplateSha256(HASH).setObjective(OBJECTIVE).build();
    }

    private static TaskOffer offer(int attempt, String objective) {
        return TaskOffer.newBuilder().setAttempt(attempt)
                .setSpec(template().getSpec().toBuilder().setObjective(objective))
                .setLeaseDuration(Duration.newBuilder().setSeconds(600))
                .setExpiresAt(Timestamp.newBuilder().setSeconds(1_800_000_000)).build();
    }

    private static void assertRule(com.google.protobuf.Message message, String ruleId) throws Exception {
        var validator = ProtoValidator.forMessageType(message.getDescriptorForType());
        assertThat(validator.validate(message).violations()).anySatisfy(
                violation -> assertThat(violation.ruleId()).isEqualTo(ruleId));
        DynamicMessage dynamic = DynamicMessage.parseFrom(message.getDescriptorForType(), message.toByteArray());
        assertThat(validator.validate(dynamic).violations()).anySatisfy(
                violation -> assertThat(violation.ruleId()).isEqualTo(ruleId));
    }

    private static void assertValidBoth(ProtoValidator validator, com.google.protobuf.Message message)
            throws Exception {
        assertThat(validator.validate(message).valid())
                .as("generated violations: %s", validator.validate(message).violations()).isTrue();
        DynamicMessage dynamic = DynamicMessage.parseFrom(message.getDescriptorForType(), message.toByteArray());
        assertThat(validator.validate(dynamic).valid()).isTrue();
    }

    private static void assertInvalidBoth(ProtoValidator validator, com.google.protobuf.Message message)
            throws Exception {
        assertThat(validator.validate(message).valid()).isFalse();
        DynamicMessage dynamic = DynamicMessage.parseFrom(message.getDescriptorForType(), message.toByteArray());
        assertThat(validator.validate(dynamic).valid()).isFalse();
    }
}
