package ai.protomolt.proto.samples;

import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.delegation.DeliverableContracts;
import ai.protomolt.proto.delegation.v1.CheckEvidence;
import ai.protomolt.proto.delegation.v1.CheckVerdict;
import ai.protomolt.proto.delegation.v1.DeliverableContract;
import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.grpc.service.contract.ProtoMoltServiceSchema;
import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import ai.protomolt.proto.projection.MessageProjection;
import ai.protomolt.proto.correction.v1.ContactGrounding;
import ai.protomolt.proto.correction.v1.CorrectedContact;
import ai.protomolt.proto.correction.v1.RawContact;
import ai.protomolt.proto.samples.starter.v1.WorkflowDeliverable;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.workflow.CompiledWorkflow;
import ai.protomolt.proto.workflow.WorkflowCompiler;
import ai.protomolt.proto.workflow.WorkflowJson;
import ai.protomolt.proto.workflow.WorkflowVerifier;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.ByteString;
import com.google.protobuf.Any;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.JsonFormat;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Executable examples of caller contracts; no model or new host handlers. */
class StarterContractTest {
    private static final String SHA = "a".repeat(64);

    private static String resource(String path) throws IOException {
        try (var stream = StarterContractTest.class.getResourceAsStream("/starter/" + path)) {
            if (stream == null) throw new IOException("Missing fixture " + path);
            return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static void valid(Message message) {
        var result = ProtoValidator.forMessageType(message.getDescriptorForType()).validate(message);
        assertThat(result.violations()).isEmpty();
    }

    private static ActionContext context() {
        var registry = DescriptorRegistry.create(false);
        RawContact.getDescriptor().getFile().getMessageTypes().forEach(registry::register);
        return ActionContext.builder().registry(registry).build();
    }

    private static CompiledWorkflow workflow() throws Exception {
        var context = context();
        var fixture = (ObjectNode) context.objectMapper().readTree(resource("correct-contact.workflow.json"));
        return WorkflowJson.parse(fixture, context);
    }

    @Test
    void namedWorkflowDefinitionUsesTheExistingStructuredStepAndCheckedProjection() throws Exception {
        var workflow = workflow();
        assertThat(new WorkflowVerifier().verify(workflow)).isEmpty();
        var compiled = WorkflowCompiler.compile(workflow);
        valid(compiled);
        assertThat(compiled.getName()).isEqualTo("correct-contact");
        assertThat(compiled.getStepsCount()).isEqualTo(1);
        var step = compiled.getSteps(0);
        assertThat(step.getStructured().getTargetType()).isEqualTo(CorrectedContact.getDescriptor().getFullName());
        assertThat(step.getStructured().getMaxAttempts()).isEqualTo(3);
        assertThat(step.getEdge().getValidate()).isTrue();
        assertThat(step.getEdge().getProjectTo()).isEqualTo(ContactGrounding.getDescriptor().getFullName());
        assertThat(step.getMethod()).isEmpty();
    }

    @Test
    void permissiveInputProjectsOnlyModelVisibleFields() throws Exception {
        var raw = RawContact.newBuilder();
        JsonFormat.parser().merge(resource("raw-contact.json"), raw);
        valid(raw.build());
        var projection = MessageProjection.forTarget(ContactGrounding.getDescriptor(), context().registry()).orElseThrow();
        var grounding = projection.project(raw.build());
        valid(grounding);
        assertThat(grounding.getField(grounding.getDescriptorForType().findFieldByName("record_id")))
                .isEqualTo("contact-1");
        String json = JsonFormat.printer().print(grounding);
        assertThat(json).contains("ada [at] example.org").doesNotContain("source-account-private", "internalNotes");
    }

    @Test
    void resultRulesRejectMalformedAddressAndRequireExplicitReviewForMissingAddress() throws Exception {
        var corrected = CorrectedContact.newBuilder();
        JsonFormat.parser().merge(resource("corrected-contact.json"), corrected);
        valid(corrected.build());
        var invalid = CorrectedContact.newBuilder();
        JsonFormat.parser().merge(resource("invalid-contact.json"), invalid);
        var validator = ProtoValidator.forMessageType(CorrectedContact.getDescriptor());
        assertThat(validator.validate(invalid.build()).violations())
                .anyMatch(v -> v.path().equals("email") && v.ruleId().equals("string.email"));
        assertThat(validator.validate(corrected.clone().clearEmail().build()).violations())
                .anyMatch(v -> v.ruleId().equals("deliverable-contact-method"));
        valid(corrected.clone().clearEmail().setNeedsReview(true).build());
        // A syntactically valid invented address still needs evidence/policy review.
        valid(corrected.clone().setEmail("invented@example.net").build());
    }

    @Test
    void schemaSourceRequiresExactlyOneResolutionFormAndNamedExecutionKeepsExistingEnvelope() throws Exception {
        var file = ProtoMoltServiceSchema.file();
        var schema = file.findMessageTypeByName("SchemaSource");
        var validator = ProtoValidator.forMessageType(schema);
        assertThat(validator.validate(DynamicMessage.getDefaultInstance(schema)).violations())
                .anyMatch(v -> v.ruleId().equals("schema_source.exactly_one"));
        var registered = DynamicMessage.newBuilder(schema)
                .setField(schema.findFieldByName("type"), RawContact.getDescriptor().getFullName());
        valid(registered.build());
        assertThat(validator.validate(registered.clone()
                .setField(schema.findFieldByName("descriptor_set_base64"), "AQ==").build()).violations())
                .anyMatch(v -> v.ruleId().equals("schema_source.exactly_one"));
        var request = file.findMessageTypeByName("RunWorkflowRequest");
        var message = DynamicMessage.newBuilder(request);
        JsonFormat.parser().merge("{\"workflowName\":\"correct-contact\",\"input\":{\"recordId\":\"contact-1\",\"contactText\":\"Ada\"}}", message);
        valid(message.build());
    }

    private static ArtifactReference artifact() {
        return ArtifactReference.newBuilder().setSha256(SHA).setMediaType("application/x-protobuf")
                .setSizeBytes(42).build();
    }

    private static WorkflowDeliverable deliverable() throws Exception {
        return WorkflowDeliverable.newBuilder().setWorkflow(WorkflowCompiler.compile(workflow()))
                .setWorkflowArtifact(artifact()).setDescriptors(artifact()).addFixtures(artifact())
                .addChecks(CheckEvidence.newBuilder().setCheckName("contract-tests")
                        .setVerdict(CheckVerdict.CHECK_VERDICT_PASSED)
                        .setRanAt(Timestamp.newBuilder().setSeconds(1700000000)).addArtifacts(artifact()))
                .setRunId("contract-fixture-run").setReceipt(artifact()).build();
    }

    @Test
    void workflowDeliverableReusesEvidenceAndRejectsMissingFailedOrDuplicateChecks() throws Exception {
        var deliverable = deliverable();
        valid(deliverable);
        var validator = ProtoValidator.forMessageType(WorkflowDeliverable.getDescriptor());
        assertThat(validator.validate(deliverable.toBuilder().clearChecks().build()).violations())
                .anyMatch(v -> v.path().equals("checks") && v.ruleId().equals("repeated.min_items"));
        assertThat(validator.validate(deliverable.toBuilder().setChecks(0, deliverable.getChecks(0).toBuilder()
                .setVerdict(CheckVerdict.CHECK_VERDICT_FAILED)).build()).violations())
                .anyMatch(v -> v.ruleId().equals("deliverable-checks-passed"));
        assertThat(validator.validate(deliverable.toBuilder().addChecks(deliverable.getChecks(0)).build()).violations())
                .anyMatch(v -> v.ruleId().equals("deliverable-unique-checks"));
        assertThat(validator.validate(deliverable.toBuilder().clearReceipt().build()).violations())
                .anyMatch(v -> v.path().equals("receipt") && v.ruleId().equals("required"));
        assertThat(validator.validate(deliverable.toBuilder().setWorkflow(deliverable.getWorkflow().toBuilder().clearSteps()).build()).violations())
                .anyMatch(v -> v.ruleId().equals("deliverable-has-steps"));
    }

    @Test
    void deliverableDescriptorSetIncludesEveryImportAndFitsTheExistingTaskContract() throws Exception {
        Map<String, FileDescriptor> closure = new LinkedHashMap<>();
        collect(WorkflowDeliverable.getDescriptor().getFile(), closure);
        var set = FileDescriptorSet.newBuilder();
        closure.values().forEach(file -> set.addFile(file.toProto()));
        ByteString bytes = set.build().toByteString();
        var contract = DeliverableContract.newBuilder().setDescriptorSet(bytes)
                .setTypeName(WorkflowDeliverable.getDescriptor().getFullName()).build();
        valid(contract);
        assertThat(DeliverableContracts.compile(contract).descriptor().getFullName())
                .isEqualTo(WorkflowDeliverable.getDescriptor().getFullName());
        assertThat(DeliverableContracts.check(contract, Any.pack(deliverable()))).isEmpty();
        assertThat(DeliverableContracts.check(contract, Any.pack(deliverable().toBuilder().clearChecks().build())))
                .anyMatch(message -> message.contains("result.checks"));
        var incomplete = FileDescriptorSet.newBuilder().addFile(WorkflowDeliverable.getDescriptor().getFile().toProto());
        assertThatThrownBy(() -> DeliverableContracts.compile(contract.toBuilder()
                .setDescriptorSet(incomplete.build().toByteString()).build()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(bytes.size()).isLessThan(1024 * 1024);
        assertThat(closure.values()).allSatisfy(file ->
                assertThat(closure.keySet()).containsAll(file.toProto().getDependencyList()));
    }

    private static void collect(FileDescriptor file, Map<String, FileDescriptor> files) {
        if (files.containsKey(file.getName())) return;
        file.getDependencies().forEach(dependency -> collect(dependency, files));
        files.put(file.getName(), file);
    }
}
