package ai.protomolt.proto.workflow;

import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.sources.CompiledProtos;
import ai.protomolt.proto.sources.ProtoSourceCompiler;
import ai.protomolt.proto.sources.ProtoSourceSet;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Every failure path in {@link WorkflowJson#parse}: a malformed envelope must come back as a
 * {@link WorkflowJson.WorkflowParseException} carrying the step it belongs to, because the verbs
 * turn that pair into a typed finding rather than a stack trace.
 */
class WorkflowJsonParseTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String PROTO = """
            syntax = "proto3";
            package workflow.test;
            message Text { string text = 1; }
            message Tokens { repeated int64 ids = 1; }
            message Embedding { string source_text = 1; }
            service Tokenizer { rpc Tokenize(Text) returns (Tokens); }
            """;

    private static ActionContext context;
    private static ObjectNode template;

    @BeforeAll
    static void compile() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("workflow/test/workflow.proto", PROTO, "test").build());
        String descriptorSet = Base64.getEncoder()
                .encodeToString(compiled.descriptorSet().toByteArray());
        context = ActionContext.create();
        template = (ObjectNode) MAPPER.readTree("""
                {"name": "embed-text",
                 "schema": {"descriptorSetBase64": "%s"},
                 "inputType": "workflow.test.Text",
                 "steps": [
                   {"name": "tokenize", "target": "in-process",
                    "method": "workflow.test.Tokenizer/Tokenize",
                    "rules": ["text = input.text"]}
                 ]}
                """.formatted(descriptorSet));
    }

    private static ObjectNode workflow() {
        return template.deepCopy();
    }

    private static ObjectNode firstStep(ObjectNode workflow) {
        return (ObjectNode) workflow.get("steps").get(0);
    }

    /** Parses and requires a failure, returning it so the message and step can be asserted. */
    private static WorkflowJson.WorkflowParseException failure(ObjectNode workflow) {
        try {
            CompiledWorkflow parsed = WorkflowJson.parse(workflow, context);
            throw new AssertionError("expected a parse failure, but parsed " + parsed.name());
        } catch (WorkflowJson.WorkflowParseException e) {
            return e;
        }
    }

    @Test
    void theTemplateParsesAndTakesTheDefaultWorkflowDeadline() throws Exception {
        CompiledWorkflow parsed = WorkflowJson.parse(workflow(), context);
        assertThat(parsed.name()).isEqualTo("embed-text");
        assertThat(parsed.inputType().getFullName()).isEqualTo("workflow.test.Text");
        assertThat(parsed.steps()).hasSize(1);
        assertThat(parsed.steps().get(0).method().getFullName())
                .isEqualTo("workflow.test.Tokenizer.Tokenize");
        assertThat(parsed.deadlineMs()).isEqualTo(30_000);
        assertThat(parsed.output()).isNull();
    }

    @Test
    void aStructuredStepParsesFromTheAgentFacingJsonShape() throws Exception {
        ObjectNode workflow = workflow();
        ObjectNode step = firstStep(workflow);
        step.remove("target");
        step.remove("method");
        step.remove("rules");
        step.putObject("structured")
                .put("targetType", "workflow.test.Embedding")
                .put("model", "structured-model")
                .put("maxAttempts", 2);

        CompiledWorkflow parsed = WorkflowJson.parse(workflow, context);

        assertThat(parsed.steps().getFirst().method()).isNull();
        assertThat(parsed.steps().getFirst().structured().targetType().getFullName())
                .isEqualTo("workflow.test.Embedding");
        assertThat(parsed.steps().getFirst().structured().model())
                .isEqualTo("structured-model");
        assertThat(parsed.steps().getFirst().structured().maxAttempts()).isEqualTo(2);
        assertThat(new WorkflowVerifier().verify(parsed)).isEmpty();
    }

    @Test
    void malformedOrMixedStructuredStepsAreAttributedToTheStep() {
        ObjectNode mixed = workflow();
        firstStep(mixed).putObject("structured")
                .put("targetType", "workflow.test.Embedding")
                .put("model", "structured-model");
        assertThat(failure(mixed).step).isEqualTo("tokenize");
        assertThat(failure(mixed).getMessage())
                .contains("must not declare target, method, or tls");

        ObjectNode missingModel = workflow();
        ObjectNode step = firstStep(missingModel);
        step.remove("target");
        step.remove("method");
        step.putObject("structured").put("targetType", "workflow.test.Embedding");
        assertThat(failure(missingModel).getMessage())
                .contains("needs 'targetType' and 'model'");

        ObjectNode nonIntegralAttempts = workflow();
        ObjectNode structuredStep = firstStep(nonIntegralAttempts);
        structuredStep.remove("target");
        structuredStep.remove("method");
        structuredStep.putObject("structured")
                .put("targetType", "workflow.test.Embedding")
                .put("model", "structured-model")
                .put("maxAttempts", "three");
        assertThat(failure(nonIntegralAttempts).getMessage()).contains("Not an int32 value");

        ObjectNode overflowingAttempts = workflow();
        ObjectNode overflowingStep = firstStep(overflowingAttempts);
        overflowingStep.remove("target");
        overflowingStep.remove("method");
        overflowingStep.putObject("structured")
                .put("targetType", "workflow.test.Embedding")
                .put("model", "structured-model")
                .put("maxAttempts", 4_294_967_296L);
        assertThat(failure(overflowingAttempts).getMessage()).contains("int32");
    }

    @Test
    void anUnresolvableSchemaIsAWorkflowLevelFailure() {
        ObjectNode workflow = workflow();
        workflow.putObject("schema");
        WorkflowJson.WorkflowParseException e = failure(workflow);
        assertThat(e.step).isEmpty();
        assertThat(e.getMessage()).contains("exactly one of");
    }

    @Test
    void anUnknownInputTypeIsAWorkflowLevelFailure() {
        ObjectNode workflow = workflow();
        workflow.put("inputType", "workflow.test.Missing");
        WorkflowJson.WorkflowParseException e = failure(workflow);
        assertThat(e.step).isEmpty();
        assertThat(e.getMessage()).isEqualTo("Unknown type 'workflow.test.Missing'");
    }

    @Test
    void stepsMustBeAPresentNonEmptyArray() {
        ObjectNode missing = workflow();
        missing.remove("steps");
        assertThat(failure(missing).getMessage()).isEqualTo("'steps' must be a non-empty array");
        assertThat(failure(missing).step).isEmpty();

        ObjectNode empty = workflow();
        empty.putArray("steps");
        assertThat(failure(empty).getMessage()).isEqualTo("'steps' must be a non-empty array");

        // A definition whose steps are not a list is not a workflow at all, so the read
        // refuses it before anything can be attributed to a step.
        ObjectNode notAnArray = workflow();
        notAnArray.putObject("steps");
        assertThat(failure(notAnArray).getMessage()).contains("steps");
    }

    /** A step that is not an object is not a WorkflowStep, so the read refuses it. */
    @Test
    void aStepThatIsNotAnObjectIsRejected() {
        ObjectNode workflow = workflow();
        workflow.putArray("steps").add("tokenize");
        WorkflowJson.WorkflowParseException e = failure(workflow);
        assertThat(e.step).isEmpty();
        assertThat(e.getMessage()).contains("Expect message object");
    }

    @Test
    void aStepWithoutANameIsRejectedBeforeTheStepCanBeNamed() {
        ObjectNode workflow = workflow();
        firstStep(workflow).remove("name");
        WorkflowJson.WorkflowParseException e = failure(workflow);
        // No name to attribute the finding to, so it is workflow-level.
        assertThat(e.step).isEmpty();
        assertThat(e.getMessage()).isEqualTo("each step needs a 'name'");

        ObjectNode blank = workflow();
        firstStep(blank).put("name", "   ");
        assertThat(failure(blank).getMessage()).isEqualTo("each step needs a 'name'");
    }

    @Test
    void aStepMissingTargetOrMethodIsAttributedToThatStep() {
        ObjectNode noTarget = workflow();
        firstStep(noTarget).remove("target");
        WorkflowJson.WorkflowParseException e = failure(noTarget);
        assertThat(e.step).isEqualTo("tokenize");
        assertThat(e.getMessage()).isEqualTo("a step needs 'target' and 'method'");

        ObjectNode noMethod = workflow();
        firstStep(noMethod).remove("method");
        assertThat(failure(noMethod).step).isEqualTo("tokenize");
        assertThat(failure(noMethod).getMessage()).isEqualTo("a step needs 'target' and 'method'");
    }

    @Test
    void anUnresolvableMethodIsAttributedToThatStep() {
        ObjectNode notFound = workflow();
        firstStep(notFound).put("method", "workflow.test.Tokenizer/Missing");
        WorkflowJson.WorkflowParseException e = failure(notFound);
        assertThat(e.step).isEqualTo("tokenize");
        assertThat(e.getMessage())
                .isEqualTo("method 'workflow.test.Tokenizer/Missing' not found in the workflow's schema");

        ObjectNode malformed = workflow();
        firstStep(malformed).put("method", "Tokenize");
        assertThat(failure(malformed).step).isEqualTo("tokenize");
        assertThat(failure(malformed).getMessage())
                .isEqualTo("method must be 'package.Service/Method'; got 'Tokenize'");
    }

    @Test
    void anOutputWithoutATypeIsRejected() {
        ObjectNode workflow = workflow();
        workflow.putObject("output").putArray("rules").add("source_text = input.text");
        WorkflowJson.WorkflowParseException e = failure(workflow);
        assertThat(e.step).isEmpty();
        assertThat(e.getMessage()).isEqualTo("'output' needs a 'type'");
    }

    @Test
    void anUnknownOutputTypeIsRejected() {
        ObjectNode workflow = workflow();
        workflow.putObject("output").put("type", "workflow.test.Missing");
        WorkflowJson.WorkflowParseException e = failure(workflow);
        assertThat(e.step).isEmpty();
        assertThat(e.getMessage()).isEqualTo("Unknown type 'workflow.test.Missing'");
    }

    /**
     * A CEL rule that is not an object is not a CelRule, so the read refuses the definition
     * rather than the verb attributing it to a step: there is no step to read.
     */
    @Test
    void aCelRuleThatIsNotAnObjectIsRejected() {
        ObjectNode workflow = workflow();
        firstStep(workflow).putArray("celRules").add("text = input.text");
        WorkflowJson.WorkflowParseException e = failure(workflow);
        assertThat(e.step).isEmpty();
        assertThat(e.getMessage()).contains("Expect message object");
    }

    @Test
    void aCelRuleWithoutATargetIsAttributedToItsStep() {
        ObjectNode workflow = workflow();
        ArrayNode rules = firstStep(workflow).putArray("celRules");
        rules.addObject().put("selector", "input.text");
        WorkflowJson.WorkflowParseException e = failure(workflow);
        assertThat(e.step).isEqualTo("tokenize");
        assertThat(e.getMessage()).isEqualTo("a CEL rule needs a 'target' path");
    }

    /** The output mapping's CEL rules report under the reserved step name "output". */
    @Test
    void aBadOutputCelRuleIsAttributedToTheOutputMapping() {
        ObjectNode workflow = workflow();
        ObjectNode output = workflow.putObject("output");
        output.put("type", "workflow.test.Embedding");
        output.putArray("celRules").addObject().put("selector", "input.text");
        WorkflowJson.WorkflowParseException e = failure(workflow);
        assertThat(e.step).isEqualTo("output");
        assertThat(e.getMessage()).isEqualTo("a CEL rule needs a 'target' path");
    }
}
