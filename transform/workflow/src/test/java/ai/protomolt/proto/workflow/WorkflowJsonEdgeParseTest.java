package ai.protomolt.proto.workflow;

import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.sources.CompiledProtos;
import ai.protomolt.proto.sources.ProtoSourceCompiler;
import ai.protomolt.proto.sources.ProtoSourceSet;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The {@code edge} and {@code fanOut} step keys parse from the agent-facing JSON
 * envelope with the same attribution discipline as every other step shape: anything
 * unresolvable comes back as a {@link WorkflowJson.WorkflowParseException} naming the step.
 */
class WorkflowJsonEdgeParseTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String VALIDATE = "ai/protomolt/proto/validate/v1/validate.proto";

    private static final String PROTO = """
            syntax = "proto3";
            package workflow.edgejson.test;
            import "ai/pipestream/proto/validate/v1/validate.proto";
            message Ticket { string title = 1; }
            message Summary {
              string headline = 1 [(ai.pipestream.proto.validate.v1.field) = {
                required: true
                string: {min_len: 3, max_len: 200}
              }];
            }
            message Batch { repeated Ticket items = 1; }
            message BatchResult { repeated Ticket results = 1; }
            service Worker { rpc Process(Ticket) returns (Ticket); }
            """;

    private static ActionContext context;
    private static ObjectNode template;

    @BeforeAll
    static void compile() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add(VALIDATE, resource(VALIDATE), "test")
                .add("workflow/edgejson/test/workflow.proto", PROTO, "test").build());
        String descriptorSet = Base64.getEncoder()
                .encodeToString(compiled.descriptorSet().toByteArray());
        context = ActionContext.create();
        template = (ObjectNode) MAPPER.readTree("""
                {"name": "edge-workflow",
                 "schema": {"descriptorSetBase64": "%s"},
                 "inputType": "workflow.edgejson.test.Batch",
                 "steps": [
                   {"name": "process", "target": "in-process",
                    "method": "workflow.edgejson.test.Worker/Process",
                    "edge": {"sources": ["input"],
                             "produceType": "workflow.edgejson.test.Batch",
                             "rules": ["items = input.items"]},
                    "fanOut": {"items": "items", "maxItems": 8, "maxConcurrency": 2,
                               "failurePolicy": "CONTINUE",
                               "collectType": "workflow.edgejson.test.BatchResult",
                               "collectInto": "results"}}
                 ]}
                """.formatted(descriptorSet));
    }

    private static String resource(String name) {
        try (InputStream in = WorkflowJsonEdgeParseTest.class.getClassLoader()
                .getResourceAsStream(name)) {
            if (in == null) {
                throw new IllegalStateException(name + " not on the test classpath");
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static ObjectNode workflow() {
        return template.deepCopy();
    }

    private static ObjectNode firstStep(ObjectNode workflow) {
        return (ObjectNode) workflow.get("steps").get(0);
    }

    private static WorkflowJson.WorkflowParseException failure(ObjectNode workflow) {
        try {
            CompiledWorkflow parsed = WorkflowJson.parse(workflow, context);
            throw new AssertionError("expected a parse failure, but parsed " + parsed.name());
        } catch (WorkflowJson.WorkflowParseException e) {
            return e;
        }
    }

    @Test
    void anEdgeAndFanOutStepParsesAndVerifies() throws Exception {
        CompiledWorkflow parsed = WorkflowJson.parse(workflow(), context);
        CompiledWorkflow.Step step = parsed.steps().getFirst();
        assertThat(step.edge().sources()).containsExactly("input");
        assertThat(step.edge().produceType().getFullName())
                .isEqualTo("workflow.edgejson.test.Batch");
        assertThat(step.edge().rules()).containsExactly("items = input.items");
        assertThat(step.fanOut().items()).isEqualTo("items");
        assertThat(step.fanOut().maxItems()).isEqualTo(8);
        assertThat(step.fanOut().maxConcurrency()).isEqualTo(2);
        assertThat(step.fanOut().failurePolicy())
                .isEqualTo(CompiledWorkflow.BranchFailurePolicy.CONTINUE);
        assertThat(step.fanOut().collectType().getFullName())
                .isEqualTo("workflow.edgejson.test.BatchResult");
        assertThat(step.fanOut().collectInto()).isEqualTo("results");
        assertThat(new WorkflowVerifier().verify(parsed)).isEmpty();
    }

    @Test
    void anEdgeOnAStructuredStepParses() throws Exception {
        ObjectNode workflow = workflow();
        ObjectNode step = firstStep(workflow);
        step.remove("target");
        step.remove("method");
        step.remove("fanOut");
        step.putObject("structured")
                .put("targetType", "workflow.edgejson.test.Summary")
                .put("model", "structured-model");
        ((ObjectNode) step.get("edge")).put("validate", true);

        CompiledWorkflow parsed = WorkflowJson.parse(workflow, context);

        assertThat(parsed.steps().getFirst().structured().targetType().getFullName())
                .isEqualTo("workflow.edgejson.test.Summary");
        assertThat(parsed.steps().getFirst().edge().validate()).isTrue();
        assertThat(new WorkflowVerifier().verify(parsed)).isEmpty();
    }

    @Test
    void aMalformedEdgeIsAttributedToTheStep() {
        ObjectNode noProduceType = workflow();
        ((ObjectNode) firstStep(noProduceType).get("edge")).remove("produceType");
        WorkflowJson.WorkflowParseException missing = failure(noProduceType);
        assertThat(missing.step).isEqualTo("process");
        assertThat(missing.getMessage())
                .contains("an edge needs 'sources' and 'produceType'");

        ObjectNode unknownType = workflow();
        ((ObjectNode) firstStep(unknownType).get("edge"))
                .put("produceType", "workflow.edgejson.test.Ghost");
        WorkflowJson.WorkflowParseException unknown = failure(unknownType);
        assertThat(unknown.step).isEqualTo("process");
        assertThat(unknown.getMessage()).contains("Ghost");

        // An edge that is not an object is not a WorkflowEdge, so the read refuses the
        // definition rather than the parser attributing it to a step.
        ObjectNode notAnObject = workflow();
        firstStep(notAnObject).put("edge", "input");
        assertThat(failure(notAnObject).getMessage()).contains("Expect message object");
    }

    @Test
    void aMalformedFanOutIsAttributedToTheStep() {
        ObjectNode noPolicy = workflow();
        ((ObjectNode) firstStep(noPolicy).get("fanOut")).remove("failurePolicy");
        assertThat(failure(noPolicy).getMessage())
                .contains("a fanOut needs 'items', 'collectType', 'collectInto', and "
                        + "'failurePolicy'");

        ObjectNode badPolicy = workflow();
        ((ObjectNode) firstStep(badPolicy).get("fanOut")).put("failurePolicy", "retry");
        WorkflowJson.WorkflowParseException bad = failure(badPolicy);
        assertThat(bad.step).isEqualTo("process");
        assertThat(bad.getMessage()).contains("FAIL_FAST").contains("CONTINUE");

        // The field is an int32, so a fractional value is not one and the read says so.
        ObjectNode fractional = workflow();
        ((ObjectNode) firstStep(fractional).get("fanOut")).put("maxItems", 2.5);
        assertThat(failure(fractional).getMessage()).contains("Not an int32 value");
    }

    @Test
    void aFanOutWithoutAnEdgeIsRejected() {
        ObjectNode workflow = workflow();
        firstStep(workflow).remove("edge");
        WorkflowJson.WorkflowParseException e = failure(workflow);
        assertThat(e.step).isEqualTo("process");
        assertThat(e.getMessage()).contains("requires an edge");
    }

    @Test
    void aFanOutStepCompilesToTheWorkflowContract() throws Exception {
        CompiledWorkflow parsed = WorkflowJson.parse(workflow(), context);
        var workflow = WorkflowCompiler.compile(parsed);
        var step = workflow.getSteps(0);
        assertThat(step.hasEdge()).isTrue();
        assertThat(step.getEdge().getProduceType())
                .isEqualTo("workflow.edgejson.test.Batch");
        assertThat(step.getEdge().getSourcesList()).containsExactly("input");
        assertThat(step.hasFanOut()).isTrue();
        assertThat(step.getFanOut().getFailurePolicy()).isEqualTo(
                ai.protomolt.proto.grpc.workflow.v1.BranchFailurePolicy
                        .BRANCH_FAILURE_POLICY_CONTINUE);
        assertThat(step.getFanOut().getCollectInto()).isEqualTo("results");
        assertThat(ai.protomolt.proto.grpc.workflow.WorkflowValidation.edgeFingerprint(step))
                .matches("[0-9a-f]{64}");
    }

    @Test
    void aStructuredEdgeStepKeepsTheStructuredRestrictions() throws Exception {
        ObjectNode workflow = workflow();
        ObjectNode step = firstStep(workflow);
        step.remove("target");
        step.remove("method");
        step.remove("fanOut");
        step.putObject("structured")
                .put("targetType", "workflow.edgejson.test.Summary")
                .put("model", "structured-model");
        step.put("when", "input.items.size() > 0");

        CompiledWorkflow parsed = WorkflowJson.parse(workflow, context);
        assertThat(new WorkflowVerifier().verify(parsed))
                .extracting(WorkflowVerifier.Finding::error)
                .anySatisfy(error -> assertThat(error)
                        .contains("structured steps do not support when gates"));
    }
}
