package ai.pipestream.proto.chain;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
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
 * unresolvable comes back as a {@link ChainJson.ChainParseException} naming the step.
 */
class ChainJsonEdgeParseTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String VALIDATE = "ai/pipestream/proto/validate/v1/validate.proto";

    private static final String PROTO = """
            syntax = "proto3";
            package chain.edgejson.test;
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
                .add("chain/edgejson/test/chain.proto", PROTO, "test").build());
        String descriptorSet = Base64.getEncoder()
                .encodeToString(compiled.descriptorSet().toByteArray());
        context = ActionContext.create();
        template = (ObjectNode) MAPPER.readTree("""
                {"name": "edge-chain",
                 "schema": {"descriptorSetBase64": "%s"},
                 "inputType": "chain.edgejson.test.Batch",
                 "steps": [
                   {"name": "process", "target": "in-process",
                    "method": "chain.edgejson.test.Worker/Process",
                    "edge": {"sources": ["input"],
                             "produceType": "chain.edgejson.test.Batch",
                             "rules": ["items = input.items"]},
                    "fanOut": {"items": "items", "maxItems": 8, "maxConcurrency": 2,
                               "failurePolicy": "CONTINUE",
                               "collectType": "chain.edgejson.test.BatchResult",
                               "collectInto": "results"}}
                 ]}
                """.formatted(descriptorSet));
    }

    private static String resource(String name) {
        try (InputStream in = ChainJsonEdgeParseTest.class.getClassLoader()
                .getResourceAsStream(name)) {
            if (in == null) {
                throw new IllegalStateException(name + " not on the test classpath");
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static ObjectNode chain() {
        return template.deepCopy();
    }

    private static ObjectNode firstStep(ObjectNode chain) {
        return (ObjectNode) chain.get("steps").get(0);
    }

    private static ChainJson.ChainParseException failure(ObjectNode chain) {
        try {
            ChainDefinition parsed = ChainJson.parse(chain, context);
            throw new AssertionError("expected a parse failure, but parsed " + parsed.name());
        } catch (ChainJson.ChainParseException e) {
            return e;
        }
    }

    @Test
    void anEdgeAndFanOutStepParsesAndVerifies() throws Exception {
        ChainDefinition parsed = ChainJson.parse(chain(), context);
        ChainDefinition.Step step = parsed.steps().getFirst();
        assertThat(step.edge().sources()).containsExactly("input");
        assertThat(step.edge().produceType().getFullName())
                .isEqualTo("chain.edgejson.test.Batch");
        assertThat(step.edge().rules()).containsExactly("items = input.items");
        assertThat(step.fanOut().items()).isEqualTo("items");
        assertThat(step.fanOut().maxItems()).isEqualTo(8);
        assertThat(step.fanOut().maxConcurrency()).isEqualTo(2);
        assertThat(step.fanOut().failurePolicy())
                .isEqualTo(ChainDefinition.BranchFailurePolicy.CONTINUE);
        assertThat(step.fanOut().collectType().getFullName())
                .isEqualTo("chain.edgejson.test.BatchResult");
        assertThat(step.fanOut().collectInto()).isEqualTo("results");
        assertThat(new ChainVerifier().verify(parsed)).isEmpty();
    }

    @Test
    void anEdgeOnAStructuredStepParses() throws Exception {
        ObjectNode chain = chain();
        ObjectNode step = firstStep(chain);
        step.remove("target");
        step.remove("method");
        step.remove("fanOut");
        step.putObject("structured")
                .put("targetType", "chain.edgejson.test.Summary")
                .put("model", "structured-model");
        ((ObjectNode) step.get("edge")).put("validate", true);

        ChainDefinition parsed = ChainJson.parse(chain, context);

        assertThat(parsed.steps().getFirst().structured().targetType().getFullName())
                .isEqualTo("chain.edgejson.test.Summary");
        assertThat(parsed.steps().getFirst().edge().validate()).isTrue();
        assertThat(new ChainVerifier().verify(parsed)).isEmpty();
    }

    @Test
    void aMalformedEdgeIsAttributedToTheStep() {
        ObjectNode noProduceType = chain();
        ((ObjectNode) firstStep(noProduceType).get("edge")).remove("produceType");
        ChainJson.ChainParseException missing = failure(noProduceType);
        assertThat(missing.step).isEqualTo("process");
        assertThat(missing.getMessage())
                .contains("an edge needs 'sources' and 'produceType'");

        ObjectNode unknownType = chain();
        ((ObjectNode) firstStep(unknownType).get("edge"))
                .put("produceType", "chain.edgejson.test.Ghost");
        ChainJson.ChainParseException unknown = failure(unknownType);
        assertThat(unknown.step).isEqualTo("process");
        assertThat(unknown.getMessage()).contains("Ghost");

        ObjectNode notAnObject = chain();
        firstStep(notAnObject).put("edge", "input");
        assertThat(failure(notAnObject).getMessage()).contains("'edge' must be an object");
    }

    @Test
    void aMalformedFanOutIsAttributedToTheStep() {
        ObjectNode noPolicy = chain();
        ((ObjectNode) firstStep(noPolicy).get("fanOut")).remove("failurePolicy");
        assertThat(failure(noPolicy).getMessage())
                .contains("a fanOut needs 'items', 'collectType', 'collectInto', and "
                        + "'failurePolicy'");

        ObjectNode badPolicy = chain();
        ((ObjectNode) firstStep(badPolicy).get("fanOut")).put("failurePolicy", "retry");
        ChainJson.ChainParseException bad = failure(badPolicy);
        assertThat(bad.step).isEqualTo("process");
        assertThat(bad.getMessage()).contains("FAIL_FAST").contains("CONTINUE");

        ObjectNode fractional = chain();
        ((ObjectNode) firstStep(fractional).get("fanOut")).put("maxItems", 2.5);
        assertThat(failure(fractional).getMessage())
                .contains("fanOut.maxItems must be a 32-bit integer");
    }

    @Test
    void aFanOutWithoutAnEdgeIsRejected() {
        ObjectNode chain = chain();
        firstStep(chain).remove("edge");
        ChainJson.ChainParseException e = failure(chain);
        assertThat(e.step).isEqualTo("process");
        assertThat(e.getMessage()).contains("requires an edge");
    }

    @Test
    void aFanOutStepCompilesToTheRecipeContract() throws Exception {
        ChainDefinition parsed = ChainJson.parse(chain(), context);
        var recipe = ChainRecipeCompiler.compile(parsed);
        var step = recipe.getSteps(0);
        assertThat(step.hasEdge()).isTrue();
        assertThat(step.getEdge().getProduceType())
                .isEqualTo("chain.edgejson.test.Batch");
        assertThat(step.getEdge().getSourcesList()).containsExactly("input");
        assertThat(step.hasFanOut()).isTrue();
        assertThat(step.getFanOut().getFailurePolicy()).isEqualTo(
                ai.pipestream.proto.grpc.recipe.v1.BranchFailurePolicy
                        .BRANCH_FAILURE_POLICY_CONTINUE);
        assertThat(step.getFanOut().getCollectInto()).isEqualTo("results");
        assertThat(ai.pipestream.proto.grpc.recipe.RecipeValidation.edgeFingerprint(step))
                .matches("[0-9a-f]{64}");
    }

    @Test
    void aStructuredEdgeStepKeepsTheStructuredRestrictions() throws Exception {
        ObjectNode chain = chain();
        ObjectNode step = firstStep(chain);
        step.remove("target");
        step.remove("method");
        step.remove("fanOut");
        step.putObject("structured")
                .put("targetType", "chain.edgejson.test.Summary")
                .put("model", "structured-model");
        step.put("when", "input.items.size() > 0");

        ChainDefinition parsed = ChainJson.parse(chain, context);
        assertThat(new ChainVerifier().verify(parsed))
                .extracting(ChainVerifier.Finding::error)
                .anySatisfy(error -> assertThat(error)
                        .contains("structured steps do not support when gates"));
    }
}
