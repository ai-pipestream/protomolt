package ai.pipestream.proto.chain;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Every failure path in {@link ChainJson#parse}: a malformed envelope must come back as a
 * {@link ChainJson.ChainParseException} carrying the step it belongs to, because the verbs
 * turn that pair into a typed finding rather than a stack trace.
 */
class ChainJsonParseTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String PROTO = """
            syntax = "proto3";
            package chain.test;
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
                .add("chain/test/chain.proto", PROTO, "test").build());
        String descriptorSet = Base64.getEncoder()
                .encodeToString(compiled.descriptorSet().toByteArray());
        context = ActionContext.create();
        template = (ObjectNode) MAPPER.readTree("""
                {"name": "embed-text",
                 "schema": {"descriptorSetBase64": "%s"},
                 "inputType": "chain.test.Text",
                 "steps": [
                   {"name": "tokenize", "target": "in-process",
                    "method": "chain.test.Tokenizer/Tokenize",
                    "rules": ["text = input.text"]}
                 ]}
                """.formatted(descriptorSet));
    }

    private static ObjectNode chain() {
        return template.deepCopy();
    }

    private static ObjectNode firstStep(ObjectNode chain) {
        return (ObjectNode) chain.get("steps").get(0);
    }

    /** Parses and requires a failure, returning it so the message and step can be asserted. */
    private static ChainJson.ChainParseException failure(ObjectNode chain) {
        try {
            ChainDefinition parsed = ChainJson.parse(chain, context);
            throw new AssertionError("expected a parse failure, but parsed " + parsed.name());
        } catch (ChainJson.ChainParseException e) {
            return e;
        }
    }

    @Test
    void theTemplateParsesAndTakesTheDefaultChainDeadline() throws Exception {
        ChainDefinition parsed = ChainJson.parse(chain(), context);
        assertThat(parsed.name()).isEqualTo("embed-text");
        assertThat(parsed.inputType().getFullName()).isEqualTo("chain.test.Text");
        assertThat(parsed.steps()).hasSize(1);
        assertThat(parsed.steps().get(0).method().getFullName())
                .isEqualTo("chain.test.Tokenizer.Tokenize");
        assertThat(parsed.deadlineMs()).isEqualTo(30_000);
        assertThat(parsed.output()).isNull();
    }

    @Test
    void anUnresolvableSchemaIsAChainLevelFailure() {
        ObjectNode chain = chain();
        chain.putObject("schema");
        ChainJson.ChainParseException e = failure(chain);
        assertThat(e.step).isEmpty();
        assertThat(e.getMessage()).isEqualTo("Schema must contain exactly one of 'type', "
                + "'sources', 'descriptorSetBase64' but had 0 (at '/schema')");
    }

    @Test
    void anUnknownInputTypeIsAChainLevelFailure() {
        ObjectNode chain = chain();
        chain.put("inputType", "chain.test.Missing");
        ChainJson.ChainParseException e = failure(chain);
        assertThat(e.step).isEmpty();
        assertThat(e.getMessage()).isEqualTo("Unknown type 'chain.test.Missing'");
    }

    @Test
    void stepsMustBeAPresentNonEmptyArray() {
        ObjectNode missing = chain();
        missing.remove("steps");
        assertThat(failure(missing).getMessage()).isEqualTo("'steps' must be a non-empty array");
        assertThat(failure(missing).step).isEmpty();

        ObjectNode empty = chain();
        empty.putArray("steps");
        assertThat(failure(empty).getMessage()).isEqualTo("'steps' must be a non-empty array");

        ObjectNode notAnArray = chain();
        notAnArray.putObject("steps");
        assertThat(failure(notAnArray).getMessage()).isEqualTo("'steps' must be a non-empty array");
    }

    @Test
    void aStepThatIsNotAnObjectIsRejected() {
        ObjectNode chain = chain();
        chain.putArray("steps").add("tokenize");
        ChainJson.ChainParseException e = failure(chain);
        assertThat(e.step).isEmpty();
        assertThat(e.getMessage()).isEqualTo("each step must be an object");
    }

    @Test
    void aStepWithoutANameIsRejectedBeforeTheStepCanBeNamed() {
        ObjectNode chain = chain();
        firstStep(chain).remove("name");
        ChainJson.ChainParseException e = failure(chain);
        // No name to attribute the finding to, so it is chain-level.
        assertThat(e.step).isEmpty();
        assertThat(e.getMessage()).isEqualTo("each step needs a 'name'");

        ObjectNode blank = chain();
        firstStep(blank).put("name", "   ");
        assertThat(failure(blank).getMessage()).isEqualTo("each step needs a 'name'");
    }

    @Test
    void aStepMissingTargetOrMethodIsAttributedToThatStep() {
        ObjectNode noTarget = chain();
        firstStep(noTarget).remove("target");
        ChainJson.ChainParseException e = failure(noTarget);
        assertThat(e.step).isEqualTo("tokenize");
        assertThat(e.getMessage()).isEqualTo("a step needs 'target' and 'method'");

        ObjectNode noMethod = chain();
        firstStep(noMethod).remove("method");
        assertThat(failure(noMethod).step).isEqualTo("tokenize");
        assertThat(failure(noMethod).getMessage()).isEqualTo("a step needs 'target' and 'method'");
    }

    @Test
    void anUnresolvableMethodIsAttributedToThatStep() {
        ObjectNode notFound = chain();
        firstStep(notFound).put("method", "chain.test.Tokenizer/Missing");
        ChainJson.ChainParseException e = failure(notFound);
        assertThat(e.step).isEqualTo("tokenize");
        assertThat(e.getMessage())
                .isEqualTo("method 'chain.test.Tokenizer/Missing' not found in the chain's schema");

        ObjectNode malformed = chain();
        firstStep(malformed).put("method", "Tokenize");
        assertThat(failure(malformed).step).isEqualTo("tokenize");
        assertThat(failure(malformed).getMessage())
                .isEqualTo("method must be 'package.Service/Method'; got 'Tokenize'");
    }

    @Test
    void anOutputWithoutATypeIsRejected() {
        ObjectNode chain = chain();
        chain.putObject("output").putArray("rules").add("source_text = input.text");
        ChainJson.ChainParseException e = failure(chain);
        assertThat(e.step).isEmpty();
        assertThat(e.getMessage()).isEqualTo("'output' needs a 'type'");
    }

    @Test
    void anUnknownOutputTypeIsRejected() {
        ObjectNode chain = chain();
        chain.putObject("output").put("type", "chain.test.Missing");
        ChainJson.ChainParseException e = failure(chain);
        assertThat(e.step).isEmpty();
        assertThat(e.getMessage()).isEqualTo("Unknown type 'chain.test.Missing'");
    }

    @Test
    void aCelRuleThatIsNotAnObjectIsAttributedToItsStep() {
        ObjectNode chain = chain();
        firstStep(chain).putArray("celRules").add("text = input.text");
        ChainJson.ChainParseException e = failure(chain);
        assertThat(e.step).isEqualTo("tokenize");
        assertThat(e.getMessage()).isEqualTo("each CEL rule must be an object");
    }

    @Test
    void aCelRuleWithoutATargetIsAttributedToItsStep() {
        ObjectNode chain = chain();
        ArrayNode rules = firstStep(chain).putArray("celRules");
        rules.addObject().put("selector", "input.text");
        ChainJson.ChainParseException e = failure(chain);
        assertThat(e.step).isEqualTo("tokenize");
        assertThat(e.getMessage()).isEqualTo("a CEL rule needs a 'target' path");
    }

    /** The output mapping's CEL rules report under the reserved step name "output". */
    @Test
    void aBadOutputCelRuleIsAttributedToTheOutputMapping() {
        ObjectNode chain = chain();
        ObjectNode output = chain.putObject("output");
        output.put("type", "chain.test.Embedding");
        output.putArray("celRules").addObject().put("selector", "input.text");
        ChainJson.ChainParseException e = failure(chain);
        assertThat(e.step).isEqualTo("output");
        assertThat(e.getMessage()).isEqualTo("a CEL rule needs a 'target' path");
    }
}
