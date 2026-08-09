package ai.pipestream.proto.chain;

import ai.pipestream.proto.cel.CelMappingRule;
import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.MethodDescriptor;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The {@link ChainDefinition} record's own contract: constructor validation and defaults,
 * defensive copies, {@code Step.external()} semantics, and {@code resolveMethod}'s
 * qualified-name parsing across the chain's files.
 */
class ChainDefinitionTest {

    private static final String PROTO = """
            syntax = "proto3";
            package chain.def;
            message Text { string text = 1; }
            message Tokens { repeated int64 ids = 1; }
            service Tokenizer { rpc Tokenize(Text) returns (Tokens); }
            service Embedder { rpc Embed(Tokens) returns (Tokens); }
            """;

    private static FileDescriptor file;

    @BeforeAll
    static void compile() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("chain/def/chain.proto", PROTO, "test").build());
        file = compiled.descriptorFor("chain/def/chain.proto").orElseThrow();
    }

    private static MethodDescriptor tokenize() {
        return ChainDefinition.resolveMethod(List.of(file), "chain.def.Tokenizer/Tokenize");
    }

    private static ChainDefinition.Step step(String name) {
        return new ChainDefinition.Step(name, "in-process", false, tokenize(),
                null, List.of(), List.of(), false, 0, "");
    }

    @Test
    void aChainNeedsAtLeastOneStep() {
        assertThatThrownBy(() -> new ChainDefinition("empty", List.of(file),
                file.findMessageTypeByName("Text"), 10_000, List.of(), null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("A chain needs at least one step");
    }

    @Test
    void aNonPositiveDeadlineTakesTheDefaultBudget() {
        for (long deadline : List.of(0L, -1L, -30_000L)) {
            ChainDefinition chain = new ChainDefinition("d", List.of(file),
                    file.findMessageTypeByName("Text"), deadline, List.of(step("one")), null);
            assertThat(chain.deadlineMs()).isEqualTo(30_000);
        }
    }

    @Test
    void inputTypeIsRequired() {
        assertThatNullPointerException().isThrownBy(() -> new ChainDefinition("n",
                List.of(file), null, 10_000, List.of(step("one")), null))
                .withMessage("inputType");
    }

    @Test
    void filesAndStepsAreDefensivelyCopied() {
        List<FileDescriptor> files = new ArrayList<>(List.of(file));
        List<ChainDefinition.Step> steps = new ArrayList<>(List.of(step("one")));
        ChainDefinition chain = new ChainDefinition("copies", files,
                file.findMessageTypeByName("Text"), 10_000, steps, null);

        files.clear();
        steps.clear();
        assertThat(chain.files()).hasSize(1);
        assertThat(chain.steps()).hasSize(1);
        assertThatThrownBy(() -> chain.steps().add(step("two")))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> chain.files().add(file))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void aStepRequiresNameTargetAndMethod() {
        assertThatNullPointerException().isThrownBy(() -> new ChainDefinition.Step(
                null, "t", false, tokenize(), null, List.of(), List.of(), false, 0, ""))
                .withMessage("name");
        assertThatNullPointerException().isThrownBy(() -> new ChainDefinition.Step(
                "s", null, false, tokenize(), null, List.of(), List.of(), false, 0, ""))
                .withMessage("target");
        assertThatNullPointerException().isThrownBy(() -> new ChainDefinition.Step(
                "s", "t", false, null, null, List.of(), List.of(), false, 0, ""))
                .withMessage("method");
    }

    @Test
    void aNullCompletionMeansInvokeAndOnlyExternalParks() {
        ChainDefinition.Step invoke = new ChainDefinition.Step("s", "t", false, tokenize(),
                null, List.of(), List.of(), false, 0, null);
        assertThat(invoke.completion()).isEmpty();
        assertThat(invoke.external()).isFalse();

        ChainDefinition.Step external = new ChainDefinition.Step("s", "t", false, tokenize(),
                null, List.of(), List.of(), false, 0, "external");
        assertThat(external.external()).isTrue();

        // Anything else is not external; the verifier rejects it before a run.
        ChainDefinition.Step bogus = new ChainDefinition.Step("s", "t", false, tokenize(),
                null, List.of(), List.of(), false, 0, "later");
        assertThat(bogus.external()).isFalse();
    }

    @Test
    void stepRuleListsAreDefensivelyCopied() {
        List<String> rules = new ArrayList<>(List.of("text = input.text"));
        List<CelMappingRule> celRules = new ArrayList<>(
                List.of(new CelMappingRule(null, "input.text", "text")));
        ChainDefinition.Step step = new ChainDefinition.Step("s", "t", false, tokenize(),
                null, rules, celRules, false, 0, "");

        rules.clear();
        celRules.clear();
        assertThat(step.rules()).containsExactly("text = input.text");
        assertThat(step.celRules()).hasSize(1);
        assertThatThrownBy(() -> step.rules().add("x = y"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void anOutputRequiresItsTypeAndCopiesItsRules() {
        assertThatNullPointerException().isThrownBy(() ->
                new ChainDefinition.Output(null, List.of(), List.of()))
                .withMessage("type");
        // Null rule lists are rejected too (List.copyOf), not silently emptied — ChainJson
        // always passes lists.
        assertThatNullPointerException().isThrownBy(() ->
                new ChainDefinition.Output(file.findMessageTypeByName("Tokens"), null, List.of()));
        assertThatNullPointerException().isThrownBy(() ->
                new ChainDefinition.Output(file.findMessageTypeByName("Tokens"), List.of(), null));

        List<String> rules = new ArrayList<>(List.of("text = input.text"));
        ChainDefinition.Output output = new ChainDefinition.Output(
                file.findMessageTypeByName("Tokens"), rules, List.of());
        rules.clear();
        assertThat(output.rules()).containsExactly("text = input.text");
        assertThat(output.celRules()).isEmpty();
    }

    @Test
    void resolveMethodFindsMethodsAcrossServices() {
        assertThat(ChainDefinition.resolveMethod(List.of(file), "chain.def.Tokenizer/Tokenize")
                .getFullName()).isEqualTo("chain.def.Tokenizer.Tokenize");
        assertThat(ChainDefinition.resolveMethod(List.of(file), "chain.def.Embedder/Embed")
                .getFullName()).isEqualTo("chain.def.Embedder.Embed");
    }

    @Test
    void resolveMethodRejectsNamesWithoutBothHalves() {
        for (String malformed : List.of("Tokenize", "chain.def.Tokenizer/",
                "/Tokenize", "/")) {
            assertThatThrownBy(() -> ChainDefinition.resolveMethod(List.of(file), malformed))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("method must be 'package.Service/Method'; got '" + malformed + "'");
        }
    }

    @Test
    void resolveMethodReportsUnknownServicesAndMethodsAlike() {
        assertThatThrownBy(() -> ChainDefinition.resolveMethod(
                List.of(file), "chain.def.Missing/Tokenize"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("method 'chain.def.Missing/Tokenize' not found in the chain's schema");
        assertThatThrownBy(() -> ChainDefinition.resolveMethod(
                List.of(file), "chain.def.Tokenizer/Missing"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("method 'chain.def.Tokenizer/Missing' not found in the chain's schema");
    }

    @Test
    void resolveMethodSearchesEveryFileInTheChain() throws Exception {
        CompiledProtos second = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("chain/def/other.proto", """
                        syntax = "proto3";
                        package chain.other;
                        message Ping { string text = 1; }
                        service Pinger { rpc Ping(Ping) returns (Ping); }
                        """, "test").build());
        FileDescriptor other = second.descriptorFor("chain/def/other.proto").orElseThrow();

        MethodDescriptor resolved = ChainDefinition.resolveMethod(
                List.of(file, other), "chain.other.Pinger/Ping");
        assertThat(resolved.getFullName()).isEqualTo("chain.other.Pinger.Ping");
    }
}
