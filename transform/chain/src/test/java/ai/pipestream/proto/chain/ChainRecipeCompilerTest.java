package ai.pipestream.proto.chain;

import ai.pipestream.proto.cel.CelMappingRule;
import ai.pipestream.proto.grpc.recipe.v1.GrpcRecipe;
import ai.pipestream.proto.grpc.recipe.v1.ServiceDependency;
import ai.pipestream.proto.grpc.recipe.v1.StepCompletion;
import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Chain to recipe compilation: the mapping is lossless (rules, gates, deadlines, validation
 * flags all carry over), deterministic (equal chains compile to equal bytes, file order does
 * not change fingerprints), and every method and type reference in the recipe resolves
 * against the chain's own descriptors.
 */
class ChainRecipeCompilerTest {

    private static final String PROTO = """
            syntax = "proto3";
            package chain.def;
            message Text { string text = 1; }
            message Tokens { repeated int64 ids = 1; }
            message Summary { string text = 1; }
            service Tokenizer { rpc Tokenize(Text) returns (Tokens); }
            service Embedder { rpc Embed(Tokens) returns (Tokens); }
            """;

    private static final String EXTRA_PROTO = """
            syntax = "proto3";
            package chain.def;
            message Extra { string note = 1; }
            """;

    private static FileDescriptor file;
    private static FileDescriptor extraFile;
    private static Descriptor text;
    private static Descriptor summary;

    @BeforeAll
    static void compile() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("chain/def/chain.proto", PROTO, "test")
                .add("chain/def/extra.proto", EXTRA_PROTO, "test").build());
        file = compiled.descriptorFor("chain/def/chain.proto").orElseThrow();
        extraFile = compiled.descriptorFor("chain/def/extra.proto").orElseThrow();
        text = file.findMessageTypeByName("Text");
        summary = file.findMessageTypeByName("Summary");
    }

    private static ChainDefinition chain(String name) {
        return new ChainDefinition(name, List.of(file), text, 45_000, List.of(
                ChainDefinition.Step.grpc("tokenize", "localhost:9090", false,
                        ChainDefinition.resolveMethod(List.of(file),
                                "chain.def.Tokenizer/Tokenize"),
                        "input.text.size() > 0", List.of("text=input.text"),
                        List.of(new CelMappingRule(null, "input.text", "text",
                                List.of("text=input.text"))),
                        true, 5_000, ""),
                ChainDefinition.Step.grpc("embed", "localhost:9090", false,
                        ChainDefinition.resolveMethod(List.of(file),
                                "chain.def.Embedder/Embed"),
                        null, List.of(), List.of(), false, 0, "external")),
                new ChainDefinition.Output(summary,
                        List.of("text=tokenize.text"), List.of()));
    }

    @Test
    void carriesEveryStepFieldIntoTheRecipe() {
        GrpcRecipe recipe = ChainRecipeCompiler.compile(chain("analyze"));

        assertThat(recipe.getName()).isEqualTo("analyze");
        assertThat(recipe.getInputType()).isEqualTo("chain.def.Text");
        assertThat(recipe.getDeadline().getSeconds()).isEqualTo(45);

        assertThat(recipe.getStepsCount()).isEqualTo(2);
        var tokenize = recipe.getSteps(0);
        assertThat(tokenize.getName()).isEqualTo("tokenize");
        assertThat(tokenize.getDependency()).isEqualTo("chain.def.Tokenizer");
        assertThat(tokenize.getMethod()).isEqualTo("chain.def.Tokenizer/Tokenize");
        assertThat(tokenize.getWhen()).isEqualTo("input.text.size() > 0");
        assertThat(tokenize.getRulesList()).containsExactly("text=input.text");
        assertThat(tokenize.getCelRulesList()).singleElement().satisfies(rule -> {
            assertThat(rule.getTarget()).isEqualTo("text");
            assertThat(rule.getSelector()).isEqualTo("input.text");
            assertThat(rule.getFallbackList()).containsExactly("text=input.text");
        });
        assertThat(tokenize.getValidateResponse()).isTrue();
        assertThat(tokenize.getDeadline().getSeconds()).isEqualTo(5);
        assertThat(tokenize.getCompletion()).isEqualTo(StepCompletion.STEP_COMPLETION_LIVE);

        var embed = recipe.getSteps(1);
        assertThat(embed.getCompletion()).isEqualTo(StepCompletion.STEP_COMPLETION_EXTERNAL);
        assertThat(embed.hasDeadline()).isFalse();
        assertThat(embed.getWhen()).isEmpty();

        assertThat(recipe.getOutput().getType()).isEqualTo("chain.def.Summary");
        assertThat(recipe.getOutput().getRulesList()).containsExactly("text=tokenize.text");
    }

    @Test
    void derivesOneDependencyPerServiceWithSanitizedEndpoint() {
        GrpcRecipe recipe = ChainRecipeCompiler.compile(chain("deps"));

        assertThat(recipe.getDependenciesList())
                .extracting(ServiceDependency::getAlias)
                .containsExactly("chain.def.Tokenizer", "chain.def.Embedder");
        ServiceDependency tokenizer = recipe.getDependencies(0);
        assertThat(tokenizer.getServiceProfile()).isEqualTo("chain.def.Tokenizer");
        // "localhost:9090" folds to a path-safe placeholder; the promoter rebinds it.
        assertThat(tokenizer.getEndpoint()).isEqualTo("localhost-9090");
        assertThat(tokenizer.getDescriptorFingerprint()).matches("[0-9a-f]{64}");
    }

    @Test
    void compilesDeterministicallyRegardlessOfFileOrder() {
        ChainDefinition forward = new ChainDefinition("stable", List.of(file, extraFile),
                text, 45_000, chain("stable").steps(), chain("stable").output());
        ChainDefinition reversed = new ChainDefinition("stable", List.of(extraFile, file),
                text, 45_000, chain("stable").steps(), chain("stable").output());

        GrpcRecipe first = ChainRecipeCompiler.compile(forward);
        GrpcRecipe second = ChainRecipeCompiler.compile(reversed);

        assertThat(second.toByteArray()).isEqualTo(first.toByteArray());
        assertThat(ChainRecipeCompiler.compile(forward).toByteArray())
                .isEqualTo(first.toByteArray());
    }

    @Test
    void everyMethodAndTypeReferenceResolvesAgainstTheChainDescriptors() {
        GrpcRecipe recipe = ChainRecipeCompiler.compile(chain("resolvable"));

        for (var step : recipe.getStepsList()) {
            assertThat(ChainDefinition.resolveMethod(List.of(file), step.getMethod()))
                    .isNotNull();
        }
        assertThat(file.findMessageTypeByName(
                recipe.getInputType().substring(recipe.getInputType().lastIndexOf('.') + 1)))
                .isNotNull();
    }

    @Test
    void rejectsIdentitiesTheRecipeContractCannotHold() {
        assertThatThrownBy(() -> ChainRecipeCompiler.compile(chain("not a safe name!")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("recipe.name");
    }

    @Test
    void missingOutputStaysAbsent() {
        ChainDefinition bare = new ChainDefinition("bare", List.of(file), text, 0,
                chain("bare").steps(), null);

        GrpcRecipe recipe = ChainRecipeCompiler.compile(bare);

        assertThat(recipe.hasOutput()).isFalse();
        // The chain defaulted its deadline to 30s; the recipe carries that budget.
        assertThat(recipe.getDeadline().getSeconds()).isEqualTo(30);
    }
}
