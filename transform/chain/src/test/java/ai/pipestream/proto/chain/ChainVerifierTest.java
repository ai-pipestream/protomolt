package ai.pipestream.proto.chain;

import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.google.protobuf.Descriptors.FileDescriptor;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The chain-level findings {@link ChainVerifier} raises before it ever looks at a mapping
 * rule: names that cannot become scope variables, and methods the runner cannot call.
 */
class ChainVerifierTest {

    private static final String PROTO = """
            syntax = "proto3";
            package chain.verify;
            message Text { string text = 1; }
            message Tokens { repeated int64 ids = 1; }
            service Tokenizer {
              rpc Tokenize(Text) returns (Tokens);
              rpc TokenizeStream(Text) returns (stream Tokens);
              rpc TokenizeAll(stream Text) returns (Tokens);
            }
            """;

    private static FileDescriptor file;

    @BeforeAll
    static void compile() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("chain/verify/chain.proto", PROTO, "test").build());
        file = compiled.descriptorFor("chain/verify/chain.proto").orElseThrow();
    }

    private static ChainDefinition.Step step(String name, String method) {
        return new ChainDefinition.Step(name, "in-process", false,
                ChainDefinition.resolveMethod(List.of(file), method), null,
                List.of("text = input.text"), List.of(), false, 0);
    }

    private static ChainDefinition chain(ChainDefinition.Step... steps) {
        return new ChainDefinition("verify", List.of(file),
                file.findMessageTypeByName("Text"), 10_000, List.of(steps), null);
    }

    @Test
    void aRepeatedStepNameIsReportedOnceAndStopsThatStep() {
        List<ChainVerifier.Finding> findings = new ChainVerifier().verify(chain(
                step("tokenize", "chain.verify.Tokenizer/Tokenize"),
                step("tokenize", "chain.verify.Tokenizer/Tokenize")));

        assertThat(findings).hasSize(1);
        assertThat(findings.get(0).step()).isEqualTo("tokenize");
        assertThat(findings.get(0).kind()).isEqualTo("chain");
        assertThat(findings.get(0).error()).isEqualTo("duplicate step name");
    }

    @Test
    void stepNamesThatCollideWithReservedScopeVariablesAreRejected() {
        for (String reserved : List.of("input", "target")) {
            List<ChainVerifier.Finding> findings = new ChainVerifier().verify(
                    chain(step(reserved, "chain.verify.Tokenizer/Tokenize")));
            assertThat(findings).hasSize(1);
            assertThat(findings.get(0).step()).isEqualTo(reserved);
            assertThat(findings.get(0).kind()).isEqualTo("chain");
            assertThat(findings.get(0).error())
                    .isEqualTo("step name must be an identifier other than 'input'/'target'");
        }
    }

    @Test
    void aStepNameThatIsNotAnIdentifierIsRejected() {
        List<ChainVerifier.Finding> findings = new ChainVerifier().verify(
                chain(step("tokenize-1", "chain.verify.Tokenizer/Tokenize")));

        assertThat(findings).hasSize(1);
        assertThat(findings.get(0).step()).isEqualTo("tokenize-1");
        assertThat(findings.get(0).kind()).isEqualTo("chain");
        assertThat(findings.get(0).error())
                .isEqualTo("step name must be an identifier other than 'input'/'target'");
    }

    @Test
    void streamingMethodsAreReportedAsNotUnary() {
        List<ChainVerifier.Finding> serverStreaming = new ChainVerifier().verify(
                chain(step("tokenize", "chain.verify.Tokenizer/TokenizeStream")));
        assertThat(serverStreaming).hasSize(1);
        assertThat(serverStreaming.get(0).step()).isEqualTo("tokenize");
        assertThat(serverStreaming.get(0).kind()).isEqualTo("method");
        assertThat(serverStreaming.get(0).error())
                .isEqualTo("chain.verify.Tokenizer.TokenizeStream is not unary; chains call "
                        + "unary methods (streaming is a later phase)");

        List<ChainVerifier.Finding> clientStreaming = new ChainVerifier().verify(
                chain(step("tokenize", "chain.verify.Tokenizer/TokenizeAll")));
        assertThat(clientStreaming).hasSize(1);
        assertThat(clientStreaming.get(0).kind()).isEqualTo("method");
        assertThat(clientStreaming.get(0).error())
                .startsWith("chain.verify.Tokenizer.TokenizeAll is not unary");
    }

    /**
     * A non-unary method is a finding, not a stop: the step's name still enters the scope so
     * later steps are checked against the shape they would really see.
     */
    @Test
    void aNonUnaryStepStillContributesItsOutputToTheScope() {
        List<ChainVerifier.Finding> findings = new ChainVerifier().verify(chain(
                step("tokenize", "chain.verify.Tokenizer/TokenizeStream"),
                new ChainDefinition.Step("count", "in-process", false,
                        ChainDefinition.resolveMethod(List.of(file),
                                "chain.verify.Tokenizer/Tokenize"),
                        null, List.of("text = tokenize.missing"), List.of(), false, 0)));

        assertThat(findings).hasSize(2);
        assertThat(findings.get(0).kind()).isEqualTo("method");
        assertThat(findings.get(1).step()).isEqualTo("count");
        assertThat(findings.get(1).kind()).isEqualTo("rule");
        assertThat(findings.get(1).error()).contains("missing");
    }
}
