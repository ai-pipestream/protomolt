package ai.protomolt.proto.workflow;

import ai.protomolt.proto.sources.CompiledProtos;
import ai.protomolt.proto.sources.ProtoSourceCompiler;
import ai.protomolt.proto.sources.ProtoSourceSet;
import com.google.protobuf.Descriptors.FileDescriptor;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The workflow-level findings {@link WorkflowVerifier} raises before it ever looks at a mapping
 * rule: names that cannot become scope variables, and methods the runner cannot call.
 */
class WorkflowVerifierTest {

    private static final String PROTO = """
            syntax = "proto3";
            package workflow.verify;
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
                .add("workflow/verify/workflow.proto", PROTO, "test").build());
        file = compiled.descriptorFor("workflow/verify/workflow.proto").orElseThrow();
    }

    private static CompiledWorkflow.Step step(String name, String method) {
        return CompiledWorkflow.Step.grpc(name, "in-process", false,
                CompiledWorkflow.resolveMethod(List.of(file), method), null,
                List.of("text = input.text"), List.of(), false, 0, "");
    }

    private static CompiledWorkflow workflow(CompiledWorkflow.Step... steps) {
        return new CompiledWorkflow("verify", List.of(file),
                file.findMessageTypeByName("Text"), 10_000, List.of(steps), null);
    }

    @Test
    void aRepeatedStepNameIsReportedOnceAndStopsThatStep() {
        List<WorkflowVerifier.Finding> findings = new WorkflowVerifier().verify(workflow(
                step("tokenize", "workflow.verify.Tokenizer/Tokenize"),
                step("tokenize", "workflow.verify.Tokenizer/Tokenize")));

        assertThat(findings).hasSize(1);
        assertThat(findings.get(0).step()).isEqualTo("tokenize");
        assertThat(findings.get(0).kind()).isEqualTo("workflow");
        assertThat(findings.get(0).error()).isEqualTo("duplicate step name");
    }

    @Test
    void stepNamesThatCollideWithReservedScopeVariablesAreRejected() {
        for (String reserved : List.of("input", "target")) {
            List<WorkflowVerifier.Finding> findings = new WorkflowVerifier().verify(
                    workflow(step(reserved, "workflow.verify.Tokenizer/Tokenize")));
            assertThat(findings).hasSize(1);
            assertThat(findings.get(0).step()).isEqualTo(reserved);
            assertThat(findings.get(0).kind()).isEqualTo("workflow");
            assertThat(findings.get(0).error())
                    .isEqualTo("step name must be an identifier other than 'input'/'target'");
        }
    }

    @Test
    void aStepNameThatIsNotAnIdentifierIsRejected() {
        List<WorkflowVerifier.Finding> findings = new WorkflowVerifier().verify(
                workflow(step("tokenize-1", "workflow.verify.Tokenizer/Tokenize")));

        assertThat(findings).hasSize(1);
        assertThat(findings.get(0).step()).isEqualTo("tokenize-1");
        assertThat(findings.get(0).kind()).isEqualTo("workflow");
        assertThat(findings.get(0).error())
                .isEqualTo("step name must be an identifier other than 'input'/'target'");
    }

    @Test
    void streamingMethodsAreReportedAsNotUnary() {
        List<WorkflowVerifier.Finding> serverStreaming = new WorkflowVerifier().verify(
                workflow(step("tokenize", "workflow.verify.Tokenizer/TokenizeStream")));
        assertThat(serverStreaming).hasSize(1);
        assertThat(serverStreaming.get(0).step()).isEqualTo("tokenize");
        assertThat(serverStreaming.get(0).kind()).isEqualTo("method");
        assertThat(serverStreaming.get(0).error())
                .isEqualTo("workflow.verify.Tokenizer.TokenizeStream is not unary; workflows call "
                        + "unary methods (streaming is a later phase)");

        List<WorkflowVerifier.Finding> clientStreaming = new WorkflowVerifier().verify(
                workflow(step("tokenize", "workflow.verify.Tokenizer/TokenizeAll")));
        assertThat(clientStreaming).hasSize(1);
        assertThat(clientStreaming.get(0).kind()).isEqualTo("method");
        assertThat(clientStreaming.get(0).error())
                .startsWith("workflow.verify.Tokenizer.TokenizeAll is not unary");
    }

    /**
     * A non-unary method is a finding, not a stop: the step's name still enters the scope so
     * later steps are checked against the shape they would really see.
     */
    @Test
    void aNonUnaryStepStillContributesItsOutputToTheScope() {
        List<WorkflowVerifier.Finding> findings = new WorkflowVerifier().verify(workflow(
                step("tokenize", "workflow.verify.Tokenizer/TokenizeStream"),
                CompiledWorkflow.Step.grpc("count", "in-process", false,
                        CompiledWorkflow.resolveMethod(List.of(file),
                                "workflow.verify.Tokenizer/Tokenize"),
                        null, List.of("text = tokenize.missing"), List.of(), false, 0, "")));

        assertThat(findings).hasSize(2);
        assertThat(findings.get(0).kind()).isEqualTo("method");
        assertThat(findings.get(1).step()).isEqualTo("count");
        assertThat(findings.get(1).kind()).isEqualTo("rule");
        assertThat(findings.get(1).error()).contains("missing");
    }
}
