package ai.pipestream.proto.workflow;

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
 * The {@link CompiledWorkflow} record's own contract: constructor validation and defaults,
 * defensive copies, {@code Step.external()} semantics, and {@code resolveMethod}'s
 * qualified-name parsing across the workflow's files.
 */
class CompiledWorkflowTest {

    private static final String PROTO = """
            syntax = "proto3";
            package workflow.def;
            message Text { string text = 1; }
            message Tokens { repeated int64 ids = 1; }
            service Tokenizer { rpc Tokenize(Text) returns (Tokens); }
            service Embedder { rpc Embed(Tokens) returns (Tokens); }
            """;

    private static FileDescriptor file;

    @BeforeAll
    static void compile() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("workflow/def/workflow.proto", PROTO, "test").build());
        file = compiled.descriptorFor("workflow/def/workflow.proto").orElseThrow();
    }

    private static MethodDescriptor tokenize() {
        return CompiledWorkflow.resolveMethod(List.of(file), "workflow.def.Tokenizer/Tokenize");
    }

    private static CompiledWorkflow.Step step(String name) {
        return CompiledWorkflow.Step.grpc(name, "in-process", false, tokenize(),
                null, List.of(), List.of(), false, 0, "");
    }

    @Test
    void aWorkflowNeedsAtLeastOneStep() {
        assertThatThrownBy(() -> new CompiledWorkflow("empty", List.of(file),
                file.findMessageTypeByName("Text"), 10_000, List.of(), null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("A workflow needs at least one step");
    }

    @Test
    void aNonPositiveDeadlineTakesTheDefaultBudget() {
        for (long deadline : List.of(0L, -1L, -30_000L)) {
            CompiledWorkflow workflow = new CompiledWorkflow("d", List.of(file),
                    file.findMessageTypeByName("Text"), deadline, List.of(step("one")), null);
            assertThat(workflow.deadlineMs()).isEqualTo(30_000);
        }
    }

    @Test
    void inputTypeIsRequired() {
        assertThatNullPointerException().isThrownBy(() -> new CompiledWorkflow("n",
                List.of(file), null, 10_000, List.of(step("one")), null))
                .withMessage("inputType");
    }

    @Test
    void filesAndStepsAreDefensivelyCopied() {
        List<FileDescriptor> files = new ArrayList<>(List.of(file));
        List<CompiledWorkflow.Step> steps = new ArrayList<>(List.of(step("one")));
        CompiledWorkflow workflow = new CompiledWorkflow("copies", files,
                file.findMessageTypeByName("Text"), 10_000, steps, null);

        files.clear();
        steps.clear();
        assertThat(workflow.files()).hasSize(1);
        assertThat(workflow.steps()).hasSize(1);
        assertThatThrownBy(() -> workflow.steps().add(step("two")))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> workflow.files().add(file))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void aStepRequiresNameTargetAndMethod() {
        assertThatNullPointerException().isThrownBy(() -> CompiledWorkflow.Step.grpc(
                null, "t", false, tokenize(), null, List.of(), List.of(), false, 0, ""))
                .withMessage("name");
        assertThatNullPointerException().isThrownBy(() -> CompiledWorkflow.Step.grpc(
                "s", null, false, tokenize(), null, List.of(), List.of(), false, 0, ""))
                .withMessage("target");
        assertThatNullPointerException().isThrownBy(() -> CompiledWorkflow.Step.grpc(
                "s", "t", false, null, null, List.of(), List.of(), false, 0, ""))
                .withMessage("method");
    }

    @Test
    void theOriginalGrpcStepConstructorRemainsCompatible() {
        CompiledWorkflow.Step existing = new CompiledWorkflow.Step("s", "t", false,
                tokenize(), null, List.of(), List.of(), false, 0, "");

        assertThat(existing.method()).isEqualTo(tokenize());
        assertThat(existing.structured()).isNull();
    }

    @Test
    void aNullCompletionMeansInvokeAndOnlyExternalParks() {
        CompiledWorkflow.Step invoke = CompiledWorkflow.Step.grpc("s", "t", false, tokenize(),
                null, List.of(), List.of(), false, 0, null);
        assertThat(invoke.completion()).isEmpty();
        assertThat(invoke.external()).isFalse();

        CompiledWorkflow.Step external = CompiledWorkflow.Step.grpc("s", "t", false, tokenize(),
                null, List.of(), List.of(), false, 0, "external");
        assertThat(external.external()).isTrue();

        // Anything else is not external; the verifier rejects it before a run.
        CompiledWorkflow.Step bogus = CompiledWorkflow.Step.grpc("s", "t", false, tokenize(),
                null, List.of(), List.of(), false, 0, "later");
        assertThat(bogus.external()).isFalse();
    }

    @Test
    void stepRuleListsAreDefensivelyCopied() {
        List<String> rules = new ArrayList<>(List.of("text = input.text"));
        List<CelMappingRule> celRules = new ArrayList<>(
                List.of(new CelMappingRule(null, "input.text", "text")));
        CompiledWorkflow.Step step = CompiledWorkflow.Step.grpc("s", "t", false, tokenize(),
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
                new CompiledWorkflow.Output(null, List.of(), List.of()))
                .withMessage("type");
        // Null rule lists are rejected too (List.copyOf), not silently emptied — WorkflowJson
        // always passes lists.
        assertThatNullPointerException().isThrownBy(() ->
                new CompiledWorkflow.Output(file.findMessageTypeByName("Tokens"), null, List.of()));
        assertThatNullPointerException().isThrownBy(() ->
                new CompiledWorkflow.Output(file.findMessageTypeByName("Tokens"), List.of(), null));

        List<String> rules = new ArrayList<>(List.of("text = input.text"));
        CompiledWorkflow.Output output = new CompiledWorkflow.Output(
                file.findMessageTypeByName("Tokens"), rules, List.of());
        rules.clear();
        assertThat(output.rules()).containsExactly("text = input.text");
        assertThat(output.celRules()).isEmpty();
    }

    @Test
    void resolveMethodFindsMethodsAcrossServices() {
        assertThat(CompiledWorkflow.resolveMethod(List.of(file), "workflow.def.Tokenizer/Tokenize")
                .getFullName()).isEqualTo("workflow.def.Tokenizer.Tokenize");
        assertThat(CompiledWorkflow.resolveMethod(List.of(file), "workflow.def.Embedder/Embed")
                .getFullName()).isEqualTo("workflow.def.Embedder.Embed");
    }

    @Test
    void resolveMethodRejectsNamesWithoutBothHalves() {
        for (String malformed : List.of("Tokenize", "workflow.def.Tokenizer/",
                "/Tokenize", "/")) {
            assertThatThrownBy(() -> CompiledWorkflow.resolveMethod(List.of(file), malformed))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("method must be 'package.Service/Method'; got '" + malformed + "'");
        }
    }

    @Test
    void resolveMethodReportsUnknownServicesAndMethodsAlike() {
        assertThatThrownBy(() -> CompiledWorkflow.resolveMethod(
                List.of(file), "workflow.def.Missing/Tokenize"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("method 'workflow.def.Missing/Tokenize' not found in the workflow's schema");
        assertThatThrownBy(() -> CompiledWorkflow.resolveMethod(
                List.of(file), "workflow.def.Tokenizer/Missing"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("method 'workflow.def.Tokenizer/Missing' not found in the workflow's schema");
    }

    @Test
    void resolveMethodSearchesEveryFileInTheWorkflow() throws Exception {
        CompiledProtos second = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("workflow/def/other.proto", """
                        syntax = "proto3";
                        package workflow.other;
                        message Ping { string text = 1; }
                        service Pinger { rpc Ping(Ping) returns (Ping); }
                        """, "test").build());
        FileDescriptor other = second.descriptorFor("workflow/def/other.proto").orElseThrow();

        MethodDescriptor resolved = CompiledWorkflow.resolveMethod(
                List.of(file, other), "workflow.other.Pinger/Ping");
        assertThat(resolved.getFullName()).isEqualTo("workflow.other.Pinger.Ping");
    }
}
