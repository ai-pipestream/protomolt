package ai.pipestream.proto.workflow;

import ai.pipestream.proto.cel.CelMappingRule;
import ai.pipestream.proto.grpc.workflow.v1.Workflow;
import ai.pipestream.proto.grpc.workflow.v1.ServiceDependency;
import ai.pipestream.proto.grpc.workflow.v1.StepCompletion;
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
 * Workflow to workflow compilation: the mapping is lossless (rules, gates, deadlines, validation
 * flags all carry over), deterministic (equal workflows compile to equal bytes, file order does
 * not change fingerprints), and every method and type reference in the workflow resolves
 * against the workflow's own descriptors.
 */
class WorkflowCompilerTest {

    private static final String PROTO = """
            syntax = "proto3";
            package workflow.def;
            message Text { string text = 1; }
            message Tokens { repeated int64 ids = 1; }
            message Summary { string text = 1; }
            service Tokenizer { rpc Tokenize(Text) returns (Tokens); }
            service Embedder { rpc Embed(Tokens) returns (Tokens); }
            """;

    private static final String EXTRA_PROTO = """
            syntax = "proto3";
            package workflow.def;
            message Extra { string note = 1; }
            """;

    private static FileDescriptor file;
    private static FileDescriptor extraFile;
    private static Descriptor text;
    private static Descriptor summary;

    @BeforeAll
    static void compile() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("workflow/def/workflow.proto", PROTO, "test")
                .add("workflow/def/extra.proto", EXTRA_PROTO, "test").build());
        file = compiled.descriptorFor("workflow/def/workflow.proto").orElseThrow();
        extraFile = compiled.descriptorFor("workflow/def/extra.proto").orElseThrow();
        text = file.findMessageTypeByName("Text");
        summary = file.findMessageTypeByName("Summary");
    }

    private static CompiledWorkflow workflow(String name) {
        return new CompiledWorkflow(name, List.of(file), text, 45_000, List.of(
                CompiledWorkflow.Step.grpc("tokenize", "localhost:9090", false,
                        CompiledWorkflow.resolveMethod(List.of(file),
                                "workflow.def.Tokenizer/Tokenize"),
                        "input.text.size() > 0", List.of("text=input.text"),
                        List.of(new CelMappingRule(null, "input.text", "text",
                                List.of("text=input.text"))),
                        true, 5_000, ""),
                CompiledWorkflow.Step.grpc("embed", "localhost:9090", false,
                        CompiledWorkflow.resolveMethod(List.of(file),
                                "workflow.def.Embedder/Embed"),
                        null, List.of(), List.of(), false, 0, "external")),
                new CompiledWorkflow.Output(summary,
                        List.of("text=tokenize.text"), List.of()));
    }

    @Test
    void carriesEveryStepFieldIntoTheWorkflow() {
        Workflow workflow = WorkflowCompiler.compile(workflow("analyze"));

        assertThat(workflow.getName()).isEqualTo("analyze");
        assertThat(workflow.getInputType()).isEqualTo("workflow.def.Text");
        assertThat(workflow.getDeadline().getSeconds()).isEqualTo(45);

        assertThat(workflow.getStepsCount()).isEqualTo(2);
        var tokenize = workflow.getSteps(0);
        assertThat(tokenize.getName()).isEqualTo("tokenize");
        assertThat(tokenize.getDependency()).isEqualTo("workflow.def.Tokenizer");
        assertThat(tokenize.getMethod()).isEqualTo("workflow.def.Tokenizer/Tokenize");
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

        var embed = workflow.getSteps(1);
        assertThat(embed.getCompletion()).isEqualTo(StepCompletion.STEP_COMPLETION_EXTERNAL);
        assertThat(embed.hasDeadline()).isFalse();
        assertThat(embed.getWhen()).isEmpty();

        assertThat(workflow.getOutput().getType()).isEqualTo("workflow.def.Summary");
        assertThat(workflow.getOutput().getRulesList()).containsExactly("text=tokenize.text");
    }

    @Test
    void derivesOneDependencyPerServiceWithSanitizedEndpoint() {
        Workflow workflow = WorkflowCompiler.compile(workflow("deps"));

        assertThat(workflow.getDependenciesList())
                .extracting(ServiceDependency::getAlias)
                .containsExactly("workflow.def.Tokenizer", "workflow.def.Embedder");
        ServiceDependency tokenizer = workflow.getDependencies(0);
        assertThat(tokenizer.getServiceProfile()).isEqualTo("workflow.def.Tokenizer");
        // "localhost:9090" folds to a path-safe placeholder; the promoter rebinds it.
        assertThat(tokenizer.getEndpoint()).isEqualTo("localhost-9090");
        assertThat(tokenizer.getDescriptorFingerprint()).matches("[0-9a-f]{64}");
    }

    @Test
    void compilesDeterministicallyRegardlessOfFileOrder() {
        CompiledWorkflow forward = new CompiledWorkflow("stable", List.of(file, extraFile),
                text, 45_000, workflow("stable").steps(), workflow("stable").output());
        CompiledWorkflow reversed = new CompiledWorkflow("stable", List.of(extraFile, file),
                text, 45_000, workflow("stable").steps(), workflow("stable").output());

        Workflow first = WorkflowCompiler.compile(forward);
        Workflow second = WorkflowCompiler.compile(reversed);

        assertThat(second.toByteArray()).isEqualTo(first.toByteArray());
        assertThat(WorkflowCompiler.compile(forward).toByteArray())
                .isEqualTo(first.toByteArray());
    }

    @Test
    void everyMethodAndTypeReferenceResolvesAgainstTheWorkflowDescriptors() {
        Workflow workflow = WorkflowCompiler.compile(workflow("resolvable"));

        for (var step : workflow.getStepsList()) {
            assertThat(CompiledWorkflow.resolveMethod(List.of(file), step.getMethod()))
                    .isNotNull();
        }
        assertThat(file.findMessageTypeByName(
                workflow.getInputType().substring(workflow.getInputType().lastIndexOf('.') + 1)))
                .isNotNull();
    }

    @Test
    void rejectsIdentitiesTheWorkflowContractCannotHold() {
        assertThatThrownBy(() -> WorkflowCompiler.compile(workflow("not a safe name!")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("[name] string.slug");
    }

    @Test
    void missingOutputStaysAbsent() {
        CompiledWorkflow bare = new CompiledWorkflow("bare", List.of(file), text, 0,
                workflow("bare").steps(), null);

        Workflow workflow = WorkflowCompiler.compile(bare);

        assertThat(workflow.hasOutput()).isFalse();
        // The workflow defaulted its deadline to 30s; the workflow carries that budget.
        assertThat(workflow.getDeadline().getSeconds()).isEqualTo(30);
    }
}
