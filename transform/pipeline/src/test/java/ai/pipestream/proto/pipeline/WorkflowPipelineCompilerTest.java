package ai.pipestream.proto.pipeline;

import ai.pipestream.proto.workflow.CompiledWorkflow;
import ai.pipestream.proto.workflow.WorkflowCompiler;
import ai.pipestream.proto.workflow.WorkflowVerifier;
import ai.pipestream.proto.grpc.workflow.WorkflowValidation;
import ai.pipestream.proto.grpc.workflow.v1.Workflow;
import ai.pipestream.proto.grpc.workflow.v1.WorkflowStep;
import ai.pipestream.proto.grpc.workflow.v1.ServiceDependency;
import ai.pipestream.proto.grpc.workflow.v1.StepCompletion;
import ai.pipestream.proto.pipeline.v1.EdgeCardinality;
import ai.pipestream.proto.pipeline.v1.MethodShape;
import ai.pipestream.proto.pipeline.v1.Pipeline;
import ai.pipestream.proto.pipeline.v1.PipelineStep;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Duration;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End to end, fully offline: existing workflows compile into workflows with the landed
 * {@link WorkflowCompiler}, workflows compile into pipelines, and the pipelines verify
 * against the same descriptors. The typed edge and fan-out cross the boundary byte-for-byte,
 * compilation is deterministic, and every broken reference fails fast with its name.
 */
class WorkflowPipelineCompilerTest {

    private final PipelineChecker checker = new PipelineChecker();

    private static MethodDescriptor method(String service, String name) {
        return PipelineFixtures.file().findServiceByName(service).findMethodByName(name);
    }

    @Test
    void workflowCompilesToCheckingPipeline() {
        CompiledWorkflow definition = new CompiledWorkflow("fetch-flow", PipelineFixtures.files(),
                PipelineFixtures.type(PipelineFixtures.TICKET), 30_000,
                List.of(CompiledWorkflow.Step.grpc("fetch", "inprocess:lookup", false,
                        method("Lookup", "Fetch"), null, List.of("title = input.title"),
                        List.of(), true, 0, "")),
                null);
        assertThat(new WorkflowVerifier().verify(definition)).isEmpty();

        Workflow workflow = WorkflowCompiler.compile(definition);
        Pipeline pipeline = WorkflowPipelineCompiler.compile(workflow, PipelineFixtures.files());

        assertThat(pipeline.getSourceWorkflowName()).isEqualTo("fetch-flow");
        assertThat(pipeline.getSourceWorkflowFingerprint())
                .isEqualTo(WorkflowValidation.fingerprint(workflow));
        assertThat(pipeline.getDescriptorFingerprint())
                .isEqualTo(PipelineFixtures.fingerprint());
        assertThat(pipeline.getMaxStreamMessages()).isEqualTo(10_000);
        assertThat(pipeline.getStepsList()).singleElement().satisfies(step -> {
            // The top-level rule lane is normalized into an explicit edge over the scope.
            assertThat(step.getGrpcCall().getMethod())
                    .isEqualTo(PipelineFixtures.FETCH);
            assertThat(step.getGrpcCall().getMethodShape())
                    .isEqualTo(MethodShape.METHOD_SHAPE_UNARY);
            assertThat(step.getGrpcCall().getEdge().getSourcesList())
                    .containsExactly("input");
            assertThat(step.getGrpcCall().getEdge().getProduceType())
                    .isEqualTo(PipelineFixtures.TICKET);
            assertThat(step.getGrpcCall().getEdge().getRulesList())
                    .containsExactly("title = input.title");
            assertThat(step.getGrpcCall().getEdgeCardinality())
                    .isEqualTo(EdgeCardinality.EDGE_CARDINALITY_ONE);
            assertThat(step.getGrpcCall().getOutputCardinality())
                    .isEqualTo(EdgeCardinality.EDGE_CARDINALITY_ONE);
            assertThat(step.getGrpcCall().getValidateResponse()).isTrue();
        });
        assertThat(checker.verify(pipeline, PipelineFixtures.files())).isEmpty();
    }

    @Test
    void compilationIsDeterministic() {
        Workflow workflow = workflowWithSteps(WorkflowStep.newBuilder()
                .setName("fetch")
                .setDependency("pipeline.test.Lookup")
                .setMethod(PipelineFixtures.FETCH)
                .addRules("title = input.title")
                .setCompletion(StepCompletion.STEP_COMPLETION_LIVE)
                .build());
        Pipeline first = WorkflowPipelineCompiler.compile(workflow, PipelineFixtures.files());
        Pipeline second = WorkflowPipelineCompiler.compile(workflow, PipelineFixtures.files());
        assertThat(first).isEqualTo(second);
        assertThat(first.toByteArray()).isEqualTo(second.toByteArray());
    }

    @Test
    void typedEdgeAndFanOutCrossByteForByte() {
        var edgeSpec = new CompiledWorkflow.EdgeSpec(List.of("input"),
                PipelineFixtures.type(PipelineFixtures.BATCH), List.of(), List.of(), null,
                false);
        var fanOutSpec = new CompiledWorkflow.FanOutSpec("items", 10, 4,
                CompiledWorkflow.BranchFailurePolicy.CONTINUE,
                PipelineFixtures.type(PipelineFixtures.TICKET_BOX), "tickets");
        CompiledWorkflow.Step step = new CompiledWorkflow.Step("process",
                "inprocess:worker", false, method("Worker", "Process"), null, List.of(),
                List.of(), false, 0, "", null, edgeSpec, fanOutSpec);
        CompiledWorkflow definition = new CompiledWorkflow("fanout-flow", PipelineFixtures.files(),
                PipelineFixtures.type(PipelineFixtures.BATCH), 30_000, List.of(step), null);
        assertThat(new WorkflowVerifier().verify(definition)).isEmpty();

        Workflow workflow = WorkflowCompiler.compile(definition);
        Pipeline pipeline = WorkflowPipelineCompiler.compile(workflow, PipelineFixtures.files());

        PipelineStep compiled = pipeline.getSteps(0);
        WorkflowStep workflowStep = workflow.getSteps(0);
        assertThat(compiled.getGrpcCall().getEdge()).isEqualTo(workflowStep.getEdge());
        assertThat(compiled.getGrpcCall().getFanOut()).isEqualTo(workflowStep.getFanOut());
        assertThat(compiled.getGrpcCall().getOutputCardinality())
                .isEqualTo(EdgeCardinality.EDGE_CARDINALITY_ONE);
        assertThat(checker.verify(pipeline, PipelineFixtures.files())).isEmpty();
    }

    @Test
    void structuredWorkflowCompilesToCheckingPipeline() {
        CompiledWorkflow definition = new CompiledWorkflow("structured-flow",
                PipelineFixtures.files(), PipelineFixtures.type(PipelineFixtures.TICKET),
                30_000,
                List.of(CompiledWorkflow.Step.grpc("fetch", "inprocess:lookup", false,
                                method("Lookup", "Fetch"), null,
                                List.of("title = input.title"), List.of(), false, 0, ""),
                        CompiledWorkflow.Step.structured("summarize",
                                PipelineFixtures.type(PipelineFixtures.SUMMARY),
                                "test-model", 1)),
                null);
        assertThat(new WorkflowVerifier().verify(definition)).isEmpty();

        Workflow workflow = WorkflowCompiler.compile(definition);
        Pipeline pipeline = WorkflowPipelineCompiler.compile(workflow, PipelineFixtures.files());

        PipelineStep summarize = pipeline.getSteps(1);
        assertThat(summarize.getStructured().getSpec().getTargetType())
                .isEqualTo(PipelineFixtures.SUMMARY);
        assertThat(summarize.getDependency()).isEqualTo("structured-generation");
        assertThat(checker.verify(pipeline, PipelineFixtures.files())).isEmpty();
    }

    @Test
    void streamingWorkflowCompilesAndVerifies() {
        // A hand-authored workflow may name a server-streaming method; the compiler
        // declares the shape and the MANY output binding from the descriptor.
        Workflow workflow = workflowWithSteps(WorkflowStep.newBuilder()
                .setName("search")
                .setDependency("pipeline.test.Search")
                .setMethod(PipelineFixtures.STREAM)
                .addRules("title = input.title")
                .setCompletion(StepCompletion.STEP_COMPLETION_LIVE)
                .build());
        Pipeline pipeline = WorkflowPipelineCompiler.compile(workflow, PipelineFixtures.files());
        assertThat(pipeline.getSteps(0).getGrpcCall().getMethodShape())
                .isEqualTo(MethodShape.METHOD_SHAPE_SERVER_STREAMING);
        assertThat(pipeline.getSteps(0).getGrpcCall().getOutputCardinality())
                .isEqualTo(EdgeCardinality.EDGE_CARDINALITY_MANY);
        assertThat(checker.verify(pipeline, PipelineFixtures.files())).isEmpty();
    }

    @Test
    void streamFedUnaryWorkflowCompilesButFailsChecking() {
        // The compiler translates faithfully and declares the stream edge; the checker
        // is the gate that rejects the missing collect.
        Workflow workflow = workflowWithSteps(
                WorkflowStep.newBuilder()
                        .setName("search")
                        .setDependency("pipeline.test.Search")
                        .setMethod(PipelineFixtures.STREAM)
                        .addRules("title = input.title")
                        .setCompletion(StepCompletion.STEP_COMPLETION_LIVE)
                        .build(),
                WorkflowStep.newBuilder()
                        .setName("fetch")
                        .setDependency("pipeline.test.Lookup")
                        .setMethod(PipelineFixtures.FETCH)
                        .addRules("title = search.title")
                        .setCompletion(StepCompletion.STEP_COMPLETION_LIVE)
                        .build());
        Pipeline pipeline = WorkflowPipelineCompiler.compile(workflow, PipelineFixtures.files());
        assertThat(pipeline.getSteps(1).getGrpcCall().getEdgeCardinality())
                .isEqualTo(EdgeCardinality.EDGE_CARDINALITY_MANY);
        assertThat(checker.verify(pipeline, PipelineFixtures.files()))
                .anySatisfy(finding -> {
                    assertThat(finding.step()).isEqualTo("fetch");
                    assertThat(finding.kind()).isEqualTo("cardinality");
                    assertThat(finding.error()).contains("collect");
                });
    }

    @Test
    void consumedWorkflowStreamDoesNotLeakIntoLaterSynthesizedEdges() {
        Workflow workflow = workflowWithSteps(
                WorkflowStep.newBuilder()
                        .setName("search")
                        .setDependency("pipeline.test.Search")
                        .setMethod(PipelineFixtures.STREAM)
                        .addRules("title = input.title")
                        .setCompletion(StepCompletion.STEP_COMPLETION_LIVE)
                        .build(),
                WorkflowStep.newBuilder()
                        .setName("upload")
                        .setDependency("pipeline.test.Ingest")
                        .setMethod(PipelineFixtures.UPLOAD)
                        .setEdge(PipelineFixtures.edge(PipelineFixtures.TICKET,
                                List.of("search"), "title = search.title"))
                        .setCompletion(StepCompletion.STEP_COMPLETION_LIVE)
                        .build(),
                WorkflowStep.newBuilder()
                        .setName("fetch")
                        .setDependency("pipeline.test.Lookup")
                        .setMethod(PipelineFixtures.FETCH)
                        .addRules("title = input.title")
                        .setCompletion(StepCompletion.STEP_COMPLETION_LIVE)
                        .build());

        Pipeline pipeline = WorkflowPipelineCompiler.compile(workflow,
                PipelineFixtures.files());
        assertThat(pipeline.getSteps(2).getGrpcCall().getEdge().getSourcesList())
                .containsExactly("input", "upload");
        assertThat(pipeline.getSteps(2).getGrpcCall().getEdgeCardinality())
                .isEqualTo(EdgeCardinality.EDGE_CARDINALITY_ONE);
        assertThat(checker.verify(pipeline, PipelineFixtures.files())).isEmpty();
    }

    @Test
    void dependencyFingerprintMismatchFailsFast() {
        Workflow workflow = workflowWithSteps(WorkflowStep.newBuilder()
                .setName("fetch")
                .setDependency("pipeline.test.Lookup")
                .setMethod(PipelineFixtures.FETCH)
                .addRules("title = input.title")
                .setCompletion(StepCompletion.STEP_COMPLETION_LIVE)
                .build());
        Workflow tampered = workflow.toBuilder()
                .setDependencies(0, workflow.getDependencies(0).toBuilder()
                        .setDescriptorFingerprint(
                                "1111111111111111111111111111111111111111111111111111111111111111")
                        .build())
                .build();
        assertThatThrownBy(() -> WorkflowPipelineCompiler.compile(tampered,
                PipelineFixtures.files()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("pipeline.test.Lookup")
                .hasMessageContaining(PipelineFixtures.fingerprint());
    }

    @Test
    void unresolvedMethodFailsFastWithFileList() {
        Workflow workflow = workflowWithSteps(WorkflowStep.newBuilder()
                .setName("fetch")
                .setDependency("pipeline.test.Lookup")
                .setMethod("pipeline.test.Lookup/Nope")
                .setCompletion(StepCompletion.STEP_COMPLETION_LIVE)
                .build());
        assertThatThrownBy(() -> WorkflowPipelineCompiler.compile(workflow,
                PipelineFixtures.files()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("pipeline.test.Lookup/Nope")
                .hasMessageContaining("pipeline/test/pipeline.proto");
    }

    @Test
    void nonIdentifierStepNameFailsFast() {
        Workflow workflow = workflowWithSteps(WorkflowStep.newBuilder()
                .setName("my-step")
                .setDependency("pipeline.test.Lookup")
                .setMethod(PipelineFixtures.FETCH)
                .setCompletion(StepCompletion.STEP_COMPLETION_LIVE)
                .build());
        assertThatThrownBy(() -> WorkflowPipelineCompiler.compile(workflow,
                PipelineFixtures.files()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("my-step")
                .hasMessageContaining("scope identifier");
    }

    @Test
    void invalidWorkflowIsRejectedBeforeCompilation() {
        assertThatThrownBy(() -> WorkflowPipelineCompiler.compile(Workflow.getDefaultInstance(),
                PipelineFixtures.files()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("workflow.name");
    }

    private static Workflow workflowWithSteps(WorkflowStep... steps) {
        Workflow.Builder workflow = Workflow.newBuilder()
                .setName("authored-workflow")
                .setInputType(PipelineFixtures.TICKET)
                .setDeadline(Duration.newBuilder().setSeconds(30).build());
        java.util.Set<String> services = new java.util.LinkedHashSet<>();
        for (WorkflowStep step : steps) {
            String service = step.getMethod().isEmpty()
                    ? step.getDependency()
                    : step.getMethod().substring(0, step.getMethod().indexOf('/'));
            if (services.add(service)) {
                workflow.addDependencies(ServiceDependency.newBuilder()
                        .setAlias(service)
                        .setServiceProfile(service)
                        .setEndpoint("local")
                        .setDescriptorFingerprint(PipelineFixtures.fingerprint())
                        .build());
            }
            workflow.addSteps(step);
        }
        Workflow built = workflow.build();
        WorkflowValidation.validate(built);
        return built;
    }
}
