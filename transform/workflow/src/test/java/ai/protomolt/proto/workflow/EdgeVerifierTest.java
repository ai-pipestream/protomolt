package ai.protomolt.proto.workflow;

import ai.protomolt.proto.sources.CompiledProtos;
import ai.protomolt.proto.sources.ProtoSourceCompiler;
import ai.protomolt.proto.sources.ProtoSourceSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The static edge contract: dual rule ownership, source scoping, produce and projection
 * type agreement, item type versus method request type, collect field existence and
 * element type, and the fan-out bounds all fail at verify time, never at run time.
 */
class EdgeVerifierTest {

    private static final String VALIDATE = "ai/protomolt/proto/validate/v1/validate.proto";
    private static final String PROJECTION =
            "ai/protomolt/proto/projection/v1/projection.proto";

    private static final String PROTO = """
            syntax = "proto3";
            package workflow.edgecheck.test;
            import "ai/protomolt/proto/validate/v1/validate.proto";
            import "ai/protomolt/proto/projection/v1/projection.proto";
            message Ticket { string title = 1; }
            message LookupResult { string doc_id = 1; string internal_notes = 2; }
            message DocGrounding {
              option (ai.protomolt.proto.projection.v1.sources) = {
                source: "workflow.edgecheck.test.LookupResult"
              };
              string doc_id = 1 [(ai.protomolt.proto.projection.v1.from) = {
                paths: {path: "doc_id"}
              }];
            }
            message Plain { string doc_id = 1; }
            message Summary { string headline = 1; }
            message Batch { repeated Ticket items = 1; }
            message LookupBatch { repeated LookupResult items = 1; }
            message BatchResult { repeated Ticket results = 1; }
            message WrongCollect { repeated Summary results = 1; }
            service Lookup { rpc Fetch(Ticket) returns (LookupResult); }
            service Worker { rpc Process(Ticket) returns (Ticket); }
            """;

    private static FileDescriptor file;
    private static Descriptor ticket;
    private static Descriptor lookupResult;
    private static Descriptor grounding;
    private static Descriptor plain;
    private static Descriptor summary;
    private static Descriptor batch;
    private static Descriptor lookupBatch;
    private static Descriptor batchResult;
    private static Descriptor wrongCollect;

    @BeforeAll
    static void compile() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add(VALIDATE, resource(VALIDATE), "test")
                .add(PROJECTION, resource(PROJECTION), "test")
                .add("workflow/edgecheck/test/edge.proto", PROTO, "test").build());
        file = compiled.descriptorFor("workflow/edgecheck/test/edge.proto").orElseThrow();
        ticket = file.findMessageTypeByName("Ticket");
        lookupResult = file.findMessageTypeByName("LookupResult");
        grounding = file.findMessageTypeByName("DocGrounding");
        plain = file.findMessageTypeByName("Plain");
        summary = file.findMessageTypeByName("Summary");
        batch = file.findMessageTypeByName("Batch");
        lookupBatch = file.findMessageTypeByName("LookupBatch");
        batchResult = file.findMessageTypeByName("BatchResult");
        wrongCollect = file.findMessageTypeByName("WrongCollect");
    }

    private static String resource(String name) {
        try (InputStream in = EdgeVerifierTest.class.getClassLoader()
                .getResourceAsStream(name)) {
            if (in == null) {
                throw new IllegalStateException(name + " not on the test classpath");
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static com.google.protobuf.Descriptors.MethodDescriptor method(String service,
                                                                           String name) {
        return CompiledWorkflow.resolveMethod(List.of(file),
                "workflow.edgecheck.test." + service + "/" + name);
    }

    private static CompiledWorkflow.Step grpcEdgeStep(String name,
                                                     CompiledWorkflow.EdgeSpec edge,
                                                     CompiledWorkflow.FanOutSpec fanOut) {
        return new CompiledWorkflow.Step(name, "in-process", false,
                method("Worker", "Process"), null, List.of(), List.of(), false, 0, "",
                null, edge, fanOut);
    }

    private static CompiledWorkflow.Step structuredEdgeStep(
            String name, CompiledWorkflow.EdgeSpec edge) {
        return new CompiledWorkflow.Step(name, CompiledWorkflow.Step.STRUCTURED_DEPENDENCY,
                false, null, null, List.of(), List.of(), false, 0, "",
                new CompiledWorkflow.StructuredSpec(summary, "model", 0), edge, null);
    }

    private static CompiledWorkflow workflowOf(CompiledWorkflow.Step... steps) {
        Descriptor input = steps[0].fanOut() != null ? batch : ticket;
        return new CompiledWorkflow("check", List.of(file), input, 10_000,
                List.of(steps), null);
    }

    private static List<String> errors(CompiledWorkflow workflow) {
        return new WorkflowVerifier().verify(workflow).stream()
                .map(WorkflowVerifier.Finding::error).toList();
    }

    @Test
    void aCleanEdgeWorkflowVerifies() {
        CompiledWorkflow.EdgeSpec edge = new CompiledWorkflow.EdgeSpec(List.of("input"),
                ticket, List.of("title = input.title"), List.of(), null, false);
        assertThat(errors(workflowOf(grpcEdgeStep("process", edge, null)))).isEmpty();

        CompiledWorkflow.EdgeSpec projecting = new CompiledWorkflow.EdgeSpec(
                List.of("input"), lookupResult,
                List.of("doc_id = input.title", "internal_notes = input.title"),
                List.of(), grounding, true);
        assertThat(errors(workflowOf(new CompiledWorkflow.Step("fetch", "in-process", false,
                method("Lookup", "Fetch"), null, List.of("title = input.title"),
                List.of(), false, 0, "", null, null, null),
                structuredEdgeStep("fill", projecting)))).isEmpty();
    }

    @Test
    void anEdgeOwnsTheStepsMappingRules() {
        CompiledWorkflow.EdgeSpec edge = new CompiledWorkflow.EdgeSpec(List.of("input"),
                ticket, List.of("title = input.title"), List.of(), null, false);
        CompiledWorkflow.Step step = new CompiledWorkflow.Step("process", "in-process",
                false, method("Worker", "Process"), null, List.of("title = input.title"),
                List.of(), false, 0, "", null, edge, null);
        assertThat(errors(workflowOf(step)))
                .anySatisfy(error -> assertThat(error).contains("owns request mapping"));
    }

    @Test
    void edgeSourcesMustBeDeclaredScopeEntries() {
        CompiledWorkflow.EdgeSpec edge = new CompiledWorkflow.EdgeSpec(List.of("ghost"),
                ticket, List.of("title = ghost.title"), List.of(), null, false);
        assertThat(errors(workflowOf(grpcEdgeStep("process", edge, null))))
                .anySatisfy(error -> assertThat(error)
                        .contains("not 'input' or a prior step"));
    }

    @Test
    void aGrpcStepsEdgeMustProduceTheRequestType() {
        CompiledWorkflow.EdgeSpec edge = new CompiledWorkflow.EdgeSpec(List.of("input"),
                lookupResult, List.of("doc_id = input.title"), List.of(), null, false);
        assertThat(errors(workflowOf(grpcEdgeStep("process", edge, null))))
                .anySatisfy(error -> assertThat(error)
                        .contains("must produce the method's request type"));
    }

    @Test
    void aGrpcStepWithoutFanOutTakesNoProjection() {
        CompiledWorkflow.EdgeSpec edge = new CompiledWorkflow.EdgeSpec(List.of("input"),
                lookupResult, List.of("doc_id = input.title"), List.of(), grounding,
                false);
        assertThat(errors(workflowOf(grpcEdgeStep("process", edge, null))))
                .anySatisfy(error -> assertThat(error).contains("takes no projection"));
    }

    @Test
    void aProjectionTargetMustDeclareSourcesAndSupportTheProducedType() {
        CompiledWorkflow.EdgeSpec noSources = new CompiledWorkflow.EdgeSpec(
                List.of("input"), lookupResult, List.of("doc_id = input.title"),
                List.of(), plain, false);
        assertThat(errors(workflowOf(structuredEdgeStep("fill", noSources))))
                .anySatisfy(error -> assertThat(error)
                        .contains("declares no projection sources"));

        CompiledWorkflow.EdgeSpec unsupported = new CompiledWorkflow.EdgeSpec(
                List.of("input"), ticket, List.of("title = input.title"), List.of(),
                grounding, false);
        assertThat(errors(workflowOf(structuredEdgeStep("fill", unsupported))))
                .anySatisfy(error -> assertThat(error).contains("does not support"));
    }

    @Test
    void fanOutRequiresAnEdge() {
        assertThatThrownBy(() -> grpcEdgeStep("process", null,
                new CompiledWorkflow.FanOutSpec("items", 8, 2,
                        CompiledWorkflow.BranchFailurePolicy.CONTINUE, batchResult,
                        "results")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("requires an edge");
    }

    @Test
    void fanOutChecksTheItemsPathCapsAndCollectTarget() {
        CompiledWorkflow.EdgeSpec edge = new CompiledWorkflow.EdgeSpec(List.of("input"),
                batch, List.of("items = input.items"), List.of(), null, false);

        // A bad items path.
        assertThat(errors(workflowOf(grpcEdgeStep("process", edge,
                new CompiledWorkflow.FanOutSpec("nope", 8, 2,
                        CompiledWorkflow.BranchFailurePolicy.CONTINUE, batchResult,
                        "results")))))
                .anySatisfy(error -> assertThat(error).contains("items path"));

        // Caps.
        assertThat(errors(workflowOf(grpcEdgeStep("process", edge,
                new CompiledWorkflow.FanOutSpec("items", 0, 2,
                        CompiledWorkflow.BranchFailurePolicy.CONTINUE, batchResult,
                        "results")))))
                .anySatisfy(error -> assertThat(error).contains("maxItems"));
        assertThat(errors(workflowOf(grpcEdgeStep("process", edge,
                new CompiledWorkflow.FanOutSpec("items", 8, 0,
                        CompiledWorkflow.BranchFailurePolicy.CONTINUE, batchResult,
                        "results")))))
                .anySatisfy(error -> assertThat(error).contains("maxConcurrency"));

        // A missing collect field.
        assertThat(errors(workflowOf(grpcEdgeStep("process", edge,
                new CompiledWorkflow.FanOutSpec("items", 8, 2,
                        CompiledWorkflow.BranchFailurePolicy.CONTINUE, batchResult,
                        "missing")))))
                .anySatisfy(error -> assertThat(error).contains("has no field"));

        // A collect field of the wrong element type.
        assertThat(errors(workflowOf(grpcEdgeStep("process", edge,
                new CompiledWorkflow.FanOutSpec("items", 8, 2,
                        CompiledWorkflow.BranchFailurePolicy.CONTINUE, wrongCollect,
                        "results")))))
                .anySatisfy(error -> assertThat(error).contains("branch output type"));
    }

    @Test
    void aFannedOutGrpcStepsItemTypeMustBeTheRequestType() {
        CompiledWorkflow.EdgeSpec edge = new CompiledWorkflow.EdgeSpec(List.of("input"),
                lookupBatch, List.of("items = input.items"), List.of(), null, false);
        CompiledWorkflow.Step step = new CompiledWorkflow.Step("process", "in-process",
                false, method("Worker", "Process"), null, List.of(), List.of(), false,
                0, "", null, edge,
                new CompiledWorkflow.FanOutSpec("items", 8, 2,
                        CompiledWorkflow.BranchFailurePolicy.CONTINUE, batchResult,
                        "results"));
        CompiledWorkflow workflow = new CompiledWorkflow("check", List.of(file), lookupBatch,
                10_000, List.of(step), null);
        assertThat(errors(workflow))
                .anySatisfy(error -> assertThat(error)
                        .contains("item type must be the method's request type"));
    }

    @Test
    void externalStepsDoNotCarryEdges() {
        CompiledWorkflow.EdgeSpec edge = new CompiledWorkflow.EdgeSpec(List.of("input"),
                ticket, List.of("title = input.title"), List.of(), null, false);
        CompiledWorkflow.Step step = new CompiledWorkflow.Step("process", "in-process",
                false, method("Worker", "Process"), null, List.of(), List.of(), false,
                0, CompiledWorkflow.Step.COMPLETION_EXTERNAL, null, edge, null);
        assertThat(errors(workflowOf(step)))
                .anySatisfy(error -> assertThat(error)
                        .contains("external-completion steps do not carry edges"));
    }
}
