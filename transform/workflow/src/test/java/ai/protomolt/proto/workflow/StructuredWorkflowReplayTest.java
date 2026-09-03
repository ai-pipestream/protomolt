package ai.protomolt.proto.workflow;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.grpc.invoke.DynamicGrpcCalls;
import ai.protomolt.proto.grpc.workflow.ArtifactRepository;
import ai.protomolt.proto.grpc.workflow.FileSystemArtifactRepository;
import ai.protomolt.proto.grpc.workflow.FileSystemRunEvidenceRepository;
import ai.protomolt.proto.grpc.workflow.RunEvidenceRepository;
import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import ai.protomolt.proto.grpc.workflow.v1.Workflow;
import ai.protomolt.proto.grpc.workflow.v1.RunEvidence;
import ai.protomolt.proto.grpc.workflow.v1.RunStatus;
import ai.protomolt.proto.grpc.workflow.v1.StepStatus;
import ai.protomolt.proto.inference.spi.ChunkObserver;
import ai.protomolt.proto.inference.spi.InferenceCatalog;
import ai.protomolt.proto.inference.spi.InferenceEngines;
import ai.protomolt.proto.inference.spi.InferenceException;
import ai.protomolt.proto.inference.spi.InferenceProvider;
import ai.protomolt.proto.inference.structured.StructuredGenerator;
import ai.protomolt.proto.inference.v1.AttemptOutcome;
import ai.protomolt.proto.inference.v1.FinishReason;
import ai.protomolt.proto.inference.v1.GenerateRequest;
import ai.protomolt.proto.inference.v1.GenerateResponse;
import ai.protomolt.proto.inference.v1.GenerateStreamRequest;
import ai.protomolt.proto.inference.v1.ModelCapabilities;
import ai.protomolt.proto.inference.v1.ModelEntry;
import ai.protomolt.proto.inference.v1.Usage;
import ai.protomolt.proto.sources.CompiledProtos;
import ai.protomolt.proto.sources.ProtoSourceCompiler;
import ai.protomolt.proto.sources.ProtoSourceSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.ServiceDescriptor;
import com.google.protobuf.DynamicMessage;
import io.grpc.Server;
import io.grpc.ServerServiceDefinition;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.ServerCalls;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Structured-generation workflow steps end to end: a scripted in-process provider fills the
 * form, the recorder persists bounded redacted evidence (fingerprints and attempt scalars,
 * never raw model text), and offline replay verifies the recording against the workflow with
 * no provider and no network - including every forgery attempt.
 */
class StructuredWorkflowReplayTest {

    private static final String VALIDATE = "ai/protomolt/proto/validate/v1/validate.proto";

    private static final String PROTO = """
            syntax = "proto3";
            package workflow.structured.test;
            import "ai/pipestream/proto/validate/v1/validate.proto";
            message Ticket { string title = 1; }
            message IntakeForm {
              string name = 1 [(ai.pipestream.proto.validate.v1.field) = {
                required: true
                string: {min_len: 3, max_len: 200}
              }];
              int32 quantity = 2 [(ai.pipestream.proto.validate.v1.field) = {
                int32: {gte: 1, lte: 100}
              }];
            }
            message Label { string label = 1; }
            service Intake { rpc Echo(Ticket) returns (Ticket); }
            """;

    private static final String MODEL = "structured-model";
    private static final String FORM = "workflow.structured.test.IntakeForm";
    private static final String VALID_FORM = "{\"name\": \"Ada Lovelace\", \"quantity\": 3}";

    private static FileDescriptor file;
    private static Descriptor ticket;
    private static Descriptor form;
    private static Descriptor label;
    private static Server server;
    private static String serverName;

    private ScriptedProvider provider;
    private StructuredGenerator generator;

    @BeforeAll
    static void compile() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add(VALIDATE, resource(VALIDATE), "test")
                .add("workflow/structured/test/workflow.proto", PROTO, "test").build());
        file = compiled.descriptorFor("workflow/structured/test/workflow.proto").orElseThrow();
        ticket = file.findMessageTypeByName("Ticket");
        form = file.findMessageTypeByName("IntakeForm");
        label = file.findMessageTypeByName("Label");

        ServiceDescriptor intake = file.findServiceByName("Intake");
        var echo = DynamicGrpcCalls.methodDescriptor(intake.findMethodByName("Echo"));
        serverName = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(serverName)
                .addService(ServerServiceDefinition
                        .builder(io.grpc.ServiceDescriptor.newBuilder(intake.getFullName())
                                .addMethod(echo).build())
                        .addMethod(echo, ServerCalls.asyncUnaryCall((request, out) -> {
                            out.onNext(request);
                            out.onCompleted();
                        }))
                        .build())
                .build()
                .start();
    }

    @AfterAll
    static void stop() {
        server.shutdownNow();
    }

    @BeforeEach
    void setUp() {
        provider = new ScriptedProvider();
        InferenceCatalog catalog = new InferenceCatalog();
        InferenceEngines engines = new InferenceEngines(catalog, List.of(provider));
        engines.register(ModelEntry.newBuilder()
                .setId(MODEL)
                .setProvider("scripted")
                .setEndpoint("in-process://scripted")
                .setCapabilities(ModelCapabilities.newBuilder().setStructuredOutput(true))
                .build());
        // Workflow execution supplies its exact resolved descriptor. Deliberately leave
        // the generator registry empty to prove inline/action-scoped schemas work.
        DescriptorRegistry descriptors = new DescriptorRegistry();
        generator = new StructuredGenerator(engines, descriptors);
    }

    private static String resource(String name) {
        try (InputStream in = StructuredWorkflowReplayTest.class.getClassLoader()
                .getResourceAsStream(name)) {
            if (in == null) {
                throw new IllegalStateException(name + " not on the test classpath");
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static DynamicMessage ticket(String title) {
        return DynamicMessage.newBuilder(ticket)
                .setField(ticket.findFieldByName("title"), title).build();
    }

    private static DynamicMessage form(String name, int quantity) {
        return DynamicMessage.newBuilder(form)
                .setField(form.findFieldByName("name"), name)
                .setField(form.findFieldByName("quantity"), quantity)
                .build();
    }

    private WorkflowRunner runner() {
        return new WorkflowRunner(
                step -> InProcessChannelBuilder.forName(serverName).build(), generator);
    }

    /** A workflow whose only step fills the intake form with the scripted model. */
    private static CompiledWorkflow structuredWorkflow() {
        return new CompiledWorkflow("fill-intake", List.of(file), ticket, 10_000,
                List.of(CompiledWorkflow.Step.structured("fill", form, MODEL, 0)), null);
    }

    /** A gRPC echo step feeding the structured step, projecting the form's name. */
    private static CompiledWorkflow mixedWorkflow() {
        return new CompiledWorkflow("echo-and-fill", List.of(file), ticket, 10_000,
                List.of(CompiledWorkflow.Step.grpc("echo", "in-process", false,
                                CompiledWorkflow.resolveMethod(List.of(file),
                                        "workflow.structured.test.Intake/Echo"),
                                null, List.of("title = input.title"), List.of(),
                                false, 0, ""),
                        CompiledWorkflow.Step.structured("fill", form, MODEL, 0)),
                new CompiledWorkflow.Output(label, List.of("label = fill.name"), List.of()));
    }

    private static ArtifactReference save(ArtifactRepository artifacts,
                                          com.google.protobuf.Message message)
            throws IOException {
        return artifacts.save(message.toByteArray(), "application/x-protobuf", true);
    }

    private static RunEvidence tampered(RunEvidence honest,
                                        ai.protomolt.proto.grpc.workflow.v1
                                                .StructuredGenerationEvidence structured) {
        return honest.toBuilder()
                .setSteps(0, honest.getSteps(0).toBuilder()
                        .setStructured(structured).build())
                .build();
    }

    @Test
    void repairedGenerationRecordsBoundedEvidenceAndReplaysOffline(@TempDir Path dir)
            throws Exception {
        provider.script("this is not json", VALID_FORM);
        ArtifactRepository artifacts = new FileSystemArtifactRepository(dir.resolve("a"));
        RunEvidenceRepository runs = new FileSystemRunEvidenceRepository(dir.resolve("r"));
        CompiledWorkflow definition = structuredWorkflow();

        RunEvidence evidence = new WorkflowRunRecorder(runner(), artifacts, runs)
                .record("run-1", null, definition, ticket("anything"));

        assertThat(provider.invocations()).isEqualTo(2);
        var step = evidence.getSteps(0);
        assertThat(step.getMethod()).isEmpty();
        assertThat(step.getStatus()).isEqualTo(StepStatus.STEP_STATUS_SUCCEEDED);
        assertThat(step.getGrpcStatusCode()).isEqualTo(0);
        assertThat(step.getRequestArtifact().getRedacted()).isTrue();
        assertThat(step.getResponseArtifact().getRedacted()).isTrue();

        var structured = step.getStructured();
        assertThat(structured.getTargetType()).isEqualTo(FORM);
        assertThat(structured.getModel()).isEqualTo(MODEL);
        assertThat(structured.getProvider()).isEqualTo("scripted");
        assertThat(structured.getModelVersion()).isEqualTo("scripted-v1");
        assertThat(structured.getPromptFingerprint()).matches("[0-9a-f]{64}");
        assertThat(structured.getSchemaFingerprint()).matches("[0-9a-f]{64}");
        assertThat(structured.getValidationPassed()).isTrue();
        assertThat(structured.getAttemptsCount()).isEqualTo(2);
        assertThat(structured.getAttempts(0).getOutcome())
                .isEqualTo(AttemptOutcome.ATTEMPT_OUTCOME_PARSE_FAILED);
        assertThat(structured.getAttempts(1).getOutcome())
                .isEqualTo(AttemptOutcome.ATTEMPT_OUTCOME_SUCCEEDED);
        assertThat(structured.getTotalUsage()).isEqualTo(Usage.newBuilder()
                .setPromptTokens(20).setCompletionTokens(10).build());

        // The bounded contract: raw model output and retry feedback are nowhere in
        // the evidence or the stored artifacts.
        assertThat(new String(evidence.toByteArray(), StandardCharsets.ISO_8859_1))
                .doesNotContain("this is not json");
        byte[] responseBytes = artifacts.find(step.getResponseArtifact().getSha256())
                .orElseThrow().content();
        assertThat(new String(responseBytes, StandardCharsets.ISO_8859_1))
                .doesNotContain("this is not json");

        Workflow workflow = WorkflowCompiler.compile(definition);
        WorkflowReplay.ReplayResult replay = WorkflowReplay.replay(
                workflow, evidence, List.of(file), artifacts);
        assertThat(replay.ok()).as(replay.failure()).isTrue();
    }

    @Test
    void alteredPromptOrSchemaFingerprintFailsReplayNamingTheStep(@TempDir Path dir)
            throws Exception {
        provider.script(VALID_FORM);
        ArtifactRepository artifacts = new FileSystemArtifactRepository(dir.resolve("a"));
        RunEvidenceRepository runs = new FileSystemRunEvidenceRepository(dir.resolve("r"));
        CompiledWorkflow definition = structuredWorkflow();
        RunEvidence honest = new WorkflowRunRecorder(runner(), artifacts, runs)
                .record("run-2", null, definition, ticket("anything"));
        Workflow workflow = WorkflowCompiler.compile(definition);

        RunEvidence promptForgery = tampered(honest, honest.getSteps(0).getStructured()
                .toBuilder().setPromptFingerprint("f".repeat(64)).build());
        WorkflowReplay.ReplayResult promptResult = WorkflowReplay.replay(
                workflow, promptForgery, List.of(file), artifacts);
        assertThat(promptResult.ok()).isFalse();
        assertThat(promptResult.failure()).contains("fill").contains("prompt fingerprint");

        RunEvidence schemaForgery = tampered(honest, honest.getSteps(0).getStructured()
                .toBuilder().setSchemaFingerprint("f".repeat(64)).build());
        WorkflowReplay.ReplayResult schemaResult = WorkflowReplay.replay(
                workflow, schemaForgery, List.of(file), artifacts);
        assertThat(schemaResult.ok()).isFalse();
        assertThat(schemaResult.failure()).contains("fill").contains("schema fingerprint");
    }

    @Test
    void aRecordedResponseThatFailsValidationFailsReplay(@TempDir Path dir) throws Exception {
        provider.script(VALID_FORM);
        ArtifactRepository artifacts = new FileSystemArtifactRepository(dir.resolve("a"));
        RunEvidenceRepository runs = new FileSystemRunEvidenceRepository(dir.resolve("r"));
        CompiledWorkflow definition = structuredWorkflow();
        RunEvidence honest = new WorkflowRunRecorder(runner(), artifacts, runs)
                .record("run-3", null, definition, ticket("anything"));

        // The forgery: swap the typed output for one that breaks the form's rules.
        // The evidence still claims validation passed; replay re-validates and disagrees.
        RunEvidence forged = honest.toBuilder()
                .setSteps(0, honest.getSteps(0).toBuilder()
                        .setResponseArtifact(save(artifacts, form("Al", 500)))
                        .build())
                .build();

        WorkflowReplay.ReplayResult result = WorkflowReplay.replay(
                WorkflowCompiler.compile(definition), forged, List.of(file), artifacts);
        assertThat(result.ok()).isFalse();
        assertThat(result.failure()).contains("fill").contains("validation");
    }

    @Test
    void anInconsistentAttemptHistoryIsRejected(@TempDir Path dir) throws Exception {
        provider.script(VALID_FORM);
        ArtifactRepository artifacts = new FileSystemArtifactRepository(dir.resolve("a"));
        RunEvidenceRepository runs = new FileSystemRunEvidenceRepository(dir.resolve("r"));
        CompiledWorkflow definition = structuredWorkflow();
        RunEvidence honest = new WorkflowRunRecorder(runner(), artifacts, runs)
                .record("run-4", null, definition, ticket("anything"));

        // Token sums that do not add up are contract-invalid evidence.
        RunEvidence forged = tampered(honest, honest.getSteps(0).getStructured().toBuilder()
                .setTotalUsage(Usage.newBuilder()
                        .setPromptTokens(999).setCompletionTokens(10).build())
                .build());
        assertThatThrownBy(() -> WorkflowReplay.replay(
                WorkflowCompiler.compile(definition), forged, List.of(file), artifacts))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("total_usage");
    }

    @Test
    void anUnknownTargetTypeFailsReplayClearly(@TempDir Path dir) throws Exception {
        provider.script(VALID_FORM);
        ArtifactRepository artifacts = new FileSystemArtifactRepository(dir.resolve("a"));
        RunEvidenceRepository runs = new FileSystemRunEvidenceRepository(dir.resolve("r"));
        CompiledWorkflow definition = structuredWorkflow();
        RunEvidence honest = new WorkflowRunRecorder(runner(), artifacts, runs)
                .record("run-5", null, definition, ticket("anything"));

        // The workflow now claims the step filled a type the schema does not carry.
        Workflow workflow = WorkflowCompiler.compile(definition);
        Workflow edited = workflow.toBuilder()
                .setSteps(0, workflow.getSteps(0).toBuilder()
                        .setStructured(workflow.getSteps(0).getStructured().toBuilder()
                                .setTargetType("workflow.structured.test.Ghost").build())
                        .build())
                .build();
        RunEvidence evidence = honest.toBuilder()
                .setWorkflowFingerprint(
                        ai.protomolt.proto.grpc.workflow.WorkflowValidation.fingerprint(edited))
                .setSteps(0, honest.getSteps(0).toBuilder()
                        .setStructured(honest.getSteps(0).getStructured().toBuilder()
                                .setTargetType("workflow.structured.test.Ghost").build())
                        .build())
                .build();

        WorkflowReplay.ReplayResult result = WorkflowReplay.replay(
                edited, evidence, List.of(file), artifacts);
        assertThat(result.ok()).isFalse();
        assertThat(result.failure()).contains("fill")
                .contains("Ghost").contains("not in the replay schema");
    }

    @Test
    void aMixedWorkflowRecordsAndReplaysWithTheTypedOutputInScope(@TempDir Path dir)
            throws Exception {
        provider.script(VALID_FORM);
        ArtifactRepository artifacts = new FileSystemArtifactRepository(dir.resolve("a"));
        RunEvidenceRepository runs = new FileSystemRunEvidenceRepository(dir.resolve("r"));
        CompiledWorkflow definition = mixedWorkflow();
        assertThat(new WorkflowVerifier().verify(definition)).isEmpty();

        RunEvidence evidence = new WorkflowRunRecorder(runner(), artifacts, runs)
                .record("run-6", null, definition, ticket("echoed"));

        assertThat(evidence.getStatus()).isEqualTo(RunStatus.RUN_STATUS_SUCCEEDED);
        assertThat(evidence.getStepsCount()).isEqualTo(2);
        assertThat(evidence.getSteps(0).getMethod())
                .isEqualTo("workflow.structured.test.Intake/Echo");
        assertThat(evidence.getSteps(1).getStructured().getModel()).isEqualTo(MODEL);

        // The output projection mapped the structured step's typed output.
        var output = DynamicMessage.parseFrom(label,
                artifacts.find(evidence.getOutputArtifact().getSha256())
                        .orElseThrow().content());
        assertThat(output.getField(label.findFieldByName("label"))).isEqualTo("Ada Lovelace");

        WorkflowReplay.ReplayResult replay = WorkflowReplay.replay(
                WorkflowCompiler.compile(definition), evidence, List.of(file), artifacts);
        assertThat(replay.ok()).as(replay.failure()).isTrue();
    }

    @Test
    void aStructuredStepWithoutAGeneratorFailsFastNamingTheStep() {
        WorkflowRunner noGenerator = new WorkflowRunner(
                step -> InProcessChannelBuilder.forName(serverName).build());

        assertThatThrownBy(() -> noGenerator.run(structuredWorkflow(), ticket("anything")))
                .isInstanceOfSatisfying(WorkflowRunner.WorkflowExecutionException.class, e -> {
                    assertThat(e.step()).isEqualTo("fill");
                    assertThat(e.kind()).isEqualTo(WorkflowRunner.FailureKind.STRUCTURED);
                    assertThat(e.getMessage()).contains("fill")
                            .contains("StructuredGenerator");
                });
        assertThat(provider.invocations()).isZero();
    }

    @Test
    void aFailedGenerationRecordsItsAttemptsAndEndsTheRun(@TempDir Path dir)
            throws Exception {
        provider.script("garbage one", "garbage two", "garbage three");
        ArtifactRepository artifacts = new FileSystemArtifactRepository(dir.resolve("a"));
        RunEvidenceRepository runs = new FileSystemRunEvidenceRepository(dir.resolve("r"));
        CompiledWorkflow definition = structuredWorkflow();

        WorkflowRunRecorder recorder = new WorkflowRunRecorder(runner(), artifacts, runs);
        assertThatThrownBy(() -> recorder.record("run-7", null, definition, ticket("anything")))
                .isInstanceOfSatisfying(WorkflowRunner.WorkflowExecutionException.class,
                        e -> assertThat(e.step()).isEqualTo("fill"));

        RunEvidence evidence = runs.find("run-7").orElseThrow();
        assertThat(evidence.getStatus()).isEqualTo(RunStatus.RUN_STATUS_FAILED);
        var step = evidence.getSteps(0);
        assertThat(step.getStatus()).isEqualTo(StepStatus.STEP_STATUS_FAILED);
        var structured = step.getStructured();
        assertThat(structured.getValidationPassed()).isFalse();
        assertThat(structured.getAttemptsCount()).isEqualTo(3);
        assertThat(structured.getAttemptsList()).allSatisfy(attempt ->
                assertThat(attempt.getOutcome())
                        .isEqualTo(AttemptOutcome.ATTEMPT_OUTCOME_PARSE_FAILED));
        assertThat(structured.getTotalUsage()).isEqualTo(Usage.newBuilder()
                .setPromptTokens(30).setCompletionTokens(15).build());
        // Even on failure the raw garbage never lands in evidence.
        assertThat(new String(evidence.toByteArray(), StandardCharsets.ISO_8859_1))
                .doesNotContain("garbage");

        // A failed structured step ends the run; replay verifies what was recorded.
        WorkflowReplay.ReplayResult replay = WorkflowReplay.replay(
                WorkflowCompiler.compile(definition), evidence, List.of(file), artifacts);
        assertThat(replay.ok()).as(replay.failure()).isTrue();
    }

    @Test
    void providerDiagnosticsNeverEnterPersistedFailureEvidence(@TempDir Path dir)
            throws Exception {
        String secret = "provider-secret-response-body";
        provider.fail(secret);
        ArtifactRepository artifacts = new FileSystemArtifactRepository(dir.resolve("a"));
        RunEvidenceRepository runs = new FileSystemRunEvidenceRepository(dir.resolve("r"));

        assertThatThrownBy(() -> new WorkflowRunRecorder(runner(), artifacts, runs)
                .record("run-secret", null, structuredWorkflow(), ticket("anything")))
                .isInstanceOf(WorkflowRunner.WorkflowExecutionException.class)
                .hasMessageContaining(secret);

        RunEvidence evidence = runs.find("run-secret").orElseThrow();
        assertThat(evidence.getFailureSummary()).doesNotContain(secret);
        assertThat(evidence.getSteps(0).getSummary()).doesNotContain(secret);
        assertThat(new String(evidence.toByteArray(), StandardCharsets.ISO_8859_1))
                .doesNotContain(secret);
    }

    @Test
    void theVerifierRejectsGrpcConcernsOnStructuredSteps() {
        assertThat(new WorkflowVerifier().verify(structuredWorkflow())).isEmpty();
        assertThat(new WorkflowVerifier().verify(mixedWorkflow())).isEmpty();

        CompiledWorkflow.Step withRules = new CompiledWorkflow.Step("fill",
                CompiledWorkflow.Step.STRUCTURED_DEPENDENCY, false, null,
                "input.title != ''", List.of("name = input.title"), List.of(),
                false, 0, "", new CompiledWorkflow.StructuredSpec(form, MODEL, 0));
        CompiledWorkflow broken = new CompiledWorkflow("broken", List.of(file), ticket,
                10_000, List.of(withRules), null);
        List<WorkflowVerifier.Finding> findings = new WorkflowVerifier().verify(broken);
        assertThat(findings).extracting(WorkflowVerifier.Finding::kind)
                .contains("structured");
        assertThat(findings).extracting(WorkflowVerifier.Finding::error)
                .anySatisfy(error -> assertThat(error).contains("gate"))
                .anySatisfy(error -> assertThat(error).contains("no mapping rules"));

        CompiledWorkflow.Step outOfSchema = CompiledWorkflow.Step.structured("fill",
                form, MODEL, 4);
        CompiledWorkflow wrongFiles = new CompiledWorkflow("wrong", List.of(file), ticket,
                10_000, List.of(outOfSchema), null);
        assertThat(new WorkflowVerifier().verify(wrongFiles))
                .extracting(WorkflowVerifier.Finding::error)
                .anySatisfy(error -> assertThat(error).contains("between 0 and 3"));
    }

    /**
     * The scripted in-process provider: replays queued raw response texts and counts
     * invocations. No container, no GPU, no network.
     */
    private static final class ScriptedProvider implements InferenceProvider {

        private final Queue<String> script = new ArrayDeque<>();
        private final AtomicInteger invocations = new AtomicInteger();
        private String failure;

        void script(String... responses) {
            script.addAll(List.of(responses));
        }

        int invocations() {
            return invocations.get();
        }

        void fail(String message) {
            failure = message;
        }

        @Override
        public String id() {
            return "scripted";
        }

        @Override
        public GenerateResponse generate(ModelEntry model, GenerateRequest request) {
            invocations.incrementAndGet();
            if (failure != null) {
                throw new InferenceException(failure);
            }
            String next = script.poll();
            if (next == null) {
                throw new InferenceException("scripted provider ran out of responses");
            }
            return GenerateResponse.newBuilder()
                    .setText(next)
                    .setModel(model.getId())
                    .setProvider(id())
                    .setModelVersion("scripted-v1")
                    .setFinishReason(FinishReason.FINISH_REASON_STOP)
                    .setUsage(Usage.newBuilder().setPromptTokens(10).setCompletionTokens(5))
                    .build();
        }

        @Override
        public void generateStream(ModelEntry model, GenerateStreamRequest request,
                ChunkObserver observer) {
            throw new InferenceException("the scripted provider does not stream");
        }
    }
}
