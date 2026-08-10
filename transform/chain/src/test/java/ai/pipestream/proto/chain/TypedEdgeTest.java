package ai.pipestream.proto.chain;

import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.grpc.invoke.DynamicGrpcCalls;
import ai.pipestream.proto.grpc.recipe.ArtifactRepository;
import ai.pipestream.proto.grpc.recipe.FileSystemArtifactRepository;
import ai.pipestream.proto.grpc.recipe.FileSystemRunEvidenceRepository;
import ai.pipestream.proto.grpc.recipe.RecipeValidation;
import ai.pipestream.proto.grpc.recipe.RunEvidenceRepository;
import ai.pipestream.proto.grpc.recipe.v1.ArtifactReference;
import ai.pipestream.proto.grpc.recipe.v1.GrpcRecipe;
import ai.pipestream.proto.grpc.recipe.v1.RunEvidence;
import ai.pipestream.proto.grpc.recipe.v1.RunStatus;
import ai.pipestream.proto.grpc.recipe.v1.StepStatus;
import ai.pipestream.proto.inference.spi.ChunkObserver;
import ai.pipestream.proto.inference.spi.InferenceCatalog;
import ai.pipestream.proto.inference.spi.InferenceEngines;
import ai.pipestream.proto.inference.spi.InferenceException;
import ai.pipestream.proto.inference.spi.InferenceProvider;
import ai.pipestream.proto.inference.structured.StructuredGenerator;
import ai.pipestream.proto.inference.v1.FinishReason;
import ai.pipestream.proto.inference.v1.GenerateRequest;
import ai.pipestream.proto.inference.v1.GenerateResponse;
import ai.pipestream.proto.inference.v1.GenerateStreamRequest;
import ai.pipestream.proto.inference.v1.ModelCapabilities;
import ai.pipestream.proto.inference.v1.ModelEntry;
import ai.pipestream.proto.inference.v1.Usage;
import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.ServiceDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import io.grpc.Server;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
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
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Typed inter-step edges and bounded fan-out end to end: an upstream gRPC result is
 * mapped and projected into a structured step's grounding (excluded fields never reach
 * the model), an invalid projected value is rejected before any provider invocation,
 * fan-out runs branches with deterministic identities under a concurrency bound, the
 * failure policies decide the step, and every recorded run replays fully offline -
 * including every forgery attempt. No container, no GPU, no network.
 */
class TypedEdgeTest {

    private static final String VALIDATE = "ai/pipestream/proto/validate/v1/validate.proto";
    private static final String PROJECTION =
            "ai/pipestream/proto/projection/v1/projection.proto";

    private static final String PROTO = """
            syntax = "proto3";
            package chain.edge.test;
            import "ai/pipestream/proto/validate/v1/validate.proto";
            import "ai/pipestream/proto/projection/v1/projection.proto";
            message Ticket { string title = 1; }
            message LookupResult {
              string doc_id = 1;
              string title = 2;
              string internal_notes = 3;
            }
            // The consumer-visible grounding: internal_notes is never projected.
            message DocGrounding {
              option (ai.pipestream.proto.projection.v1.sources) = {
                source: "chain.edge.test.LookupResult"
              };
              string doc_id = 1 [
                (ai.pipestream.proto.projection.v1.from) = {paths: {path: "doc_id"}},
                (ai.pipestream.proto.validate.v1.field) = {
                  string: {min_len: 3, max_len: 64}
                }
              ];
              string title = 2 [(ai.pipestream.proto.projection.v1.from) = {
                paths: {path: "title"}
              }];
            }
            message Summary {
              string headline = 1 [(ai.pipestream.proto.validate.v1.field) = {
                required: true
                string: {min_len: 3, max_len: 200}
              }];
            }
            message Summaries { repeated Summary summaries = 1; }
            message Batch { repeated Ticket items = 1; }
            message BatchResult { repeated Ticket results = 1; }
            service Lookup { rpc Fetch(Ticket) returns (LookupResult); }
            service Worker { rpc Process(Ticket) returns (Ticket); }
            """;

    private static final String MODEL = "edge-model";
    private static final String LOOKUP_RESULT = "chain.edge.test.LookupResult";
    private static final String GROUNDING = "chain.edge.test.DocGrounding";
    private static final String SUMMARY = "chain.edge.test.Summary";
    private static final String SECRET_NOTES = "do-not-leak-internal-notes";

    private static FileDescriptor file;
    private static Descriptor ticket;
    private static Descriptor lookupResult;
    private static Descriptor grounding;
    private static Descriptor summary;
    private static Descriptor summaries;
    private static Descriptor batch;
    private static Descriptor batchResult;
    private static Server server;
    private static String serverName;

    /** The fixed LookupResult the fake Fetch returns; reset per test. */
    private static final AtomicReference<DynamicMessage> fetchResult =
            new AtomicReference<>();
    /** Per-test Worker behavior: latency, failure, and in-flight tracking. */
    private static final AtomicInteger inFlight = new AtomicInteger();
    private static final AtomicInteger maxInFlight = new AtomicInteger();
    private static final AtomicInteger workerCalls = new AtomicInteger();

    private ScriptedProvider provider;
    private StructuredGenerator generator;

    @BeforeAll
    static void compile() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add(VALIDATE, resource(VALIDATE), "test")
                .add(PROJECTION, resource(PROJECTION), "test")
                .add("chain/edge/test/edge.proto", PROTO, "test").build());
        file = compiled.descriptorFor("chain/edge/test/edge.proto").orElseThrow();
        ticket = file.findMessageTypeByName("Ticket");
        lookupResult = file.findMessageTypeByName("LookupResult");
        grounding = file.findMessageTypeByName("DocGrounding");
        summary = file.findMessageTypeByName("Summary");
        summaries = file.findMessageTypeByName("Summaries");
        batch = file.findMessageTypeByName("Batch");
        batchResult = file.findMessageTypeByName("BatchResult");

        ServiceDescriptor lookup = file.findServiceByName("Lookup");
        ServiceDescriptor worker = file.findServiceByName("Worker");
        var fetch = DynamicGrpcCalls.methodDescriptor(lookup.findMethodByName("Fetch"));
        var process = DynamicGrpcCalls.methodDescriptor(worker.findMethodByName("Process"));
        serverName = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(serverName)
                .addService(ServerServiceDefinition
                        .builder(io.grpc.ServiceDescriptor.newBuilder(lookup.getFullName())
                                .addMethod(fetch).build())
                        .addMethod(fetch, ServerCalls.asyncUnaryCall((request, out) -> {
                            out.onNext(fetchResult.get());
                            out.onCompleted();
                        }))
                        .build())
                .addService(ServerServiceDefinition
                        .builder(io.grpc.ServiceDescriptor.newBuilder(worker.getFullName())
                                .addMethod(process).build())
                        .addMethod(process, ServerCalls.asyncUnaryCall((request, out) -> {
                            workerCalls.incrementAndGet();
                            int now = inFlight.incrementAndGet();
                            maxInFlight.accumulateAndGet(now, Math::max);
                            // The gauge window closes before the response goes out:
                            // the client releases its permit only after the response
                            // arrives, so an overlap here is a real semaphore breach.
                            DynamicMessage message = (DynamicMessage) request;
                            String title = (String) message.getField(
                                    ticket.findFieldByName("title"));
                            DynamicMessage response = null;
                            io.grpc.StatusRuntimeException failure = null;
                            try {
                                if (title.contains("bad")) {
                                    failure = Status.INVALID_ARGUMENT
                                            .withDescription("rejected item: " + title)
                                            .asRuntimeException();
                                } else {
                                    Thread.sleep(title.contains("slow") ? 400 : 100);
                                    response = DynamicMessage.newBuilder(ticket)
                                            .setField(ticket.findFieldByName("title"),
                                                    title + "-done")
                                            .build();
                                }
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                failure = Status.CANCELLED.asRuntimeException();
                            } finally {
                                inFlight.decrementAndGet();
                            }
                            if (failure != null) {
                                out.onError(failure);
                            } else {
                                out.onNext(response);
                                out.onCompleted();
                            }
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
        inFlight.set(0);
        maxInFlight.set(0);
        workerCalls.set(0);
        fetchResult.set(lookup("24-cv-00117", "State v. Example", SECRET_NOTES));
        provider = new ScriptedProvider();
        InferenceCatalog catalog = new InferenceCatalog();
        InferenceEngines engines = new InferenceEngines(catalog, List.of(provider));
        engines.register(ModelEntry.newBuilder()
                .setId(MODEL)
                .setProvider("scripted")
                .setEndpoint("in-process://scripted")
                .setCapabilities(ModelCapabilities.newBuilder().setStructuredOutput(true))
                .build());
        // Chain execution supplies its exact resolved descriptor; the generator
        // registry stays empty to prove action-scoped schemas work.
        generator = new StructuredGenerator(engines, new DescriptorRegistry());
    }

    private static String resource(String name) {
        try (InputStream in = TypedEdgeTest.class.getClassLoader()
                .getResourceAsStream(name)) {
            if (in == null) {
                throw new IllegalStateException(name + " not on the test classpath");
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static DynamicMessage ticketOf(String title) {
        return DynamicMessage.newBuilder(ticket)
                .setField(ticket.findFieldByName("title"), title).build();
    }

    private static DynamicMessage lookup(String docId, String title, String notes) {
        return DynamicMessage.newBuilder(lookupResult)
                .setField(lookupResult.findFieldByName("doc_id"), docId)
                .setField(lookupResult.findFieldByName("title"), title)
                .setField(lookupResult.findFieldByName("internal_notes"), notes)
                .build();
    }

    private static DynamicMessage batchOf(String... titles) {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(batch);
        for (String title : titles) {
            builder.addRepeatedField(batch.findFieldByName("items"), ticketOf(title));
        }
        return builder.build();
    }

    private ChainRunner runner() {
        return new ChainRunner(
                step -> InProcessChannelBuilder.forName(serverName).build(), generator);
    }

    private static ChainDefinition.EdgeSpec groundingEdge(boolean validate) {
        return new ChainDefinition.EdgeSpec(List.of("fetch"), lookupResult,
                List.of("doc_id = fetch.doc_id", "title = fetch.title",
                        "internal_notes = fetch.internal_notes"),
                List.of(), grounding, validate);
    }

    /** fetch (gRPC) feeds fill (structured) through a projecting, validating edge. */
    private static ChainDefinition groundedChain(boolean validate) {
        return new ChainDefinition("fetch-and-fill", List.of(file), ticket, 15_000,
                List.of(ChainDefinition.Step.grpc("fetch", "in-process", false,
                                ChainDefinition.resolveMethod(List.of(file),
                                        "chain.edge.test.Lookup/Fetch"),
                                null, List.of("title = input.title"), List.of(),
                                false, 0, ""),
                        new ChainDefinition.Step("fill",
                                ChainDefinition.Step.STRUCTURED_DEPENDENCY, false, null,
                                null, List.of(), List.of(), false, 0, "",
                                new ChainDefinition.StructuredSpec(summary, MODEL, 0),
                                groundingEdge(validate), null)),
                null);
    }

    /** Fan-out over a batch input into the Worker fake. */
    private static ChainDefinition fanOutChain(int maxConcurrency,
                                               ChainDefinition.BranchFailurePolicy policy) {
        return new ChainDefinition("process-batch", List.of(file), batch, 30_000,
                List.of(new ChainDefinition.Step("process", "in-process", false,
                        ChainDefinition.resolveMethod(List.of(file),
                                "chain.edge.test.Worker/Process"),
                        null, List.of(), List.of(), false, 0, "", null,
                        new ChainDefinition.EdgeSpec(List.of("input"), batch,
                                List.of("items = input.items"), List.of(), null, false),
                        new ChainDefinition.FanOutSpec("items", 8, maxConcurrency, policy,
                                batchResult, "results"))),
                null);
    }

    /** Fan-out over a batch input into a structured step. */
    private static ChainDefinition structuredFanOutChain() {
        return new ChainDefinition("summarize-batch", List.of(file), batch, 30_000,
                List.of(new ChainDefinition.Step("summarize",
                        ChainDefinition.Step.STRUCTURED_DEPENDENCY, false, null,
                        null, List.of(), List.of(), false, 0, "",
                        new ChainDefinition.StructuredSpec(summary, MODEL, 0),
                        new ChainDefinition.EdgeSpec(List.of("input"), batch,
                                List.of("items = input.items"), List.of(), null, false),
                        new ChainDefinition.FanOutSpec("items", 8, 1,
                                ChainDefinition.BranchFailurePolicy.CONTINUE,
                                summaries, "summaries"))),
                null);
    }

    private static ArtifactReference save(ArtifactRepository artifacts, Message message)
            throws IOException {
        return artifacts.save(message.toByteArray(), "application/x-protobuf", true);
    }

    private static RecipeReplay.ReplayResult replay(ChainDefinition chain,
                                                    RunEvidence evidence,
                                                    ArtifactRepository artifacts)
            throws IOException {
        return RecipeReplay.replay(ChainRecipeCompiler.compile(chain), evidence,
                List.of(file), artifacts);
    }

    @Test
    void projectedGroundingReachesTheModelWithoutExcludedFields(@TempDir Path dir)
            throws Exception {
        provider.script("{\"headline\": \"State v. Example decided\"}");
        ArtifactRepository artifacts = new FileSystemArtifactRepository(dir.resolve("a"));
        RunEvidenceRepository runs = new FileSystemRunEvidenceRepository(dir.resolve("r"));
        ChainDefinition chain = groundedChain(false);
        assertThat(new ChainVerifier().verify(chain)).isEmpty();

        RunEvidence evidence = new RecipeRunRecorder(runner(), artifacts, runs)
                .record("run-edge-1", null, chain, ticketOf("anything"));

        assertThat(provider.invocations()).isEqualTo(1);
        // The grounding packet - the rendered instructions the model was briefed with -
        // carries only the projected fields; the excluded value appears nowhere.
        String instructions = provider.lastRequest().getMessages(0).getContent();
        assertThat(instructions).contains("24-cv-00117").contains("State v. Example");
        assertThat(instructions).doesNotContain(SECRET_NOTES);
        assertThat(instructions).contains("Document-specific context");

        // The edge evidence binds the exact edge and records the clean verdict.
        var step = evidence.getSteps(1);
        assertThat(step.getStatus()).isEqualTo(StepStatus.STEP_STATUS_SUCCEEDED);
        assertThat(step.hasEdge()).isTrue();
        GrpcRecipe recipe = ChainRecipeCompiler.compile(chain);
        assertThat(step.getEdge().getEdgeFingerprint())
                .isEqualTo(RecipeValidation.edgeFingerprint(recipe.getSteps(1)));
        assertThat(step.getEdge().getValidationPassed()).isTrue();
        assertThat(step.getEdge().getSourceCount()).isEqualTo(1);
        assertThat(step.getEdge().getItemCount()).isZero();
        assertThat(step.getEdge().getBranchesCount()).isZero();
        assertThat(step.getStructured().getModel()).isEqualTo(MODEL);

        // The request artifact is the edge-produced (pre-projection) message.
        Message produced = DynamicMessage.parseFrom(lookupResult,
                artifacts.find(step.getRequestArtifact().getSha256())
                        .orElseThrow().content());
        assertThat(produced.getField(lookupResult.findFieldByName("internal_notes")))
                .isEqualTo(SECRET_NOTES);

        RecipeReplay.ReplayResult replay = replay(chain, evidence, artifacts);
        assertThat(replay.ok()).as(replay.failure()).isTrue();
    }

    @Test
    void anInvalidProjectedValueIsRejectedBeforeAnyInvocation(@TempDir Path dir)
            throws Exception {
        fetchResult.set(lookup("x", "State v. Example", SECRET_NOTES));
        ArtifactRepository artifacts = new FileSystemArtifactRepository(dir.resolve("a"));
        RunEvidenceRepository runs = new FileSystemRunEvidenceRepository(dir.resolve("r"));
        ChainDefinition chain = groundedChain(true);
        assertThat(new ChainVerifier().verify(chain)).isEmpty();

        RecipeRunRecorder recorder = new RecipeRunRecorder(runner(), artifacts, runs);
        assertThatThrownBy(() -> recorder.record("run-edge-2", null, chain,
                ticketOf("anything")))
                .isInstanceOfSatisfying(ChainRunner.ChainExecutionException.class, e -> {
                    assertThat(e.step()).isEqualTo("fill");
                    assertThat(e.kind()).isEqualTo(ChainRunner.FailureKind.VALIDATION);
                    assertThat(e.getMessage()).contains("before the step executed");
                });
        assertThat(provider.invocations()).isZero();

        RunEvidence evidence = runs.find("run-edge-2").orElseThrow();
        assertThat(evidence.getStatus()).isEqualTo(RunStatus.RUN_STATUS_FAILED);
        var step = evidence.getSteps(1);
        assertThat(step.getStatus()).isEqualTo(StepStatus.STEP_STATUS_FAILED);
        assertThat(step.getEdge().getValidationPassed()).isFalse();
        assertThat(step.hasRequestArtifact()).isTrue();

        // The honest failed run replays; a flipped verdict fails naming the step.
        RecipeReplay.ReplayResult replay = replay(chain, evidence, artifacts);
        assertThat(replay.ok()).as(replay.failure()).isTrue();

        RunEvidence forged = evidence.toBuilder()
                .setSteps(1, step.toBuilder()
                        .setEdge(step.getEdge().toBuilder()
                                .setValidationPassed(true).build())
                        .build())
                .build();
        RecipeReplay.ReplayResult forgedReplay = replay(chain, forged, artifacts);
        assertThat(forgedReplay.ok()).isFalse();
        assertThat(forgedReplay.failure()).contains("fill").contains("verdict");
    }

    @Test
    void fanOutCollectsInIndexOrderWithDeterministicIdentities(@TempDir Path dir)
            throws Exception {
        ArtifactRepository artifacts = new FileSystemArtifactRepository(dir.resolve("a"));
        RunEvidenceRepository runs = new FileSystemRunEvidenceRepository(dir.resolve("r"));
        ChainDefinition chain = fanOutChain(2,
                ChainDefinition.BranchFailurePolicy.CONTINUE);
        assertThat(new ChainVerifier().verify(chain)).isEmpty();

        RunEvidence evidence = new RecipeRunRecorder(runner(), artifacts, runs)
                .record("run-edge-3", null, chain, batchOf("slow", "fast"));

        // Branch 1 (fast) completes long before branch 0 (slow); the collect stays in
        // index order and the branch identities are stable.
        assertThat(maxInFlight.get()).isEqualTo(2);
        var step = evidence.getSteps(0);
        assertThat(step.getStatus()).isEqualTo(StepStatus.STEP_STATUS_SUCCEEDED);
        assertThat(step.getEdge().getItemCount()).isEqualTo(2);
        assertThat(step.getEdge().getBranchesCount()).isEqualTo(2);
        assertThat(step.getEdge().getBranches(0).getBranchId()).isEqualTo("process#0");
        assertThat(step.getEdge().getBranches(1).getBranchId()).isEqualTo("process#1");
        assertThat(step.getEdge().getBranchesList()).allSatisfy(branch ->
                assertThat(branch.getStatus()).isEqualTo(StepStatus.STEP_STATUS_SUCCEEDED));

        Message collected = DynamicMessage.parseFrom(batchResult,
                artifacts.find(step.getResponseArtifact().getSha256())
                        .orElseThrow().content());
        List<?> results = (List<?>) collected.getField(
                batchResult.findFieldByName("results"));
        assertThat(results).hasSize(2);
        assertThat(((Message) results.get(0)).getField(ticket.findFieldByName("title")))
                .isEqualTo("slow-done");
        assertThat(((Message) results.get(1)).getField(ticket.findFieldByName("title")))
                .isEqualTo("fast-done");

        RecipeReplay.ReplayResult replay = replay(chain, evidence, artifacts);
        assertThat(replay.ok()).as(replay.failure()).isTrue();
    }

    @Test
    void fanOutRespectsTheConcurrencyBound(@TempDir Path dir) throws Exception {
        ArtifactRepository artifacts = new FileSystemArtifactRepository(dir.resolve("a"));
        RunEvidenceRepository runs = new FileSystemRunEvidenceRepository(dir.resolve("r"));
        ChainDefinition chain = fanOutChain(1,
                ChainDefinition.BranchFailurePolicy.CONTINUE);

        RunEvidence evidence = new RecipeRunRecorder(runner(), artifacts, runs)
                .record("run-edge-4", null, chain, batchOf("one", "two"));

        assertThat(maxInFlight.get()).isEqualTo(1);
        assertThat(evidence.getSteps(0).getEdge().getBranchesList()).allSatisfy(branch ->
                assertThat(branch.getStatus()).isEqualTo(StepStatus.STEP_STATUS_SUCCEEDED));
        RecipeReplay.ReplayResult replay = replay(chain, evidence, artifacts);
        assertThat(replay.ok()).as(replay.failure()).isTrue();
    }

    @Test
    void failFastAbandonsRemainingBranchesAndFailsTheStep(@TempDir Path dir)
            throws Exception {
        ArtifactRepository artifacts = new FileSystemArtifactRepository(dir.resolve("a"));
        RunEvidenceRepository runs = new FileSystemRunEvidenceRepository(dir.resolve("r"));
        ChainDefinition chain = fanOutChain(2,
                ChainDefinition.BranchFailurePolicy.FAIL_FAST);

        RecipeRunRecorder recorder = new RecipeRunRecorder(runner(), artifacts, runs);
        assertThatThrownBy(() -> recorder.record("run-edge-5", null, chain,
                batchOf("bad", "slow")))
                .isInstanceOfSatisfying(ChainRunner.ChainExecutionException.class, e -> {
                    assertThat(e.step()).isEqualTo("process");
                    assertThat(e.getMessage()).contains("process#0");
                });

        RunEvidence evidence = runs.find("run-edge-5").orElseThrow();
        var edge = evidence.getSteps(0).getEdge();
        assertThat(edge.getItemCount()).isEqualTo(2);
        assertThat(edge.getBranchesCount()).isEqualTo(2);
        assertThat(edge.getBranches(0).getBranchId()).isEqualTo("process#0");
        assertThat(edge.getBranches(0).getStatus())
                .isEqualTo(StepStatus.STEP_STATUS_FAILED);
        assertThat(edge.getBranches(0).getSummary()).contains("INVALID_ARGUMENT");
        assertThat(edge.getBranches(1).getBranchId()).isEqualTo("process#1");
        assertThat(edge.getBranches(1).getStatus())
                .isEqualTo(StepStatus.STEP_STATUS_FAILED);

        RecipeReplay.ReplayResult replay = replay(chain, evidence, artifacts);
        assertThat(replay.ok()).as(replay.failure()).isTrue();
    }

    @Test
    void continueCollectsTheSurvivorsAndRecordsTheFailedBranch(@TempDir Path dir)
            throws Exception {
        ArtifactRepository artifacts = new FileSystemArtifactRepository(dir.resolve("a"));
        RunEvidenceRepository runs = new FileSystemRunEvidenceRepository(dir.resolve("r"));
        ChainDefinition chain = fanOutChain(2,
                ChainDefinition.BranchFailurePolicy.CONTINUE);

        RunEvidence evidence = new RecipeRunRecorder(runner(), artifacts, runs)
                .record("run-edge-6", null, chain, batchOf("bad", "good"));

        assertThat(evidence.getStatus()).isEqualTo(RunStatus.RUN_STATUS_SUCCEEDED);
        var edge = evidence.getSteps(0).getEdge();
        assertThat(edge.getBranches(0).getStatus())
                .isEqualTo(StepStatus.STEP_STATUS_FAILED);
        assertThat(edge.getBranches(0).getSummary()).contains("INVALID_ARGUMENT");
        assertThat(edge.getBranches(0).hasResponseArtifact()).isFalse();
        assertThat(edge.getBranches(1).getStatus())
                .isEqualTo(StepStatus.STEP_STATUS_SUCCEEDED);
        assertThat(edge.getBranches(1).hasResponseArtifact()).isTrue();

        Message collected = DynamicMessage.parseFrom(batchResult,
                artifacts.find(evidence.getSteps(0).getResponseArtifact().getSha256())
                        .orElseThrow().content());
        List<?> results = (List<?>) collected.getField(
                batchResult.findFieldByName("results"));
        assertThat(results).hasSize(1);
        assertThat(((Message) results.get(0)).getField(ticket.findFieldByName("title")))
                .isEqualTo("good-done");

        RecipeReplay.ReplayResult replay = replay(chain, evidence, artifacts);
        assertThat(replay.ok()).as(replay.failure()).isTrue();
    }

    @Test
    void structuredFanOutGroundsEachBranchOnItsOwnItem(@TempDir Path dir)
            throws Exception {
        // Each branch answers from its own grounding: the provider derives the
        // headline from the grounded item, so collection order is provable.
        provider.respondWith(request -> {
            String content = request.getMessages(0).getContent();
            String item = content.contains("alpha") ? "alpha" : "beta";
            return "{\"headline\": \"summary of " + item + "\"}";
        });
        ArtifactRepository artifacts = new FileSystemArtifactRepository(dir.resolve("a"));
        RunEvidenceRepository runs = new FileSystemRunEvidenceRepository(dir.resolve("r"));
        ChainDefinition chain = structuredFanOutChain();
        assertThat(new ChainVerifier().verify(chain)).isEmpty();

        RunEvidence evidence = new RecipeRunRecorder(runner(), artifacts, runs)
                .record("run-edge-7", null, chain, batchOf("alpha", "beta"));

        assertThat(provider.invocations()).isEqualTo(2);
        var edge = evidence.getSteps(0).getEdge();
        assertThat(edge.getItemCount()).isEqualTo(2);
        assertThat(edge.getBranchesList())
                .extracting(b -> b.getBranchId())
                .containsExactly("summarize#0", "summarize#1");

        Message collected = DynamicMessage.parseFrom(summaries,
                artifacts.find(evidence.getSteps(0).getResponseArtifact().getSha256())
                        .orElseThrow().content());
        List<?> collectedSummaries = (List<?>) collected.getField(
                summaries.findFieldByName("summaries"));
        assertThat(collectedSummaries).hasSize(2);
        assertThat(((Message) collectedSummaries.get(0)).getField(
                summary.findFieldByName("headline"))).isEqualTo("summary of alpha");
        assertThat(((Message) collectedSummaries.get(1)).getField(
                summary.findFieldByName("headline"))).isEqualTo("summary of beta");

        // A structured fan-out step records no per-step structured evidence: there is
        // one generation per branch, not one per step.
        assertThat(evidence.getSteps(0).hasStructured()).isFalse();
        assertThat(evidence.getSteps(0).getMethod()).isEmpty();

        RecipeReplay.ReplayResult replay = replay(chain, evidence, artifacts);
        assertThat(replay.ok()).as(replay.failure()).isTrue();
    }

    @Test
    void replayRejectsEveryEdgeForgeryNamingTheStep(@TempDir Path dir) throws Exception {
        ArtifactRepository artifacts = new FileSystemArtifactRepository(dir.resolve("a"));
        RunEvidenceRepository runs = new FileSystemRunEvidenceRepository(dir.resolve("r"));
        ChainDefinition chain = fanOutChain(2,
                ChainDefinition.BranchFailurePolicy.CONTINUE);
        RunEvidence honest = new RecipeRunRecorder(runner(), artifacts, runs)
                .record("run-edge-8", null, chain, batchOf("one", "two"));
        assertThat(replay(chain, honest, artifacts).ok()).isTrue();
        var step = honest.getSteps(0);

        // An altered edge fingerprint.
        RunEvidence fingerprintForgery = honest.toBuilder()
                .setSteps(0, step.toBuilder()
                        .setEdge(step.getEdge().toBuilder()
                                .setEdgeFingerprint("f".repeat(64)).build())
                        .build())
                .build();
        RecipeReplay.ReplayResult fingerprintResult =
                replay(chain, fingerprintForgery, artifacts);
        assertThat(fingerprintResult.ok()).isFalse();
        assertThat(fingerprintResult.failure()).contains("process")
                .contains("edge fingerprint");

        // A drifted request artifact: three items where two were recorded.
        RunEvidence drifted = honest.toBuilder()
                .setSteps(0, step.toBuilder()
                        .setRequestArtifact(save(artifacts,
                                batchOf("one", "two", "three")))
                        .build())
                .build();
        RecipeReplay.ReplayResult driftResult = replay(chain, drifted, artifacts);
        assertThat(driftResult.ok()).isFalse();
        assertThat(driftResult.failure()).contains("process");

        // A flipped validation verdict on a succeeded fan-out step.
        RunEvidence flipped = honest.toBuilder()
                .setSteps(0, step.toBuilder()
                        .setEdge(step.getEdge().toBuilder()
                                .setValidationPassed(false).build())
                        .build())
                .build();
        RecipeReplay.ReplayResult flipResult = replay(chain, flipped, artifacts);
        assertThat(flipResult.ok()).isFalse();
        assertThat(flipResult.failure()).contains("process").contains("verdict");

        // A wrong item count is contract-invalid evidence: branches must cover items.
        RunEvidence wrongCount = honest.toBuilder()
                .setSteps(0, step.toBuilder()
                        .setEdge(step.getEdge().toBuilder().setItemCount(3).build())
                        .build())
                .build();
        assertThatThrownBy(() -> replay(chain, wrongCount, artifacts))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("branches");
    }

    /**
     * The scripted in-process provider: replays queued raw response texts, counts
     * invocations, and captures the last request for grounding inspection.
     */
    private static final class ScriptedProvider implements InferenceProvider {

        private final Queue<String> script = new ArrayDeque<>();
        private final AtomicInteger invocations = new AtomicInteger();
        private final AtomicReference<GenerateRequest> lastRequest =
                new AtomicReference<>();
        private java.util.function.Function<GenerateRequest, String> dynamic;

        void script(String... responses) {
            script.addAll(List.of(responses));
        }

        /** Answers each request from its content instead of a fixed queue. */
        void respondWith(java.util.function.Function<GenerateRequest, String> dynamic) {
            this.dynamic = dynamic;
        }

        int invocations() {
            return invocations.get();
        }

        GenerateRequest lastRequest() {
            return lastRequest.get();
        }

        @Override
        public String id() {
            return "scripted";
        }

        @Override
        public GenerateResponse generate(ModelEntry model, GenerateRequest request) {
            invocations.incrementAndGet();
            lastRequest.set(request);
            String next = dynamic != null ? dynamic.apply(request) : script.poll();
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
