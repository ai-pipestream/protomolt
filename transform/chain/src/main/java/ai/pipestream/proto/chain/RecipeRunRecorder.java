package ai.pipestream.proto.chain;

import ai.pipestream.proto.grpc.recipe.ArtifactRepository;
import ai.pipestream.proto.grpc.recipe.RecipeValidation;
import ai.pipestream.proto.grpc.recipe.RunEvidenceRepository;
import ai.pipestream.proto.grpc.recipe.v1.ArtifactReference;
import ai.pipestream.proto.grpc.recipe.v1.GrpcRecipe;
import ai.pipestream.proto.grpc.recipe.v1.RunEvidence;
import ai.pipestream.proto.grpc.recipe.v1.RunStatus;
import ai.pipestream.proto.grpc.recipe.v1.StepEvidence;
import ai.pipestream.proto.grpc.recipe.v1.StepStatus;
import ai.pipestream.proto.meta.SensitivityMasker;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Executes a checked chain and persists a bounded, sensitivity-redacted evidence snapshot. */
public final class RecipeRunRecorder {

    private static final String PROTOBUF_MEDIA_TYPE = "application/x-protobuf";
    private static final Set<String> SENSITIVE_CLASSES = Set.of("pii", "secret");

    private final ChainRunner runner;
    private final ArtifactRepository artifacts;
    private final RunEvidenceRepository runs;
    private final Clock clock;

    public RecipeRunRecorder(ChainRunner runner, ArtifactRepository artifacts,
                             RunEvidenceRepository runs) {
        this(runner, artifacts, runs, Clock.systemUTC());
    }

    RecipeRunRecorder(ChainRunner runner, ArtifactRepository artifacts,
                      RunEvidenceRepository runs, Clock clock) {
        this.runner = Objects.requireNonNull(runner, "runner");
        this.artifacts = Objects.requireNonNull(artifacts, "artifacts");
        this.runs = Objects.requireNonNull(runs, "runs");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /** Runs {@code chain}, stores its fixtures, and returns the immutable evidence record. */
    public RunEvidence record(String runId, String recipeVersion, ChainDefinition chain,
                              DynamicMessage input)
            throws IOException, ChainRunner.ChainExecutionException {
        Objects.requireNonNull(chain, "chain");
        Objects.requireNonNull(input, "input");
        RecipeValidation.validateName(runId, "run_id");
        if (recipeVersion != null && !recipeVersion.isBlank()) {
            RecipeValidation.validateName(recipeVersion, "recipe_version");
        }
        GrpcRecipe recipe = ChainRecipeCompiler.compile(chain);
        Instant runStarted = clock.instant();
        RecordingObserver observer = new RecordingObserver();
        ChainRunner.Result result;
        try {
            result = runner.run(chain, input, observer);
        } catch (ChainRunner.ChainExecutionException failure) {
            Instant completed = clock.instant();
            RunEvidence evidence = failed(runId, recipeVersion, recipe, input,
                    runStarted, completed, observer, failure);
            runs.save(evidence);
            throw failure;
        }
        Instant runCompleted = clock.instant();
        RunEvidence evidence = succeeded(runId, recipeVersion, recipe, input, result.output(),
                runStarted, runCompleted, observer.completed);
        runs.save(evidence);
        return evidence;
    }

    private RunEvidence succeeded(String runId, String recipeVersion, GrpcRecipe recipe,
                                  Message input, Message output, Instant started,
                                  Instant completed, List<Trace> traces) throws IOException {
        RunEvidence.Builder evidence = base(runId, recipeVersion, recipe, input, started)
                .setStatus(RunStatus.RUN_STATUS_SUCCEEDED)
                .setCompletedAt(timestamp(completed))
                .setOutputArtifact(save(output));
        for (Trace trace : traces) {
            evidence.addSteps(stepEvidence(trace));
        }
        RunEvidence built = evidence.build();
        RecipeValidation.validate(built);
        return built;
    }

    private RunEvidence failed(String runId, String recipeVersion, GrpcRecipe recipe,
                               Message input, Instant started, Instant completed,
                               RecordingObserver observer,
                               ChainRunner.ChainExecutionException failure) throws IOException {
        RunEvidence.Builder evidence = base(runId, recipeVersion, recipe, input, started)
                .setStatus(RunStatus.RUN_STATUS_FAILED)
                .setCompletedAt(timestamp(completed))
                .setFailureSummary(bounded(failure.getMessage()));
        for (Trace trace : observer.completed) {
            evidence.addSteps(stepEvidence(trace));
        }
        if (observer.pending != null) {
            Trace pending = observer.pending;
            evidence.addSteps(StepEvidence.newBuilder()
                    .setStepName(pending.step().name())
                    .setMethod(method(pending.step()))
                    .setStatus(StepStatus.STEP_STATUS_FAILED)
                    .setStartedAt(timestamp(pending.started()))
                    .setCompletedAt(timestamp(completed))
                    .setRequestArtifact(save(pending.request()))
                    .setGrpcStatusCode(failure.grpcCode() == null
                            ? io.grpc.Status.Code.UNKNOWN.value()
                            : failure.grpcCode().value())
                    .setSummary(bounded(failure.getMessage())));
        } else if (observer.completed.stream()
                .noneMatch(trace -> trace.step().name().equals(failure.step()))) {
            String failedMethod = recipe.getStepsList().stream()
                    .filter(step -> step.getName().equals(failure.step()))
                    .map(ai.pipestream.proto.grpc.recipe.v1.RecipeStep::getMethod)
                    .findFirst().orElse("unknown.Service/Unknown");
            evidence.addSteps(StepEvidence.newBuilder()
                    .setStepName(failure.step())
                    .setMethod(failedMethod)
                    .setStatus(StepStatus.STEP_STATUS_FAILED)
                    .setStartedAt(timestamp(completed))
                    .setCompletedAt(timestamp(completed))
                    .setGrpcStatusCode(failure.grpcCode() == null
                            ? io.grpc.Status.Code.UNKNOWN.value()
                            : failure.grpcCode().value())
                    .setSummary(bounded(failure.getMessage())));
        }
        RunEvidence built = evidence.build();
        RecipeValidation.validate(built);
        return built;
    }

    private RunEvidence.Builder base(String runId, String recipeVersion, GrpcRecipe recipe,
                                     Message input, Instant started) throws IOException {
        RunEvidence.Builder evidence = RunEvidence.newBuilder()
                .setRunId(runId)
                .setRecipeName(recipe.getName())
                .setRecipeFingerprint(RecipeValidation.fingerprint(recipe))
                .setStartedAt(timestamp(started))
                .addAllDependencies(recipe.getDependenciesList())
                .setInputArtifact(save(input));
        if (recipeVersion != null && !recipeVersion.isBlank()) {
            evidence.setRecipeVersion(recipeVersion);
        }
        return evidence;
    }

    private StepEvidence stepEvidence(Trace trace) throws IOException {
        StepEvidence.Builder step = StepEvidence.newBuilder()
                .setStepName(trace.step().name())
                .setMethod(method(trace.step()))
                .setStatus(trace.skipped()
                        ? StepStatus.STEP_STATUS_SKIPPED : StepStatus.STEP_STATUS_SUCCEEDED)
                .setStartedAt(timestamp(trace.started()))
                .setCompletedAt(timestamp(trace.completed()))
                .setGrpcStatusCode(io.grpc.Status.Code.OK.value());
        if (!trace.skipped()) {
            step.setRequestArtifact(save(trace.request()));
            step.setResponseArtifact(save(trace.response()));
        }
        return step.build();
    }

    private ArtifactReference save(Message message) throws IOException {
        SensitivityMasker.MaskResult masked = SensitivityMasker.mask(message,
                SENSITIVE_CLASSES, SensitivityMasker.Strategy.REMOVE);
        if (!masked.unresolvedPaths().isEmpty()) {
            throw new IOException("cannot record unredacted Any payloads: "
                    + masked.unresolvedPaths());
        }
        return artifacts.save(masked.message().toByteArray(), PROTOBUF_MEDIA_TYPE, true);
    }

    private static String method(ChainDefinition.Step step) {
        return step.method().getService().getFullName() + "/" + step.method().getName();
    }

    private static Timestamp timestamp(Instant instant) {
        return Timestamp.newBuilder().setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano()).build();
    }

    private static String bounded(String message) {
        String value = message == null ? "execution failed" : message;
        return value.length() <= RecipeValidation.MAX_TEXT_LENGTH
                ? value : value.substring(0, RecipeValidation.MAX_TEXT_LENGTH);
    }

    private record Trace(ChainDefinition.Step step, DynamicMessage request,
                         DynamicMessage response, boolean skipped,
                         Instant started, Instant completed) {
    }

    private static final class RecordingObserver implements ChainRunner.ExecutionObserver {
        private final List<Trace> completed = new ArrayList<>();
        private Trace pending;

        @Override
        public void stepStarted(ChainDefinition.Step step, DynamicMessage request,
                                Instant startedAt) {
            pending = new Trace(step, request, null, false, startedAt, null);
        }

        @Override
        public void stepCompleted(ChainDefinition.Step step, DynamicMessage request,
                                  DynamicMessage response, boolean skipped,
                                  Instant startedAt, Instant completedAt) {
            completed.add(new Trace(step, request, response, skipped, startedAt, completedAt));
            pending = null;
        }
    }
}
