package ai.pipestream.proto.workflow;

import ai.pipestream.proto.grpc.workflow.ArtifactRepository;
import ai.pipestream.proto.grpc.workflow.WorkflowValidation;
import ai.pipestream.proto.grpc.workflow.RunEvidenceRepository;
import ai.pipestream.proto.grpc.workflow.v1.ArtifactReference;
import ai.pipestream.proto.grpc.workflow.v1.BranchEvidence;
import ai.pipestream.proto.grpc.workflow.v1.EdgeEvidence;
import ai.pipestream.proto.grpc.workflow.v1.Workflow;
import ai.pipestream.proto.grpc.workflow.v1.WorkflowStep;
import ai.pipestream.proto.grpc.workflow.v1.RunEvidence;
import ai.pipestream.proto.grpc.workflow.v1.RunStatus;
import ai.pipestream.proto.grpc.workflow.v1.StepEvidence;
import ai.pipestream.proto.grpc.workflow.v1.StepStatus;
import ai.pipestream.proto.grpc.workflow.v1.StructuredAttemptEvidence;
import ai.pipestream.proto.grpc.workflow.v1.StructuredGenerationEvidence;
import ai.pipestream.proto.inference.structured.StructuredGenerationException;
import ai.pipestream.proto.inference.v1.GenerateStructuredRequest;
import ai.pipestream.proto.inference.v1.GenerateStructuredResponse;
import ai.pipestream.proto.inference.v1.StructuredAttempt;
import ai.pipestream.proto.inference.v1.Usage;
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

/** Executes a checked workflow and persists a bounded, sensitivity-redacted evidence snapshot. */
public final class WorkflowRunRecorder {

    private static final String PROTOBUF_MEDIA_TYPE = "application/x-protobuf";
    private static final Set<String> SENSITIVE_CLASSES = Set.of("pii", "secret");

    private final WorkflowRunner runner;
    private final ArtifactRepository artifacts;
    private final RunEvidenceRepository runs;
    private final Clock clock;

    public WorkflowRunRecorder(WorkflowRunner runner, ArtifactRepository artifacts,
                             RunEvidenceRepository runs) {
        this(runner, artifacts, runs, Clock.systemUTC());
    }

    WorkflowRunRecorder(WorkflowRunner runner, ArtifactRepository artifacts,
                      RunEvidenceRepository runs, Clock clock) {
        this.runner = Objects.requireNonNull(runner, "runner");
        this.artifacts = Objects.requireNonNull(artifacts, "artifacts");
        this.runs = Objects.requireNonNull(runs, "runs");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /** Runs {@code definition}, stores its fixtures, and returns the immutable evidence record. */
    public RunEvidence record(String runId, String workflowVersion, CompiledWorkflow definition,
                              DynamicMessage input)
            throws IOException, WorkflowRunner.WorkflowExecutionException {
        Objects.requireNonNull(definition, "definition");
        Objects.requireNonNull(input, "input");
        WorkflowValidation.validateName(runId, "run_id");
        if (workflowVersion != null && !workflowVersion.isBlank()) {
            WorkflowValidation.validateName(workflowVersion, "workflow_version");
        }
        Workflow workflow = WorkflowCompiler.compile(definition);
        Instant runStarted = clock.instant();
        RecordingObserver observer = new RecordingObserver();
        WorkflowRunner.Result result;
        try {
            result = runner.run(definition, input, observer);
        } catch (WorkflowRunner.WorkflowExecutionException failure) {
            Instant completed = clock.instant();
            RunEvidence evidence = failed(runId, workflowVersion, workflow, definition, input,
                    runStarted, completed, observer, failure);
            runs.save(evidence);
            throw failure;
        }
        Instant runCompleted = clock.instant();
        RunEvidence evidence = succeeded(runId, workflowVersion, workflow, input, result.output(),
                runStarted, runCompleted, observer.completed);
        runs.save(evidence);
        return evidence;
    }

    private RunEvidence succeeded(String runId, String workflowVersion, Workflow workflow,
                                  Message input, Message output, Instant started,
                                  Instant completed, List<Trace> traces) throws IOException {
        RunEvidence.Builder evidence = base(runId, workflowVersion, workflow, input, started)
                .setStatus(RunStatus.RUN_STATUS_SUCCEEDED)
                .setCompletedAt(timestamp(completed))
                .setOutputArtifact(save(output));
        for (Trace trace : traces) {
            evidence.addSteps(stepEvidence(trace, workflow));
        }
        RunEvidence built = evidence.build();
        WorkflowValidation.validate(built);
        return built;
    }

    private RunEvidence failed(String runId, String workflowVersion, Workflow workflow,
                               CompiledWorkflow definition,
                               Message input, Instant started, Instant completed,
                               RecordingObserver observer,
                               WorkflowRunner.WorkflowExecutionException failure) throws IOException {
        RunEvidence.Builder evidence = base(runId, workflowVersion, workflow, input, started)
                .setStatus(RunStatus.RUN_STATUS_FAILED)
                .setCompletedAt(timestamp(completed))
                .setFailureSummary(evidenceSummary(failure));
        for (Trace trace : observer.completed) {
            evidence.addSteps(stepEvidence(trace, workflow));
        }
        if (observer.pendingEdge != null) {
            // A failed edge step: the masked produced message is the request fixture,
            // and the edge evidence carries the verdict, the counts, and every branch
            // outcome recorded before the failure - including abandoned branches.
            EdgeTrace edge = observer.pendingEdge;
            Trace pending = observer.pending;
            StepEvidence.Builder step = StepEvidence.newBuilder()
                    .setStepName(edge.step().name())
                    .setMethod(method(edge.step()))
                    .setStatus(StepStatus.STEP_STATUS_FAILED)
                    .setStartedAt(timestamp(pending != null ? pending.started() : completed))
                    .setCompletedAt(timestamp(completed))
                    .setRequestArtifact(save(edge.produced()))
                    .setGrpcStatusCode(failure.grpcCode() == null
                            ? io.grpc.Status.Code.UNKNOWN.value()
                            : failure.grpcCode().value())
                    .setSummary(evidenceSummary(failure))
                    .setEdge(edgeEvidence(edge, workflow));
            if (edge.step().structured() != null && edge.step().fanOut() == null) {
                step.setStructured(failedStructuredEvidence(edge.step(),
                        failureAttempts(failure)));
            }
            evidence.addSteps(step);
        } else if (observer.pending != null) {
            Trace pending = observer.pending;
            StepEvidence.Builder step = StepEvidence.newBuilder()
                    .setStepName(pending.step().name())
                    .setMethod(method(pending.step()))
                    .setStatus(StepStatus.STEP_STATUS_FAILED)
                    .setStartedAt(timestamp(pending.started()))
                    .setCompletedAt(timestamp(completed))
                    .setRequestArtifact(save(pending.request()))
                    .setGrpcStatusCode(failure.grpcCode() == null
                            ? io.grpc.Status.Code.UNKNOWN.value()
                            : failure.grpcCode().value())
                    .setSummary(evidenceSummary(failure));
            if (pending.step().structured() != null) {
                step.setStructured(failedStructuredEvidence(pending.step(),
                        failureAttempts(failure)));
            }
            evidence.addSteps(step);
        } else if (observer.completed.stream()
                .noneMatch(trace -> trace.step().name().equals(failure.step()))) {
            CompiledWorkflow.Step failedStep = definition.steps().stream()
                    .filter(step -> step.name().equals(failure.step()))
                    .findFirst().orElse(null);
            StepEvidence.Builder step = StepEvidence.newBuilder()
                    .setStepName(failure.step())
                    .setStatus(StepStatus.STEP_STATUS_FAILED)
                    .setStartedAt(timestamp(completed))
                    .setCompletedAt(timestamp(completed))
                    .setGrpcStatusCode(failure.grpcCode() == null
                            ? io.grpc.Status.Code.UNKNOWN.value()
                            : failure.grpcCode().value())
                    .setSummary(evidenceSummary(failure));
            if (failedStep != null && failedStep.structured() != null) {
                step.setMethod("");
                step.setStructured(failedStructuredEvidence(failedStep, List.of()));
            } else {
                step.setMethod(workflow.getStepsList().stream()
                        .filter(s -> s.getName().equals(failure.step()))
                        .map(ai.pipestream.proto.grpc.workflow.v1.WorkflowStep::getMethod)
                        .findFirst().orElse("unknown.Service/Unknown"));
            }
            evidence.addSteps(step);
        }
        RunEvidence built = evidence.build();
        WorkflowValidation.validate(built);
        return built;
    }

    private RunEvidence.Builder base(String runId, String workflowVersion, Workflow workflow,
                                     Message input, Instant started) throws IOException {
        RunEvidence.Builder evidence = RunEvidence.newBuilder()
                .setRunId(runId)
                .setWorkflowName(workflow.getName())
                .setWorkflowFingerprint(WorkflowValidation.fingerprint(workflow))
                .setStartedAt(timestamp(started))
                .addAllDependencies(workflow.getDependenciesList())
                .setInputArtifact(save(input));
        if (workflowVersion != null && !workflowVersion.isBlank()) {
            evidence.setWorkflowVersion(workflowVersion);
        }
        return evidence;
    }

    private StepEvidence stepEvidence(Trace trace, Workflow workflow) throws IOException {
        StepEvidence.Builder step = StepEvidence.newBuilder()
                .setStepName(trace.step().name())
                .setMethod(method(trace.step()))
                .setStatus(trace.skipped()
                        ? StepStatus.STEP_STATUS_SKIPPED : StepStatus.STEP_STATUS_SUCCEEDED)
                .setStartedAt(timestamp(trace.started()))
                .setCompletedAt(timestamp(trace.completed()))
                .setGrpcStatusCode(io.grpc.Status.Code.OK.value());
        if (!trace.skipped()) {
            // An edge step's request fixture is the edge-produced message (the fan-out
            // items holder), never the projected or per-branch form; everything else
            // records what was invoked.
            step.setRequestArtifact(save(trace.edge() != null
                    ? trace.edge().produced() : trace.request()));
            step.setResponseArtifact(save(trace.response()));
        }
        if (trace.structured() != null) {
            step.setStructured(structuredEvidence(trace.structured()));
        }
        if (trace.edge() != null) {
            step.setEdge(edgeEvidence(trace.edge(), workflow));
        }
        return step.build();
    }

    /**
     * The bounded evidence of one typed edge: the fingerprint binds it to the workflow's
     * exact edge spec; the verdict, counts, and per-branch outcomes are scalars and
     * masked artifact references - produced and projected values live only in the
     * artifacts.
     */
    private EdgeEvidence edgeEvidence(EdgeTrace trace, Workflow workflow) throws IOException {
        WorkflowStep workflowStep = workflow.getStepsList().stream()
                .filter(s -> s.getName().equals(trace.step().name()))
                .findFirst()
                .orElseThrow(() -> new IOException("the compiled workflow has no step '"
                        + trace.step().name() + "'"));
        EdgeEvidence.Builder evidence = EdgeEvidence.newBuilder()
                .setEdgeFingerprint(WorkflowValidation.edgeFingerprint(workflowStep))
                .setValidationPassed(trace.validationPassed())
                .setSourceCount(trace.sourceCount())
                .setItemCount(trace.itemCount());
        for (BranchRecord branch : trace.branches()) {
            BranchEvidence.Builder branchEvidence = BranchEvidence.newBuilder()
                    .setBranchId(branch.branchId())
                    .setStatus(branch.response() != null
                            ? StepStatus.STEP_STATUS_SUCCEEDED
                            : StepStatus.STEP_STATUS_FAILED);
            if (branch.response() != null) {
                branchEvidence.setResponseArtifact(save(branch.response()));
            } else {
                branchEvidence.setSummary(bounded(branch.summary()));
            }
            evidence.addBranches(branchEvidence.build());
        }
        return evidence.build();
    }

    /**
     * The bounded evidence of a successful structured step, from the coordinator's
     * envelope: fingerprints, provider provenance, token sums, and per-attempt
     * scalars. Raw response text and feedback never cross into evidence.
     */
    private static StructuredGenerationEvidence structuredEvidence(
            GenerateStructuredResponse response) {
        StructuredGenerationEvidence.Builder evidence = StructuredGenerationEvidence
                .newBuilder()
                .setTargetType(response.getTargetType())
                .setModel(response.getModel())
                .setProvider(response.getProvider())
                .setModelVersion(response.getModelVersion())
                .setPromptFingerprint(response.getPromptFingerprint())
                .setSchemaFingerprint(response.getSchemaFingerprint())
                .setValidationPassed(true)
                .setTotalUsage(response.getTotalUsage());
        for (StructuredAttempt attempt : response.getAttemptsList()) {
            evidence.addAttempts(attemptEvidence(attempt));
        }
        return evidence.build();
    }

    /**
     * The bounded evidence of a failed structured step: spec identity, recomputed
     * fingerprints (they derive from the target descriptor alone), and whatever
     * attempts the coordinator recorded before failing - empty when the failure
     * happened before the first model invocation.
     */
    private static StructuredGenerationEvidence failedStructuredEvidence(
            CompiledWorkflow.Step step, List<StructuredAttempt> attempts) {
        com.google.protobuf.Descriptors.Descriptor target = step.structured().targetType();
        Usage.Builder total = Usage.newBuilder();
        StructuredGenerationEvidence.Builder evidence = StructuredGenerationEvidence
                .newBuilder()
                .setTargetType(target.getFullName())
                .setModel(step.structured().model())
                .setPromptFingerprint(StructuredProvenance.promptFingerprint(target))
                .setSchemaFingerprint(StructuredProvenance.schemaFingerprint(target))
                .setValidationPassed(false);
        for (StructuredAttempt attempt : attempts) {
            evidence.addAttempts(attemptEvidence(attempt));
            total.setPromptTokens(total.getPromptTokens()
                    + attempt.getUsage().getPromptTokens());
            total.setCompletionTokens(total.getCompletionTokens()
                    + attempt.getUsage().getCompletionTokens());
        }
        return evidence.setTotalUsage(total).build();
    }

    /** One attempt's scalar provenance; response text and feedback are dropped here. */
    private static StructuredAttemptEvidence attemptEvidence(StructuredAttempt attempt) {
        return StructuredAttemptEvidence.newBuilder()
                .setAttempt(attempt.getAttempt())
                .setOutcome(attempt.getOutcome())
                .setUsage(attempt.getUsage())
                .setFinishReason(attempt.getFinishReason())
                .build();
    }

    /** The attempts a structured coordinator failure recorded, or none. */
    private static List<StructuredAttempt> failureAttempts(
            WorkflowRunner.WorkflowExecutionException failure) {
        return failure.getCause() instanceof StructuredGenerationException structured
                ? structured.getAttempts()
                : List.of();
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

    /** The recorded method identity: empty exactly for structured steps. */
    private static String method(CompiledWorkflow.Step step) {
        return step.structured() != null
                ? ""
                : step.method().getService().getFullName() + "/" + step.method().getName();
    }

    private static Timestamp timestamp(Instant instant) {
        return Timestamp.newBuilder().setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano()).build();
    }

    private static String bounded(String message) {
        String value = message == null ? "execution failed" : message;
        return value.length() <= WorkflowValidation.MAX_TEXT_LENGTH
                ? value : value.substring(0, WorkflowValidation.MAX_TEXT_LENGTH);
    }

    /**
     * Provider exceptions may contain response bodies, headers, or credential-bearing
     * diagnostics. Persist only a stable structured-step summary; the in-memory
     * exception still carries the operational detail to the immediate caller.
     */
    private static String evidenceSummary(WorkflowRunner.WorkflowExecutionException failure) {
        if (failure.kind() == WorkflowRunner.FailureKind.STRUCTURED) {
            return bounded("structured generation failed at step '" + failure.step() + "'");
        }
        return bounded(failure.getMessage());
    }

    private record Trace(CompiledWorkflow.Step step, Message request,
                         Message response, boolean skipped,
                         Instant started, Instant completed,
                         GenerateStructuredResponse structured, EdgeTrace edge) {
    }

    /**
     * One step's recorded edge evaluation: the produced (pre-projection) message, the
     * validation verdict, the declared source count, the fan-out cardinality, and the
     * per-branch outcomes in index order.
     */
    private record EdgeTrace(CompiledWorkflow.Step step, DynamicMessage produced,
                             boolean validationPassed, int sourceCount, int itemCount,
                             List<BranchRecord> branches) {
    }

    /** One recorded branch outcome; {@code response} is null on failure. */
    private record BranchRecord(String branchId, Message response, String summary) {
    }

    private static final class RecordingObserver implements WorkflowRunner.ExecutionObserver {
        private final List<Trace> completed = new ArrayList<>();
        private Trace pending;
        private EdgeTrace pendingEdge;

        @Override
        public void stepStarted(CompiledWorkflow.Step step, DynamicMessage request,
                                Instant startedAt) {
            pending = new Trace(step, request, null, false, startedAt, null, null, null);
        }

        @Override
        public void stepCompleted(CompiledWorkflow.Step step, DynamicMessage request,
                                  DynamicMessage response, boolean skipped,
                                  Instant startedAt, Instant completedAt) {
            completed.add(new Trace(step, request, response, skipped, startedAt,
                    completedAt, null, takeEdge(step)));
        }

        @Override
        public void structuredStepStarted(CompiledWorkflow.Step step,
                                          GenerateStructuredRequest request,
                                          Instant startedAt) {
            pending = new Trace(step, request, null, false, startedAt, null, null, null);
        }

        @Override
        public void structuredStepCompleted(CompiledWorkflow.Step step,
                                            GenerateStructuredRequest request,
                                            Message output,
                                            GenerateStructuredResponse structuredResponse,
                                            Instant startedAt, Instant completedAt) {
            completed.add(new Trace(step, request, output, false, startedAt, completedAt,
                    structuredResponse, takeEdge(step)));
        }

        @Override
        public void edgeEvaluated(CompiledWorkflow.Step step, DynamicMessage produced,
                                  boolean validationPassed, int sourceCount,
                                  int itemCount) {
            pendingEdge = new EdgeTrace(step, produced, validationPassed, sourceCount,
                    itemCount, new ArrayList<>());
        }

        @Override
        public void branchCompleted(CompiledWorkflow.Step step, String branchId,
                                    int branchIndex, Message response,
                                    String failureSummary) {
            if (pendingEdge != null && pendingEdge.step().name().equals(step.name())) {
                pendingEdge.branches().add(
                        new BranchRecord(branchId, response, failureSummary));
            }
        }

        /** The step's recorded edge evaluation, snapshot so later steps cannot mutate it. */
        private EdgeTrace takeEdge(CompiledWorkflow.Step step) {
            pending = null;
            if (pendingEdge == null || !pendingEdge.step().name().equals(step.name())) {
                return null;
            }
            EdgeTrace edge = pendingEdge;
            pendingEdge = null;
            return new EdgeTrace(edge.step(), edge.produced(), edge.validationPassed(),
                    edge.sourceCount(), edge.itemCount(), List.copyOf(edge.branches()));
        }
    }
}
