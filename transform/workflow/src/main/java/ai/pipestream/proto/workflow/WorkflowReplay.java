package ai.pipestream.proto.workflow;

import ai.pipestream.proto.cel.CelEnvironmentFactory;
import ai.pipestream.proto.cel.CelEvaluator;
import ai.pipestream.proto.cel.CelProtoMapper;
import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.grpc.workflow.WorkflowValidation;
import ai.pipestream.proto.grpc.workflow.ArtifactRepository;
import ai.pipestream.proto.grpc.workflow.v1.Workflow;
import ai.pipestream.proto.grpc.workflow.v1.RunEvidence;
import ai.pipestream.proto.grpc.workflow.v1.StepEvidence;
import ai.pipestream.proto.grpc.workflow.v1.StepStatus;
import ai.pipestream.proto.grpc.workflow.v1.StructuredGenerationEvidence;
import ai.pipestream.proto.grpc.workflow.v1.StructuredGenerationSpec;
import ai.pipestream.proto.inference.v1.GenerateStructuredRequest;
import ai.pipestream.proto.meta.SensitivityMasker;
import ai.pipestream.proto.shapes.MessageScope;
import ai.pipestream.proto.shapes.ScopedProtoMapper;
import ai.pipestream.proto.validate.ProtoValidator;
import ai.pipestream.proto.validate.ValidationResult;
import com.google.protobuf.Any;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Offline replay of a recorded workflow run: given the workflow, its {@link RunEvidence}, the
 * schema the run was checked against, and the artifact store holding the fixtures, verify
 * that the recording is exactly what the workflow produces — with no server and no network.
 *
 * <p>Replay re-derives every step request from the recorded scope (the input fixture plus
 * every prior step's recorded response) using the same scoped mapper the live runner uses,
 * and compares it against the recorded request. Gates are re-evaluated against the recorded
 * scope, recorded responses are re-validated when the step declares validation, and the
 * evidence's workflow and descriptor fingerprints must match what is being replayed. Any
 * alteration — request, response, gate verdict, schema, or workflow content — fails with the
 * step and the mismatch named.</p>
 *
 * <p>The verdict is data, not an exception: expected mismatches return a failed
 * {@link ReplayResult}. Only store and parse failures throw.</p>
 */
public final class WorkflowReplay {

    private WorkflowReplay() {
    }

    /** One step's replay verdict; {@code detail} names the mismatch when not ok. */
    public record StepReplay(String stepName, StepStatus recordedStatus, boolean ok,
                             String detail) {
    }

    /** The run's replay verdict: ok only when every check passed, else the first failure. */
    public record ReplayResult(boolean ok, String failure, List<StepReplay> steps) {

        static ReplayResult failed(String failure, List<StepReplay> steps) {
            return new ReplayResult(false, failure, List.copyOf(steps));
        }

        static ReplayResult ok(List<StepReplay> steps) {
            return new ReplayResult(true, "", List.copyOf(steps));
        }
    }

    /**
     * Replays {@code evidence} against {@code workflow}.
     *
     * @param schema    the descriptors the run was checked against; every method and type
     *                  reference in the workflow must resolve here, and its fingerprint must
     *                  match the evidence's dependency fingerprints
     * @param artifacts the store holding the recorded fixtures
     * @throws IOException when a referenced artifact is missing or unreadable
     */
    public static ReplayResult replay(Workflow workflow, RunEvidence evidence,
                                      List<FileDescriptor> schema,
                                      ArtifactRepository artifacts) throws IOException {
        Objects.requireNonNull(workflow, "workflow");
        Objects.requireNonNull(evidence, "evidence");
        Objects.requireNonNull(artifacts, "artifacts");
        // Both sides answer to the contract before any comparison happens.
        WorkflowValidation.validate(workflow);
        WorkflowValidation.validate(evidence);

        List<StepReplay> steps = new ArrayList<>();
        if (!evidence.getWorkflowName().equals(workflow.getName())
                || !evidence.getWorkflowFingerprint().equals(WorkflowValidation.fingerprint(workflow))) {
            return ReplayResult.failed(
                    "evidence was recorded for different workflow content", steps);
        }
        String schemaFingerprint = WorkflowCompiler.descriptorFingerprint(schema);
        for (var dependency : evidence.getDependenciesList()) {
            if (!dependency.getDescriptorFingerprint().equals(schemaFingerprint)) {
                return ReplayResult.failed("dependency " + dependency.getAlias()
                        + " names a different descriptor set than the replay schema", steps);
            }
        }

        DescriptorRegistry registry = DescriptorRegistry.create();
        for (FileDescriptor file : schema) {
            registry.registerFile(file);
        }
        ScopedProtoMapper mapper = new ScopedProtoMapper(registry);

        Descriptor inputType = findMessage(schema, workflow.getInputType());
        if (inputType == null) {
            return ReplayResult.failed(
                    "input type " + workflow.getInputType() + " not in the replay schema", steps);
        }
        Map<String, Message> scope = new LinkedHashMap<>();
        scope.put("input", parse(artifacts, evidence.getInputArtifact(), inputType,
                "run input"));
        // Mirrors the live runner: a gate-skipped step binds its name but never becomes
        // the result the workflow returns.
        Message last = scope.get("input");

        for (int i = 0; i < workflow.getStepsCount(); i++) {
            var step = workflow.getSteps(i);
            if (i >= evidence.getStepsCount()) {
                return ReplayResult.failed("evidence ends before step " + step.getName(), steps);
            }
            StepEvidence recorded = evidence.getSteps(i);
            if (!recorded.getStepName().equals(step.getName())) {
                return ReplayResult.failed("evidence step " + i + " is "
                        + recorded.getStepName() + " but the workflow's is " + step.getName()
                        + "; the workflow changed under the evidence", steps);
            }
            if (!recorded.getMethod().equals(step.getMethod())) {
                return fail(steps, step.getName(), recorded.getStatus(),
                        "recorded method " + recorded.getMethod()
                                + " differs from the workflow's " + step.getMethod());
            }
            if (step.hasStructured() && !step.hasEdge()) {
                ReplayResult structured = replayStructured(workflow, evidence, i, step,
                        recorded, schema, artifacts, scope, steps);
                if (structured != null) {
                    return structured;
                }
                last = scope.get(step.getName());
                continue;
            }
            MethodDescriptor method = step.hasStructured()
                    ? null
                    : CompiledWorkflow.resolveMethod(schema, step.getMethod());

            if (recorded.getStatus() == StepStatus.STEP_STATUS_SKIPPED) {
                String detail = verifySkip(step, scope, method);
                if (detail != null) {
                    return fail(steps, step.getName(), recorded.getStatus(), detail);
                }
                scope.put(step.getName(), DynamicMessage.getDefaultInstance(
                        step.hasFanOut()
                                ? collectType(schema, step)
                                : method.getOutputType()));
                steps.add(new StepReplay(step.getName(), recorded.getStatus(), true, ""));
                continue;
            }
            if (step.hasEdge()) {
                ReplayResult edge = replayEdge(workflow, evidence, i, step, recorded, schema,
                        artifacts, scope, steps, mapper, registry);
                if (edge != null) {
                    return edge;
                }
                last = scope.get(step.getName());
                continue;
            }
            if (recorded.getStatus() != StepStatus.STEP_STATUS_SUCCEEDED) {
                // A failed or cancelled step ends the run; nothing after it can be replayed.
                steps.add(new StepReplay(step.getName(), recorded.getStatus(), true, ""));
                return terminalTail(workflow, evidence, i, steps);
            }
            if (recorded.getGrpcStatusCode() != 0) {
                return fail(steps, step.getName(), recorded.getStatus(),
                        "succeeded step records gRPC status " + recorded.getGrpcStatusCode());
            }

            // The recorded request must be exactly what the mapping derives from the
            // recorded scope; a drifted fixture or a changed rule both surface here.
            Message expected;
            try {
                expected = buildMessage(mapper, scope, method.getInputType(),
                        step.getRulesList(), step.getCelRulesList());
            } catch (Exception e) {
                return fail(steps, step.getName(), recorded.getStatus(),
                        "request mapping could not be re-derived: " + e.getMessage());
            }
            if (recorded.hasRequestArtifact()) {
                Message request = parse(artifacts, recorded.getRequestArtifact(),
                        method.getInputType(), "request of step " + step.getName());
                if (!request.equals(expected)) {
                    return fail(steps, step.getName(), recorded.getStatus(),
                            "recorded request differs from what the workflow's mapping "
                                    + "derives from the recorded scope");
                }
            } else if (step.getCompletion()
                    == ai.pipestream.proto.grpc.workflow.v1.StepCompletion.STEP_COMPLETION_LIVE) {
                return fail(steps, step.getName(), recorded.getStatus(),
                        "a live step records no request artifact");
            }

            if (!recorded.hasResponseArtifact()) {
                return fail(steps, step.getName(), recorded.getStatus(),
                        "succeeded step records no response artifact");
            }
            Message response = parse(artifacts, recorded.getResponseArtifact(),
                    method.getOutputType(), "response of step " + step.getName());
            if (step.getValidateResponse()) {
                ValidationResult result = ProtoValidator
                        .forMessageType(method.getOutputType()).validate(response);
                if (!result.valid()) {
                    return fail(steps, step.getName(), recorded.getStatus(),
                            "recorded response fails validation: " + result.violations());
                }
            }
            scope.put(step.getName(), response);
            last = response;
            steps.add(new StepReplay(step.getName(), recorded.getStatus(), true, ""));
        }

        if (evidence.getStepsCount() > workflow.getStepsCount()) {
            return ReplayResult.failed("evidence records more steps than the workflow", steps);
        }
        if (evidence.hasOutputArtifact()) {
            Message expected;
            try {
                expected = workflow.hasOutput()
                        ? buildMessage(mapper, scope, outputType(schema, workflow),
                                workflow.getOutput().getRulesList(),
                                workflow.getOutput().getCelRulesList())
                        : last;
            } catch (Exception e) {
                return ReplayResult.failed(
                        "output mapping could not be re-derived: " + e.getMessage(), steps);
            }
            Descriptor outputDescriptor = workflow.hasOutput()
                    ? outputType(schema, workflow)
                    : last.getDescriptorForType();
            Message output = parse(artifacts, evidence.getOutputArtifact(),
                    outputDescriptor, "run output");
            if (!output.equals(expected)) {
                return ReplayResult.failed(
                        "recorded output differs from what the workflow derives", steps);
            }
        }
        return ReplayResult.ok(steps);
    }

    /**
     * Offline verification of one structured-generation step: no provider, no network,
     * no inference engines. The recorded evidence must carry the spec's identity, the
     * prompt and schema fingerprints must match a fresh persona-free rendering from the
     * replay schema, the request artifact must equal the request the spec produces
     * (after the same redaction pass the recorder applies), and the recorded typed
     * output must re-validate to the recorded verdict. The verified output binds into
     * the scope exactly like a gRPC step response, so downstream steps and the output
     * projection replay unchanged.
     *
     * @return null when the step verified; the terminal result otherwise
     */
    private static ReplayResult replayStructured(Workflow workflow, RunEvidence evidence,
                                                 int index,
                                                 ai.pipestream.proto.grpc.workflow.v1.WorkflowStep step,
                                                 StepEvidence recorded,
                                                 List<FileDescriptor> schema,
                                                 ArtifactRepository artifacts,
                                                 Map<String, Message> scope,
                                                 List<StepReplay> steps) throws IOException {
        String name = step.getName();
        if (!recorded.hasStructured()) {
            return fail(steps, name, recorded.getStatus(),
                    "structured step records no structured-generation evidence");
        }
        StructuredGenerationEvidence provenance = recorded.getStructured();
        StructuredGenerationSpec spec = step.getStructured();
        if (!provenance.getTargetType().equals(spec.getTargetType())) {
            return fail(steps, name, recorded.getStatus(),
                    "recorded target type " + provenance.getTargetType()
                            + " differs from the workflow's " + spec.getTargetType());
        }
        if (!provenance.getModel().equals(spec.getModel())) {
            return fail(steps, name, recorded.getStatus(),
                    "recorded model " + provenance.getModel()
                            + " differs from the workflow's " + spec.getModel());
        }
        if (recorded.getStatus() == StepStatus.STEP_STATUS_SKIPPED) {
            return fail(steps, name, recorded.getStatus(),
                    "structured steps declare no gate; a skipped record is a forgery");
        }
        Descriptor target = findMessage(schema, spec.getTargetType());
        if (target == null) {
            return fail(steps, name, recorded.getStatus(),
                    "target type " + spec.getTargetType() + " not in the replay schema");
        }
        if (!StructuredProvenance.promptFingerprint(target)
                .equals(provenance.getPromptFingerprint())) {
            return fail(steps, name, recorded.getStatus(),
                    "recorded prompt fingerprint does not match the replay schema's "
                            + "rendering of " + spec.getTargetType());
        }
        if (!StructuredProvenance.schemaFingerprint(target)
                .equals(provenance.getSchemaFingerprint())) {
            return fail(steps, name, recorded.getStatus(),
                    "recorded schema fingerprint does not match the replay schema's "
                            + "rendering of " + spec.getTargetType());
        }
        if (recorded.getStatus() != StepStatus.STEP_STATUS_SUCCEEDED) {
            // A failed structured step ends the run; nothing after it can be replayed.
            steps.add(new StepReplay(name, recorded.getStatus(), true, ""));
            return terminalTail(workflow, evidence, index, steps);
        }
        if (!recorded.hasRequestArtifact()) {
            return fail(steps, name, recorded.getStatus(),
                    "a live structured step records no request artifact");
        }
        Message expectedRequest = maskStructuredRequest(GenerateStructuredRequest
                .newBuilder()
                .setTargetType(spec.getTargetType())
                .setModel(spec.getModel())
                .setMaxAttempts(spec.getMaxAttempts())
                .build());
        Message request = parse(artifacts, recorded.getRequestArtifact(),
                GenerateStructuredRequest.getDescriptor(), "request of step " + name);
        if (!request.equals(expectedRequest)) {
            return fail(steps, name, recorded.getStatus(),
                    "recorded request differs from what the step's structured "
                            + "specification produces");
        }
        if (!recorded.hasResponseArtifact()) {
            return fail(steps, name, recorded.getStatus(),
                    "succeeded structured step records no response artifact");
        }
        Message response = parse(artifacts, recorded.getResponseArtifact(), target,
                "response of step " + name);
        boolean valid = ProtoValidator.forMessageType(target).validate(response).valid();
        if (valid != provenance.getValidationPassed()) {
            return fail(steps, name, recorded.getStatus(),
                    "recorded validation outcome " + provenance.getValidationPassed()
                            + " but the recorded response re-validates to " + valid);
        }
        scope.put(name, response);
        steps.add(new StepReplay(name, recorded.getStatus(), true, ""));
        return null;
    }

    /** The recorder's redaction pass, applied to the spec-derived request. */
    private static Message maskStructuredRequest(GenerateStructuredRequest request) {
        SensitivityMasker.MaskResult masked = SensitivityMasker.mask(request,
                Set.of("pii", "secret"), SensitivityMasker.Strategy.REMOVE);
        return masked.message();
    }

    /** The recorder's redaction pass, applied to a re-derived edge value. */
    private static Message mask(Message message) {
        return SensitivityMasker.mask(message, Set.of("pii", "secret"),
                SensitivityMasker.Strategy.REMOVE).message();
    }

    /**
     * Offline verification of one edge-carrying step: no provider, no network, no
     * inference engines. The edge fingerprint is recomputed from the workflow spec, the
     * produced value is re-derived from the recorded scope with the same mapper and
     * compared to the recorded request artifact, the projection and validation verdict
     * are re-run and compared, and for fan-out the cardinality, branch identities, and
     * collected message are rebuilt and compared. The verified output - the response
     * or the collected message - binds into the scope under the step's name.
     *
     * @return null when the step verified; the terminal result otherwise
     */
    private static ReplayResult replayEdge(Workflow workflow, RunEvidence evidence,
                                           int index,
                                           ai.pipestream.proto.grpc.workflow.v1.WorkflowStep step,
                                           StepEvidence recorded,
                                           List<FileDescriptor> schema,
                                           ArtifactRepository artifacts,
                                           Map<String, Message> scope,
                                           List<StepReplay> steps,
                                           ScopedProtoMapper mapper,
                                           DescriptorRegistry registry) throws IOException {
        String name = step.getName();
        if (!recorded.hasEdge()) {
            return fail(steps, name, recorded.getStatus(),
                    "step carries an edge but records no edge evidence");
        }
        ai.pipestream.proto.grpc.workflow.v1.EdgeEvidence edgeEvidence = recorded.getEdge();
        if (!WorkflowValidation.edgeFingerprint(step)
                .equals(edgeEvidence.getEdgeFingerprint())) {
            return fail(steps, name, recorded.getStatus(),
                    "recorded edge fingerprint does not match the workflow's edge");
        }
        var edge = step.getEdge();
        if (edgeEvidence.getSourceCount() != edge.getSourcesCount()) {
            return fail(steps, name, recorded.getStatus(),
                    "recorded source count " + edgeEvidence.getSourceCount()
                            + " differs from the edge's declared "
                            + edge.getSourcesCount());
        }
        Map<String, Message> restricted = new LinkedHashMap<>();
        for (String source : edge.getSourcesList()) {
            Message value = scope.get(source);
            if (value == null) {
                return fail(steps, name, recorded.getStatus(),
                        "edge source '" + source + "' is not in the recorded scope");
            }
            restricted.put(source, value);
        }
        Descriptor produceType = findMessage(schema, edge.getProduceType());
        if (produceType == null) {
            return fail(steps, name, recorded.getStatus(),
                    "produce type " + edge.getProduceType() + " not in the replay schema");
        }
        Message produced;
        try {
            produced = buildMessage(mapper, restricted, produceType, edge.getRulesList(),
                    edge.getCelRulesList());
        } catch (Exception e) {
            return fail(steps, name, recorded.getStatus(),
                    "edge mapping could not be re-derived: " + e.getMessage());
        }
        if (!recorded.hasRequestArtifact()) {
            return fail(steps, name, recorded.getStatus(),
                    "an edge step records no request artifact");
        }
        Message recordedProduced = parse(artifacts, recorded.getRequestArtifact(),
                produceType, "produced value of step " + name);
        if (!recordedProduced.equals(mask(produced))) {
            return fail(steps, name, recorded.getStatus(),
                    "recorded produced value differs from what the edge derives from "
                            + "the recorded scope");
        }

        ai.pipestream.proto.projection.MessageProjection projection = null;
        if (!edge.getProjectTo().isEmpty()) {
            Descriptor projectTo = findMessage(schema, edge.getProjectTo());
            if (projectTo == null) {
                return fail(steps, name, recorded.getStatus(),
                        "projection target " + edge.getProjectTo()
                                + " not in the replay schema");
            }
            try {
                projection = ai.pipestream.proto.projection.MessageProjection
                        .forTarget(projectTo, registry).orElse(null);
            } catch (ai.pipestream.proto.projection.ProjectionException e) {
                return fail(steps, name, recorded.getStatus(),
                        "projection target is broken: " + e.getMessage());
            }
            if (projection == null) {
                return fail(steps, name, recorded.getStatus(),
                        "projection target " + edge.getProjectTo()
                                + " declares no projection sources");
            }
        }

        if (step.hasFanOut()) {
            return replayFanOut(workflow, evidence, index, step, recorded, schema,
                    artifacts, scope, steps, produced, projection, edgeEvidence);
        }

        Message delivered = produced;
        if (projection != null) {
            try {
                delivered = projection.project(produced);
            } catch (ai.pipestream.proto.projection.ProjectionException e) {
                return fail(steps, name, recorded.getStatus(),
                        "edge projection could not be re-run: " + e.getMessage());
            }
        }
        boolean valid = !edge.getValidate()
                || ProtoValidator.forMessageType(delivered.getDescriptorForType())
                        .validate(delivered).valid();
        if (valid != edgeEvidence.getValidationPassed()) {
            return fail(steps, name, recorded.getStatus(),
                    "recorded edge verdict " + edgeEvidence.getValidationPassed()
                            + " but the re-derived value validates to " + valid);
        }
        if (recorded.getStatus() != StepStatus.STEP_STATUS_SUCCEEDED) {
            // A failed edge step ends the run; nothing after it can be replayed.
            steps.add(new StepReplay(name, recorded.getStatus(), true, ""));
            return terminalTail(workflow, evidence, index, steps);
        }
        if (recorded.getGrpcStatusCode() != 0) {
            return fail(steps, name, recorded.getStatus(),
                    "succeeded step records gRPC status " + recorded.getGrpcStatusCode());
        }
        if (step.hasStructured()) {
            return replayEdgeStructured(step, recorded, schema, artifacts, scope, steps,
                    delivered);
        }
        if (!recorded.hasResponseArtifact()) {
            return fail(steps, name, recorded.getStatus(),
                    "succeeded step records no response artifact");
        }
        MethodDescriptor method = CompiledWorkflow.resolveMethod(schema, step.getMethod());
        Message response = parse(artifacts, recorded.getResponseArtifact(),
                method.getOutputType(), "response of step " + name);
        if (step.getValidateResponse()) {
            ValidationResult result = ProtoValidator
                    .forMessageType(method.getOutputType()).validate(response);
            if (!result.valid()) {
                return fail(steps, name, recorded.getStatus(),
                        "recorded response fails validation: " + result.violations());
            }
        }
        scope.put(name, response);
        steps.add(new StepReplay(name, recorded.getStatus(), true, ""));
        return null;
    }

    /**
     * The structured-generation checks of an edge step: the provenance identity and
     * schema fingerprint as ever, but the prompt fingerprint is recomputed with the
     * re-derived grounding rendered in, and the request artifact was already compared
     * against the edge-produced value by the caller.
     *
     * @return null when the step verified; the terminal result otherwise
     */
    private static ReplayResult replayEdgeStructured(
            ai.pipestream.proto.grpc.workflow.v1.WorkflowStep step, StepEvidence recorded,
            List<FileDescriptor> schema, ArtifactRepository artifacts,
            Map<String, Message> scope, List<StepReplay> steps, Message delivered)
            throws IOException {
        String name = step.getName();
        if (!recorded.hasStructured()) {
            return fail(steps, name, recorded.getStatus(),
                    "structured step records no structured-generation evidence");
        }
        StructuredGenerationEvidence provenance = recorded.getStructured();
        StructuredGenerationSpec spec = step.getStructured();
        if (!provenance.getTargetType().equals(spec.getTargetType())) {
            return fail(steps, name, recorded.getStatus(),
                    "recorded target type " + provenance.getTargetType()
                            + " differs from the workflow's " + spec.getTargetType());
        }
        if (!provenance.getModel().equals(spec.getModel())) {
            return fail(steps, name, recorded.getStatus(),
                    "recorded model " + provenance.getModel()
                            + " differs from the workflow's " + spec.getModel());
        }
        Descriptor target = findMessage(schema, spec.getTargetType());
        if (target == null) {
            return fail(steps, name, recorded.getStatus(),
                    "target type " + spec.getTargetType() + " not in the replay schema");
        }
        String promptFingerprint = StructuredProvenance.promptFingerprint(target,
                Any.pack(delivered), typeRegistry(schema));
        if (!promptFingerprint.equals(provenance.getPromptFingerprint())) {
            return fail(steps, name, recorded.getStatus(),
                    "recorded prompt fingerprint does not match the replay schema's "
                            + "grounded rendering of " + spec.getTargetType());
        }
        if (!StructuredProvenance.schemaFingerprint(target)
                .equals(provenance.getSchemaFingerprint())) {
            return fail(steps, name, recorded.getStatus(),
                    "recorded schema fingerprint does not match the replay schema's "
                            + "rendering of " + spec.getTargetType());
        }
        if (!recorded.hasResponseArtifact()) {
            return fail(steps, name, recorded.getStatus(),
                    "succeeded structured step records no response artifact");
        }
        Message response = parse(artifacts, recorded.getResponseArtifact(), target,
                "response of step " + name);
        boolean valid = ProtoValidator.forMessageType(target).validate(response).valid();
        if (valid != provenance.getValidationPassed()) {
            return fail(steps, name, recorded.getStatus(),
                    "recorded validation outcome " + provenance.getValidationPassed()
                            + " but the recorded response re-validates to " + valid);
        }
        scope.put(name, response);
        steps.add(new StepReplay(name, recorded.getStatus(), true, ""));
        return null;
    }

    /**
     * Offline verification of one fanned-out edge step: the recorded item count must
     * equal the re-derived items cardinality and the branch count; every branch id
     * must be its stable identity; a branch whose item fails re-validation must be
     * recorded FAILED; each recorded branch output re-parses and re-validates; and
     * the collected message rebuilt from the successful outputs in index order must
     * equal the recorded response artifact.
     *
     * @return null when the step verified; the terminal result otherwise
     */
    private static ReplayResult replayFanOut(
            Workflow workflow, RunEvidence evidence, int index,
            ai.pipestream.proto.grpc.workflow.v1.WorkflowStep step, StepEvidence recorded,
            List<FileDescriptor> schema, ArtifactRepository artifacts,
            Map<String, Message> scope, List<StepReplay> steps, Message produced,
            ai.pipestream.proto.projection.MessageProjection projection,
            ai.pipestream.proto.grpc.workflow.v1.EdgeEvidence edgeEvidence)
            throws IOException {
        String name = step.getName();
        var fanOut = step.getFanOut();
        List<Message> items;
        try {
            items = EdgeFlow.items(produced, fanOut.getItems());
        } catch (IllegalArgumentException e) {
            return fail(steps, name, recorded.getStatus(),
                    "fan-out items path rejected: " + e.getMessage());
        }
        if (edgeEvidence.getItemCount() != items.size()) {
            return fail(steps, name, recorded.getStatus(),
                    "recorded item count " + edgeEvidence.getItemCount()
                            + " differs from the produced message's items cardinality "
                            + items.size());
        }
        if (edgeEvidence.getBranchesCount() != items.size()) {
            return fail(steps, name, recorded.getStatus(),
                    "recorded branch count " + edgeEvidence.getBranchesCount()
                            + " differs from the item count " + items.size());
        }
        Descriptor branchOutputType = step.hasStructured()
                ? findMessage(schema, step.getStructured().getTargetType())
                : CompiledWorkflow.resolveMethod(schema, step.getMethod()).getOutputType();
        if (branchOutputType == null) {
            return fail(steps, name, recorded.getStatus(),
                    "branch output type not in the replay schema");
        }
        Descriptor collectType = findMessage(schema, fanOut.getCollectType());
        if (collectType == null) {
            return fail(steps, name, recorded.getStatus(),
                    "collect type " + fanOut.getCollectType()
                            + " not in the replay schema");
        }
        boolean allValid = true;
        List<Message> outputs = new ArrayList<>(items.size());
        for (int i = 0; i < items.size(); i++) {
            var branch = edgeEvidence.getBranches(i);
            if (!branch.getBranchId().equals(name + "#" + i)) {
                return fail(steps, name, recorded.getStatus(),
                        "recorded branch " + i + " is '" + branch.getBranchId()
                                + "' but its stable identity is '" + name + "#" + i + "'");
            }
            // Re-run the per-item projection and validation: an item that fails here
            // never executed, so its branch must be recorded FAILED.
            Message value = items.get(i);
            boolean itemValid = true;
            if (projection != null) {
                try {
                    value = projection.project(value);
                } catch (ai.pipestream.proto.projection.ProjectionException e) {
                    itemValid = false;
                }
            }
            if (itemValid && step.getEdge().getValidate()) {
                itemValid = ProtoValidator.forMessageType(value.getDescriptorForType())
                        .validate(value).valid();
            }
            if (!itemValid) {
                allValid = false;
            }
            if (branch.getStatus() == StepStatus.STEP_STATUS_SUCCEEDED) {
                if (!itemValid) {
                    return fail(steps, name, recorded.getStatus(),
                            "recorded branch '" + branch.getBranchId()
                                    + "' succeeded but its item fails validation");
                }
                if (!branch.hasResponseArtifact()) {
                    return fail(steps, name, recorded.getStatus(),
                            "succeeded branch '" + branch.getBranchId()
                                    + "' records no response artifact");
                }
                Message output = parse(artifacts, branch.getResponseArtifact(),
                        branchOutputType, "response of branch " + branch.getBranchId());
                if (step.hasStructured() || step.getValidateResponse()) {
                    ValidationResult result = ProtoValidator
                            .forMessageType(branchOutputType).validate(output);
                    if (!result.valid()) {
                        return fail(steps, name, recorded.getStatus(),
                                "recorded branch '" + branch.getBranchId()
                                        + "' response fails validation: "
                                        + result.violations());
                    }
                }
                outputs.add(output);
            } else if (branch.hasResponseArtifact()) {
                return fail(steps, name, recorded.getStatus(),
                        "failed branch '" + branch.getBranchId()
                                + "' records a response artifact");
            }
        }
        if (allValid != edgeEvidence.getValidationPassed()) {
            return fail(steps, name, recorded.getStatus(),
                    "recorded edge verdict " + edgeEvidence.getValidationPassed()
                            + " but the re-derived items validate to " + allValid);
        }
        if (recorded.getStatus() != StepStatus.STEP_STATUS_SUCCEEDED) {
            // A failed fan-out step ends the run; nothing after it can be replayed.
            steps.add(new StepReplay(name, recorded.getStatus(), true, ""));
            return terminalTail(workflow, evidence, index, steps);
        }
        if (!recorded.hasResponseArtifact()) {
            return fail(steps, name, recorded.getStatus(),
                    "succeeded fan-out step records no response artifact");
        }
        Message collected = parse(artifacts, recorded.getResponseArtifact(), collectType,
                "collected message of step " + name);
        Message expected;
        try {
            expected = mask(EdgeFlow.collect(collectType, fanOut.getCollectInto(),
                    outputs));
        } catch (IllegalArgumentException e) {
            return fail(steps, name, recorded.getStatus(),
                    "fan-out collect could not be rebuilt: " + e.getMessage());
        }
        if (!collected.equals(expected)) {
            return fail(steps, name, recorded.getStatus(),
                    "recorded collected message differs from the branch outputs in "
                            + "index order");
        }
        scope.put(name, collected);
        steps.add(new StepReplay(name, recorded.getStatus(), true, ""));
        return null;
    }

    /** The collect type of a fanned-out step, resolved against the replay schema. */
    private static Descriptor collectType(List<FileDescriptor> schema,
                                          ai.pipestream.proto.grpc.workflow.v1.WorkflowStep step) {
        Descriptor type = findMessage(schema, step.getFanOut().getCollectType());
        if (type == null) {
            throw new IllegalArgumentException("collect type "
                    + step.getFanOut().getCollectType() + " not in the replay schema");
        }
        return type;
    }

    /** Every message type visible from the replay schema, for grounding resolution. */
    private static com.google.protobuf.util.JsonFormat.TypeRegistry typeRegistry(
            List<FileDescriptor> schema) {
        com.google.protobuf.util.JsonFormat.TypeRegistry.Builder builder =
                com.google.protobuf.util.JsonFormat.TypeRegistry.newBuilder();
        Set<String> seen = new java.util.HashSet<>();
        for (FileDescriptor file : schema) {
            collectFileTypes(file, builder, seen);
        }
        return builder.build();
    }

    private static void collectFileTypes(
            FileDescriptor file,
            com.google.protobuf.util.JsonFormat.TypeRegistry.Builder out,
            Set<String> seen) {
        if (!seen.add(file.getFullName())) {
            return;
        }
        for (Descriptor message : file.getMessageTypes()) {
            out.add(message);
        }
        for (FileDescriptor dependency : file.getDependencies()) {
            collectFileTypes(dependency, out, seen);
        }
    }

    /** A skipped step must have a gate, and the gate must evaluate false on the scope. */
    private static String verifySkip(ai.pipestream.proto.grpc.workflow.v1.WorkflowStep step,
                                     Map<String, Message> scope, MethodDescriptor method) {
        if (step.getWhen().isBlank()) {
            return "step is recorded skipped but declares no gate";
        }
        try {
            boolean go = evaluator(scope, method.getInputType())
                    .evaluateBooleanOrFail(step.getWhen(), Map.copyOf(scope));
            return go ? "step is recorded skipped but its gate evaluates true" : null;
        } catch (Exception e) {
            return "gate could not be re-evaluated: " + e.getMessage();
        }
    }

    /** Nothing may be recorded after a failed or cancelled step. */
    private static ReplayResult terminalTail(Workflow workflow, RunEvidence evidence,
                                             int failedIndex, List<StepReplay> steps) {
        if (evidence.getStepsCount() > failedIndex + 1) {
            return ReplayResult.failed(
                    "evidence records steps after a terminal step", steps);
        }
        return ReplayResult.ok(steps);
    }

    private static ReplayResult fail(List<StepReplay> steps, String name,
                                     StepStatus status, String detail) {
        steps.add(new StepReplay(name, status, false, detail));
        return ReplayResult.failed("step " + name + ": " + detail, steps);
    }

    /** The same scoped mapping the live runner uses, rule for rule. */
    private static DynamicMessage buildMessage(ScopedProtoMapper mapper,
                                               Map<String, Message> scope, Descriptor type,
                                               List<String> rules,
                                               List<ai.pipestream.proto.grpc.workflow.v1
                                                       .CelMappingRule> celRules)
            throws Exception {
        MessageScope.Builder messageScope = MessageScope.builder();
        scope.forEach(messageScope::add);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(type);
        mapper.map(messageScope.build(), builder, rules);
        if (!celRules.isEmpty()) {
            new CelProtoMapper(mapper.fieldMapper(),
                    evaluator(scope, type), "target", Map.copyOf(scope))
                    .map(builder, celRules.stream()
                            .map(WorkflowReplay::toRecord).toList());
        }
        return builder.build();
    }

    private static ai.pipestream.proto.cel.CelMappingRule toRecord(
            ai.pipestream.proto.grpc.workflow.v1.CelMappingRule rule) {
        return new ai.pipestream.proto.cel.CelMappingRule(
                rule.getFilter().isBlank() ? null : rule.getFilter(),
                rule.getSelector().isBlank() ? null : rule.getSelector(),
                rule.getTarget(), rule.getFallbackList());
    }

    private static CelEvaluator evaluator(Map<String, Message> scope, Descriptor targetType) {
        CelEnvironmentFactory factory = CelEnvironmentFactory.builder();
        scope.forEach((name, message) ->
                factory.addMessageVar(name, message.getDescriptorForType()));
        factory.addMessageVar("target", targetType);
        return new CelEvaluator(factory.build());
    }

    /** Loads one fixture; the store has already re-hashed it, so bytes are authentic. */
    private static DynamicMessage parse(ArtifactRepository artifacts,
                                        ai.pipestream.proto.grpc.workflow.v1.ArtifactReference
                                                reference,
                                        Descriptor type, String what) throws IOException {
        ArtifactRepository.StoredArtifact artifact = artifacts.find(reference.getSha256())
                .orElseThrow(() -> new IOException(
                        "evidence references missing artifact " + reference.getSha256()
                                + " (" + what + ")"));
        try {
            return DynamicMessage.parseFrom(type, artifact.content());
        } catch (Exception e) {
            throw new IOException("recorded " + what + " does not parse as "
                    + type.getFullName(), e);
        }
    }

    private static Descriptor outputType(List<FileDescriptor> schema, Workflow workflow) {
        Descriptor type = findMessage(schema, workflow.getOutput().getType());
        if (type == null) {
            throw new IllegalArgumentException("output type " + workflow.getOutput().getType()
                    + " not in the replay schema");
        }
        return type;
    }

    /** The message with the given fully qualified name, nested types included. */
    private static Descriptor findMessage(List<FileDescriptor> schema, String fullName) {
        for (FileDescriptor file : schema) {
            Descriptor found = findIn(file.getMessageTypes(), fullName);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    private static Descriptor findIn(List<Descriptor> messages, String fullName) {
        for (Descriptor message : messages) {
            if (message.getFullName().equals(fullName)) {
                return message;
            }
            Descriptor nested = findIn(message.getNestedTypes(), fullName);
            if (nested != null) {
                return nested;
            }
        }
        return null;
    }
}
