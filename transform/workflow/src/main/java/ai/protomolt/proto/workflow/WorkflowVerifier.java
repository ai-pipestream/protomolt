package ai.protomolt.proto.workflow;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.shapes.RuleChecker;
import com.google.protobuf.Descriptors.Descriptor;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Verifies a workflow without running it — {@code check-workflow}'s engine. Step names must be
 * unique identifiers (they become scope variables); every method must be unary; each step's
 * gate and request mapping is checked by {@link RuleChecker} against exactly the scope that
 * step will see ({@code input} plus every prior step's response); the output mapping is
 * checked against the full scope. A gated ({@code when}) step's output is legitimately in
 * that scope because the runner binds a skipped step's name to its output type's default
 * instance — the static scope and the runtime value map always agree. A workflow that
 * verifies cannot fail on a type error at run time — only on live-service behavior.
 * Structured-generation steps check statically instead: the spec must be bare (no
 * gate, rules, deadline, validation flag, or completion mode), the model id non-blank,
 * the attempt cap within the coordinator's bound, and the target type's file reachable
 * from the workflow's files so compile and replay fingerprint over it.
 */
public final class WorkflowVerifier {

    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    /** One problem: {@code step} is empty for workflow-level findings. */
    public record Finding(String step, String kind, String error) {
    }

    public List<Finding> verify(CompiledWorkflow workflow) {
        List<Finding> findings = new ArrayList<>();
        RuleChecker checker = new RuleChecker();
        DescriptorRegistry registry = DescriptorRegistry.create();
        for (var file : workflow.files()) {
            registry.registerFile(file);
        }
        Map<String, Descriptor> scope = new LinkedHashMap<>();
        scope.put("input", workflow.inputType());

        for (CompiledWorkflow.Step step : workflow.steps()) {
            if (!IDENTIFIER.matcher(step.name()).matches()
                    || step.name().equals("input") || step.name().equals("target")) {
                findings.add(new Finding(step.name(), "workflow",
                        "step name must be an identifier other than 'input'/'target'"));
                continue;
            }
            if (scope.containsKey(step.name())) {
                findings.add(new Finding(step.name(), "workflow", "duplicate step name"));
                continue;
            }
            if (step.structured() != null) {
                verifyStructured(workflow, step, findings);
            } else {
                if (step.method().isClientStreaming() || step.method().isServerStreaming()) {
                    findings.add(new Finding(step.name(), "method",
                            step.method().getFullName() + " is not unary; workflows call unary "
                                    + "methods (streaming is a later phase)"));
                }
                if (!step.completion().isEmpty()
                        && !CompiledWorkflow.Step.COMPLETION_EXTERNAL.equals(step.completion())) {
                    findings.add(new Finding(step.name(), "completion",
                            "completion must be '' (invoke) or 'external'; got '"
                                    + step.completion() + "'"));
                }
                if (step.edge() == null) {
                    List<String> gates = step.when() == null || step.when().isBlank()
                            ? List.of() : List.of(step.when());
                    for (RuleChecker.Finding finding : checker.checkScoped(scope,
                            step.method().getInputType(), step.rules(), step.celRules(),
                            gates)) {
                        findings.add(new Finding(step.name(),
                                finding.kind().equals("filter") ? "when" : finding.kind(),
                                finding.error() + " (" + finding.rule() + ")"));
                    }
                } else if (step.when() != null && !step.when().isBlank()) {
                    // An edge step keeps its gate; the gate sees the full scope.
                    for (RuleChecker.Finding finding : checker.checkScoped(scope,
                            step.method().getInputType(), List.of(), List.of(),
                            List.of(step.when()))) {
                        findings.add(new Finding(step.name(), "when",
                                finding.error() + " (" + finding.rule() + ")"));
                    }
                }
            }
            Descriptor binding = step.structured() != null
                    ? step.structured().targetType()
                    : step.method().getOutputType();
            if (step.edge() != null) {
                binding = verifyEdge(workflow, step, scope, checker, registry, findings);
            }
            scope.put(step.name(), binding);
        }

        if (workflow.output() != null) {
            for (RuleChecker.Finding finding : checker.checkScoped(scope,
                    workflow.output().type(), workflow.output().rules(),
                    workflow.output().celRules(), List.of())) {
                findings.add(new Finding("", "output",
                        finding.error() + " (" + finding.rule() + ")"));
            }
        }
        return findings;
    }

    /**
     * A typed edge owns its step's request mapping. Statically: the top-level rule
     * lists must be empty; every declared source must be {@code input} or a prior
     * step; the edge's mapping rules check against exactly the restricted scope; the
     * produced type (and any projection target) must be reachable from the workflow's
     * files; a projection must support the type it projects; a gRPC step without
     * fan-out takes no projection and must produce the method's request type; a
     * structured step's grounding type must be visible from the target type's file
     * set; and a fan-out's items path, caps, and collect target must resolve.
     *
     * @return the descriptor the step binds into the scope: the collect type for a
     *         fanned-out step, the usual response or target type otherwise
     */
    private static Descriptor verifyEdge(CompiledWorkflow workflow, CompiledWorkflow.Step step,
                                         Map<String, Descriptor> scope, RuleChecker checker,
                                         DescriptorRegistry registry,
                                         List<Finding> findings) {
        CompiledWorkflow.EdgeSpec edge = step.edge();
        CompiledWorkflow.FanOutSpec fanOut = step.fanOut();
        Descriptor branchOutput = step.structured() != null
                ? step.structured().targetType()
                : step.method().getOutputType();

        if (step.external()) {
            findings.add(new Finding(step.name(), "edge",
                    "external-completion steps do not carry edges; the completion lane "
                            + "owns their request"));
        }
        if (!step.rules().isEmpty() || !step.celRules().isEmpty()) {
            findings.add(new Finding(step.name(), "edge",
                    "top-level rules and celRules must be empty when an edge is set; "
                            + "the edge owns request mapping"));
        }
        Map<String, Descriptor> restricted = new LinkedHashMap<>();
        for (String source : edge.sources()) {
            if (!IDENTIFIER.matcher(source).matches()) {
                findings.add(new Finding(step.name(), "edge",
                        "edge source '" + source + "' is not an identifier"));
                continue;
            }
            Descriptor known = scope.get(source);
            if (known == null) {
                findings.add(new Finding(step.name(), "edge",
                        "edge source '" + source + "' is not 'input' or a prior step"));
            } else {
                restricted.put(source, known);
            }
        }
        if (!reachable(workflow.files(), edge.produceType().getFile())) {
            findings.add(new Finding(step.name(), "edge",
                    "produce type " + edge.produceType().getFullName() + "'s file is not "
                            + "reachable from the workflow's files"));
        }
        for (RuleChecker.Finding finding : checker.checkScoped(restricted,
                edge.produceType(), edge.rules(), edge.celRules(), List.of())) {
            findings.add(new Finding(step.name(), "edge",
                    finding.error() + " (" + finding.rule() + ")"));
        }

        // The type a projection reads: the produced message, or each item when fanned out.
        Descriptor projectedSource = edge.produceType();
        if (fanOut != null) {
            try {
                projectedSource = EdgeFlow.itemType(edge.produceType(), fanOut.items());
            } catch (IllegalArgumentException e) {
                findings.add(new Finding(step.name(), "fanOut", e.getMessage()));
            }
            verifyFanOut(workflow, step, fanOut, projectedSource, branchOutput, findings);
        }
        if (edge.projectTo() != null) {
            verifyProjection(workflow, step, edge.projectTo(), projectedSource, registry,
                    findings);
        } else if (fanOut == null && step.structured() == null
                && !edge.produceType().getFullName()
                        .equals(step.method().getInputType().getFullName())) {
            findings.add(new Finding(step.name(), "edge",
                    "a gRPC step's edge must produce the method's request type "
                            + step.method().getInputType().getFullName() + "; got "
                            + edge.produceType().getFullName()));
        }

        // The value reaching a step: the projection output when projected, else the
        // produced message or item. gRPC steps require it to be the request type;
        // structured steps pack it as grounding, which must be visible from the
        // target type's file set for the coordinator's prompt registry.
        Descriptor delivered = edge.projectTo() != null
                ? edge.projectTo() : projectedSource;
        if (step.structured() == null && fanOut != null
                && !delivered.getFullName()
                        .equals(step.method().getInputType().getFullName())) {
            findings.add(new Finding(step.name(), "fanOut",
                    "a fanned-out gRPC step's item type must be the method's request "
                            + "type " + step.method().getInputType().getFullName()
                            + "; got " + delivered.getFullName()));
        } else if (step.structured() != null && !reachable(
                List.of(step.structured().targetType().getFile()), delivered.getFile())) {
            findings.add(new Finding(step.name(), "edge",
                    "grounding type " + delivered.getFullName() + "'s file is not "
                            + "visible from the target type's file set; import it so "
                            + "the coordinator can render the grounding"));
        }
        return fanOut != null ? fanOut.collectType() : branchOutput;
    }

    private static void verifyFanOut(CompiledWorkflow workflow, CompiledWorkflow.Step step,
                                     CompiledWorkflow.FanOutSpec fanOut,
                                     Descriptor itemType, Descriptor branchOutput,
                                     List<Finding> findings) {
        if (fanOut.maxItems() < 1
                || fanOut.maxItems() > ai.protomolt.proto.grpc.workflow.WorkflowValidation
                        .MAX_FANOUT_ITEMS) {
            findings.add(new Finding(step.name(), "fanOut",
                    "fan-out maxItems must be between 1 and "
                            + ai.protomolt.proto.grpc.workflow.WorkflowValidation
                                    .MAX_FANOUT_ITEMS + "; got " + fanOut.maxItems()));
        }
        if (fanOut.maxConcurrency() < 1
                || fanOut.maxConcurrency() > ai.protomolt.proto.grpc.workflow.WorkflowValidation
                        .MAX_FANOUT_CONCURRENCY) {
            findings.add(new Finding(step.name(), "fanOut",
                    "fan-out maxConcurrency must be between 1 and "
                            + ai.protomolt.proto.grpc.workflow.WorkflowValidation
                                    .MAX_FANOUT_CONCURRENCY + "; got "
                            + fanOut.maxConcurrency()));
        }
        if (!reachable(workflow.files(), fanOut.collectType().getFile())) {
            findings.add(new Finding(step.name(), "fanOut",
                    "collect type " + fanOut.collectType().getFullName() + "'s file is "
                            + "not reachable from the workflow's files"));
        }
        var collectField = fanOut.collectType().findFieldByName(fanOut.collectInto());
        if (collectField == null) {
            findings.add(new Finding(step.name(), "fanOut",
                    "collect type " + fanOut.collectType().getFullName()
                            + " has no field '" + fanOut.collectInto() + "'"));
        } else if (!collectField.isRepeated()
                || collectField.getJavaType()
                        != com.google.protobuf.Descriptors.FieldDescriptor.JavaType.MESSAGE) {
            findings.add(new Finding(step.name(), "fanOut",
                    "collect field '" + fanOut.collectInto() + "' of "
                            + fanOut.collectType().getFullName()
                            + " must be a repeated message field"));
        } else if (!collectField.getMessageType().getFullName()
                .equals(branchOutput.getFullName())) {
            findings.add(new Finding(step.name(), "fanOut",
                    "collect field '" + fanOut.collectInto() + "' takes "
                            + collectField.getMessageType().getFullName()
                            + " but the branch output type is "
                            + branchOutput.getFullName()));
        }
    }

    private static void verifyProjection(CompiledWorkflow workflow, CompiledWorkflow.Step step,
                                         Descriptor projectTo, Descriptor projectedSource,
                                         DescriptorRegistry registry,
                                         List<Finding> findings) {
        if (step.structured() == null && step.fanOut() == null) {
            findings.add(new Finding(step.name(), "edge",
                    "a gRPC step without fan-out takes no projection; the edge value "
                            + "must be the method request"));
        }
        if (!reachable(workflow.files(), projectTo.getFile())) {
            findings.add(new Finding(step.name(), "edge",
                    "projection target " + projectTo.getFullName() + "'s file is not "
                            + "reachable from the workflow's files"));
            return;
        }
        try {
            var projection = ai.protomolt.proto.projection.MessageProjection
                    .forTarget(projectTo, registry);
            if (projection.isEmpty()) {
                findings.add(new Finding(step.name(), "edge",
                        "projection target " + projectTo.getFullName()
                                + " declares no projection sources"));
            } else if (!projection.get().supports(projectedSource)) {
                findings.add(new Finding(step.name(), "edge",
                        "projection " + projectTo.getFullName() + " does not support "
                                + projectedSource.getFullName() + " as a source"));
            }
        } catch (ai.protomolt.proto.projection.ProjectionException e) {
            findings.add(new Finding(step.name(), "edge",
                    "projection target " + projectTo.getFullName() + " is broken: "
                            + e.getMessage()));
        }
    }

    /**
     * A structured step has no gRPC method: it must be a bare generation spec whose
     * target type the workflow's file set can fingerprint and replay can resolve. Gates,
     * mapping rules, deadlines, validation flags, and completion modes all belong to
     * the gRPC lane and are rejected here.
     */
    private static void verifyStructured(CompiledWorkflow workflow, CompiledWorkflow.Step step,
                                         List<Finding> findings) {
        CompiledWorkflow.StructuredSpec spec = step.structured();
        if (spec.model().isBlank()) {
            findings.add(new Finding(step.name(), "structured",
                    "structured step model must not be blank"));
        }
        if (spec.maxAttempts() < 0 || spec.maxAttempts() > 3) {
            findings.add(new Finding(step.name(), "structured",
                    "structured step maxAttempts must be between 0 and 3; got "
                            + spec.maxAttempts()));
        }
        if (step.when() != null && !step.when().isBlank()) {
            findings.add(new Finding(step.name(), "structured",
                    "structured steps do not support when gates"));
        }
        if (!step.rules().isEmpty() || !step.celRules().isEmpty()) {
            findings.add(new Finding(step.name(), "structured",
                    "structured steps declare no mapping rules; the coordinator fills "
                            + "the target type directly"));
        }
        if (step.validate()) {
            findings.add(new Finding(step.name(), "structured",
                    "validate is meaningless on a structured step; the coordinator "
                            + "always validates its output"));
        }
        if (step.deadlineMs() != 0) {
            findings.add(new Finding(step.name(), "structured",
                    "structured steps do not support per-step deadlines"));
        }
        if (!step.completion().isEmpty()) {
            findings.add(new Finding(step.name(), "structured",
                    "structured steps do not support completion modes"));
        }
        if (!reachable(workflow.files(), spec.targetType().getFile())) {
            findings.add(new Finding(step.name(), "structured",
                    "target type " + spec.targetType().getFullName() + "'s file "
                            + spec.targetType().getFile().getFullName()
                            + " is not reachable from the workflow's files; compile and "
                            + "replay fingerprint over the workflow's file set"));
        }
    }

    /** True when {@code file} is one of {@code files} or a transitive import of one. */
    private static boolean reachable(List<com.google.protobuf.Descriptors.FileDescriptor> files,
                                     com.google.protobuf.Descriptors.FileDescriptor file) {
        for (com.google.protobuf.Descriptors.FileDescriptor candidate : files) {
            if (candidate.equals(file) || reachable(candidate.getDependencies(), file)) {
                return true;
            }
        }
        return false;
    }
}
