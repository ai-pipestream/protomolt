package ai.protomolt.proto.workflow;

import ai.protomolt.proto.grpc.profile.ServiceProfileValidation;
import ai.protomolt.proto.grpc.workflow.WorkflowValidation;
import ai.protomolt.proto.grpc.workflow.v1.Workflow;
import ai.protomolt.proto.grpc.workflow.v1.WorkflowOutput;
import ai.protomolt.proto.grpc.workflow.v1.WorkflowStep;
import ai.protomolt.proto.grpc.workflow.v1.ServiceDependency;
import ai.protomolt.proto.grpc.workflow.v1.StepCompletion;
import ai.protomolt.proto.grpc.workflow.v1.StructuredGenerationSpec;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Duration;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Compiles a resolved {@link CompiledWorkflow} into the durable {@link Workflow} contract,
 * losslessly: every mapping rule, gate, deadline, and validation flag carries over, and the
 * result always passes {@link WorkflowValidation} before it is returned.
 *
 * <p>Workflows name concrete targets; workflows name service profiles. The compiler bridges that
 * gap by deriving one dependency per distinct service full name: the alias and profile name
 * are the service's fully qualified name (stable and path-safe), the endpoint is the step's
 * target sanitized into a path-safe placeholder, and the descriptor fingerprint is the
 * SHA-256 of the workflow's files serialized in name order. Binding those placeholders to real
 * profiles and endpoints is the promoter's act, not the compiler's.</p>
 *
 * <p>Compilation is deterministic: identical workflows compile to identical bytes, and
 * reordering the workflow's file list does not change dependency fingerprints.</p>
 */
public final class WorkflowCompiler {

    private WorkflowCompiler() {
    }

    /**
     * Compiles {@code workflow} into a validated workflow.
     *
     * @throws IllegalArgumentException when any workflow identity (name, step name) is not
     *         path-safe or the result otherwise violates the workflow contract
     */
    public static Workflow compile(CompiledWorkflow workflow) {
        Objects.requireNonNull(workflow, "workflow");
        String fingerprint = descriptorFingerprint(workflow.files());

        // One dependency per distinct service, in first-use order: steps keep referencing
        // their own service even when several services share one target. Structured
        // steps share one deterministic dependency anchor whose fingerprint is the same
        // workflow file set - the target type's file is part of it, so compile and replay
        // fingerprint over the very descriptors the coordinator filled.
        Map<String, ServiceDependency> dependencies = new LinkedHashMap<>();
        List<WorkflowStep> steps = new ArrayList<>(workflow.steps().size());
        for (CompiledWorkflow.Step step : workflow.steps()) {
            if (step.structured() != null) {
                dependencies.computeIfAbsent(CompiledWorkflow.Step.STRUCTURED_DEPENDENCY,
                        name -> ServiceDependency.newBuilder()
                                .setAlias(name)
                                .setServiceProfile(name)
                                .setEndpoint("local")
                                .setDescriptorFingerprint(fingerprint)
                                .build());
                steps.add(withEdge(WorkflowStep.newBuilder()
                        .setName(step.name())
                        .setDependency(CompiledWorkflow.Step.STRUCTURED_DEPENDENCY)
                        .setStructured(StructuredGenerationSpec.newBuilder()
                                .setTargetType(step.structured().targetType().getFullName())
                                .setModel(step.structured().model())
                                .setMaxAttempts(step.structured().maxAttempts())
                                .build())
                        .setCompletion(StepCompletion.STEP_COMPLETION_LIVE), step)
                        .build());
                continue;
            }
            String service = step.method().getService().getFullName();
            dependencies.computeIfAbsent(service, name -> ServiceDependency.newBuilder()
                    .setAlias(name)
                    .setServiceProfile(name)
                    .setEndpoint(endpointName(step.target()))
                    .setDescriptorFingerprint(fingerprint)
                    .build());
            steps.add(compileStep(step, service));
        }

        Workflow.Builder builder = Workflow.newBuilder()
                .setName(workflow.name())
                .setInputType(workflow.inputType().getFullName())
                .setDeadline(millis(workflow.deadlineMs()))
                .addAllDependencies(dependencies.values())
                .addAllSteps(steps);
        if (workflow.output() != null) {
            builder.setOutput(WorkflowOutput.newBuilder()
                    .setType(workflow.output().type().getFullName())
                    .addAllRules(workflow.output().rules())
                    .addAllCelRules(workflow.output().celRules().stream()
                            .map(WorkflowCompiler::compileRule).toList())
                    .build());
        }
        Workflow built = builder.build();
        // The contract speaks last: any identity the workflow carried that a repository or MCP
        // resource could not hold fails here, at compile time, with the field named.
        WorkflowValidation.validate(built);
        return built;
    }

    private static WorkflowStep compileStep(CompiledWorkflow.Step step, String service) {
        WorkflowStep.Builder builder = WorkflowStep.newBuilder()
                .setName(step.name())
                .setDependency(service)
                .setMethod(service + "/" + step.method().getName())
                .addAllRules(step.rules())
                .addAllCelRules(step.celRules().stream()
                        .map(WorkflowCompiler::compileRule).toList())
                .setValidateResponse(step.validate())
                .setCompletion(step.external()
                        ? StepCompletion.STEP_COMPLETION_EXTERNAL
                        : StepCompletion.STEP_COMPLETION_LIVE);
        if (step.when() != null && !step.when().isBlank()) {
            builder.setWhen(step.when());
        }
        if (step.deadlineMs() > 0) {
            builder.setDeadline(millis(step.deadlineMs()));
        }
        return withEdge(builder, step).build();
    }

    /** Attaches the step's typed edge and fan-out, when declared, to the workflow step. */
    private static WorkflowStep.Builder withEdge(WorkflowStep.Builder builder,
                                               CompiledWorkflow.Step step) {
        if (step.edge() == null) {
            return builder;
        }
        CompiledWorkflow.EdgeSpec edge = step.edge();
        ai.protomolt.proto.grpc.workflow.v1.TypedEdge.Builder edgeBuilder =
                ai.protomolt.proto.grpc.workflow.v1.TypedEdge.newBuilder()
                        .addAllSources(edge.sources())
                        .setProduceType(edge.produceType().getFullName())
                        .addAllRules(edge.rules())
                        .addAllCelRules(edge.celRules().stream()
                                .map(WorkflowCompiler::compileRule).toList())
                        .setValidate(edge.validate());
        if (edge.projectTo() != null) {
            edgeBuilder.setProjectTo(edge.projectTo().getFullName());
        }
        builder.setEdge(edgeBuilder.build());
        if (step.fanOut() != null) {
            CompiledWorkflow.FanOutSpec fanOut = step.fanOut();
            builder.setFanOut(ai.protomolt.proto.grpc.workflow.v1.FanOutSpec.newBuilder()
                    .setItems(fanOut.items())
                    .setMaxItems(fanOut.maxItems())
                    .setMaxConcurrency(fanOut.maxConcurrency())
                    .setFailurePolicy(fanOut.failurePolicy()
                                    == CompiledWorkflow.BranchFailurePolicy.FAIL_FAST
                            ? ai.protomolt.proto.grpc.workflow.v1.BranchFailurePolicy
                                    .BRANCH_FAILURE_POLICY_FAIL_FAST
                            : ai.protomolt.proto.grpc.workflow.v1.BranchFailurePolicy
                                    .BRANCH_FAILURE_POLICY_CONTINUE)
                    .setCollectType(fanOut.collectType().getFullName())
                    .setCollectInto(fanOut.collectInto())
                    .build());
        }
        return builder;
    }

    private static ai.protomolt.proto.grpc.workflow.v1.CelMappingRule compileRule(
            ai.protomolt.proto.cel.CelMappingRule rule) {
        ai.protomolt.proto.grpc.workflow.v1.CelMappingRule.Builder builder =
                ai.protomolt.proto.grpc.workflow.v1.CelMappingRule.newBuilder()
                        .setTarget(rule.targetPath())
                        .addAllFallback(rule.textRuleFallback());
        if (rule.filterExpression() != null) {
            builder.setFilter(rule.filterExpression());
        }
        if (rule.selectorExpression() != null) {
            builder.setSelector(rule.selectorExpression());
        }
        return builder.build();
    }

    /**
     * The workflow's schema as one stable fingerprint: files serialized in name order, so the
     * hash depends on content alone, never on declaration order in the caller's list.
     */
    static String descriptorFingerprint(List<FileDescriptor> files) {
        FileDescriptorSet set = FileDescriptorSet.newBuilder()
                .addAllFile(files.stream()
                        .map(FileDescriptor::toProto)
                        .sorted(Comparator.comparing(proto -> proto.getName()))
                        .toList())
                .build();
        return ServiceProfileValidation.sha256(set.toByteArray());
    }

    /**
     * A target as a path-safe endpoint placeholder. Workflows carry addresses
     * ({@code localhost:9090}); the workflow contract carries names, so anything outside the
     * name alphabet folds to a dash. This is a stand-in the promoter rebinds, not a
     * connection detail.
     */
    static String endpointName(String target) {
        String sanitized = target.replaceAll("[^A-Za-z0-9._-]", "-");
        if (sanitized.isEmpty() || !Character.isLetterOrDigit(sanitized.charAt(0))) {
            sanitized = "e" + sanitized;
        }
        return sanitized.length() > 128 ? sanitized.substring(0, 128) : sanitized;
    }

    private static Duration millis(long deadlineMs) {
        return Duration.newBuilder()
                .setSeconds(deadlineMs / 1_000)
                .setNanos((int) (deadlineMs % 1_000) * 1_000_000)
                .build();
    }
}
