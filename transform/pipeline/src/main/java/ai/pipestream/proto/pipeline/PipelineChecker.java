package ai.pipestream.proto.pipeline;

import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.grpc.recipe.RecipeValidation;
import ai.pipestream.proto.pipeline.v1.CollectStep;
import ai.pipestream.proto.pipeline.v1.EdgeCardinality;
import ai.pipestream.proto.pipeline.v1.GrpcCallStep;
import ai.pipestream.proto.pipeline.v1.MethodShape;
import ai.pipestream.proto.pipeline.v1.Pipeline;
import ai.pipestream.proto.pipeline.v1.PipelineStep;
import ai.pipestream.proto.pipeline.v1.StructuredStep;
import ai.pipestream.proto.pipeline.v1.UnnestStep;
import ai.pipestream.proto.projection.MessageProjection;
import ai.pipestream.proto.projection.ProjectionException;
import ai.pipestream.proto.shapes.RuleChecker;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.MethodDescriptor;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Verifies a pipeline against its descriptor set without running it and without contacting
 * anything: no network, no reflection call, no container. The checker re-derives every
 * method's streaming shape from the descriptor's streaming flags and every binding's
 * cardinality from the dataflow, and rejects drift between the declared contract and the
 * descriptors with messages that name the file, type, field path, and flags involved.
 *
 * <p>The typed-edge checks are the chain verifier's, carried onto every streaming shape:
 * edge sources must be {@code input} or prior steps, mapping and CEL rules type-check
 * against exactly the declared source scope (stream sources bind their element type),
 * projections must support the value they project, fan-out items paths, caps, and collect
 * targets must resolve, and a gRPC step's edge must produce the method's request type. On
 * top of those, the cardinality discipline is absolute: a unary or server-streaming request
 * slot takes a ONE edge, a client-streaming or bidi slot takes a MANY edge, a structured
 * step's grounding takes ONE, fan-out branches invoke unary methods only, and a stream
 * reaches a ONE slot only through an explicit collect step. A pipeline that verifies cannot
 * fail on a shape or type error at run time, only on live-service behavior.</p>
 */
public final class PipelineChecker {

    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    /** One problem: {@code step} is empty for pipeline-level findings. */
    public record Finding(String step, String kind, String error) {
    }

    /** One scope binding: the message type plus how many values a run carries through it. */
    private record Binding(Descriptor type, EdgeCardinality cardinality) {
    }

    /**
     * Verifies {@code pipeline} against {@code files}.
     *
     * @return every finding, in step order; an empty list means the pipeline is
     *         shape-correct against the descriptor set
     */
    public List<Finding> verify(Pipeline pipeline, List<FileDescriptor> files) {
        Objects.requireNonNull(pipeline, "pipeline");
        Objects.requireNonNull(files, "files");
        List<Finding> findings = new ArrayList<>();
        try {
            PipelineValidation.validate(pipeline);
        } catch (IllegalArgumentException e) {
            findings.add(new Finding("", "pipeline", e.getMessage()));
            return findings;
        }
        String fingerprint = DescriptorSets.fingerprint(files);
        if (!pipeline.getDescriptorFingerprint().equals(fingerprint)) {
            findings.add(new Finding("", "pipeline",
                    "descriptor_fingerprint " + pipeline.getDescriptorFingerprint()
                            + " does not match the supplied descriptor set's fingerprint "
                            + fingerprint + " (files: " + DescriptorSets.fileNames(files)
                            + "); check the pipeline against the exact descriptors it was"
                            + " compiled from"));
        }
        DescriptorRegistry registry = DescriptorRegistry.create(false);
        for (FileDescriptor file : files) {
            registry.registerFile(file);
        }
        Descriptor input = registry.findDescriptorByFullName(pipeline.getInputType());
        if (input == null) {
            findings.add(new Finding("", "pipeline",
                    "input_type " + pipeline.getInputType() + " does not resolve against"
                            + " the descriptor set (files: " + DescriptorSets.fileNames(files)
                            + ")"));
            return findings;
        }

        Map<String, Binding> scope = new LinkedHashMap<>();
        scope.put("input", new Binding(input, EdgeCardinality.EDGE_CARDINALITY_ONE));
        RuleChecker checker = new RuleChecker();
        for (PipelineStep step : pipeline.getStepsList()) {
            verifyStep(step, files, registry, checker, scope, findings);
        }

        if (pipeline.hasOutput()) {
            Descriptor outputType =
                    registry.findDescriptorByFullName(pipeline.getOutput().getType());
            if (outputType == null) {
                findings.add(new Finding("", "output",
                        "output type " + pipeline.getOutput().getType() + " does not"
                                + " resolve against the descriptor set"));
            } else {
                for (Map.Entry<String, Binding> entry : scope.entrySet()) {
                    if (entry.getValue().cardinality()
                            == EdgeCardinality.EDGE_CARDINALITY_MANY) {
                        findings.add(new Finding("", "output",
                                "the output mapping reads the final scope, but binding '"
                                        + entry.getKey() + "' is a stream (MANY); collect"
                                        + " it into a single message first"));
                    }
                }
                for (RuleChecker.Finding finding : checker.checkScoped(types(scope),
                        outputType, pipeline.getOutput().getRulesList(),
                        records(pipeline.getOutput().getCelRulesList()), List.of())) {
                    findings.add(new Finding("", "output",
                            finding.error() + " (" + finding.rule() + ")"));
                }
            }
        }
        return findings;
    }

    private static void verifyStep(PipelineStep step, List<FileDescriptor> files,
                                   DescriptorRegistry registry, RuleChecker checker,
                                   Map<String, Binding> scope, List<Finding> findings) {
        if (!IDENTIFIER.matcher(step.getName()).matches()
                || step.getName().equals("input") || step.getName().equals("target")) {
            findings.add(new Finding(step.getName(), "pipeline",
                    "step name must be an identifier other than 'input'/'target'"));
            return;
        }
        if (scope.containsKey(step.getName())) {
            findings.add(new Finding(step.getName(), "pipeline", "duplicate step name"));
            return;
        }
        if (step.hasGrpcCall()) {
            verifyGrpcCall(step, files, registry, checker, scope, findings);
        } else if (step.hasStructured()) {
            verifyStructured(step, registry, checker, scope, findings);
        } else if (step.hasUnnest()) {
            verifyUnnest(step, scope, findings);
        } else if (step.hasCollect()) {
            verifyCollect(step, registry, scope, findings);
        }
    }

    private static void verifyGrpcCall(PipelineStep step, List<FileDescriptor> files,
                                       DescriptorRegistry registry, RuleChecker checker,
                                       Map<String, Binding> scope, List<Finding> findings) {
        GrpcCallStep call = step.getGrpcCall();
        if (step.getDependency().isEmpty()) {
            findings.add(new Finding(step.getName(), "step",
                    "a gRPC step must name a declared dependency"));
        }
        MethodDescriptor method;
        try {
            method = DescriptorSets.resolveMethod(files, call.getMethod());
        } catch (IllegalArgumentException e) {
            findings.add(new Finding(step.getName(), "method", e.getMessage()));
            return;
        }
        if (method == null) {
            findings.add(new Finding(step.getName(), "method",
                    "method '" + call.getMethod() + "' not found in the descriptor set"
                            + " (files: " + DescriptorSets.fileNames(files) + ")"));
            return;
        }
        MethodShape derived = RecipePipelineCompiler.shapeOf(method);
        if (call.getMethodShape() != derived) {
            findings.add(new Finding(step.getName(), "shape",
                    "method " + method.getFullName() + " in file "
                            + method.getFile().getName() + " declares clientStreaming="
                            + method.isClientStreaming() + ", serverStreaming="
                            + method.isServerStreaming() + " (" + derived
                            + ") but the step declares " + call.getMethodShape()
                            + "; the contract must match the descriptor"));
        }
        if (!step.getWhen().isBlank()) {
            for (RuleChecker.Finding finding : checker.checkScoped(types(scope),
                    method.getInputType(), List.of(), List.of(), List.of(step.getWhen()))) {
                findings.add(new Finding(step.getName(), "when",
                        finding.error() + " (" + finding.rule() + ")"));
            }
        }

        boolean external = call.getCompletion()
                == ai.pipestream.proto.grpc.recipe.v1.StepCompletion
                        .STEP_COMPLETION_EXTERNAL;
        if (external) {
            bind(step.getName(), scope, call, method.getOutputType(),
                    outputCardinality(call, method), findings);
            return;
        }
        if (!call.hasEdge()) {
            // Structural validation already reports this; nothing further to check.
            return;
        }
        Descriptor delivered = verifyEdge(step.getName(), call.getEdge(), call.hasFanOut()
                ? call.getFanOut() : null, method.getOutputType(), registry, checker, scope,
                findings);
        if (delivered == null) {
            return;
        }

        // The slot cardinality: client-streaming and bidi methods consume a request
        // stream; unary and server-streaming methods consume one request.
        EdgeCardinality edgeCardinality = edgeCardinality(call.getEdge(), scope);
        if (edgeCardinality != call.getEdgeCardinality()) {
            findings.add(new Finding(step.getName(), "cardinality",
                    "the edge declares " + call.getEdgeCardinality() + " but its sources"
                            + " make it " + edgeCardinality + "; the declared cardinality"
                            + " must match the dataflow"));
        }
        if (method.isClientStreaming()
                && edgeCardinality != EdgeCardinality.EDGE_CARDINALITY_MANY) {
            findings.add(new Finding(step.getName(), "cardinality",
                    "method " + method.getFullName() + " is " + derived
                            + " (clientStreaming=true): its request slot takes a stream"
                            + " (MANY) but the edge produces ONE value; unnest a repeated"
                            + " field of an upstream binding first"));
        }
        if (!method.isClientStreaming()
                && edgeCardinality == EdgeCardinality.EDGE_CARDINALITY_MANY) {
            findings.add(new Finding(step.getName(), "cardinality",
                    "method " + method.getFullName() + " is " + derived
                            + " (clientStreaming=false): its request slot takes ONE value"
                            + " but the edge is a stream (MANY); collect the stream or fan"
                            + " out over a repeated field"));
        }

        if (call.hasFanOut()) {
            if (edgeCardinality == EdgeCardinality.EDGE_CARDINALITY_MANY) {
                findings.add(new Finding(step.getName(), "fanOut",
                        "fan-out iterates a repeated field of one produced message, but"
                                + " the edge is a stream (MANY)"));
            }
            if (derived != MethodShape.METHOD_SHAPE_UNARY) {
                findings.add(new Finding(step.getName(), "fanOut",
                        "fan-out branches invoke the method once per item, so the method"
                                + " must be unary; " + method.getFullName() + " is "
                                + derived + " (clientStreaming="
                                + method.isClientStreaming() + ", serverStreaming="
                                + method.isServerStreaming() + ")"));
            }
            if (!delivered.getFullName().equals(method.getInputType().getFullName())) {
                findings.add(new Finding(step.getName(), "fanOut",
                        "a fanned-out gRPC step's item type must be the method's request"
                                + " type " + method.getInputType().getFullName()
                                + "; got " + delivered.getFullName()));
            }
        } else if (!call.getEdge().getProjectTo().isEmpty()) {
            findings.add(new Finding(step.getName(), "edge",
                    "a gRPC step without fan-out takes no projection; the edge value"
                            + " must be the method request"));
        } else if (!call.getEdge().getProduceType()
                .equals(method.getInputType().getFullName())) {
            findings.add(new Finding(step.getName(), "edge",
                    "a gRPC step's edge must produce the method's request type "
                            + method.getInputType().getFullName() + " (declared in file "
                            + method.getInputType().getFile().getName() + "); got "
                            + call.getEdge().getProduceType()));
        }
        bind(step.getName(), scope, call,
                call.hasFanOut()
                        ? registry.findDescriptorByFullName(call.getFanOut().getCollectType())
                        : method.getOutputType(),
                outputCardinality(call, method), findings);
    }

    /**
     * Checks a typed edge: sources resolve with their cardinalities, the mapping
     * type-checks against exactly the declared source scope, the produced type resolves,
     * a projection must support the value it projects, and a fan-out's items path, caps,
     * and collect target resolve against the descriptors.
     *
     * @return the type one edge value delivers to the step (element type for stream
     *         edges), or null when the edge is too broken to reason about
     */
    private static Descriptor verifyEdge(String stepName,
                                         ai.pipestream.proto.grpc.recipe.v1.TypedEdge edge,
                                         ai.pipestream.proto.grpc.recipe.v1.FanOutSpec fanOut,
                                         Descriptor branchOutput,
                                         DescriptorRegistry registry, RuleChecker checker,
                                         Map<String, Binding> scope,
                                         List<Finding> findings) {
        Map<String, Descriptor> restricted = new LinkedHashMap<>();
        for (String source : edge.getSourcesList()) {
            if (!IDENTIFIER.matcher(source).matches()) {
                findings.add(new Finding(stepName, "edge",
                        "edge source '" + source + "' is not an identifier"));
                continue;
            }
            Binding known = scope.get(source);
            if (known == null) {
                findings.add(new Finding(stepName, "edge",
                        "edge source '" + source + "' is not 'input' or a prior step"));
            } else {
                // A stream source binds its element type: the edge rules apply per
                // element, so the check scope sees the element type either way.
                restricted.put(source, known.type());
            }
        }
        Descriptor produced = registry.findDescriptorByFullName(edge.getProduceType());
        if (produced == null) {
            findings.add(new Finding(stepName, "edge",
                    "produce type " + edge.getProduceType() + " does not resolve against"
                            + " the descriptor set"));
            return null;
        }
        for (RuleChecker.Finding finding : checker.checkScoped(restricted, produced,
                edge.getRulesList(), records(edge.getCelRulesList()), List.of())) {
            findings.add(new Finding(stepName, "edge",
                    finding.error() + " (" + finding.rule() + ")"));
        }

        // The value a projection reads and a fan-out iterates: the produced message,
        // or each item when fanned out.
        Descriptor projectedSource = produced;
        if (fanOut != null) {
            try {
                projectedSource = StreamPaths.elementType(produced, fanOut.getItems());
            } catch (IllegalArgumentException e) {
                findings.add(new Finding(stepName, "fanOut",
                        "items " + e.getMessage()));
            }
            verifyFanOut(stepName, fanOut, branchOutput, registry, findings);
        }
        if (!edge.getProjectTo().isEmpty()) {
            Descriptor projectTo =
                    registry.findDescriptorByFullName(edge.getProjectTo());
            if (projectTo == null) {
                findings.add(new Finding(stepName, "edge",
                        "projection target " + edge.getProjectTo() + " does not resolve"
                                + " against the descriptor set"));
            } else {
                try {
                    var projection = MessageProjection.forTarget(projectTo, registry);
                    if (projection.isEmpty()) {
                        findings.add(new Finding(stepName, "edge",
                                "projection target " + edge.getProjectTo()
                                        + " declares no projection sources"));
                    } else if (!projection.get().supports(projectedSource)) {
                        findings.add(new Finding(stepName, "edge",
                                "projection " + edge.getProjectTo() + " does not support "
                                        + projectedSource.getFullName() + " as a source"));
                    }
                } catch (ProjectionException e) {
                    findings.add(new Finding(stepName, "edge",
                            "projection target " + edge.getProjectTo() + " is broken: "
                                    + e.getMessage()));
                }
            }
            return projectToDelivery(edge, registry, projectedSource);
        }
        return projectedSource;
    }

    private static Descriptor projectToDelivery(
            ai.pipestream.proto.grpc.recipe.v1.TypedEdge edge, DescriptorRegistry registry,
            Descriptor projectedSource) {
        Descriptor projectTo = registry.findDescriptorByFullName(edge.getProjectTo());
        return projectTo != null ? projectTo : projectedSource;
    }

    private static void verifyFanOut(String stepName,
                                     ai.pipestream.proto.grpc.recipe.v1.FanOutSpec fanOut,
                                     Descriptor branchOutput,
                                     DescriptorRegistry registry,
                                     List<Finding> findings) {
        Descriptor collectType =
                registry.findDescriptorByFullName(fanOut.getCollectType());
        if (collectType == null) {
            findings.add(new Finding(stepName, "fanOut",
                    "collect type " + fanOut.getCollectType() + " does not resolve"
                            + " against the descriptor set"));
            return;
        }
        FieldDescriptor collectField =
                collectType.findFieldByName(fanOut.getCollectInto());
        if (collectField == null) {
            findings.add(new Finding(stepName, "fanOut",
                    "collect type " + fanOut.getCollectType() + " (file "
                            + collectType.getFile().getName() + ") has no field '"
                            + fanOut.getCollectInto() + "'"));
        } else if (!collectField.isRepeated()
                || collectField.getJavaType() != FieldDescriptor.JavaType.MESSAGE) {
            findings.add(new Finding(stepName, "fanOut",
                    "collect field '" + fanOut.getCollectInto() + "' of "
                            + fanOut.getCollectType() + " must be a repeated message"
                            + " field"));
        } else if (!collectField.getMessageType().getFullName()
                .equals(branchOutput.getFullName())) {
            findings.add(new Finding(stepName, "fanOut",
                    "collect field '" + fanOut.getCollectInto() + "' takes "
                            + collectField.getMessageType().getFullName()
                            + " but the branch output type is "
                            + branchOutput.getFullName()));
        }
    }

    private static void verifyStructured(PipelineStep step, DescriptorRegistry registry,
                                         RuleChecker checker, Map<String, Binding> scope,
                                         List<Finding> findings) {
        StructuredStep structured = step.getStructured();
        if (step.getDependency().isEmpty()) {
            findings.add(new Finding(step.getName(), "step",
                    "a structured step must name a declared dependency"));
        }
        Descriptor targetType =
                registry.findDescriptorByFullName(structured.getSpec().getTargetType());
        if (targetType == null) {
            findings.add(new Finding(step.getName(), "structured",
                    "target type " + structured.getSpec().getTargetType() + " does not"
                            + " resolve against the descriptor set"));
            return;
        }
        Descriptor binding = targetType;
        if (structured.hasEdge()) {
            for (String source : structured.getEdge().getSourcesList()) {
                Binding known = scope.get(source);
                if (known != null && known.cardinality()
                        == EdgeCardinality.EDGE_CARDINALITY_MANY) {
                    findings.add(new Finding(step.getName(), "cardinality",
                            "structured grounding takes ONE value, but source '" + source
                                    + "' binds a stream (MANY); collect the stream"
                                    + " first"));
                }
            }
            Descriptor delivered = verifyEdge(step.getName(), structured.getEdge(),
                    structured.hasFanOut() ? structured.getFanOut() : null, targetType,
                    registry, checker, scope, findings);
            if (delivered == null) {
                return;
            }
            if (structured.hasFanOut()) {
                Descriptor collectType = registry.findDescriptorByFullName(
                        structured.getFanOut().getCollectType());
                if (collectType == null) {
                    return;
                }
                binding = collectType;
            }
        }
        scope.put(step.getName(),
                new Binding(binding, EdgeCardinality.EDGE_CARDINALITY_ONE));
    }

    private static void verifyUnnest(PipelineStep step, Map<String, Binding> scope,
                                     List<Finding> findings) {
        UnnestStep unnest = step.getUnnest();
        Binding source = scope.get(unnest.getSource());
        if (source == null) {
            findings.add(new Finding(step.getName(), "unnest",
                    "unnest source '" + unnest.getSource() + "' is not 'input' or a prior"
                            + " step"));
            return;
        }
        if (source.cardinality() != EdgeCardinality.EDGE_CARDINALITY_ONE) {
            findings.add(new Finding(step.getName(), "cardinality",
                    "unnest reads one message, but source '" + unnest.getSource()
                            + "' binds a stream (MANY)"));
            return;
        }
        Descriptor element;
        try {
            element = StreamPaths.elementType(source.type(), unnest.getPath());
        } catch (IllegalArgumentException e) {
            findings.add(new Finding(step.getName(), "unnest", e.getMessage()));
            return;
        }
        scope.put(step.getName(),
                new Binding(element, EdgeCardinality.EDGE_CARDINALITY_MANY));
    }

    private static void verifyCollect(PipelineStep step, DescriptorRegistry registry,
                                      Map<String, Binding> scope,
                                      List<Finding> findings) {
        CollectStep collect = step.getCollect();
        Binding source = scope.get(collect.getSource());
        if (source == null) {
            findings.add(new Finding(step.getName(), "collect",
                    "collect source '" + collect.getSource() + "' is not 'input' or a"
                            + " prior step"));
            return;
        }
        if (source.cardinality() != EdgeCardinality.EDGE_CARDINALITY_MANY) {
            findings.add(new Finding(step.getName(), "cardinality",
                    "collect consumes a stream, but source '" + collect.getSource()
                            + "' binds ONE value; there is nothing to collect"));
            return;
        }
        Descriptor collectType =
                registry.findDescriptorByFullName(collect.getCollectType());
        if (collectType == null) {
            findings.add(new Finding(step.getName(), "collect",
                    "collect type " + collect.getCollectType() + " does not resolve"
                            + " against the descriptor set"));
            return;
        }
        FieldDescriptor field = collectType.findFieldByName(collect.getCollectInto());
        if (field == null) {
            findings.add(new Finding(step.getName(), "collect",
                    "collect type " + collect.getCollectType() + " (file "
                            + collectType.getFile().getName() + ") has no field '"
                            + collect.getCollectInto() + "'"));
            return;
        }
        if (!field.isRepeated()
                || field.getJavaType() != FieldDescriptor.JavaType.MESSAGE) {
            findings.add(new Finding(step.getName(), "collect",
                    "collect field '" + collect.getCollectInto() + "' of "
                            + collect.getCollectType()
                            + " must be a repeated message field"));
            return;
        }
        if (!field.getMessageType().getFullName()
                .equals(source.type().getFullName())) {
            findings.add(new Finding(step.getName(), "cardinality",
                    "collect field '" + collect.getCollectInto() + "' of "
                            + collect.getCollectType() + " takes "
                            + field.getMessageType().getFullName()
                            + " but the stream '" + collect.getSource()
                            + "' carries " + source.type().getFullName()));
            return;
        }
        scope.put(step.getName(),
                new Binding(collectType, EdgeCardinality.EDGE_CARDINALITY_ONE));
    }

    /** The cardinality an edge's declared sources give it: MANY when any source streams. */
    private static EdgeCardinality edgeCardinality(
            ai.pipestream.proto.grpc.recipe.v1.TypedEdge edge,
            Map<String, Binding> scope) {
        for (String source : edge.getSourcesList()) {
            Binding known = scope.get(source);
            if (known != null && known.cardinality()
                    == EdgeCardinality.EDGE_CARDINALITY_MANY) {
                return EdgeCardinality.EDGE_CARDINALITY_MANY;
            }
        }
        return EdgeCardinality.EDGE_CARDINALITY_ONE;
    }

    private static EdgeCardinality outputCardinality(GrpcCallStep call,
                                                     MethodDescriptor method) {
        return !call.hasFanOut() && method.isServerStreaming()
                ? EdgeCardinality.EDGE_CARDINALITY_MANY
                : EdgeCardinality.EDGE_CARDINALITY_ONE;
    }

    /** Binds a gRPC step's output, checking the declared cardinality against the derived. */
    private static void bind(String stepName, Map<String, Binding> scope, GrpcCallStep call,
                             Descriptor type, EdgeCardinality derived,
                             List<Finding> findings) {
        if (call.getOutputCardinality() != derived) {
            findings.add(new Finding(stepName, "cardinality",
                    "the step declares output_cardinality " + call.getOutputCardinality()
                            + " but the dataflow derives " + derived + (call.hasFanOut()
                            ? "; a fanned-out step binds its single collect message"
                            : "")));
        }
        if (type != null) {
            scope.put(stepName, new Binding(type, derived));
        }
    }

    private static Map<String, Descriptor> types(Map<String, Binding> scope) {
        Map<String, Descriptor> types = new LinkedHashMap<>();
        scope.forEach((name, binding) -> types.put(name, binding.type()));
        return types;
    }

    private static List<ai.pipestream.proto.cel.CelMappingRule> records(
            List<ai.pipestream.proto.grpc.recipe.v1.CelMappingRule> rules) {
        return rules.stream()
                .map(rule -> new ai.pipestream.proto.cel.CelMappingRule(
                        rule.getFilter().isBlank() ? null : rule.getFilter(),
                        rule.getSelector().isBlank() ? null : rule.getSelector(),
                        rule.getTarget(), rule.getFallbackList()))
                .toList();
    }
}
