package ai.protomolt.proto.pipeline;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.formats.Formats;
import ai.protomolt.proto.grpc.workflow.v1.FanOutSpec;
import ai.protomolt.proto.grpc.workflow.v1.TypedEdge;
import ai.protomolt.proto.pipeline.PipelineChecker.Finding;
import ai.protomolt.proto.projection.MessageProjection;
import ai.protomolt.proto.projection.ProjectionException;
import ai.protomolt.proto.shapes.RuleChecker;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The typed-edge and fan-out checks shared by gRPC and structured pipeline steps. Each check
 * reads the step's edge, the descriptors, and a read-only view of the scope's binding types,
 * and appends findings to the caller's list; it never binds, removes, or consumes a scope
 * entry. Stage state (which bindings are live and at what cardinality) belongs to
 * {@link PipelineChecker} alone.
 */
final class EdgeChecks {

    private EdgeChecks() {
    }

    /**
     * Checks a typed edge: sources resolve with their cardinalities, the mapping
     * type-checks against exactly the declared source scope, the produced type resolves,
     * a projection must support the value it projects, and a fan-out's items path, caps,
     * and collect target resolve against the descriptors.
     *
     * <p>The edge reads {@code scopeTypes}, the checker's scope reduced to each binding's
     * type, and never changes it: binding a step's output and consuming stream sources stay
     * with the {@link PipelineChecker} step check that owns the scope.</p>
     *
     * @return the type one edge value delivers to the step (element type for stream
     *         edges), or null when the edge is too broken to reason about
     */
    static Descriptor verifyEdge(String stepName, TypedEdge edge, FanOutSpec fanOut,
                                 Descriptor branchOutput, DescriptorRegistry registry,
                                 RuleChecker checker, Map<String, Descriptor> scopeTypes,
                                 List<Finding> findings) {
        Map<String, Descriptor> restricted = new LinkedHashMap<>();
        for (String source : edge.getSourcesList()) {
            if (!Formats.isIdentifier(source)) {
                findings.add(new Finding(stepName, "edge",
                        "edge source '" + source + "' is not an identifier"));
                continue;
            }
            Descriptor known = scopeTypes.get(source);
            if (known == null) {
                findings.add(new Finding(stepName, "edge",
                        "edge source '" + source + "' is not 'input' or a prior step"));
            } else {
                // A stream source binds its element type: the edge rules apply per
                // element, so the check scope sees the element type either way.
                restricted.put(source, known);
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
                edge.getRulesList(), CelMappingRules.toMapperRules(edge.getCelRulesList()), List.of())) {
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

    private static Descriptor projectToDelivery(TypedEdge edge, DescriptorRegistry registry,
                                                Descriptor projectedSource) {
        Descriptor projectTo = registry.findDescriptorByFullName(edge.getProjectTo());
        return projectTo != null ? projectTo : projectedSource;
    }

    private static void verifyFanOut(String stepName, FanOutSpec fanOut,
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
}
