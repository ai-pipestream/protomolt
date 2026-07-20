package ai.pipestream.proto.quality;

import ai.pipestream.proto.cel.CelCompilationException;
import ai.pipestream.proto.cel.CelEnvironmentFactory;
import ai.pipestream.proto.cel.CelEvaluationException;
import ai.pipestream.proto.cel.CelEvaluator;
import com.google.common.primitives.UnsignedLong;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Scores messages against the quality dimensions their own schema declares.
 *
 * <p>The dimensions are {@code (ai.pipestream.proto.quality.v1.quality)} message options — CEL
 * expressions over {@code this} returning a score — so quality travels with the descriptor the
 * way validation rules do. Where validation is a gate (admissible or not), quality is a
 * measurement: every expression's result is coerced to a double and clamped to {@code [0, 1]},
 * a bool scores 1 or 0, and the composite is the weighted average of the scored dimensions.</p>
 *
 * <p>Failure semantics mirror the validator's: an expression that does not compile is a schema
 * error and throws {@link QualitySchemaException} deterministically on the type's first scoring;
 * an expression that compiles but fails on a particular message (division by a zero field, say)
 * marks that dimension failed in the report rather than throwing, because a quality measurement
 * should never take the data path down. Dimensions are compiled once per message type and
 * cached. Annotations survive descriptors linked without this extension registered — the
 * options are reparsed from their unknown fields, the same load-bearing fallback the rule
 * sources use.</p>
 */
public final class QualityScorer {

    /** Clear-on-threshold bound, matching the validator's cache discipline. */
    private static final int MAX_CACHED_TYPES = 256;

    private static final ExtensionRegistry EXTENSIONS = createExtensionRegistry();

    private final ConcurrentMap<Descriptor, CompiledDimensions> byType =
            new ConcurrentHashMap<>();

    private QualityScorer() {
    }

    public static QualityScorer create() {
        return new QualityScorer();
    }

    /** Registers the quality extension, for callers assembling their own registries. */
    public static void registerExtensions(ExtensionRegistry registry) {
        QualityProto.registerAllExtensions(registry);
    }

    private static ExtensionRegistry createExtensionRegistry() {
        ExtensionRegistry registry = ExtensionRegistry.newInstance();
        QualityProto.registerAllExtensions(registry);
        return registry;
    }

    /**
     * The message scored against its own declared dimensions. A message whose type declares none
     * returns {@link QualityReport#none()}, and costs one option read ever, so scoring can sit
     * on a hot path shared with unannotated types.
     */
    public QualityReport score(Message message) {
        CompiledDimensions compiled = compiledFor(message.getDescriptorForType());
        if (compiled.dimensions().isEmpty()) {
            return QualityReport.none();
        }
        Map<String, Double> scores = new LinkedHashMap<>();
        List<String> failed = new java.util.ArrayList<>(0);
        double weightedSum = 0;
        double weightTotal = 0;
        for (Dimension dimension : compiled.dimensions()) {
            Double score = evaluate(compiled.evaluator(), dimension, message, failed);
            if (score != null) {
                scores.put(dimension.id(), score);
                weightedSum += score * dimension.weight();
                weightTotal += dimension.weight();
            }
        }
        double composite = weightTotal > 0 ? weightedSum / weightTotal : 0;
        return new QualityReport(composite, Map.copyOf(scores), List.copyOf(failed));
    }

    private static Double evaluate(CelEvaluator evaluator, Dimension dimension, Message message,
                                   List<String> failed) {
        Object result;
        try {
            result = evaluator.evaluateValue(dimension.cel(), Map.of("this", message));
        } catch (CelCompilationException e) {
            // Compiled at assembly; reaching here means a type-check failure against this value.
            failed.add(dimension.id());
            return null;
        } catch (CelEvaluationException e) {
            failed.add(dimension.id());
            return null;
        }
        Double score = asScore(result);
        if (score == null) {
            failed.add(dimension.id());
        }
        return score;
    }

    /** Numbers coerce to double, bools to 1/0, everything else is a failed measurement. */
    private static Double asScore(Object result) {
        double value;
        if (result instanceof Boolean b) {
            value = b ? 1 : 0;
        } else if (result instanceof UnsignedLong u) {
            value = u.doubleValue();
        } else if (result instanceof Number n) {
            value = n.doubleValue();
        } else {
            return null;
        }
        if (Double.isNaN(value)) {
            return null;
        }
        return Math.max(0, Math.min(1, value));
    }

    // ---- assembly ----

    private record Dimension(String id, String cel, double weight) {
    }

    private record CompiledDimensions(List<Dimension> dimensions, CelEvaluator evaluator) {
    }

    private CompiledDimensions compiledFor(Descriptor descriptor) {
        CompiledDimensions existing = byType.get(descriptor);
        if (existing != null) {
            return existing;
        }
        if (byType.size() >= MAX_CACHED_TYPES) {
            byType.clear();
        }
        return byType.computeIfAbsent(descriptor, QualityScorer::compile);
    }

    private static CompiledDimensions compile(Descriptor descriptor) {
        QualityRules rules = qualityRules(descriptor.getOptions());
        if (rules == null || rules.getDimensionList().isEmpty()) {
            return new CompiledDimensions(List.of(), null);
        }
        // `this` is typed as the message, so a dimension reading a field the message does not
        // have fails compilation here — a schema error, surfaced deterministically.
        CelEvaluator evaluator = new CelEvaluator(CelEnvironmentFactory.builder()
                .addMessageVar("this", descriptor)
                .addFunctions(QualityCelFunctions.declarations(), QualityCelFunctions.bindings())
                .build());
        List<Dimension> dimensions = new java.util.ArrayList<>(rules.getDimensionCount());
        java.util.Set<String> ids = new java.util.HashSet<>(rules.getDimensionCount());
        for (QualityDimension declared : rules.getDimensionList()) {
            if (declared.getId().isBlank()) {
                throw new QualitySchemaException("A quality dimension on "
                        + descriptor.getFullName() + " has no id");
            }
            // The report keys dimensions by id, so a duplicate would report one score while the
            // composite weighed both. Refuse the schema instead of scoring it inconsistently.
            if (!ids.add(declared.getId())) {
                throw new QualitySchemaException("Quality dimension '" + declared.getId()
                        + "' on " + descriptor.getFullName() + " is declared more than once");
            }
            if (declared.getCel().isBlank()) {
                throw new QualitySchemaException("Quality dimension '" + declared.getId()
                        + "' on " + descriptor.getFullName() + " has no CEL expression");
            }
            if (declared.getWeight() < 0) {
                throw new QualitySchemaException("Quality dimension '" + declared.getId()
                        + "' on " + descriptor.getFullName() + " has a negative weight");
            }
            try {
                evaluator.precompile(declared.getCel());
            } catch (CelCompilationException e) {
                throw new QualitySchemaException("Quality dimension '" + declared.getId()
                        + "' on " + descriptor.getFullName() + " does not compile: "
                        + e.getMessage(), e);
            }
            dimensions.add(new Dimension(declared.getId(), declared.getCel(),
                    declared.getWeight() == 0 ? 1.0 : declared.getWeight()));
        }
        return new CompiledDimensions(List.copyOf(dimensions), evaluator);
    }

    /**
     * The quality option on {@code options}, or null when absent. Descriptors linked without
     * this extension registered carry the annotation only as an unknown field; reparse rather
     * than silently reporting the type unannotated.
     */
    private static QualityRules qualityRules(DescriptorProtos.MessageOptions options) {
        if (options.hasExtension(QualityProto.quality)) {
            return options.getExtension(QualityProto.quality);
        }
        if (!options.getUnknownFields().hasField(QualityProto.quality.getNumber())) {
            return null;
        }
        try {
            return DescriptorProtos.MessageOptions.parseFrom(options.toByteString(), EXTENSIONS)
                    .getExtension(QualityProto.quality);
        } catch (InvalidProtocolBufferException e) {
            throw new QualitySchemaException(
                    "cannot reparse options carrying (ai.pipestream.proto.quality.v1.quality): "
                            + e.getMessage(), e);
        }
    }
}
