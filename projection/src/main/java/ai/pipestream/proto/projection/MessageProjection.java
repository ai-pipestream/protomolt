package ai.pipestream.proto.projection;

import ai.pipestream.proto.cel.CelCompilationException;
import ai.pipestream.proto.cel.CelEnvironmentFactory;
import ai.pipestream.proto.cel.CelEvaluationException;
import ai.pipestream.proto.cel.CelEvaluator;
import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.helpers.TypeConverter;
import ai.pipestream.proto.mapper.MappingException;
import ai.pipestream.proto.mapper.ProtoFieldMapper;
import ai.pipestream.proto.mapper.ProtoFieldMapperImpl;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.FieldMask;
import com.google.protobuf.Message;
import com.google.protobuf.Value;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A compiled projection: builds instances of one message type (the target) from
 * instances of other message types (the declared sources), driven entirely by
 * descriptor options on the target's own {@code .proto} file.
 *
 * <p>The target message declares its sources with
 * {@code option (ai.pipestream.proto.projection.v1.sources)} and each mapped
 * field carries {@code (ai.pipestream.proto.projection.v1.from)} with one of
 * three provenance kinds: candidate source paths (first present value wins), a
 * CEL expression (evaluated with the source bound as {@code source}), or a
 * literal constant. Fields without the option are left unset.</p>
 *
 * <p>Semantics across multiple source types:</p>
 * <ul>
 *   <li>A path that does not resolve against the source type counts as absent,
 *       so candidate lists fall through to the next path.</li>
 *   <li>A CEL expression that does not compile against the source type counts
 *       as absent. An expression that compiles for no declared source fails
 *       projection construction — that is a typo, not a join.</li>
 *   <li>A CEL expression that compiles but fails at evaluation fails the
 *       projection; guard presence-dependent logic with {@code has()}.</li>
 * </ul>
 *
 * <p>The mapping also derives {@link FieldMask}s: {@link #targetMask()} names
 * every populated target field (partial-response/update masks), and
 * {@link #sourceMask(Descriptor)} names what a source type must supply
 * (read pruning).</p>
 *
 * <p>Instances are immutable and thread-safe. Requires descriptors parsed with
 * {@link #registerExtensions(ExtensionRegistry)} when they come from a runtime
 * descriptor set rather than generated classes.</p>
 */
public final class MessageProjection {

    private final Descriptor targetType;
    private final List<String> declaredSources;
    private final List<Rule> rules;
    private final ProtoFieldMapper fieldMapper;
    private final TypeConverter typeConverter = new TypeConverter();
    private final ConcurrentHashMap<String, CelEvaluator> evaluators = new ConcurrentHashMap<>();

    private MessageProjection(
            Descriptor targetType,
            List<String> declaredSources,
            List<Rule> rules,
            ProtoFieldMapper fieldMapper) {
        this.targetType = targetType;
        this.declaredSources = List.copyOf(declaredSources);
        this.rules = List.copyOf(rules);
        this.fieldMapper = fieldMapper;
    }

    /** Registers the projection extensions for runtime-parsed descriptor sets. */
    public static void registerExtensions(ExtensionRegistry registry) {
        ProjectionProto.registerAllExtensions(registry);
    }

    /**
     * Builds a projection for {@code target} when it declares projection
     * sources, using {@code registry} both to resolve source types for eager
     * validation and to back path extraction.
     *
     * @return the projection, or empty when the target carries no
     *         {@code (sources)} option
     * @throws ProjectionException when a CEL rule compiles against no
     *         resolvable declared source
     */
    public static Optional<MessageProjection> forTarget(Descriptor target, DescriptorRegistry registry) {
        Objects.requireNonNull(registry, "registry");
        return forTarget(target, SourceResolver.of(registry), new ProtoFieldMapperImpl(registry));
    }

    /**
     * Builds a projection for {@code target} when it declares projection
     * sources. The resolver feeds eager CEL validation only; projection itself
     * works against any message whose type is named in {@code (sources)},
     * resolvable or not.
     *
     * @return the projection, or empty when the target carries no
     *         {@code (sources)} option
     * @throws ProjectionException when a CEL rule compiles against no
     *         resolvable declared source
     */
    public static Optional<MessageProjection> forTarget(Descriptor target, SourceResolver sources) {
        return forTarget(target, sources, new ProtoFieldMapperImpl(DescriptorRegistry.create(false)));
    }

    private static Optional<MessageProjection> forTarget(
            Descriptor target, SourceResolver sources, ProtoFieldMapper fieldMapper) {
        Objects.requireNonNull(target, "target");
        Objects.requireNonNull(sources, "sources");
        var messageOptions = target.getOptions();
        if (!messageOptions.hasExtension(ProjectionProto.sources)) {
            return Optional.empty();
        }
        List<String> declared = messageOptions.getExtension(ProjectionProto.sources).getSourceList();
        if (declared.isEmpty()) {
            throw new ProjectionException(
                    "Projection target " + target.getFullName() + " declares no source types");
        }

        List<Rule> rules = new ArrayList<>();
        for (FieldDescriptor field : target.getFields()) {
            var fieldOptions = field.getOptions();
            if (!fieldOptions.hasExtension(ProjectionProto.from)) {
                continue;
            }
            rules.add(toRule(field, fieldOptions.getExtension(ProjectionProto.from)));
        }

        MessageProjection projection = new MessageProjection(target, declared, rules, fieldMapper);
        projection.validateCel(sources);
        return Optional.of(projection);
    }

    private static Rule toRule(FieldDescriptor field, FieldProjection provenance) {
        return switch (provenance.getProvenanceCase()) {
            case PATHS -> {
                List<String> paths = provenance.getPaths().getPathList();
                if (paths.isEmpty()) {
                    throw new ProjectionException(
                            "Projection field " + field.getFullName() + " declares an empty paths list");
                }
                yield new PathRule(field, paths);
            }
            case CEL -> {
                String expression = provenance.getCel();
                if (expression.isBlank()) {
                    throw new ProjectionException(
                            "Projection field " + field.getFullName() + " declares a blank CEL expression");
                }
                yield new CelRule(field, expression);
            }
            case LITERAL -> new LiteralRule(field, provenance.getLiteral());
            case PROVENANCE_NOT_SET -> throw new ProjectionException(
                    "Projection field " + field.getFullName() + " declares no provenance");
        };
    }

    /**
     * Pre-compiles every CEL rule against each resolvable declared source type.
     * A rule that compiles nowhere is a authoring error and fails fast; a rule
     * that compiles for some sources is absent for the others at project time.
     */
    private void validateCel(SourceResolver sources) {
        List<Descriptor> resolvable = declaredSources.stream()
                .map(sources::resolve)
                .flatMap(Optional::stream)
                .toList();
        if (resolvable.isEmpty()) {
            return;
        }
        for (Rule rule : rules) {
            if (!(rule instanceof CelRule celRule)) {
                continue;
            }
            boolean compilesSomewhere = false;
            for (Descriptor sourceType : resolvable) {
                try {
                    evaluatorFor(sourceType).precompile(celRule.expression());
                    compilesSomewhere = true;
                } catch (CelCompilationException e) {
                    // Absent for this source type; join semantics, not an error.
                }
            }
            if (!compilesSomewhere) {
                throw new ProjectionException("CEL for projection field "
                        + celRule.field().getFullName() + " compiles against no declared source of "
                        + targetType.getFullName() + ": " + celRule.expression());
            }
        }
    }

    /** The message type this projection builds. */
    public Descriptor targetType() {
        return targetType;
    }

    /** The declared source type names, as written in the {@code (sources)} option. */
    public List<String> declaredSources() {
        return declaredSources;
    }

    /** Whether {@code sourceType} is one of the declared sources (by full name). */
    public boolean supports(Descriptor sourceType) {
        return declaredSources.contains(sourceType.getFullName());
    }

    /**
     * A {@link FieldMask} over the target type naming every field this
     * projection populates. Derived from the mapping itself, so it stays in
     * sync with the {@code .proto}; usable as a partial-response or update
     * mask on APIs that serve the target type.
     */
    public FieldMask targetMask() {
        FieldMask.Builder mask = FieldMask.newBuilder();
        for (Rule rule : rules) {
            mask.addPaths(rule.field().getName());
        }
        return mask.build();
    }

    /**
     * What this projection reads from {@code sourceType}, as a {@link FieldMask}
     * over the source message: every candidate path that resolves against that
     * type. Use it to prune source fetches to the fields the mapping consumes.
     *
     * <p>The result is exact for path and literal provenance. CEL field
     * references are not statically enumerable, so when any CEL rule compiles
     * against this source type the mask is a lower bound and
     * {@link SourceMask#complete()} is {@code false} — do not prune on it.</p>
     *
     * @throws ProjectionException when {@code sourceType} is not a declared source
     */
    public SourceMask sourceMask(Descriptor sourceType) {
        if (!supports(sourceType)) {
            throw new ProjectionException("Type " + sourceType.getFullName()
                    + " is not a declared source of projection " + targetType.getFullName());
        }
        Set<String> paths = new LinkedHashSet<>();
        boolean complete = true;
        for (Rule rule : rules) {
            if (rule instanceof PathRule pathRule) {
                for (String path : pathRule.paths()) {
                    if (resolves(sourceType, path)) {
                        paths.add(path);
                    }
                }
            } else if (rule instanceof CelRule celRule && compilesFor(sourceType, celRule.expression())) {
                complete = false;
            }
        }
        return new SourceMask(FieldMask.newBuilder().addAllPaths(paths).build(), complete);
    }

    private boolean compilesFor(Descriptor sourceType, String expression) {
        try {
            evaluatorFor(sourceType).precompile(expression);
            return true;
        } catch (CelCompilationException e) {
            return false;
        }
    }

    /**
     * Whether a dotted path resolves against {@code type}. Map fields end the
     * check: their keys are dynamic, so anything below a map counts as
     * resolvable.
     */
    private static boolean resolves(Descriptor type, String path) {
        Descriptor current = type;
        for (String segment : path.trim().split("\\.")) {
            if (current == null) {
                return false;
            }
            FieldDescriptor field = current.findFieldByName(segment);
            if (field == null) {
                return false;
            }
            if (field.isMapField()) {
                return true;
            }
            current = field.getJavaType() == FieldDescriptor.JavaType.MESSAGE
                    ? field.getMessageType() : null;
        }
        return true;
    }

    /**
     * Projects one source message into a new target instance.
     *
     * @throws ProjectionException when the source type is not declared, when a
     *         CEL rule fails at evaluation, or when a value cannot be coerced
     *         to its target field type
     */
    public DynamicMessage project(Message source) {
        Objects.requireNonNull(source, "source");
        Descriptor sourceType = source.getDescriptorForType();
        if (!supports(sourceType)) {
            throw new ProjectionException("Type " + sourceType.getFullName()
                    + " is not a declared source of projection " + targetType.getFullName()
                    + " (declared: " + String.join(", ", declaredSources) + ")");
        }
        DynamicMessage.Builder out = DynamicMessage.newBuilder(targetType);
        for (Rule rule : rules) {
            Object value = resolve(rule, source, sourceType);
            if (value != null) {
                assign(out, rule.field(), value);
            }
        }
        return out.build();
    }

    private Object resolve(Rule rule, Message source, Descriptor sourceType) {
        if (rule instanceof PathRule pathRule) {
            for (String path : pathRule.paths()) {
                Object value = tryGet(source, path);
                if (value != null) {
                    return value;
                }
            }
            return null;
        }
        if (rule instanceof CelRule celRule) {
            try {
                return evaluatorFor(sourceType)
                        .evaluateValue(celRule.expression(), Map.of("source", source));
            } catch (CelCompilationException e) {
                // Does not compile against this source type: absent, per the join semantics.
                return null;
            } catch (CelEvaluationException e) {
                throw new ProjectionException("CEL failed for projection field "
                        + celRule.field().getFullName() + " on source " + sourceType.getFullName()
                        + ": " + celRule.expression(), e);
            }
        }
        if (rule instanceof LiteralRule literalRule) {
            return typeConverter.fromValue(literalRule.literal());
        }
        throw new IllegalStateException("Unknown rule kind: " + rule.getClass());
    }

    private Object tryGet(Message source, String path) {
        try {
            return fieldMapper.getValue(source, path, false);
        } catch (MappingException e) {
            // Unresolvable against this source type: absent, per the join semantics.
            return null;
        }
    }

    private void assign(DynamicMessage.Builder out, FieldDescriptor field, Object value) {
        if (field.isMapField()) {
            // Map values arrive as java.util.Map from generated sources but as MapEntry
            // lists from DynamicMessage sources; support them deliberately, not by accident.
            throw new ProjectionException(
                    "Map fields are not yet supported by projection: " + field.getFullName());
        }
        try {
            if (field.isRepeated()) {
                if (!(value instanceof List<?> elements)) {
                    throw new ProjectionException("Value for repeated projection field "
                            + field.getFullName() + " is not a list: " + value.getClass().getName());
                }
                for (Object element : elements) {
                    out.addRepeatedField(field, typeConverter.convertToFieldType(element, field));
                }
            } else {
                if (value instanceof List<?>) {
                    throw new ProjectionException("Value for singular projection field "
                            + field.getFullName() + " is a list");
                }
                out.setField(field, typeConverter.convertToFieldType(value, field));
            }
        } catch (IllegalArgumentException e) {
            throw new ProjectionException(
                    "Cannot coerce value for projection field " + field.getFullName(), e);
        }
    }

    private CelEvaluator evaluatorFor(Descriptor sourceType) {
        return evaluators.computeIfAbsent(sourceType.getFullName(), name ->
                new CelEvaluator(CelEnvironmentFactory.builder()
                        .addMessageVar("source", sourceType)
                        .build()));
    }

    /**
     * The fields a projection reads from one source type, as a {@link FieldMask}
     * over the source message. {@code complete} is {@code false} when CEL rules
     * apply to that source type and the mask is therefore a lower bound.
     */
    public record SourceMask(FieldMask fieldMask, boolean complete) {
    }

    private sealed interface Rule {
        FieldDescriptor field();
    }

    private record PathRule(FieldDescriptor field, List<String> paths) implements Rule {
    }

    private record CelRule(FieldDescriptor field, String expression) implements Rule {
    }

    private record LiteralRule(FieldDescriptor field, Value literal) implements Rule {
    }
}
