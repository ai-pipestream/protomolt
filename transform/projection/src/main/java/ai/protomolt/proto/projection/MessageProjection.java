package ai.protomolt.proto.projection;

import ai.protomolt.proto.cel.CelCompilationException;
import ai.protomolt.proto.cel.CelEnvironmentFactory;
import ai.protomolt.proto.cel.CelEvaluationException;
import ai.protomolt.proto.cel.CelEvaluator;
import ai.protomolt.proto.descriptors.DescriptorIdentity;
import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.helpers.TypeConverter;
import ai.protomolt.proto.mapper.MappingException;
import ai.protomolt.proto.mapper.ProtoFieldMapper;
import ai.protomolt.proto.mapper.ProtoFieldMapperImpl;
import com.google.protobuf.DescriptorProtos.FieldOptions;
import com.google.protobuf.DescriptorProtos.MessageOptions;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.OneofDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.FieldMask;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Value;

import java.util.ArrayList;
import java.util.LinkedHashMap;
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
 * {@code option (ai.protomolt.proto.projection.v1.sources)} and each mapped
 * field carries {@code (ai.protomolt.proto.projection.v1.from)} with one of
 * three provenance kinds: candidate source paths (first present value wins), a
 * CEL expression (evaluated with the source bound as {@code source}), or a
 * literal constant. A field may also carry {@code (default_from)}; that rule is
 * evaluated only when {@code from} produces no value. Fields without either
 * option are left unset.</p>
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
 * <p>Instances are immutable and thread-safe. Descriptors linked without the
 * projection extensions registered carry the options only as unknown fields;
 * they are re-read against a knowing registry, so runtime descriptor sets work
 * without any setup.</p>
 */
public final class MessageProjection {

    /**
     * The extension registry used to re-read projection options off descriptors
     * linked without one: those carry the annotation only as an unknown field,
     * and a projection that cannot see its sources would silently build nothing.
     */
    private static final ExtensionRegistry EXTENSIONS = extensionRegistry();

    private static ExtensionRegistry extensionRegistry() {
        ExtensionRegistry registry = ExtensionRegistry.newInstance();
        registerExtensions(registry);
        return registry;
    }

    /** The message options of {@code target} with projection extensions resolved. */
    private static MessageOptions messageOptions(Descriptor target) {
        MessageOptions options = target.getOptions();
        if (options.hasExtension(ProjectionProto.sources)) {
            return options;
        }
        try {
            return MessageOptions.parseFrom(options.toByteString(), EXTENSIONS);
        } catch (InvalidProtocolBufferException e) {
            return options;
        }
    }

    /** The field options of {@code field} with projection extensions resolved. */
    private static FieldOptions fieldOptions(FieldDescriptor field) {
        FieldOptions options = field.getOptions();
        if (options.hasExtension(ProjectionProto.from)
                || options.hasExtension(ProjectionProto.defaultFrom)) {
            return options;
        }
        try {
            return FieldOptions.parseFrom(options.toByteString(), EXTENSIONS);
        } catch (InvalidProtocolBufferException e) {
            return options;
        }
    }

    private final Descriptor targetType;
    private final DescriptorIdentity targetIdentity;
    private final List<String> declaredSources;
    private final Map<String, DescriptorIdentity> sourceIdentities;
    private final Map<String, Descriptor> sourceTypes;
    private final List<FieldRule> rules;
    private final ProtoFieldMapper fieldMapper;
    private final TypeConverter typeConverter = new TypeConverter();
    private final ConcurrentHashMap<DescriptorIdentity, CelEvaluator> evaluators =
            new ConcurrentHashMap<>();

    private MessageProjection(
            Descriptor targetType,
            Map<String, Descriptor> sourceTypes,
            List<String> declaredSources,
            List<FieldRule> rules,
            ProtoFieldMapper fieldMapper) {
        this.targetType = targetType;
        this.targetIdentity = DescriptorIdentity.of(targetType);
        this.declaredSources = List.copyOf(declaredSources);
        this.sourceTypes = Map.copyOf(sourceTypes);
        Map<String, DescriptorIdentity> identities = new LinkedHashMap<>();
        sourceTypes.forEach((name, descriptor) ->
                identities.put(name, DescriptorIdentity.of(descriptor)));
        this.sourceIdentities = Map.copyOf(identities);
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
     * sources. Every declared source must resolve so compilation can bind the
     * mapping to an exact descriptor identity before any message is processed.
     *
     * @return the projection, or empty when the target carries no
     *         {@code (sources)} option
     * @throws ProjectionException when a declared source cannot be resolved or
     *         a CEL rule compiles against no declared source
     */
    public static Optional<MessageProjection> forTarget(Descriptor target, SourceResolver sources) {
        return forTarget(target, sources, null);
    }

    private static Optional<MessageProjection> forTarget(
            Descriptor target, SourceResolver sources, ProtoFieldMapper fieldMapper) {
        Objects.requireNonNull(target, "target");
        Objects.requireNonNull(sources, "sources");
        var messageOptions = messageOptions(target);
        if (!messageOptions.hasExtension(ProjectionProto.sources)) {
            return Optional.empty();
        }
        List<String> declared = messageOptions.getExtension(ProjectionProto.sources).getSourceList();
        if (declared.isEmpty()) {
            throw new ProjectionException(
                    "Projection target " + target.getFullName() + " declares no source types");
        }
        Map<String, Descriptor> resolvedSources = resolveSources(target, declared, sources);

        List<FieldRule> rules = new ArrayList<>();
        for (FieldDescriptor field : target.getFields()) {
            var fieldOptions = fieldOptions(field);
            boolean hasFrom = fieldOptions.hasExtension(ProjectionProto.from);
            boolean hasDefault = fieldOptions.hasExtension(ProjectionProto.defaultFrom);
            if (!hasFrom && !hasDefault) {
                continue;
            }
            Rule from = hasFrom
                    ? toRule(field, "from", fieldOptions.getExtension(ProjectionProto.from))
                    : null;
            Rule defaultFrom = hasDefault
                    ? toRule(field, "default_from",
                            fieldOptions.getExtension(ProjectionProto.defaultFrom))
                    : null;
            rules.add(new FieldRule(field, from, defaultFrom));
        }

        ProtoFieldMapper resolvedMapper = fieldMapper != null
                ? fieldMapper : new ProtoFieldMapperImpl(registryFor(target, resolvedSources));
        MessageProjection projection = new MessageProjection(
                target, resolvedSources, declared, rules, resolvedMapper);
        projection.validateCel();
        return Optional.of(projection);
    }

    private static Map<String, Descriptor> resolveSources(
            Descriptor target, List<String> declared, SourceResolver sources) {
        Map<String, Descriptor> resolved = new LinkedHashMap<>();
        for (String name : declared) {
            if (name.isBlank()) {
                throw new ProjectionException(
                        "Projection target " + target.getFullName()
                                + " declares a blank source type");
            }
            Descriptor descriptor = sources.resolve(name).orElseThrow(() ->
                    new ProjectionException("Projection target " + target.getFullName()
                            + " cannot resolve declared source " + name
                            + "; exact descriptor identity is required at compilation"));
            if (!descriptor.getFullName().equals(name)) {
                throw new ProjectionException("Projection source resolver returned "
                        + descriptor.getFullName() + " for declared source " + name);
            }
            Descriptor previous = resolved.putIfAbsent(name, descriptor);
            if (previous != null) {
                throw new ProjectionException("Projection target " + target.getFullName()
                        + " declares source " + name + " more than once");
            }
        }
        return resolved;
    }

    private static DescriptorRegistry registryFor(
            Descriptor target, Map<String, Descriptor> sources) {
        DescriptorRegistry registry = DescriptorRegistry.create(false);
        registerClosure(registry, target.getFile(), new LinkedHashSet<>());
        for (Descriptor source : sources.values()) {
            registerClosure(registry, source.getFile(), new LinkedHashSet<>());
        }
        return registry;
    }

    private static void registerClosure(
            DescriptorRegistry registry, FileDescriptor file, Set<String> registered) {
        if (!registered.add(file.getName())) {
            return;
        }
        for (FileDescriptor dependency : file.getDependencies()) {
            registerClosure(registry, dependency, registered);
        }
        registry.registerFile(file);
    }

    private static Rule toRule(
            FieldDescriptor field, String optionName, FieldProjection provenance) {
        return switch (provenance.getProvenanceCase()) {
            case PATHS -> {
                List<String> paths = provenance.getPaths().getPathList();
                if (paths.isEmpty()) {
                    throw new ProjectionException(
                            "Projection field " + field.getFullName() + " declares an empty "
                                    + optionName + " paths list");
                }
                yield new PathRule(field, paths);
            }
            case CEL -> {
                String expression = provenance.getCel();
                if (expression.isBlank()) {
                    throw new ProjectionException(
                            "Projection field " + field.getFullName() + " declares a blank "
                                    + optionName + " CEL expression");
                }
                yield new CelRule(field, expression);
            }
            case LITERAL -> new LiteralRule(field, provenance.getLiteral());
            case PROVENANCE_NOT_SET -> throw new ProjectionException(
                    "Projection field " + field.getFullName() + " declares no provenance for "
                            + optionName);
        };
    }

    /**
     * Pre-compiles every CEL rule against each declared source type.
     * A rule that compiles nowhere is a authoring error and fails fast; a rule
     * that compiles for some sources is absent for the others at project time.
     */
    private void validateCel() {
        List<Descriptor> resolvable = List.copyOf(sourceTypes.values());
        for (FieldRule fieldRule : rules) {
            validateCelRule(fieldRule.from(), resolvable);
            validateCelRule(fieldRule.defaultFrom(), resolvable);
        }
    }

    private void validateCelRule(Rule rule, List<Descriptor> resolvable) {
        if (!(rule instanceof CelRule celRule)) {
            return;
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

    /** The message type this projection builds. */
    public Descriptor targetType() {
        return targetType;
    }

    /** The exact name and descriptor-closure fingerprint of the target type. */
    public DescriptorIdentity targetIdentity() {
        return targetIdentity;
    }

    /** The declared source type names, as written in the {@code (sources)} option. */
    public List<String> declaredSources() {
        return declaredSources;
    }

    /** The exact compiled descriptor identity for every declared source, keyed by full name. */
    public Map<String, DescriptorIdentity> sourceIdentities() {
        return sourceIdentities;
    }

    /** Whether {@code sourceType} is one of the exact compiled source definitions. */
    public boolean supports(Descriptor sourceType) {
        DescriptorIdentity expected = sourceIdentities.get(sourceType.getFullName());
        return expected != null && expected.matches(sourceType);
    }

    /**
     * A {@link FieldMask} over the target type naming every field this
     * projection populates. Derived from the mapping itself, so it stays in
     * sync with the {@code .proto}; usable as a partial-response or update
     * mask on APIs that serve the target type.
     */
    public FieldMask targetMask() {
        FieldMask.Builder mask = FieldMask.newBuilder();
        for (FieldRule rule : rules) {
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
            throw sourceMismatch(sourceType);
        }
        Set<String> paths = new LinkedHashSet<>();
        boolean complete = true;
        for (FieldRule rule : rules) {
            complete &= addSourceReads(rule.from(), sourceType, paths);
            complete &= addSourceReads(rule.defaultFrom(), sourceType, paths);
        }
        return new SourceMask(FieldMask.newBuilder().addAllPaths(paths).build(), complete);
    }

    private boolean addSourceReads(Rule rule, Descriptor sourceType, Set<String> paths) {
        if (rule instanceof PathRule pathRule) {
            for (String path : pathRule.paths()) {
                if (resolves(sourceType, path)) {
                    paths.add(path);
                }
            }
        } else if (rule instanceof CelRule celRule
                && compilesFor(sourceType, celRule.expression())) {
            return false;
        }
        return true;
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
     * Whether a dotted path resolves against {@code type}. A map itself is a
     * valid terminal value. Dynamic map keys are not field-path segments.
     */
    private static boolean resolves(Descriptor type, String path) {
        Descriptor current = type;
        String[] segments = path.trim().split("\\.");
        for (int index = 0; index < segments.length; index++) {
            if (current == null) {
                return false;
            }
            FieldDescriptor field = current.findFieldByName(segments[index]);
            if (field == null) {
                return false;
            }
            if (field.isMapField()) {
                return index == segments.length - 1;
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
            throw sourceMismatch(sourceType);
        }
        DynamicMessage.Builder out = DynamicMessage.newBuilder(targetType);
        Map<OneofDescriptor, FieldDescriptor> selectedOneofs = new LinkedHashMap<>();
        for (FieldRule rule : rules) {
            Object value = resolve(rule.from(), source, sourceType);
            if (value == null) {
                value = resolve(rule.defaultFrom(), source, sourceType);
            }
            if (value != null) {
                assign(out, rule.field(), value, selectedOneofs);
            }
        }
        return out.build();
    }

    private ProjectionException sourceMismatch(Descriptor sourceType) {
        DescriptorIdentity expected = sourceIdentities.get(sourceType.getFullName());
        if (expected == null) {
            return new ProjectionException("Type " + sourceType.getFullName()
                    + " is not a declared source of projection " + targetType.getFullName()
                    + " (declared: " + String.join(", ", declaredSources) + ")");
        }
        return new ProjectionException("Descriptor identity mismatch for source "
                + sourceType.getFullName() + " of projection " + targetType.getFullName()
                + ": compiled " + expected.fingerprint() + " but received "
                + DescriptorIdentity.fingerprint(sourceType));
    }

    private Object resolve(Rule rule, Message source, Descriptor sourceType) {
        // Rule is sealed: the switch is exhaustive, so a new rule kind fails
        // the compile here instead of surfacing as an unknown-kind throw.
        return switch (rule) {
            case null -> null;
            case PathRule pathRule -> {
                for (String path : pathRule.paths()) {
                    Object value = tryGet(source, path);
                    if (value != null) {
                        yield value;
                    }
                }
                yield null;
            }
            case CelRule celRule -> {
                try {
                    yield evaluatorFor(sourceType)
                            .evaluateValue(celRule.expression(), Map.of("source", source));
                } catch (CelCompilationException e) {
                    // Does not compile against this source type: absent, per the join semantics.
                    yield null;
                } catch (CelEvaluationException e) {
                    throw new ProjectionException("CEL failed for projection field "
                            + celRule.field().getFullName() + " on source "
                            + sourceType.getFullName() + ": " + celRule.expression(), e);
                }
            }
            case LiteralRule literalRule -> typeConverter.fromValue(literalRule.literal());
        };
    }

    private Object tryGet(Message source, String path) {
        try {
            return fieldMapper.getValue(source, path, false);
        } catch (MappingException e) {
            // Unresolvable against this source type: absent, per the join semantics.
            return null;
        }
    }

    private void assign(
            DynamicMessage.Builder out,
            FieldDescriptor field,
            Object value,
            Map<OneofDescriptor, FieldDescriptor> selectedOneofs) {
        OneofDescriptor oneof = field.getContainingOneof();
        if (oneof != null) {
            FieldDescriptor selected = selectedOneofs.putIfAbsent(oneof, field);
            if (selected != null && selected != field) {
                throw new ProjectionException("Projection produced both "
                        + selected.getFullName() + " and " + field.getFullName()
                        + " in target oneof " + oneof.getFullName());
            }
        }
        if (field.isMapField()) {
            assignMap(out, field, value);
            return;
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

    private void assignMap(DynamicMessage.Builder out, FieldDescriptor field, Object value) {
        FieldDescriptor keyField = field.getMessageType().findFieldByName("key");
        FieldDescriptor valueField = field.getMessageType().findFieldByName("value");
        Map<Object, Object> entries = new LinkedHashMap<>();
        if (value instanceof Map<?, ?> sourceMap) {
            sourceMap.forEach(entries::put);
        } else if (value instanceof List<?> sourceEntries) {
            for (Object sourceEntry : sourceEntries) {
                if (!(sourceEntry instanceof Message message)
                        || !message.getDescriptorForType().getOptions().getMapEntry()) {
                    throw new ProjectionException("Value for map projection field "
                            + field.getFullName() + " contains a non-map-entry value");
                }
                FieldDescriptor sourceKey = message.getDescriptorForType().findFieldByName("key");
                FieldDescriptor sourceValue = message.getDescriptorForType().findFieldByName("value");
                entries.put(message.getField(sourceKey), message.getField(sourceValue));
            }
        } else {
            throw new ProjectionException("Value for map projection field "
                    + field.getFullName() + " is neither a map nor map-entry list: "
                    + value.getClass().getName());
        }
        try {
            for (Map.Entry<Object, Object> entry : entries.entrySet()) {
                DynamicMessage mapEntry = DynamicMessage.newBuilder(field.getMessageType())
                        .setField(keyField,
                                typeConverter.convertToFieldType(entry.getKey(), keyField))
                        .setField(valueField,
                                typeConverter.convertToFieldType(entry.getValue(), valueField))
                        .build();
                out.addRepeatedField(field, mapEntry);
            }
        } catch (IllegalArgumentException e) {
            throw new ProjectionException(
                    "Cannot coerce map entry for projection field " + field.getFullName(), e);
        }
    }

    private CelEvaluator evaluatorFor(Descriptor sourceType) {
        DescriptorIdentity identity = DescriptorIdentity.of(sourceType);
        return evaluators.computeIfAbsent(identity, ignored ->
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

    private record FieldRule(FieldDescriptor field, Rule from, Rule defaultFrom) {
    }

    private record PathRule(FieldDescriptor field, List<String> paths) implements Rule {
    }

    private record CelRule(FieldDescriptor field, String expression) implements Rule {
    }

    private record LiteralRule(FieldDescriptor field, Value literal) implements Rule {
    }
}
