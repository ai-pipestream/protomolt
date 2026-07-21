package ai.pipestream.proto.validate;

import ai.pipestream.proto.cel.CelCompilationException;
import ai.pipestream.proto.cel.CelEnvironmentFactory;
import ai.pipestream.proto.cel.CelEvaluationException;
import ai.pipestream.proto.cel.CelEvaluator;
import ai.pipestream.proto.validate.cel.ValidationCelFunctions;
import ai.pipestream.proto.validate.model.BoolConstraints;
import ai.pipestream.proto.validate.model.BytesConstraints;
import ai.pipestream.proto.validate.model.BytesFormat;
import ai.pipestream.proto.validate.model.CelConstraint;
import ai.pipestream.proto.validate.model.DurationConstraints;
import ai.pipestream.proto.validate.model.EnumConstraints;
import ai.pipestream.proto.validate.model.FieldConstraints;
import ai.pipestream.proto.validate.model.FloatingConstraints;
import ai.pipestream.proto.validate.model.IgnoreMode;
import ai.pipestream.proto.validate.model.IntegralConstraints;
import ai.pipestream.proto.validate.model.MapConstraints;
import ai.pipestream.proto.validate.model.MessageConstraints;
import ai.pipestream.proto.validate.model.RepeatedConstraints;
import ai.pipestream.proto.validate.model.StringConstraints;
import ai.pipestream.proto.validate.model.StringFormat;
import ai.pipestream.proto.validate.model.TimestampConstraints;
import ai.pipestream.proto.validate.spi.ValidationRuleSource;
import ai.pipestream.proto.validate.spi.ValidationRuleSources;
import com.google.common.primitives.UnsignedLong;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import dev.cel.bundle.Cel;
import dev.cel.common.CelValidationException;
import dev.cel.common.types.CelKind;
import dev.cel.common.types.CelType;

import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Validates protobuf messages against constraint annotations. The validator core
 * evaluates a neutral {@link FieldConstraints}/{@link MessageConstraints} model; the
 * mapping from a specific annotation dialect (Pipestream {@code validate.v1},
 * {@code buf.validate}, …) onto that model lives behind {@link ValidationRuleSource}.
 * Standard constraints run in-process; custom rules use CEL with {@code this}.
 *
 * <p>By default the built-in Pipestream reader is used plus any {@link ValidationRuleSource}
 * discovered on the classpath via {@link java.util.ServiceLoader}. Every configured source
 * is consulted per field/message and all violations are merged.
 *
 * <p>The translated rule model is compiled once per message type and cached: regex patterns
 * and CEL programs are compiled eagerly when a descriptor's rules are first assembled, so
 * schema errors ({@link RuleCompilationException}) surface deterministically — even for
 * fields the validated message leaves unset. Value-dependent failures (a CEL runtime error,
 * undecodable bytes) throw {@link RuleEvaluationException} instead.
 *
 * <p>Presence semantics: standard rules run only when the field is present (proto3
 * semantics — non-zero/non-empty, or {@code hasField} for explicit presence). Repeated
 * and map fields are the exception: collection rules ({@code repeated.min_items},
 * {@code map.min_pairs}, …) also apply when the collection is empty.
 *
 * <p>Violation paths use protobuf field names, {@code [i]} subscripts for repeated
 * elements, {@code ["key"]} subscripts for map entries, and a {@code #key} suffix for
 * violations against a map key itself.
 */
public final class ProtoValidator {

    private static final String TIMESTAMP_TYPE = "google.protobuf.Timestamp";
    private static final String DURATION_TYPE = "google.protobuf.Duration";

    /** Maximum message nesting the recursive walk follows before failing the evaluation. */
    private static final int MAX_NESTING_DEPTH = 500;
    // Cache bounds. All caches are simple clear-on-threshold: when full they are wiped and
    // repopulated on demand, which keeps them thread-safe and dependency-free while preventing
    // unbounded growth for callers that validate many distinct (e.g. dynamically built) types.
    private static final int MAX_CACHED_TYPES = 256;
    private static final int MAX_CACHED_PATTERNS = 512;

    /** Well-known wrapper message types mapped to the scalar family that validates their value. */
    private static final Map<String, FieldDescriptor.JavaType> WRAPPER_TYPES = Map.of(
            "google.protobuf.Int32Value", FieldDescriptor.JavaType.INT,
            "google.protobuf.Int64Value", FieldDescriptor.JavaType.LONG,
            "google.protobuf.UInt32Value", FieldDescriptor.JavaType.INT,
            "google.protobuf.UInt64Value", FieldDescriptor.JavaType.LONG,
            "google.protobuf.FloatValue", FieldDescriptor.JavaType.FLOAT,
            "google.protobuf.DoubleValue", FieldDescriptor.JavaType.DOUBLE,
            "google.protobuf.BoolValue", FieldDescriptor.JavaType.BOOLEAN,
            "google.protobuf.StringValue", FieldDescriptor.JavaType.STRING,
            "google.protobuf.BytesValue", FieldDescriptor.JavaType.BYTE_STRING);

    /** Compiled regex patterns shared across validators, keyed by the pattern source. */
    private static final Map<String, Pattern> PATTERNS =
            new java.util.concurrent.ConcurrentHashMap<>();

    /**
     * A CEL environment paired with its evaluator. The raw {@link Cel} handle enables static
     * checks (compile errors, result type) when the environment was built by this class; it is
     * null for caller-supplied evaluators, where compilation is triggered through the evaluator.
     */
    private record CelHandle(Cel cel, CelEvaluator evaluator) {
    }

    /**
     * The fully translated and eagerly compiled rules for one message type: per-field
     * constraints from every source, the non-empty message-level constraints, and the names of
     * fields governed by a message-level oneof rule.
     */
    private record CompiledRules(
            Map<FieldDescriptor, List<FieldConstraints>> fields,
            List<MessageConstraints> messages,
            Set<String> oneofMembers) {
    }

    private final CelHandle fieldCel;
    private final List<ValidationRuleSource> sources;
    // Message-level CEL is compiled with `this` typed as the message under validation, so a rule on
    // a nested message sees its own fields. Evaluators are built lazily and cached per descriptor.
    private final Map<Descriptor, CelHandle> messageCelByType =
            new java.util.concurrent.ConcurrentHashMap<>();
    // Translated + compiled rule model per message type (see CompiledRules).
    private final Map<Descriptor, CompiledRules> rulesByType =
            new java.util.concurrent.ConcurrentHashMap<>();

    /** Uses the default rule-source chain ({@link ValidationRuleSources#defaults()}). */
    public ProtoValidator(CelEvaluator fieldCel) {
        this(fieldCel, ValidationRuleSources.defaults());
    }

    /**
     * {@code fieldCel} evaluates field-level rules. Message-level rules cannot share it: their
     * environment types {@code this} as the message being validated, so it is built per message
     * type (see {@link #messageCelFor}).
     */
    public ProtoValidator(CelEvaluator fieldCel, List<ValidationRuleSource> sources) {
        this(new CelHandle(null, Objects.requireNonNull(fieldCel, "fieldCel")), sources);
    }

    private ProtoValidator(CelHandle fieldCel, List<ValidationRuleSource> sources) {
        this.fieldCel = fieldCel;
        this.sources = List.copyOf(Objects.requireNonNull(sources, "sources"));
    }

    /**
     * Default CEL environments: {@code this} is DYN for field rules and typed as the message
     * under validation for message rules.
     */
    public static ProtoValidator create() {
        return create(ValidationRuleSources.defaults());
    }

    /** As {@link #create()} but with an explicit rule-source chain. */
    public static ProtoValidator create(List<ValidationRuleSource> sources) {
        Cel fieldEnv = celEnv().build();
        return new ProtoValidator(new CelHandle(fieldEnv, new CelEvaluator(fieldEnv)), sources);
    }

    /**
     * A CEL environment with {@code this}, {@code now} and {@code rule} bound and the format
     * standard-library functions (isHostname/isEmail/isIp/isIpPrefix/isUri/isUriRef/
     * isHostAndPort/isNan/isInf) registered. {@code rule} carries a predefined rule's
     * configured value; it is unbound for ordinary custom rules.
     */
    private static CelEnvironmentFactory celEnv() {
        return CelEnvironmentFactory.builder()
                .addVar("this")
                .addVar("now")
                .addVar("rule")
                .addFunctions(ValidationCelFunctions.declarations(), ValidationCelFunctions.bindings());
    }

    /** A message-level CEL environment whose {@code this} is typed as {@code descriptor}. */
    private CelHandle messageCelFor(Descriptor descriptor) {
        return cached(messageCelByType, MAX_CACHED_TYPES, descriptor, d -> {
            Cel env = celEnv().addMessageVar("this", d).build();
            return new CelHandle(env, new CelEvaluator(env));
        });
    }

    /**
     * Builds a validator whose message-level CEL knows {@code descriptor}'s type
     * (field access like {@code this.age}).
     */
    public static ProtoValidator forMessageType(Descriptor descriptor) {
        return forMessageType(descriptor, ValidationRuleSources.defaults());
    }

    /** As {@link #forMessageType(Descriptor)} but with an explicit rule-source chain. */
    public static ProtoValidator forMessageType(
            Descriptor descriptor, List<ValidationRuleSource> sources) {
        Objects.requireNonNull(descriptor, "descriptor");
        // Declaring `this` as the concrete message type lets message-level CEL type-check field
        // access (this.foo), surfacing type/field mismatches as compilation errors. That happens
        // per message type in messageCelFor, which covers nested messages too, so no environment
        // is pinned to `descriptor` here.
        return create(sources);
    }

    public ValidationResult validate(Message message) {
        Objects.requireNonNull(message, "message");
        List<ValidationResult.Violation> violations = new ArrayList<>();
        Descriptor descriptor = message.getDescriptorForType();
        CompiledRules rules = rulesFor(descriptor);
        for (FieldDescriptor field : descriptor.getFields()) {
            validateField(message, rules, field, field.getName(), 0, violations);
        }
        validateMessageRules(message, descriptor, rules, "", violations);
        return violations.isEmpty()
                ? ValidationResult.ok()
                : ValidationResult.failed(violations);
    }

    // ---- rule model assembly and eager compilation ----

    private CompiledRules rulesFor(Descriptor descriptor) {
        return cached(rulesByType, MAX_CACHED_TYPES, descriptor, this::compileRules);
    }

    /**
     * Translates every source's rules for {@code descriptor} and compiles them up front:
     * regex patterns and CEL programs are compiled here, and oneof rules are checked against
     * the descriptor, so malformed rules throw {@link RuleCompilationException} on the first
     * validation of the type regardless of which fields the message populates.
     */
    private CompiledRules compileRules(Descriptor descriptor) {
        Map<FieldDescriptor, List<FieldConstraints>> fields = new java.util.LinkedHashMap<>();
        for (FieldDescriptor field : descriptor.getFields()) {
            List<FieldConstraints> collected = new ArrayList<>(sources.size());
            for (ValidationRuleSource source : sources) {
                source.fieldConstraints(field).ifPresent(collected::add);
            }
            for (FieldConstraints constraints : collected) {
                compileFieldConstraints(constraints);
            }
            fields.put(field, List.copyOf(collected));
        }
        List<MessageConstraints> messages = new ArrayList<>();
        Set<String> oneofMembers = new HashSet<>();
        for (ValidationRuleSource source : sources) {
            MessageConstraints constraints = source.messageConstraints(descriptor).orElse(null);
            if (constraints == null || constraints.isEmpty()) {
                continue;
            }
            messages.add(constraints);
            // Unknown names in oneof rules are schema errors: silently treating them as
            // unpopulated (or ignoring the rule) would hide typos in third-party rule sources.
            for (MessageConstraints.Oneof oneof : constraints.oneofs()) {
                for (String name : oneof.fields()) {
                    if (descriptor.findFieldByName(name) == null) {
                        throw new RuleCompilationException(
                                "field " + name + " not found in message " + descriptor.getFullName());
                    }
                    oneofMembers.add(name);
                }
            }
            for (String oneofName : constraints.requiredOneofs()) {
                if (descriptor.getRealOneofs().stream().noneMatch(o -> o.getName().equals(oneofName))) {
                    throw new RuleCompilationException(
                            "oneof " + oneofName + " not found in message " + descriptor.getFullName());
                }
            }
            CelHandle handle = messageCelFor(descriptor);
            for (CelConstraint rule : constraints.cel()) {
                compileCel(handle, rule);
            }
        }
        return new CompiledRules(fields, List.copyOf(messages), Set.copyOf(oneofMembers));
    }

    /** Compiles every pattern and CEL rule in {@code constraints}, including nested element rules. */
    private void compileFieldConstraints(FieldConstraints constraints) {
        constraints.string().ifPresent(s -> s.pattern().ifPresent(ProtoValidator::compiledPattern));
        constraints.bytes().ifPresent(b -> b.pattern().ifPresent(ProtoValidator::compiledPattern));
        for (CelConstraint rule : constraints.cel()) {
            compileCel(fieldCel, rule);
        }
        constraints.repeated().flatMap(RepeatedConstraints::items)
                .ifPresent(this::compileFieldConstraints);
        constraints.map().ifPresent(m -> {
            m.keys().ifPresent(this::compileFieldConstraints);
            m.values().ifPresent(this::compileFieldConstraints);
        });
    }

    /**
     * Compiles a CEL rule eagerly. With a {@link Cel} handle the expression is type-checked and
     * its static result type verified (protovalidate rejects rules that return neither bool nor
     * string at compile time); with only an evaluator, compilation is triggered through it and
     * evaluation errors from unbound variables are ignored — they are not compile errors.
     */
    private static void compileCel(CelHandle handle, CelConstraint rule) {
        if (rule.expression().isBlank()) {
            return;
        }
        if (handle.cel() != null) {
            CelType result;
            try {
                result = handle.cel().compile(rule.expression()).getAst().getResultType();
            } catch (CelValidationException e) {
                throw new RuleCompilationException("Invalid CEL expression: " + e.getMessage(), e);
            }
            CelKind kind = result.kind();
            if (kind != CelKind.BOOL && kind != CelKind.STRING
                    && kind != CelKind.DYN && kind != CelKind.ANY && kind != CelKind.ERROR) {
                throw new RuleCompilationException(
                        "CEL rule must return bool or string, got " + result.name());
            }
        } else {
            try {
                handle.evaluator().evaluateValue(rule.expression(), Map.of());
            } catch (CelCompilationException e) {
                throw new RuleCompilationException(e.getMessage(), e);
            } catch (CelEvaluationException ignored) {
                // Compiled fine; failing on unbound `this`/`now`/`rule` here is expected.
            }
        }
    }

    /** The compiled form of {@code pattern}; an uncompilable pattern is a schema error. */
    private static Pattern compiledPattern(String pattern) {
        Pattern existing = PATTERNS.get(pattern);
        if (existing != null) {
            return existing;
        }
        try {
            if (PATTERNS.size() >= MAX_CACHED_PATTERNS) {
                PATTERNS.clear();
            }
            return PATTERNS.computeIfAbsent(pattern, Pattern::compile);
        } catch (PatternSyntaxException e) {
            throw new RuleCompilationException("invalid regex pattern: " + e.getMessage(), e);
        }
    }

    /** Clear-on-threshold cache lookup (see the cache-bounds note on the constants). */
    private static <K, V> V cached(
            Map<K, V> cache, int maxSize, K key, java.util.function.Function<K, V> compute) {
        V existing = cache.get(key);
        if (existing != null) {
            return existing;
        }
        if (cache.size() >= maxSize) {
            cache.clear();
        }
        return cache.computeIfAbsent(key, compute);
    }

    // ---- field walk ----

    private void validateField(
            Message message,
            CompiledRules rules,
            FieldDescriptor field,
            String path,
            int depth,
            List<ValidationResult.Violation> violations) {
        List<FieldConstraints> constraints = rules.fields().get(field);
        IgnoreMode ignore = effectiveIgnore(constraints);
        if (ignore == IgnoreMode.ALWAYS) {
            return;
        }
        boolean hasField = isPresent(message, field);

        if (constraints.stream().anyMatch(FieldConstraints::required) && !hasField) {
            violations.add(new ValidationResult.Violation(path, "required", "field is required"));
            return;
        }

        // Fields that track presence (message, optional, oneof) and fields marked ignore-if-zero are
        // skipped when unpopulated. Implicit-presence scalars/collections are validated even at their
        // zero value, so min_items / bounds on a zero apply. Members of a message-level oneof rule are
        // treated as presence-tracking too: their field-level rules only apply when populated.
        boolean skipWhenEmpty = field.hasPresence() || ignore == IgnoreMode.IF_ZERO_VALUE
                || rules.oneofMembers().contains(field.getName());
        if (skipWhenEmpty && !hasField) {
            return;
        }

        if (field.isMapField()) {
            validateMap(message, field, constraints, path, depth, violations);
            // A field-level CEL rule on a map binds `this` to the whole map, evaluated once.
            Object celMap = celMapValue(message, field);
            for (FieldConstraints c : constraints) {
                runFieldCel(c, celMap, path, violations);
            }
            return;
        }
        if (field.isRepeated()) {
            validateRepeated(message, field, constraints, path, depth, violations);
            // A field-level CEL rule on a repeated field binds `this` to the whole list.
            Object celList = celListValue(message, field);
            for (FieldConstraints c : constraints) {
                runFieldCel(c, celList, path, violations);
            }
            return;
        }

        Object value = message.getField(field);
        for (FieldConstraints c : constraints) {
            applyFieldConstraints(field, c, value, path, violations);
            runFieldCel(c, celScalar(field, value), path, violations);
        }
        if (value instanceof Message nested) {
            validateChildren(nested, path, depth, violations);
        }
    }

    private void validateChildren(
            Message nested, String path, int depth, List<ValidationResult.Violation> violations) {
        if (depth >= MAX_NESTING_DEPTH) {
            throw new RuleEvaluationException(
                    "message nesting exceeds " + MAX_NESTING_DEPTH + " levels at " + path);
        }
        Descriptor descriptor = nested.getDescriptorForType();
        CompiledRules rules = rulesFor(descriptor);
        for (FieldDescriptor child : descriptor.getFields()) {
            validateField(nested, rules, child, path + "." + child.getName(), depth + 1, violations);
        }
        validateMessageRules(nested, descriptor, rules, path, violations);
    }

    private void validateRepeated(
            Message message,
            FieldDescriptor field,
            List<FieldConstraints> constraints,
            String path,
            int depth,
            List<ValidationResult.Violation> violations) {
        int count = message.getRepeatedFieldCount(field);
        for (FieldConstraints c : constraints) {
            RepeatedConstraints r = c.repeated().orElse(null);
            if (r == null) {
                continue;
            }
            if (r.minItems().isPresent() && count < r.minItems().getAsLong()) {
                violations.add(violation(path, "repeated.min_items",
                        "must have at least " + r.minItems().getAsLong() + " items"));
            }
            if (r.maxItems().isPresent() && count > r.maxItems().getAsLong()) {
                violations.add(violation(path, "repeated.max_items",
                        "must have at most " + r.maxItems().getAsLong() + " items"));
            }
            if (r.unique()) {
                Set<Object> seen = new HashSet<>();
                for (int i = 0; i < count; i++) {
                    Object element = uniqueKey(message.getRepeatedField(field, i));
                    if (element != null && !seen.add(element)) {
                        // A single violation on the repeated field itself, not per duplicate element.
                        violations.add(violation(path, "repeated.unique",
                                "repeated values must be unique"));
                        break;
                    }
                }
            }
        }
        for (int i = 0; i < count; i++) {
            Object element = message.getRepeatedField(field, i);
            String elementPath = path + "[" + i + "]";
            boolean skipElement = false;
            for (FieldConstraints c : constraints) {
                var items = c.repeated().flatMap(RepeatedConstraints::items).orElse(null);
                if (items == null) {
                    continue;
                }
                if (skipValue(items, element, field)) {
                    // An item ignored by its own rule (IGNORE_ALWAYS) also skips embedded validation.
                    skipElement = true;
                    continue;
                }
                applyFieldConstraints(field, items, element, elementPath, violations);
                runFieldCel(items, celScalar(field, element), elementPath, violations);
            }
            if (!skipElement && element instanceof Message nested) {
                validateChildren(nested, elementPath, depth, violations);
            }
        }
    }

    /**
     * The identity used for {@code repeated.unique} duplicate detection, matching CEL numeric
     * equality: {@code -0.0} equals {@code 0.0}, and {@code NaN} equals nothing — a NaN element
     * (returned as null) can never be a duplicate.
     */
    private static Object uniqueKey(Object element) {
        if (element instanceof Double d) {
            return Double.isNaN(d) ? null : (d == 0.0d ? Double.valueOf(0.0d) : d);
        }
        if (element instanceof Float f) {
            return Float.isNaN(f) ? null : (f == 0.0f ? Float.valueOf(0.0f) : f);
        }
        return element;
    }

    private void validateMap(
            Message message,
            FieldDescriptor field,
            List<FieldConstraints> constraints,
            String path,
            int depth,
            List<ValidationResult.Violation> violations) {
        int count = message.getRepeatedFieldCount(field);
        for (FieldConstraints c : constraints) {
            MapConstraints m = c.map().orElse(null);
            if (m == null) {
                continue;
            }
            if (m.minPairs().isPresent() && count < m.minPairs().getAsLong()) {
                violations.add(violation(path, "map.min_pairs",
                        "must have at least " + m.minPairs().getAsLong() + " entries"));
            }
            if (m.maxPairs().isPresent() && count > m.maxPairs().getAsLong()) {
                violations.add(violation(path, "map.max_pairs",
                        "must have at most " + m.maxPairs().getAsLong() + " entries"));
            }
        }
        Descriptor entryType = field.getMessageType();
        FieldDescriptor keyField = entryType.findFieldByNumber(1);
        FieldDescriptor valueField = entryType.findFieldByNumber(2);
        for (int i = 0; i < count; i++) {
            Message entry = (Message) message.getRepeatedField(field, i);
            Object key = entry.getField(keyField);
            Object value = entry.getField(valueField);
            String entryPath = path + subscript(key);
            String keyPath = entryPath + "#key";
            boolean skipEntryValue = false;
            for (FieldConstraints c : constraints) {
                MapConstraints m = c.map().orElse(null);
                if (m == null) {
                    continue;
                }
                FieldConstraints keyRules = m.keys().orElse(null);
                if (keyRules != null && !skipValue(keyRules, key, keyField)) {
                    applyFieldConstraints(keyField, keyRules, key, keyPath, violations);
                    runFieldCel(keyRules, celScalar(keyField, key), keyPath, violations);
                }
                FieldConstraints valueRules = m.values().orElse(null);
                if (valueRules == null) {
                    continue;
                }
                if (skipValue(valueRules, value, valueField)) {
                    // A value ignored by its own rule (IGNORE_ALWAYS) also skips embedded validation.
                    skipEntryValue = true;
                    continue;
                }
                applyFieldConstraints(valueField, valueRules, value, entryPath, violations);
                runFieldCel(valueRules, celScalar(valueField, value), entryPath, violations);
            }
            if (!skipEntryValue && value instanceof Message nested) {
                validateChildren(nested, entryPath, depth, violations);
            }
        }
    }

    private static String subscript(Object key) {
        if (key instanceof String s) {
            // Escape backslash and quote so the quoted key round-trips unambiguously.
            return "[\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\"]";
        }
        return "[" + key + "]";
    }

    private static void applyFieldConstraints(
            FieldDescriptor field,
            FieldConstraints constraints,
            Object value,
            String path,
            List<ValidationResult.Violation> violations) {
        switch (field.getJavaType()) {
            case STRING, INT, LONG, FLOAT, DOUBLE, BOOLEAN, BYTE_STRING ->
                    applyScalar(constraints, field.getJavaType(), value, path, violations);
            case ENUM -> constraints.enumeration()
                    .ifPresent(e -> applyEnum(e, (EnumValueDescriptor) value, path, violations));
            case MESSAGE -> {
                String type = field.getMessageType().getFullName();
                if (TIMESTAMP_TYPE.equals(type)) {
                    constraints.timestamp().ifPresent(t ->
                            applyTimestamp(t, toInstant((Message) value), path, violations));
                } else if (DURATION_TYPE.equals(type)) {
                    constraints.duration().ifPresent(d ->
                            applyDuration(d, toJavaDuration((Message) value), path, violations));
                } else if ("google.protobuf.Any".equals(type)) {
                    constraints.any().ifPresent(a -> applyAny(a, (Message) value, path, violations));
                } else if ("google.protobuf.FieldMask".equals(type)) {
                    constraints.fieldMask().ifPresent(fm ->
                            applyFieldMask(fm, (Message) value, path, violations));
                } else {
                    // Well-known wrapper types (Int32Value, StringValue, …) apply their scalar rules
                    // to the wrapped value; the field is present (message presence) so this only runs
                    // when the wrapper is set.
                    FieldDescriptor.JavaType wrapped = WRAPPER_TYPES.get(type);
                    if (wrapped != null) {
                        Message wrapper = (Message) value;
                        Object inner = wrapper.getField(
                                wrapper.getDescriptorForType().findFieldByNumber(1));
                        applyScalar(constraints, wrapped, inner, path, violations);
                    }
                }
            }
        }
    }

    /** {@code google.protobuf.Any}: its type URL must be allowed by {@code in}/{@code not_in}. */
    private static void applyAny(
            ai.pipestream.proto.validate.model.AnyConstraints rules, Message any, String path,
            List<ValidationResult.Violation> violations) {
        String typeUrl = (String) any.getField(any.getDescriptorForType().findFieldByNumber(1));
        if (!rules.in().isEmpty() && !rules.in().contains(typeUrl)) {
            violations.add(violation(path, "any.in", "type URL must be one of the allowed values"));
        }
        if (!rules.notIn().isEmpty() && rules.notIn().contains(typeUrl)) {
            violations.add(violation(path, "any.not_in", "type URL must not be a forbidden value"));
        }
    }

    /** {@code google.protobuf.FieldMask}: compare the mask in its comma-joined path form. */
    private static void applyFieldMask(
            ai.pipestream.proto.validate.model.FieldMaskConstraints rules, Message mask, String path,
            List<ValidationResult.Violation> violations) {
        @SuppressWarnings("unchecked")
        List<String> paths = (List<String>) mask.getField(mask.getDescriptorForType().findFieldByNumber(1));
        if (rules.constant().isPresent() && !String.join(",", paths).equals(rules.constant().get())) {
            violations.add(violation(path, "field_mask.const", "must equal the required field mask"));
        }
        // in / not_in test each path against the rule paths by prefix coverage: a mask path "a.foo"
        // is covered by the entry "a". Every path must be covered by some in entry; no path may be
        // covered by any not_in entry.
        if (!rules.in().isEmpty()
                && !paths.stream().allMatch(p -> coveredByAny(p, rules.in()))) {
            violations.add(violation(path, "field_mask.in", "must be one of the allowed values"));
        }
        if (!rules.notIn().isEmpty()
                && paths.stream().anyMatch(p -> coveredByAny(p, rules.notIn()))) {
            violations.add(violation(path, "field_mask.not_in", "must not be one of the forbidden values"));
        }
    }

    /** True when {@code path} equals or is nested under one of {@code entries} (e.g. a.foo under a). */
    private static boolean coveredByAny(String path, List<String> entries) {
        for (String entry : entries) {
            if (path.equals(entry) || path.startsWith(entry + ".")) {
                return true;
            }
        }
        return false;
    }

    /** Applies the scalar constraint family matching {@code type} to {@code value}. */
    private static void applyScalar(
            FieldConstraints constraints, FieldDescriptor.JavaType type, Object value,
            String path, List<ValidationResult.Violation> violations) {
        switch (type) {
            case STRING -> constraints.string()
                    .ifPresent(s -> applyString(s, (String) value, path, violations));
            case INT, LONG -> constraints.integral()
                    .ifPresent(n -> applyIntegral(n, integralValue(n, value), path, violations));
            case FLOAT, DOUBLE -> constraints.floating()
                    .ifPresent(n -> applyFloating(n, ((Number) value).doubleValue(), path, violations));
            case BOOLEAN -> constraints.bool()
                    .ifPresent(b -> applyBool(b, (Boolean) value, path, violations));
            case BYTE_STRING -> constraints.bytes()
                    .ifPresent(b -> applyBytes(b, (ByteString) value, path, violations));
            default -> {
            }
        }
    }

    /** Widens the raw value to a long, honoring unsigned 32-bit semantics. */
    private static long integralValue(IntegralConstraints rules, Object value) {
        if (rules.unsigned() && value instanceof Integer i) {
            return Integer.toUnsignedLong(i);
        }
        return ((Number) value).longValue();
    }

    private static void applyString(
            StringConstraints rules, String value, String path,
            List<ValidationResult.Violation> violations) {
        long len = value.codePointCount(0, value.length());
        if (rules.constant().isPresent() && !value.equals(rules.constant().get())) {
            violations.add(violation(path, "string.const",
                    "must equal \"" + rules.constant().get() + "\""));
        }
        if (rules.len().isPresent() && len != rules.len().getAsLong()) {
            violations.add(violation(path, "string.len",
                    "length must be exactly " + rules.len().getAsLong()));
        }
        if (rules.minLen().isPresent() && len < rules.minLen().getAsLong()) {
            violations.add(violation(path, "string.min_len",
                    "length must be at least " + rules.minLen().getAsLong()));
        }
        if (rules.maxLen().isPresent() && len > rules.maxLen().getAsLong()) {
            violations.add(violation(path, "string.max_len",
                    "length must be at most " + rules.maxLen().getAsLong()));
        }
        if (rules.lenBytes().isPresent() || rules.minBytes().isPresent() || rules.maxBytes().isPresent()) {
            long bytes = value.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
            if (rules.lenBytes().isPresent() && bytes != rules.lenBytes().getAsLong()) {
                violations.add(violation(path, "string.len_bytes",
                        "must be exactly " + rules.lenBytes().getAsLong() + " bytes"));
            }
            if (rules.minBytes().isPresent() && bytes < rules.minBytes().getAsLong()) {
                violations.add(violation(path, "string.min_bytes",
                        "must be at least " + rules.minBytes().getAsLong() + " bytes"));
            }
            if (rules.maxBytes().isPresent() && bytes > rules.maxBytes().getAsLong()) {
                violations.add(violation(path, "string.max_bytes",
                        "must be at most " + rules.maxBytes().getAsLong() + " bytes"));
            }
        }
        if (rules.pattern().isPresent()
                && !compiledPattern(rules.pattern().get()).matcher(value).find()) {
            violations.add(violation(path, "string.pattern", "value does not match pattern"));
        }
        if (rules.prefix().isPresent() && !value.startsWith(rules.prefix().get())) {
            violations.add(violation(path, "string.prefix",
                    "must start with \"" + rules.prefix().get() + "\""));
        }
        if (rules.suffix().isPresent() && !value.endsWith(rules.suffix().get())) {
            violations.add(violation(path, "string.suffix",
                    "must end with \"" + rules.suffix().get() + "\""));
        }
        if (rules.contains().isPresent() && !value.contains(rules.contains().get())) {
            violations.add(violation(path, "string.contains",
                    "must contain \"" + rules.contains().get() + "\""));
        }
        if (rules.notContains().isPresent() && value.contains(rules.notContains().get())) {
            violations.add(violation(path, "string.not_contains",
                    "must not contain \"" + rules.notContains().get() + "\""));
        }
        if (!rules.in().isEmpty() && !rules.in().contains(value)) {
            violations.add(violation(path, "string.in", "must be one of " + rules.in()));
        }
        if (!rules.notIn().isEmpty() && rules.notIn().contains(value)) {
            violations.add(violation(path, "string.not_in", "must not be one of " + rules.notIn()));
        }
        for (StringFormat format : rules.formats()) {
            if (value.isEmpty()) {
                violations.add(violation(path, format.emptyRuleId(), format.emptyMessage()));
            } else if (!format.matches(value)) {
                violations.add(violation(path, format.ruleId(), format.defaultMessage()));
            }
        }
        rules.httpHeader().ifPresent(header -> {
            if (header.rejectEmpty() && value.isEmpty()) {
                violations.add(violation(path, header.emptyRuleId(), "value is empty"));
            } else if (!header.matches(value)) {
                violations.add(violation(path, header.ruleId(), "must be a valid HTTP header"));
            }
        });
    }

    private static void applyIntegral(
            IntegralConstraints rules, long value, String path,
            List<ValidationResult.Violation> violations) {
        String prefix = rules.ruleIdPrefix();
        boolean unsigned = rules.unsigned();
        if (rules.constant().isPresent() && value != rules.constant().getAsLong()) {
            violations.add(violation(path, prefix + ".const",
                    "must equal " + fmt(rules.constant().getAsLong(), unsigned)));
        }
        Comparator<Long> order = unsigned ? Long::compareUnsigned : Long::compare;
        applyRange(prefix, path, value,
                boxed(rules.gt()), boxed(rules.gte()), boxed(rules.lt()), boxed(rules.lte()),
                order, v -> fmt(v, unsigned), violations);
        if (!rules.in().isEmpty() && !rules.in().contains(value)) {
            violations.add(violation(path, prefix + ".in", "must be one of the allowed values"));
        }
        if (!rules.notIn().isEmpty() && rules.notIn().contains(value)) {
            violations.add(violation(path, prefix + ".not_in", "must not be one of the forbidden values"));
        }
    }

    private static String fmt(long value, boolean unsigned) {
        return unsigned ? Long.toUnsignedString(value) : Long.toString(value);
    }

    private static Long boxed(java.util.OptionalLong o) {
        return o.isPresent() ? o.getAsLong() : null;
    }

    private static Double boxed(java.util.OptionalDouble o) {
        return o.isPresent() ? o.getAsDouble() : null;
    }

    /**
     * Emits a single range violation for the combined lower/upper bounds, matching protovalidate's
     * semantics: when only one bound is set it fires the individual {@code gt/gte/lt/lte} rule; when
     * both are set they collapse into one {@code <lower>_<upper>} rule (or {@code …_exclusive} when
     * the bounds are reversed so the valid region is outside the range). {@code null} bounds are
     * absent. Used for every totally-ordered numeric type (integers, timestamps, durations).
     */
    private static <T> void applyRange(
            String prefix, String path, T value, T gt, T gte, T lt, T lte,
            Comparator<T> order, java.util.function.Function<T, String> fmt,
            List<ValidationResult.Violation> violations) {
        T lower = gt != null ? gt : gte;
        String lowerName = gt != null ? "gt" : (gte != null ? "gte" : null);
        boolean lowerInclusive = gt == null && gte != null;
        T upper = lt != null ? lt : lte;
        String upperName = lt != null ? "lt" : (lte != null ? "lte" : null);
        boolean upperInclusive = lt == null && lte != null;

        if (lower != null && upper != null) {
            boolean satLower = lowerInclusive
                    ? order.compare(value, lower) >= 0 : order.compare(value, lower) > 0;
            boolean satUpper = upperInclusive
                    ? order.compare(value, upper) <= 0 : order.compare(value, upper) < 0;
            boolean exclusive = order.compare(upper, lower) < 0;
            boolean ok = exclusive ? (satLower || satUpper) : (satLower && satUpper);
            if (!ok) {
                String ruleId = prefix + "." + lowerName + "_" + upperName + (exclusive ? "_exclusive" : "");
                violations.add(violation(path, ruleId, exclusive
                        ? "must be " + lowerName + " " + fmt.apply(lower) + " or " + upperName + " " + fmt.apply(upper)
                        : "must be " + lowerName + " " + fmt.apply(lower) + " and " + upperName + " " + fmt.apply(upper)));
            }
        } else if (lower != null) {
            boolean sat = lowerInclusive
                    ? order.compare(value, lower) >= 0 : order.compare(value, lower) > 0;
            if (!sat) {
                violations.add(violation(path, prefix + "." + lowerName,
                        "must be " + (lowerInclusive ? ">= " : "> ") + fmt.apply(lower)));
            }
        } else if (upper != null) {
            boolean sat = upperInclusive
                    ? order.compare(value, upper) <= 0 : order.compare(value, upper) < 0;
            if (!sat) {
                violations.add(violation(path, prefix + "." + upperName,
                        "must be " + (upperInclusive ? "<= " : "< ") + fmt.apply(upper)));
            }
        }
    }

    /**
     * IEEE-aware counterpart of {@link #applyRange} for floating-point values: a {@code NaN} value
     * satisfies no bound (every comparison is false), so it violates any range — which a total-order
     * comparator could not express.
     */
    private static void applyDoubleRange(
            String prefix, String path, double value, Double gt, Double gte, Double lt, Double lte,
            List<ValidationResult.Violation> violations) {
        Double lower = gt != null ? gt : gte;
        String lowerName = gt != null ? "gt" : (gte != null ? "gte" : null);
        boolean lowerInclusive = gt == null && gte != null;
        Double upper = lt != null ? lt : lte;
        String upperName = lt != null ? "lt" : (lte != null ? "lte" : null);
        boolean upperInclusive = lt == null && lte != null;

        if (lower != null && upper != null) {
            boolean satLower = lowerInclusive ? value >= lower : value > lower;
            boolean satUpper = upperInclusive ? value <= upper : value < upper;
            boolean exclusive = upper < lower;
            boolean ok = exclusive ? (satLower || satUpper) : (satLower && satUpper);
            if (!ok) {
                String ruleId = prefix + "." + lowerName + "_" + upperName + (exclusive ? "_exclusive" : "");
                violations.add(violation(path, ruleId, "must be within " + lowerName + "/" + upperName + " range"));
            }
        } else if (lower != null) {
            boolean sat = lowerInclusive ? value >= lower : value > lower;
            if (!sat) {
                violations.add(violation(path, prefix + "." + lowerName,
                        "must be " + (lowerInclusive ? ">= " : "> ") + lower));
            }
        } else if (upper != null) {
            boolean sat = upperInclusive ? value <= upper : value < upper;
            if (!sat) {
                violations.add(violation(path, prefix + "." + upperName,
                        "must be " + (upperInclusive ? "<= " : "< ") + upper));
            }
        }
    }

    private static void applyFloating(
            FloatingConstraints rules, double value, String path,
            List<ValidationResult.Violation> violations) {
        String prefix = rules.ruleIdPrefix();
        if (rules.constant().isPresent() && value != rules.constant().getAsDouble()) {
            violations.add(violation(path, prefix + ".const",
                    "must equal " + rules.constant().getAsDouble()));
        }
        applyDoubleRange(prefix, path, value,
                boxed(rules.gt()), boxed(rules.gte()), boxed(rules.lt()), boxed(rules.lte()),
                violations);
        if (!rules.in().isEmpty() && !containsNumeric(rules.in(), value)) {
            violations.add(violation(path, prefix + ".in", "must be one of the allowed values"));
        }
        if (!rules.notIn().isEmpty() && containsNumeric(rules.notIn(), value)) {
            violations.add(violation(path, prefix + ".not_in", "must not be one of the forbidden values"));
        }
        if (rules.finite() && !Double.isFinite(value)) {
            violations.add(violation(path, prefix + ".finite", "must be finite"));
        }
    }

    /**
     * Membership by IEEE numeric equality, matching CEL: {@code -0.0} equals {@code 0.0} and
     * {@code NaN} equals nothing — boxed {@link Double#equals} gets both edge cases wrong.
     */
    private static boolean containsNumeric(List<Double> values, double value) {
        for (double candidate : values) {
            if (candidate == value) {
                return true;
            }
        }
        return false;
    }

    private static void applyBool(
            BoolConstraints rules, boolean value, String path,
            List<ValidationResult.Violation> violations) {
        if (rules.constant().isPresent() && value != rules.constant().get()) {
            violations.add(violation(path, "bool.const", "must equal " + rules.constant().get()));
        }
    }

    private static void applyBytes(
            BytesConstraints rules, ByteString value, String path,
            List<ValidationResult.Violation> violations) {
        int size = value.size();
        if (rules.constant().isPresent() && !value.equals(rules.constant().get())) {
            violations.add(violation(path, "bytes.const", "must equal the required bytes"));
        }
        if (rules.len().isPresent() && size != rules.len().getAsLong()) {
            violations.add(violation(path, "bytes.len",
                    "length must be exactly " + rules.len().getAsLong() + " bytes"));
        }
        if (rules.minLen().isPresent() && size < rules.minLen().getAsLong()) {
            violations.add(violation(path, "bytes.min_len",
                    "length must be at least " + rules.minLen().getAsLong() + " bytes"));
        }
        if (rules.maxLen().isPresent() && size > rules.maxLen().getAsLong()) {
            violations.add(violation(path, "bytes.max_len",
                    "length must be at most " + rules.maxLen().getAsLong() + " bytes"));
        }
        if (rules.prefix().isPresent() && !value.startsWith(rules.prefix().get())) {
            violations.add(violation(path, "bytes.prefix", "must start with the required bytes"));
        }
        if (rules.suffix().isPresent() && !value.endsWith(rules.suffix().get())) {
            violations.add(violation(path, "bytes.suffix", "must end with the required bytes"));
        }
        if (rules.contains().isPresent() && !bytesContain(value, rules.contains().get())) {
            violations.add(violation(path, "bytes.contains", "must contain the required bytes"));
        }
        if (rules.pattern().isPresent()) {
            // protovalidate applies the pattern to the value decoded as UTF-8; non-UTF-8 bytes are a
            // runtime error rather than a validation failure.
            if (!decodesAsUtf8(value)) {
                throw new RuleEvaluationException(
                        "bytes.pattern", "value must be valid UTF-8 to apply regexp", null);
            }
            if (!compiledPattern(rules.pattern().get()).matcher(value.toStringUtf8()).find()) {
                violations.add(violation(path, "bytes.pattern", "value does not match pattern"));
            }
        }
        if (!rules.in().isEmpty() && !rules.in().contains(value)) {
            violations.add(violation(path, "bytes.in", "must be one of the allowed values"));
        }
        if (!rules.notIn().isEmpty() && rules.notIn().contains(value)) {
            violations.add(violation(path, "bytes.not_in", "must not be one of the forbidden values"));
        }
        for (BytesFormat format : rules.formats()) {
            if (size == 0) {
                // An empty value reports the companion <id>_empty rule, matching string formats.
                violations.add(violation(path, format.emptyRuleId(), "value is empty"));
            } else if (!format.matches(size)) {
                violations.add(violation(path, format.ruleId(), format.defaultMessage()));
            }
        }
    }

    private static boolean decodesAsUtf8(ByteString value) {
        try {
            java.nio.charset.StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(java.nio.charset.CodingErrorAction.REPORT)
                    .onUnmappableCharacter(java.nio.charset.CodingErrorAction.REPORT)
                    .decode(value.asReadOnlyByteBuffer());
            return true;
        } catch (java.nio.charset.CharacterCodingException e) {
            return false;
        }
    }

    private static boolean bytesContain(ByteString haystack, ByteString needle) {
        if (needle.isEmpty()) {
            return true;
        }
        for (int i = 0; i + needle.size() <= haystack.size(); i++) {
            boolean match = true;
            for (int j = 0; j < needle.size(); j++) {
                if (haystack.byteAt(i + j) != needle.byteAt(j)) {
                    match = false;
                    break;
                }
            }
            if (match) {
                return true;
            }
        }
        return false;
    }

    private static void applyEnum(
            EnumConstraints rules, EnumValueDescriptor value, String path,
            List<ValidationResult.Violation> violations) {
        int number = value.getNumber();
        if (rules.constant().isPresent() && number != rules.constant().getAsInt()) {
            violations.add(violation(path, "enum.const", "must equal " + rules.constant().getAsInt()));
        }
        // Unknown numbers surface as synthetic value descriptors with index -1.
        if (rules.definedOnly() && value.getIndex() < 0) {
            violations.add(violation(path, "enum.defined_only",
                    "must be a defined enum value, got " + number));
        }
        if (!rules.in().isEmpty() && !rules.in().contains(number)) {
            violations.add(violation(path, "enum.in", "must be one of the allowed values"));
        }
        if (!rules.notIn().isEmpty() && rules.notIn().contains(number)) {
            violations.add(violation(path, "enum.not_in", "must not be one of the forbidden values"));
        }
    }

    private static void applyTimestamp(
            TimestampConstraints rules, Instant value, String path,
            List<ValidationResult.Violation> violations) {
        Instant now = Instant.now();
        if (rules.constant().isPresent() && !value.equals(rules.constant().get())) {
            violations.add(violation(path, "timestamp.const", "must equal " + rules.constant().get()));
        }
        applyRange("timestamp", path, value,
                rules.gt().orElse(null), rules.gte().orElse(null),
                rules.lt().orElse(null), rules.lte().orElse(null),
                Comparator.naturalOrder(), Instant::toString, violations);
        if (rules.ltNow() && value.compareTo(now) >= 0) {
            violations.add(violation(path, "timestamp.lt_now", "must be in the past"));
        }
        if (rules.gtNow() && value.compareTo(now) <= 0) {
            violations.add(violation(path, "timestamp.gt_now", "must be in the future"));
        }
        if (rules.within().isPresent()) {
            Duration distance = Duration.between(value, now).abs();
            if (distance.compareTo(rules.within().get()) > 0) {
                violations.add(violation(path, "timestamp.within",
                        "must be within " + rules.within().get() + " of now"));
            }
        }
    }

    private static void applyDuration(
            DurationConstraints rules, Duration value, String path,
            List<ValidationResult.Violation> violations) {
        if (rules.constant().isPresent() && !value.equals(rules.constant().get())) {
            violations.add(violation(path, "duration.const",
                    "must equal " + rules.constant().get()));
        }
        applyRange("duration", path, value,
                rules.gt().orElse(null), rules.gte().orElse(null),
                rules.lt().orElse(null), rules.lte().orElse(null),
                Comparator.naturalOrder(), Duration::toString, violations);
        if (!rules.in().isEmpty() && !rules.in().contains(value)) {
            violations.add(violation(path, "duration.in", "must be one of the allowed values"));
        }
        if (!rules.notIn().isEmpty() && rules.notIn().contains(value)) {
            violations.add(violation(path, "duration.not_in", "must not be one of the forbidden values"));
        }
    }

    private static Instant toInstant(Message timestamp) {
        Descriptor d = timestamp.getDescriptorForType();
        long seconds = (Long) timestamp.getField(d.findFieldByName("seconds"));
        int nanos = (Integer) timestamp.getField(d.findFieldByName("nanos"));
        try {
            return Instant.ofEpochSecond(seconds, nanos);
        } catch (DateTimeException | ArithmeticException e) {
            // Out-of-range seconds/nanos are a runtime failure, not a raw unchecked leak.
            throw new RuleEvaluationException("timestamp value out of range: " + e.getMessage(), e);
        }
    }

    private static Duration toJavaDuration(Message duration) {
        Descriptor d = duration.getDescriptorForType();
        long seconds = (Long) duration.getField(d.findFieldByName("seconds"));
        int nanos = (Integer) duration.getField(d.findFieldByName("nanos"));
        try {
            return Duration.ofSeconds(seconds, nanos);
        } catch (DateTimeException | ArithmeticException e) {
            throw new RuleEvaluationException("duration value out of range: " + e.getMessage(), e);
        }
    }

    /** Runs a field's CEL rules against an already CEL-converted {@code this} value. */
    private void runFieldCel(
            FieldConstraints constraints, Object celValue, String path,
            List<ValidationResult.Violation> violations) {
        if (constraints.cel().isEmpty()) {
            return;
        }
        // cel[N] and cel_expression[N] are indexed independently within their own repeated field;
        // predefined rules carry an explicit extension-shaped rule path instead.
        Map<String, Integer> next = new java.util.HashMap<>();
        for (CelConstraint rule : constraints.cel()) {
            String rulePath = rule.rulePath();
            if (rulePath.isEmpty()) {
                int i = next.merge(rule.celField(), 1, Integer::sum) - 1;
                rulePath = rule.celField() + "[" + i + "]";
            }
            evalCel(fieldCel.evaluator(), rule, celValue, path, rulePath, violations);
        }
    }

    private void validateMessageRules(
            Message message,
            Descriptor descriptor,
            CompiledRules rules,
            String path,
            List<ValidationResult.Violation> violations) {
        for (MessageConstraints constraints : rules.messages()) {
            // Message-level CEL rules report no FieldRules rule path (they are not on any field),
            // and top-level violations carry an empty field path: the rule targets the message
            // itself, not any named field.
            CelEvaluator evaluator = messageCelFor(descriptor).evaluator();
            for (CelConstraint rule : constraints.cel()) {
                evalCel(evaluator, rule, message, path, "", violations);
            }
            for (MessageConstraints.Oneof oneof : constraints.oneofs()) {
                validateMessageOneof(message, descriptor, oneof, path, violations);
            }
            for (String oneofName : constraints.requiredOneofs()) {
                validateRequiredOneof(message, descriptor, oneofName, path, violations);
            }
        }
    }

    /**
     * A real protobuf oneof marked {@code required}: exactly one member must be set. The violation
     * reports {@code required} on the oneof name itself (a bare {@code field_name} path element).
     * The oneof's existence was checked when the rule model was compiled.
     */
    private static void validateRequiredOneof(
            Message message,
            Descriptor descriptor,
            String oneofName,
            String path,
            List<ValidationResult.Violation> violations) {
        var oneof = descriptor.getRealOneofs().stream()
                .filter(o -> o.getName().equals(oneofName))
                .findFirst()
                .orElseThrow(() -> new RuleCompilationException(
                        "oneof " + oneofName + " not found in message " + descriptor.getFullName()));
        if (!message.hasOneof(oneof)) {
            String oneofPath = path.isEmpty() ? oneofName : path + "." + oneofName;
            violations.add(new ValidationResult.Violation(
                    oneofPath, "required", "exactly one field is required in oneof"));
        }
    }

    /**
     * A message-level {@code oneof} rule: at most one member may be populated, and when
     * {@code required} at least one must be. Both failures report {@code message.oneof} on the
     * message path with the member list spelled out, matching protovalidate's wording. Member
     * names were resolved against the descriptor when the rule model was compiled.
     */
    private static void validateMessageOneof(
            Message message,
            Descriptor descriptor,
            MessageConstraints.Oneof oneof,
            String path,
            List<ValidationResult.Violation> violations) {
        int populated = 0;
        for (String name : oneof.fields()) {
            FieldDescriptor fd = descriptor.findFieldByName(name);
            if (fd == null) {
                throw new RuleCompilationException(
                        "field " + name + " not found in message " + descriptor.getFullName());
            }
            if (isPresent(message, fd)) {
                populated++;
            }
        }
        String members = String.join(", ", oneof.fields());
        if (populated > 1) {
            violations.add(new ValidationResult.Violation(
                    path, "message.oneof", "only one of " + members + " can be set"));
        } else if (oneof.required() && populated == 0) {
            violations.add(new ValidationResult.Violation(
                    path, "message.oneof", "one of " + members + " must be set"));
        }
    }

    private static void evalCel(
            CelEvaluator evaluator,
            CelConstraint rule,
            Object thisValue,
            String path,
            String rulePath,
            List<ValidationResult.Violation> violations) {
        if (rule.expression().isBlank()) {
            return;
        }
        // With no explicit id protovalidate uses the expression text as the rule id.
        String id = rule.id().isBlank() ? rule.expression() : rule.id();
        try {
            Map<String, Object> bindings = new java.util.HashMap<>();
            bindings.put("this", thisValue);
            // protovalidate exposes the current time as `now`; a single value keeps now == now true.
            bindings.put("now", java.time.Instant.now());
            if (rule.ruleValue() != null) {
                // Predefined rules see their configured value as `rule`.
                bindings.put("rule", rule.ruleValue());
            }
            Object result = evaluator.evaluateValue(rule.expression(), bindings);
            if (result instanceof Boolean ok) {
                if (!ok) {
                    String msg = rule.message().isBlank()
                            ? "\"" + rule.expression() + "\" returned false" : rule.message();
                    violations.add(new ValidationResult.Violation(path, id, msg, rulePath));
                }
            } else if (result instanceof String text) {
                if (!text.isEmpty()) {
                    violations.add(new ValidationResult.Violation(path, id, text, rulePath));
                }
            } else {
                // Statically bool/string-typed programs never land here; a dyn program returning
                // another type is still a per-value failure.
                violations.add(new ValidationResult.Violation(
                        path, id, "CEL rule must return bool or string", rulePath));
            }
        } catch (CelCompilationException e) {
            // A rule whose CEL does not compile (type error, unknown field) is a compilation error.
            throw new RuleCompilationException(e.getMessage(), e);
        } catch (CelEvaluationException e) {
            // A rule that compiles but fails at evaluation is a runtime error, not a violation.
            throw new RuleEvaluationException(id, "CEL runtime error: " + e.getMessage(), e);
        }
    }

    /** The whole repeated field as a CEL list, each element converted to its CEL Java type. */
    private static Object celListValue(Message message, FieldDescriptor field) {
        int count = message.getRepeatedFieldCount(field);
        List<Object> list = new java.util.ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            list.add(celScalar(field, message.getRepeatedField(field, i)));
        }
        return list;
    }

    /** The whole map field as a CEL map, keys and values converted to their CEL Java types. */
    private static Object celMapValue(Message message, FieldDescriptor field) {
        Descriptor entryType = field.getMessageType();
        FieldDescriptor keyField = entryType.findFieldByNumber(1);
        FieldDescriptor valueField = entryType.findFieldByNumber(2);
        int count = message.getRepeatedFieldCount(field);
        Map<Object, Object> map = new java.util.LinkedHashMap<>();
        for (int i = 0; i < count; i++) {
            Message entry = (Message) message.getRepeatedField(field, i);
            map.put(celScalar(keyField, entry.getField(keyField)),
                    celScalar(valueField, entry.getField(valueField)));
        }
        return map;
    }

    /** Converts a scalar protobuf value to the Java type CEL expects (unsigned for uint types). */
    private static Object celScalar(FieldDescriptor field, Object value) {
        return switch (field.getType()) {
            case UINT32, FIXED32 -> UnsignedLong.fromLongBits(Integer.toUnsignedLong((Integer) value));
            case UINT64, FIXED64 -> UnsignedLong.fromLongBits((Long) value);
            case INT32, SINT32, SFIXED32 -> ((Integer) value).longValue();
            case FLOAT -> ((Float) value).doubleValue();
            case ENUM -> (long) ((EnumValueDescriptor) value).getNumber();
            case MESSAGE, GROUP -> celMessage((Message) value);
            default -> value;
        };
    }

    /** Wrapper messages bind as their unwrapped scalar; Timestamp/Duration as temporal values. */
    private static Object celMessage(Message value) {
        Descriptor descriptor = value.getDescriptorForType();
        String type = descriptor.getFullName();
        if (TIMESTAMP_TYPE.equals(type)) {
            return toInstant(value);
        }
        if (DURATION_TYPE.equals(type)) {
            return toJavaDuration(value);
        }
        if (WRAPPER_TYPES.containsKey(type)) {
            FieldDescriptor inner = descriptor.findFieldByNumber(1);
            return celScalar(inner, value.getField(inner));
        }
        return value;
    }

    /** Whether an element (repeated item, map key/value) is skipped by its own ignore mode. */
    private static boolean skipValue(FieldConstraints constraints, Object value, FieldDescriptor field) {
        return switch (constraints.ignore()) {
            case ALWAYS -> true;
            case IF_ZERO_VALUE -> isZeroValue(value, field);
            case UNSPECIFIED -> false;
        };
    }

    private static boolean isZeroValue(Object value, FieldDescriptor field) {
        return switch (field.getJavaType()) {
            case INT -> (Integer) value == 0;
            case LONG -> (Long) value == 0L;
            case FLOAT -> (Float) value == 0f;
            case DOUBLE -> (Double) value == 0d;
            case BOOLEAN -> !((Boolean) value);
            case STRING -> ((String) value).isEmpty();
            case BYTE_STRING -> ((ByteString) value).isEmpty();
            case ENUM -> ((EnumValueDescriptor) value).getNumber() == 0;
            case MESSAGE -> false;
        };
    }

    /** The strongest ignore mode declared across a field's rule sources. */
    private static IgnoreMode effectiveIgnore(List<FieldConstraints> constraints) {
        IgnoreMode mode = IgnoreMode.UNSPECIFIED;
        for (FieldConstraints c : constraints) {
            if (c.ignore().ordinal() > mode.ordinal()) {
                mode = c.ignore();
            }
        }
        return mode;
    }

    private static boolean isPresent(Message message, FieldDescriptor field) {
        if (field.isRepeated()) {
            return message.getRepeatedFieldCount(field) > 0;
        }
        if (field.hasPresence()) {
            return message.hasField(field);
        }
        Object value = message.getField(field);
        return switch (field.getJavaType()) {
            case STRING -> value instanceof String s && !s.isEmpty();
            case BYTE_STRING -> value instanceof ByteString b && !b.isEmpty();
            case MESSAGE -> message.hasField(field);
            case ENUM -> ((EnumValueDescriptor) value).getNumber() != 0;
            case BOOLEAN -> (Boolean) value;
            case INT, LONG -> ((Number) value).longValue() != 0L;
            // Bitwise comparison so -0.0 counts as set (its raw bits differ from +0.0).
            case FLOAT, DOUBLE ->
                    Double.doubleToRawLongBits(((Number) value).doubleValue()) != 0L;
            default -> true;
        };
    }

    private static ValidationResult.Violation violation(String path, String id, String message) {
        return new ValidationResult.Violation(path, id, message);
    }
}
