package ai.pipestream.proto.validate;

import ai.pipestream.proto.cel.CelEnvironmentFactory;
import ai.pipestream.proto.cel.CelEvaluationException;
import ai.pipestream.proto.cel.CelEvaluator;
import ai.pipestream.proto.validate.cel.ValidationCelFunctions;
import ai.pipestream.proto.validate.model.BoolConstraints;
import ai.pipestream.proto.validate.model.BytesConstraints;
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
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;

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

    private final CelEvaluator fieldCel;
    private final CelEvaluator messageCel;
    private final List<ValidationRuleSource> sources;

    /** Uses the default rule-source chain ({@link ValidationRuleSources#defaults()}). */
    public ProtoValidator(CelEvaluator fieldCel, CelEvaluator messageCel) {
        this(fieldCel, messageCel, ValidationRuleSources.defaults());
    }

    public ProtoValidator(
            CelEvaluator fieldCel, CelEvaluator messageCel, List<ValidationRuleSource> sources) {
        this.fieldCel = Objects.requireNonNull(fieldCel, "fieldCel");
        this.messageCel = Objects.requireNonNull(messageCel, "messageCel");
        this.sources = List.copyOf(Objects.requireNonNull(sources, "sources"));
    }

    /** Default CEL environments: {@code this} is DYN for field and message rules. */
    public static ProtoValidator create() {
        return create(ValidationRuleSources.defaults());
    }

    /** As {@link #create()} but with an explicit rule-source chain. */
    public static ProtoValidator create(List<ValidationRuleSource> sources) {
        CelEvaluator field = new CelEvaluator(celEnv().build());
        CelEvaluator message = new CelEvaluator(celEnv().build());
        return new ProtoValidator(field, message, sources);
    }

    /**
     * A CEL environment with {@code this} bound and the format standard-library functions
     * (isHostname/isEmail/isIp/isIpPrefix/isUri/isUriRef/isHostAndPort/isNan/isInf) registered.
     */
    private static CelEnvironmentFactory celEnv() {
        return CelEnvironmentFactory.builder()
                .addVar("this")
                .addFunctions(ValidationCelFunctions.declarations(), ValidationCelFunctions.bindings());
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
        CelEvaluator field = new CelEvaluator(celEnv().build());
        CelEvaluator message = new CelEvaluator(celEnv().addMessageType(descriptor).build());
        return new ProtoValidator(field, message, sources);
    }

    public ValidationResult validate(Message message) {
        Objects.requireNonNull(message, "message");
        List<ValidationResult.Violation> violations = new ArrayList<>();
        Descriptor descriptor = message.getDescriptorForType();
        for (FieldDescriptor field : descriptor.getFields()) {
            validateField(message, field, field.getName(), violations);
        }
        validateMessageRules(message, descriptor, "", violations);
        return violations.isEmpty()
                ? ValidationResult.ok()
                : ValidationResult.failed(violations);
    }

    private List<FieldConstraints> fieldConstraints(FieldDescriptor field) {
        List<FieldConstraints> collected = new ArrayList<>(sources.size());
        for (ValidationRuleSource source : sources) {
            source.fieldConstraints(field).ifPresent(collected::add);
        }
        return collected;
    }

    private void validateField(
            Message message,
            FieldDescriptor field,
            String path,
            List<ValidationResult.Violation> violations) {
        List<FieldConstraints> constraints = fieldConstraints(field);
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
                || isMessageOneofMember(field);
        if (skipWhenEmpty && !hasField) {
            return;
        }

        if (field.isMapField()) {
            validateMap(message, field, constraints, path, violations);
            return;
        }
        if (field.isRepeated()) {
            validateRepeated(message, field, constraints, path, violations);
            return;
        }

        Object value = message.getField(field);
        for (FieldConstraints c : constraints) {
            applyFieldConstraints(field, c, value, path, violations);
            runFieldCel(c, value, path, violations);
        }
        if (value instanceof Message nested) {
            validateChildren(nested, path, violations);
        }
    }

    private void validateChildren(
            Message nested, String path, List<ValidationResult.Violation> violations) {
        for (FieldDescriptor child : nested.getDescriptorForType().getFields()) {
            validateField(nested, child, path + "." + child.getName(), violations);
        }
        validateMessageRules(nested, nested.getDescriptorForType(), path, violations);
    }

    private void validateRepeated(
            Message message,
            FieldDescriptor field,
            List<FieldConstraints> constraints,
            String path,
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
                    if (!seen.add(message.getRepeatedField(field, i))) {
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
            for (FieldConstraints c : constraints) {
                c.repeated().flatMap(RepeatedConstraints::items).ifPresent(items -> {
                    if (!skipValue(items, element, field)) {
                        applyFieldConstraints(field, items, element, elementPath, violations);
                        runFieldCel(items, element, elementPath, violations);
                    }
                });
                runFieldCel(c, element, elementPath, violations);
            }
            if (element instanceof Message nested) {
                validateChildren(nested, elementPath, violations);
            }
        }
    }

    private void validateMap(
            Message message,
            FieldDescriptor field,
            List<FieldConstraints> constraints,
            String path,
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
            for (FieldConstraints c : constraints) {
                MapConstraints m = c.map().orElse(null);
                if (m != null) {
                    m.keys().ifPresent(k -> {
                        if (!skipValue(k, key, keyField)) {
                            applyFieldConstraints(keyField, k, key, keyPath, violations);
                            runFieldCel(k, key, keyPath, violations);
                        }
                    });
                    m.values().ifPresent(v -> {
                        if (!skipValue(v, value, valueField)) {
                            applyFieldConstraints(valueField, v, value, entryPath, violations);
                            runFieldCel(v, value, entryPath, violations);
                        }
                    });
                }
                runFieldCel(c, value, entryPath, violations);
            }
            if (value instanceof Message nested) {
                validateChildren(nested, entryPath, violations);
            }
        }
    }

    private static String subscript(Object key) {
        return key instanceof String s ? "[\"" + s + "\"]" : "[" + key + "]";
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
        if (rules.pattern().isPresent()) {
            try {
                if (!Pattern.compile(rules.pattern().get()).matcher(value).find()) {
                    violations.add(violation(path, "string.pattern", "value does not match pattern"));
                }
            } catch (PatternSyntaxException e) {
                violations.add(violation(path, "string.pattern",
                        "invalid pattern: " + e.getMessage()));
            }
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

    private static int compare(long a, long b, boolean unsigned) {
        return unsigned ? Long.compareUnsigned(a, b) : Long.compare(a, b);
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
        if (!rules.in().isEmpty() && !rules.in().contains(value)) {
            violations.add(violation(path, prefix + ".in", "must be one of the allowed values"));
        }
        if (!rules.notIn().isEmpty() && rules.notIn().contains(value)) {
            violations.add(violation(path, prefix + ".not_in", "must not be one of the forbidden values"));
        }
        if (rules.finite() && !Double.isFinite(value)) {
            violations.add(violation(path, prefix + ".finite", "must be finite"));
        }
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
        applyRange("duration", path, value,
                rules.gt().orElse(null), rules.gte().orElse(null),
                rules.lt().orElse(null), rules.lte().orElse(null),
                Comparator.naturalOrder(), Duration::toString, violations);
    }

    private static Instant toInstant(Message timestamp) {
        Descriptor d = timestamp.getDescriptorForType();
        long seconds = (Long) timestamp.getField(d.findFieldByName("seconds"));
        int nanos = (Integer) timestamp.getField(d.findFieldByName("nanos"));
        return Instant.ofEpochSecond(seconds, nanos);
    }

    private static Duration toJavaDuration(Message duration) {
        Descriptor d = duration.getDescriptorForType();
        long seconds = (Long) duration.getField(d.findFieldByName("seconds"));
        int nanos = (Integer) duration.getField(d.findFieldByName("nanos"));
        return Duration.ofSeconds(seconds, nanos);
    }

    private void runFieldCel(
            FieldConstraints constraints, Object value, String path,
            List<ValidationResult.Violation> violations) {
        if (constraints.cel().isEmpty()) {
            return;
        }
        Object celValue = value instanceof EnumValueDescriptor evd ? (long) evd.getNumber() : value;
        for (CelConstraint rule : constraints.cel()) {
            evalCel(fieldCel, rule, celValue, path, violations);
        }
    }

    private void validateMessageRules(
            Message message,
            Descriptor descriptor,
            String path,
            List<ValidationResult.Violation> violations) {
        for (ValidationRuleSource source : sources) {
            MessageConstraints constraints = source.messageConstraints(descriptor).orElse(null);
            if (constraints == null || constraints.isEmpty()) {
                continue;
            }
            String msgPath = path.isEmpty() ? descriptor.getName() : path;
            for (CelConstraint rule : constraints.cel()) {
                evalCel(messageCel, rule, message, msgPath, violations);
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
                .orElse(null);
        if (oneof != null && !message.hasOneof(oneof)) {
            String oneofPath = path.isEmpty() ? oneofName : path + "." + oneofName;
            violations.add(new ValidationResult.Violation(
                    oneofPath, "required", "exactly one field is required in oneof"));
        }
    }

    /**
     * A message-level {@code oneof} rule: at most one member may be populated, and when
     * {@code required} at least one must be. Both failures report {@code message.oneof} on the
     * message path with the member list spelled out, matching protovalidate's wording.
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
            if (fd != null && isPresent(message, fd)) {
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
            List<ValidationResult.Violation> violations) {
        if (rule.expression().isBlank()) {
            return;
        }
        String id = rule.id().isBlank() ? "cel" : rule.id();
        try {
            Object result = evaluator.evaluateValue(rule.expression(), Map.of("this", thisValue));
            if (result instanceof Boolean ok) {
                if (!ok) {
                    String msg = rule.message().isBlank() ? "CEL rule failed" : rule.message();
                    violations.add(new ValidationResult.Violation(path, id, msg));
                }
            } else if (result instanceof String text) {
                if (!text.isEmpty()) {
                    violations.add(new ValidationResult.Violation(path, id, text));
                }
            } else {
                violations.add(new ValidationResult.Violation(
                        path, id, "CEL rule must return bool or string"));
            }
        } catch (CelEvaluationException e) {
            violations.add(new ValidationResult.Violation(
                    path, id, "CEL evaluation error: " + e.getMessage()));
        }
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
    /** True when {@code field} is named by a message-level oneof rule on its containing message. */
    private boolean isMessageOneofMember(FieldDescriptor field) {
        Descriptor type = field.getContainingType();
        for (ValidationRuleSource source : sources) {
            MessageConstraints mc = source.messageConstraints(type).orElse(null);
            if (mc == null) {
                continue;
            }
            for (MessageConstraints.Oneof oneof : mc.oneofs()) {
                if (oneof.fields().contains(field.getName())) {
                    return true;
                }
            }
        }
        return false;
    }

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
            case FLOAT, DOUBLE -> ((Number) value).doubleValue() != 0.0d;
            default -> true;
        };
    }

    private static ValidationResult.Violation violation(String path, String id, String message) {
        return new ValidationResult.Violation(path, id, message);
    }
}
