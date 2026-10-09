package ai.protomolt.proto.validate;

import ai.protomolt.proto.validate.model.AnyConstraints;
import ai.protomolt.proto.validate.model.BoolConstraints;
import ai.protomolt.proto.validate.model.BytesConstraints;
import ai.protomolt.proto.validate.model.BytesFormat;
import ai.protomolt.proto.validate.model.DurationConstraints;
import ai.protomolt.proto.validate.model.EnumConstraints;
import ai.protomolt.proto.validate.model.FieldConstraints;
import ai.protomolt.proto.validate.model.FieldMaskConstraints;
import ai.protomolt.proto.validate.model.FloatingConstraints;
import ai.protomolt.proto.validate.model.IntegralConstraints;
import ai.protomolt.proto.validate.model.StringConstraints;
import ai.protomolt.proto.validate.model.StringFormat;
import ai.protomolt.proto.validate.model.TimestampConstraints;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;

import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import static ai.protomolt.proto.validate.Violations.violation;
import static ai.protomolt.proto.validate.WellKnownValues.DURATION_TYPE;
import static ai.protomolt.proto.validate.WellKnownValues.TIMESTAMP_TYPE;
import static ai.protomolt.proto.validate.WellKnownValues.WRAPPER_TYPES;
import static ai.protomolt.proto.validate.WellKnownValues.toInstant;
import static ai.protomolt.proto.validate.WellKnownValues.toJavaDuration;

/**
 * The standard rule checks: given one already-present value and the constraints declared on it,
 * append a violation per rule the value fails. One entry point, {@link #applyFieldConstraints},
 * dispatches on the field's Java type to the family that governs it.
 *
 * <p>These checks see a single value and nothing else — no descriptor walk, no nesting, no CEL.
 * {@link ProtoValidator} owns the traversal that decides which values reach them and at which
 * path. Collection rules ({@code repeated.min_items}, {@code map.max_pairs}) therefore stay with
 * the traversal: they govern a container rather than a value.</p>
 *
 * <p>Range semantics match protovalidate: a lone bound fires its own {@code gt/gte/lt/lte} rule,
 * two bounds collapse into one combined rule, and reversed bounds mean the valid region sits
 * outside the range.</p>
 */
final class ValueChecks {

    private static final int MAX_CACHED_PATTERNS = 512;

    /** Compiled regex patterns shared across validators, keyed by the pattern source. */
    private static final Map<String, Pattern> PATTERNS = new ConcurrentHashMap<>();

    private ValueChecks() {
    }

    static void applyFieldConstraints(
            FieldDescriptor field,
            FieldConstraints constraints,
            Object value,
            String path,
            List<ValidationResult.Violation> violations, Instant evaluatedAt) {
        switch (field.getJavaType()) {
            case STRING, INT, LONG, FLOAT, DOUBLE, BOOLEAN, BYTE_STRING ->
                    applyScalar(constraints, field.getJavaType(), value, path, violations);
            case ENUM -> constraints.enumeration()
                    .ifPresent(e -> applyEnum(e, (EnumValueDescriptor) value, path, violations));
            case MESSAGE -> {
                String type = field.getMessageType().getFullName();
                switch (type) {
                    case TIMESTAMP_TYPE -> constraints.timestamp().ifPresent(t ->
                            applyTimestamp(t, toInstant((Message) value), path, violations, evaluatedAt));
                    case DURATION_TYPE -> constraints.duration().ifPresent(d ->
                            applyDuration(d, toJavaDuration((Message) value), path, violations));
                    case "google.protobuf.Any" -> constraints.any()
                            .ifPresent(a -> applyAny(a, (Message) value, path, violations));
                    case "google.protobuf.FieldMask" -> constraints.fieldMask().ifPresent(fm ->
                            applyFieldMask(fm, (Message) value, path, violations));
                    default -> {
                        // Well-known wrapper types (Int32Value, StringValue, …) apply their scalar
                        // rules to the wrapped value; the field is present (message presence) so
                        // this only runs when the wrapper is set.
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
    }

    /** The compiled form of {@code pattern}; an uncompilable pattern is a schema error. */
    static Pattern compiledPattern(String pattern) {
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

    // ---- well-known message families ----

    /** {@code google.protobuf.Any}: its type URL must be allowed by {@code in}/{@code not_in}. */
    private static void applyAny(
            AnyConstraints rules, Message any, String path,
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
            FieldMaskConstraints rules, Message mask, String path,
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

    // ---- scalars ----

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
            long bytes = value.getBytes(StandardCharsets.UTF_8).length;
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

    private static Long boxed(OptionalLong o) {
        return o.isPresent() ? o.getAsLong() : null;
    }

    private static Double boxed(OptionalDouble o) {
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
            Comparator<T> order, Function<T, String> fmt,
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
            StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(value.asReadOnlyByteBuffer());
            return true;
        } catch (CharacterCodingException e) {
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

    // ---- temporal ----

    private static void applyTimestamp(
            TimestampConstraints rules, Instant value, String path,
            List<ValidationResult.Violation> violations, Instant now) {
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
}
