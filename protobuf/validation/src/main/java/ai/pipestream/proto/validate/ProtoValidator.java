package ai.pipestream.proto.validate;

import ai.pipestream.proto.cel.CelEnvironmentFactory;
import ai.pipestream.proto.cel.CelEvaluationException;
import ai.pipestream.proto.cel.CelEvaluator;
import ai.pipestream.proto.validate.model.BoolConstraints;
import ai.pipestream.proto.validate.model.BytesConstraints;
import ai.pipestream.proto.validate.model.CelConstraint;
import ai.pipestream.proto.validate.model.DurationConstraints;
import ai.pipestream.proto.validate.model.EnumConstraints;
import ai.pipestream.proto.validate.model.FieldConstraints;
import ai.pipestream.proto.validate.model.FloatingConstraints;
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
        CelEvaluator field = new CelEvaluator(CelEnvironmentFactory.builder()
                .addVar("this")
                .build());
        CelEvaluator message = new CelEvaluator(CelEnvironmentFactory.builder()
                .addVar("this")
                .build());
        return new ProtoValidator(field, message, sources);
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
        CelEvaluator field = new CelEvaluator(CelEnvironmentFactory.builder()
                .addVar("this")
                .build());
        CelEvaluator message = new CelEvaluator(CelEnvironmentFactory.builder()
                .addMessageType(descriptor)
                .addVar("this")
                .build());
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
        boolean present = isPresent(message, field);

        boolean requiredUnset = constraints.stream().anyMatch(FieldConstraints::required) && !present;
        if (requiredUnset) {
            violations.add(new ValidationResult.Violation(path, "required", "field is required"));
            return;
        }

        // Collection rules apply even to empty collections (min_items / min_pairs).
        if (field.isMapField()) {
            validateMap(message, field, constraints, path, violations);
            return;
        }
        if (field.isRepeated()) {
            validateRepeated(message, field, constraints, path, violations);
            return;
        }
        if (!present) {
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
                        violations.add(violation(path + "[" + i + "]", "repeated.unique",
                                "repeated values must be unique"));
                    }
                }
            }
        }
        for (int i = 0; i < count; i++) {
            Object element = message.getRepeatedField(field, i);
            String elementPath = path + "[" + i + "]";
            for (FieldConstraints c : constraints) {
                c.repeated().flatMap(RepeatedConstraints::items).ifPresent(items -> {
                    applyFieldConstraints(field, items, element, elementPath, violations);
                    runFieldCel(items, element, elementPath, violations);
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
                        applyFieldConstraints(keyField, k, key, keyPath, violations);
                        runFieldCel(k, key, keyPath, violations);
                    });
                    m.values().ifPresent(v -> {
                        applyFieldConstraints(valueField, v, value, entryPath, violations);
                        runFieldCel(v, value, entryPath, violations);
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
                }
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
            if (!format.matches(value)) {
                violations.add(violation(path, format.ruleId(), format.defaultMessage()));
            }
        }
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
        if (rules.gt().isPresent() && compare(value, rules.gt().getAsLong(), unsigned) <= 0) {
            violations.add(violation(path, prefix + ".gt",
                    "must be > " + fmt(rules.gt().getAsLong(), unsigned)));
        }
        if (rules.gte().isPresent() && compare(value, rules.gte().getAsLong(), unsigned) < 0) {
            violations.add(violation(path, prefix + ".gte",
                    "must be >= " + fmt(rules.gte().getAsLong(), unsigned)));
        }
        if (rules.lt().isPresent() && compare(value, rules.lt().getAsLong(), unsigned) >= 0) {
            violations.add(violation(path, prefix + ".lt",
                    "must be < " + fmt(rules.lt().getAsLong(), unsigned)));
        }
        if (rules.lte().isPresent() && compare(value, rules.lte().getAsLong(), unsigned) > 0) {
            violations.add(violation(path, prefix + ".lte",
                    "must be <= " + fmt(rules.lte().getAsLong(), unsigned)));
        }
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

    private static void applyFloating(
            FloatingConstraints rules, double value, String path,
            List<ValidationResult.Violation> violations) {
        String prefix = rules.ruleIdPrefix();
        if (rules.constant().isPresent() && value != rules.constant().getAsDouble()) {
            violations.add(violation(path, prefix + ".const",
                    "must equal " + rules.constant().getAsDouble()));
        }
        if (rules.gt().isPresent() && !(value > rules.gt().getAsDouble())) {
            violations.add(violation(path, prefix + ".gt", "must be > " + rules.gt().getAsDouble()));
        }
        if (rules.gte().isPresent() && !(value >= rules.gte().getAsDouble())) {
            violations.add(violation(path, prefix + ".gte", "must be >= " + rules.gte().getAsDouble()));
        }
        if (rules.lt().isPresent() && !(value < rules.lt().getAsDouble())) {
            violations.add(violation(path, prefix + ".lt", "must be < " + rules.lt().getAsDouble()));
        }
        if (rules.lte().isPresent() && !(value <= rules.lte().getAsDouble())) {
            violations.add(violation(path, prefix + ".lte", "must be <= " + rules.lte().getAsDouble()));
        }
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
        if (rules.gt().isPresent() && value.compareTo(rules.gt().get()) <= 0) {
            violations.add(violation(path, "timestamp.gt", "must be after " + rules.gt().get()));
        }
        if (rules.gte().isPresent() && value.compareTo(rules.gte().get()) < 0) {
            violations.add(violation(path, "timestamp.gte", "must be at or after " + rules.gte().get()));
        }
        if (rules.lt().isPresent() && value.compareTo(rules.lt().get()) >= 0) {
            violations.add(violation(path, "timestamp.lt", "must be before " + rules.lt().get()));
        }
        if (rules.lte().isPresent() && value.compareTo(rules.lte().get()) > 0) {
            violations.add(violation(path, "timestamp.lte", "must be at or before " + rules.lte().get()));
        }
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
        if (rules.gt().isPresent() && value.compareTo(rules.gt().get()) <= 0) {
            violations.add(violation(path, "duration.gt", "must be > " + rules.gt().get()));
        }
        if (rules.gte().isPresent() && value.compareTo(rules.gte().get()) < 0) {
            violations.add(violation(path, "duration.gte", "must be >= " + rules.gte().get()));
        }
        if (rules.lt().isPresent() && value.compareTo(rules.lt().get()) >= 0) {
            violations.add(violation(path, "duration.lt", "must be < " + rules.lt().get()));
        }
        if (rules.lte().isPresent() && value.compareTo(rules.lte().get()) > 0) {
            violations.add(violation(path, "duration.lte", "must be <= " + rules.lte().get()));
        }
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
