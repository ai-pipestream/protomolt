package ai.pipestream.proto.validate.conformance;

import ai.pipestream.proto.validate.ProtoValidator;
import ai.pipestream.proto.validate.RuleCompilationException;
import ai.pipestream.proto.validate.ValidationResult;
import build.buf.validate.FieldPath;
import build.buf.validate.FieldPathElement;
import build.buf.validate.FieldRules;
import build.buf.validate.Violation;
import build.buf.validate.Violations;
import buf.validate.conformance.harness.Harness.TestResult;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;

/**
 * Runs {@link ProtoValidator} over a single conformance case and expresses the outcome as the
 * conformance {@link TestResult}.
 *
 * <p>This is the shared core of both phases: the in-build JUnit harness and the stdin/stdout
 * executor drive the exact same {@link #run(Message)} so a green build and the authoritative buf
 * pass-rate exercise identical code.
 *
 * <p>Our {@link ValidationResult.Violation} carries a string field path and a rule id; both are
 * expanded into structured {@link build.buf.validate.FieldPath}s — the field path against the
 * message descriptor, the rule path against {@link FieldRules} — because the conformance runner
 * compares them with {@code proto.Equal}. The {@code #key} suffix our validator appends for
 * map-key violations maps onto the {@code for_key} flag.
 */
public final class ConformanceRunner {

    private static final String KEY_SUFFIX = "#key";

    private final ProtoValidator validator;
    private final PredefinedRules predefined;

    /** Uses the default source chain, which includes the buf.validate dialect via ServiceLoader. */
    public ConformanceRunner() {
        this(ProtoValidator.create());
    }

    public ConformanceRunner(ProtoValidator validator) {
        this(validator, null);
    }

    ConformanceRunner(ProtoValidator validator, PredefinedRules predefined) {
        this.validator = validator;
        this.predefined = predefined;
    }

    /** Validates {@code message} and maps the outcome to a conformance {@link TestResult}. */
    public TestResult run(Message message) {
        try {
            ValidationResult result = validator.validate(message);
            Violations.Builder violations = Violations.newBuilder();
            Descriptor root = message.getDescriptorForType();
            for (ValidationResult.Violation v : result.violations()) {
                violations.addViolations(toViolation(root, v));
            }
            if (predefined != null) {
                violations.addAllViolations(predefined.evaluate(message));
            }
            if (violations.getViolationsCount() == 0) {
                return TestResult.newBuilder().setSuccess(true).build();
            }
            return TestResult.newBuilder().setValidationError(violations.build()).build();
        } catch (RuleCompilationException e) {
            return TestResult.newBuilder().setCompilationError(String.valueOf(e.getMessage())).build();
        } catch (RuntimeException e) {
            return TestResult.newBuilder().setRuntimeError(String.valueOf(e.getMessage())).build();
        }
    }

    private static Violation toViolation(Descriptor root, ValidationResult.Violation v) {
        Violation.Builder b = Violation.newBuilder()
                .setRuleId(v.ruleId())
                .setMessage(v.message());

        String path = v.path();
        boolean forKey = path.endsWith(KEY_SUFFIX);
        if (forKey) {
            path = path.substring(0, path.length() - KEY_SUFFIX.length());
            b.setForKey(true);
        }
        FieldPath fieldPath = null;
        if (!path.isEmpty()) {
            try {
                fieldPath = FieldPaths.unmarshal(root, path);
                b.setField(fieldPath);
            } catch (RuntimeException ignored) {
                // Leave field unset when the path cannot be resolved; the case simply won't match.
            }
        }
        // An oneof-level required rule targets the oneof name itself (a bare field_name element with
        // no field number). It comes from OneofRules, not FieldRules, so it carries no rule path.
        if (!targetsOneofName(fieldPath)) {
            try {
                // Container-level rules (repeated.*/map.*) live on the container even when their field
                // path is subscripted; only element-type rules take a repeated.items/map.* prefix.
                String ruleId = v.ruleId();
                boolean containerLevel = ruleId.startsWith("repeated.") || ruleId.startsWith("map.");
                String prefix = containerLevel ? "" : containerPrefix(fieldPath, forKey);
                b.setRule(FieldPaths.unmarshal(FieldRules.getDescriptor(), prefix + rulePath(ruleId)));
            } catch (RuntimeException ignored) {
                // Rule ids without a FieldRules mapping (e.g. bare "cel") leave the rule path unset.
            }
        }
        return b.build();
    }

    /**
     * True when the field path targets a oneof by name: its final element is a bare {@code field_name}
     * with no field number (a real field always has a positive number). Such violations come from
     * oneof rules rather than {@code FieldRules} and therefore carry no rule path.
     */
    private static boolean targetsOneofName(FieldPath fieldPath) {
        if (fieldPath == null || fieldPath.getElementsCount() == 0) {
            return false;
        }
        FieldPathElement last = fieldPath.getElements(fieldPath.getElementsCount() - 1);
        return last.getFieldNumber() == 0 && !last.getFieldName().isEmpty();
    }

    /**
     * The {@code FieldRules} rule-path prefix implied by validating a rule directly on a repeated
     * item or map entry: {@code repeated.items.}, {@code map.keys.}, or {@code map.values.}. Empty
     * when the violation is on the field itself (last path element carries no subscript) or the rule
     * is a container-level rule ({@code repeated.*}/{@code map.*}), which lives on the container, not
     * the element, even though its field path may be subscripted.
     */
    private static String containerPrefix(FieldPath fieldPath, boolean forKey) {
        if (fieldPath == null || fieldPath.getElementsCount() == 0) {
            return "";
        }
        FieldPathElement last = fieldPath.getElements(fieldPath.getElementsCount() - 1);
        switch (last.getSubscriptCase()) {
            case INDEX -> {
                return "repeated.items.";
            }
            case BOOL_KEY, INT_KEY, UINT_KEY, STRING_KEY -> {
                return forKey ? "map.keys." : "map.values.";
            }
            default -> {
                return "";
            }
        }
    }

    /**
     * The dotted {@code FieldRules} path a rule id points at. Most ids are themselves valid paths;
     * the combined-range ids ({@code <type>.gt_lt}, {@code …_exclusive}, etc.) are defined as
     * predefined rules on the lower-bound field, so their path is {@code <type>.gt}/{@code .gte}.
     */
    static String rulePath(String ruleId) {
        int dot = ruleId.indexOf('.');
        if (dot < 0) {
            return ruleId;
        }
        String prefix = ruleId.substring(0, dot);
        String suffix = ruleId.substring(dot + 1);
        // The well_known_regex rule reports header_name/header_value ids but lives on one field.
        if (suffix.startsWith("well_known_regex")) {
            return prefix + ".well_known_regex";
        }
        // Format rules that reject an empty value report <id>_empty but live on the <id> field.
        if (suffix.endsWith("_empty")) {
            return prefix + "." + suffix.substring(0, suffix.length() - "_empty".length());
        }
        if (suffix.endsWith("_exclusive")) {
            suffix = suffix.substring(0, suffix.length() - "_exclusive".length());
        }
        return switch (suffix) {
            case "gt_lt", "gt_lte" -> prefix + ".gt";
            case "gte_lt", "gte_lte" -> prefix + ".gte";
            default -> ruleId;
        };
    }
}

