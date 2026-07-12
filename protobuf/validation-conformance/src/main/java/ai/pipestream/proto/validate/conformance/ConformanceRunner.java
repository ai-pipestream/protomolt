package ai.pipestream.proto.validate.conformance;

import ai.pipestream.proto.validate.ProtoValidator;
import ai.pipestream.proto.validate.ValidationResult;
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

    /** Uses the default source chain, which includes the buf.validate dialect via ServiceLoader. */
    public ConformanceRunner() {
        this(ProtoValidator.create());
    }

    public ConformanceRunner(ProtoValidator validator) {
        this.validator = validator;
    }

    /** Validates {@code message} and maps the outcome to a conformance {@link TestResult}. */
    public TestResult run(Message message) {
        try {
            ValidationResult result = validator.validate(message);
            if (result.valid()) {
                return TestResult.newBuilder().setSuccess(true).build();
            }
            return TestResult.newBuilder()
                    .setValidationError(toViolations(message.getDescriptorForType(), result))
                    .build();
        } catch (RuntimeException e) {
            return TestResult.newBuilder().setRuntimeError(String.valueOf(e.getMessage())).build();
        }
    }

    private static Violations toViolations(Descriptor root, ValidationResult result) {
        Violations.Builder out = Violations.newBuilder();
        for (ValidationResult.Violation v : result.violations()) {
            out.addViolations(toViolation(root, v));
        }
        return out.build();
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
        if (!path.isEmpty()) {
            try {
                b.setField(FieldPaths.unmarshal(root, path));
            } catch (RuntimeException ignored) {
                // Leave field unset when the path cannot be resolved; the case simply won't match.
            }
        }
        try {
            b.setRule(FieldPaths.unmarshal(FieldRules.getDescriptor(), rulePath(v.ruleId())));
        } catch (RuntimeException ignored) {
            // Rule ids without a FieldRules mapping (e.g. bare "cel") leave the rule path unset.
        }
        return b.build();
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

