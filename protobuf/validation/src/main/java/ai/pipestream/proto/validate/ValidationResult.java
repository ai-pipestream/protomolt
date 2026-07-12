package ai.pipestream.proto.validate;

import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.Message;

import java.util.List;
import java.util.Objects;

/** Result of validating a protobuf message against descriptor options. */
public record ValidationResult(boolean valid, List<Violation> violations) {

    public ValidationResult {
        Objects.requireNonNull(violations, "violations");
        violations = List.copyOf(violations);
    }

    public static ValidationResult ok() {
        return new ValidationResult(true, List.of());
    }

    public static ValidationResult failed(List<Violation> violations) {
        return new ValidationResult(false, violations);
    }

    public void throwIfInvalid() {
        if (!valid) {
            throw new ValidationException(this);
        }
    }

    /**
     * Single constraint failure. {@code rulePath} is an optional dotted {@code FieldRules} path the
     * rule lives at when it cannot be derived from {@code ruleId} alone (notably custom CEL rules,
     * whose path is {@code cel[N]}); empty means "derive from the rule id".
     */
    public record Violation(String path, String ruleId, String message, String rulePath) {
        public Violation {
            Objects.requireNonNull(path, "path");
            Objects.requireNonNull(ruleId, "ruleId");
            Objects.requireNonNull(message, "message");
            rulePath = rulePath == null ? "" : rulePath;
        }

        public Violation(String path, String ruleId, String message) {
            this(path, ruleId, message, "");
        }
    }

    /** Thrown when {@link #throwIfInvalid()} fails or indexing chains validation. */
    public static final class ValidationException extends RuntimeException {
        private final ValidationResult result;

        public ValidationException(ValidationResult result) {
            super(format(result));
            this.result = Objects.requireNonNull(result, "result");
        }

        public ValidationResult result() {
            return result;
        }

        private static String format(ValidationResult result) {
            StringBuilder sb = new StringBuilder("Message validation failed:");
            for (Violation v : result.violations()) {
                sb.append(" [").append(v.path()).append("] ")
                        .append(v.ruleId()).append(": ").append(v.message());
            }
            return sb.toString();
        }
    }

    /** Convenience for callers that already hold a message. */
    public static ValidationResult validate(Message message) {
        return ProtoValidator.create().validate(message);
    }

    public static void registerExtensions(ExtensionRegistry registry) {
        ValidateProto.registerAllExtensions(Objects.requireNonNull(registry, "registry"));
    }
}
