package ai.protomolt.proto.validate;

/**
 * Builds the {@link ValidationResult.Violation} values the rule checks report. The three-argument
 * form covers every standard rule; only CEL rules carry the fourth {@code rulePath} argument, so
 * they build the record directly.
 */
final class Violations {

    private Violations() {
    }

    static ValidationResult.Violation violation(String path, String id, String message) {
        return new ValidationResult.Violation(path, id, message);
    }
}
