package ai.protomolt.proto.validate;

/**
 * Signals that a rule failed at evaluation time — the rules compiled, but applying one to a
 * particular value errored (a CEL runtime error, bytes that cannot be decoded for a pattern
 * match, an out-of-range timestamp, runaway nesting). This is neither a validation failure nor
 * a malformed rule set; it is the typed counterpart of {@link RuleCompilationException} for
 * runtime failures, so callers never lose collected violations to a bare unchecked exception.
 *
 * <p>The conformance executor maps it to a {@code runtime_error} result.
 */
public class RuleEvaluationException extends RuntimeException {

    private final String ruleId;

    public RuleEvaluationException(String message) {
        this("", message, null);
    }

    public RuleEvaluationException(String message, Throwable cause) {
        this("", message, cause);
    }

    public RuleEvaluationException(String ruleId, String message, Throwable cause) {
        super(message, cause);
        this.ruleId = ruleId == null ? "" : ruleId;
    }

    /** The id of the rule that failed, or empty when the failure is not tied to a single rule. */
    public String ruleId() {
        return ruleId;
    }
}
