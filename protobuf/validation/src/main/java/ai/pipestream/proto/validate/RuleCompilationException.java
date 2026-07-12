package ai.pipestream.proto.validate;

/**
 * Signals that a message's validation rules are themselves invalid — the rule set cannot be
 * compiled, independent of any particular message value. Examples: a numeric rule applied to a
 * field of the wrong type, or a message {@code oneof} rule naming a field that does not exist,
 * is duplicated, or lists no fields at all.
 *
 * <p>This is distinct from a validation failure: the rules are malformed, so no meaningful
 * validation can run. The conformance executor maps it to a {@code compilation_error} result.
 */
public class RuleCompilationException extends RuntimeException {

    public RuleCompilationException(String message) {
        super(message);
    }
}
