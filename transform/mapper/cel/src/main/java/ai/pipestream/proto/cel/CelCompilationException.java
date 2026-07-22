package ai.pipestream.proto.cel;

/**
 * Raised specifically when a CEL expression fails to <em>compile</em> (a syntax or type error),
 * as opposed to failing at evaluation time. Callers can distinguish this from a runtime failure to
 * report it as a rule compilation error rather than a runtime error.
 */
public class CelCompilationException extends CelEvaluationException {
    public CelCompilationException(String message) {
        super(message);
    }

    public CelCompilationException(String message, Throwable cause) {
        super(message, cause);
    }
}
