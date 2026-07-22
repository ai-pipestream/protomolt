package ai.pipestream.proto.cel;

/** Raised when a CEL expression cannot be compiled or evaluated. */
public class CelEvaluationException extends RuntimeException {
    public CelEvaluationException(String message) {
        super(message);
    }

    public CelEvaluationException(String message, Throwable cause) {
        super(message, cause);
    }
}
