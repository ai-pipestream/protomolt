package ai.protomolt.proto.inference.structured;

import ai.protomolt.proto.inference.v1.StructuredAttempt;

import java.util.List;
import java.util.Objects;

/**
 * A loud structured-generation failure: an invalid request, an unknown target
 * type, an unknown or structured-output-incapable model, a provider failure,
 * or exhaustion of the attempt budget.
 *
 * <p>The exception carries every attempt recorded before the failure (empty
 * when the failure happened before the first invocation), plus the request's
 * model id and target type, so a caller can persist the provenance of a failed
 * generation exactly as it would a successful one. The coordinator never
 * substitutes a default type, a fallback model, or a guessed fix.</p>
 */
public class StructuredGenerationException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final List<StructuredAttempt> attempts;
    private final String model;
    private final String targetType;

    /**
     * Creates the exception with the failure context.
     *
     * @param message what failed and with which model or type
     * @param model the request's catalog model id
     * @param targetType the request's target message type
     * @param attempts the attempts recorded before the failure, in order
     */
    public StructuredGenerationException(String message, String model, String targetType,
            List<StructuredAttempt> attempts) {
        super(message);
        this.model = Objects.requireNonNull(model, "model");
        this.targetType = Objects.requireNonNull(targetType, "targetType");
        this.attempts = List.copyOf(Objects.requireNonNull(attempts, "attempts"));
    }

    /**
     * Creates the exception with the failure context and the underlying cause.
     *
     * @param message what failed and with which model or type
     * @param cause the provider or rendering failure underneath
     * @param model the request's catalog model id
     * @param targetType the request's target message type
     * @param attempts the attempts recorded before the failure, in order
     */
    public StructuredGenerationException(String message, Throwable cause, String model,
            String targetType, List<StructuredAttempt> attempts) {
        super(message, cause);
        this.model = Objects.requireNonNull(model, "model");
        this.targetType = Objects.requireNonNull(targetType, "targetType");
        this.attempts = List.copyOf(Objects.requireNonNull(attempts, "attempts"));
    }

    /**
     * Every attempt recorded before the failure, oldest first; empty when the
     * failure happened before the first model invocation.
     *
     * @return the immutable attempt history
     */
    public List<StructuredAttempt> getAttempts() {
        return attempts;
    }

    /**
     * The catalog model id of the failed request.
     *
     * @return the model id
     */
    public String getModel() {
        return model;
    }

    /**
     * The target message type of the failed request.
     *
     * @return the fully qualified type name
     */
    public String getTargetType() {
        return targetType;
    }
}
