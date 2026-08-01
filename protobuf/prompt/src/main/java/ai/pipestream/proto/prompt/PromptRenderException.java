package ai.pipestream.proto.prompt;

import ai.pipestream.proto.validate.ValidationResult;

import java.util.Optional;

/**
 * Thrown when a prompt render cannot proceed: an invalid request, a target/descriptor
 * mismatch, or an override the renderer was not given the means to resolve. Always
 * loud — the renderer has no defaults and never drops information silently.
 */
public final class PromptRenderException extends RuntimeException {

    private final transient ValidationResult requestViolations;

    public PromptRenderException(String message) {
        super(message);
        this.requestViolations = null;
    }

    private PromptRenderException(String message, ValidationResult requestViolations) {
        super(message);
        this.requestViolations = requestViolations;
    }

    /** The request failed its own {@code validate.v1} rules. */
    public static PromptRenderException requestInvalid(ValidationResult result) {
        StringBuilder message = new StringBuilder("RenderPromptRequest is invalid:");
        for (ValidationResult.Violation v : result.violations()) {
            message.append(" [").append(v.path()).append(": ").append(v.message()).append(']');
        }
        return new PromptRenderException(message.toString(), result);
    }

    /** The request's violations, when this exception carries them. */
    public Optional<ValidationResult> requestViolations() {
        return Optional.ofNullable(requestViolations);
    }
}
