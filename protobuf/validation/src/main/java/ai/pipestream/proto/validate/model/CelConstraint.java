package ai.pipestream.proto.validate.model;

import java.util.Objects;

/**
 * A single CEL predicate attached to a field or message, in the neutral rule model.
 *
 * <p>The {@code expression} returns {@code bool} (true = ok) or {@code string}
 * (non-empty = failure message). {@code id} is a stable identifier used as the
 * violation rule id (blank ids are normalised to {@code "cel"} at evaluation time),
 * and {@code message} is the human-readable failure text used when the expression
 * returns a boolean.
 */
public record CelConstraint(String id, String expression, String message, String celField) {

    public CelConstraint {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(expression, "expression");
        Objects.requireNonNull(message, "message");
        celField = (celField == null || celField.isBlank()) ? "cel" : celField;
    }

    /** The rule lives on the {@code cel} repeated field by default. */
    public CelConstraint(String id, String expression, String message) {
        this(id, expression, message, "cel");
    }
}
