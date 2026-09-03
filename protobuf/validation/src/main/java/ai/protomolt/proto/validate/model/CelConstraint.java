package ai.protomolt.proto.validate.model;

import java.util.Objects;

/**
 * A single CEL predicate attached to a field or message, in the neutral rule model.
 *
 * <p>The {@code expression} returns {@code bool} (true = ok) or {@code string}
 * (non-empty = failure message). {@code id} is a stable identifier used as the
 * violation rule id (blank ids are normalised to {@code "cel"} at evaluation time),
 * and {@code message} is the human-readable failure text used when the expression
 * returns a boolean.
 *
 * <p>{@code rulePath} optionally overrides the violation rule path; when blank the
 * path is derived from {@code celField} as {@code <celField>[N]}. {@code ruleValue}
 * optionally carries a value bound as the CEL variable {@code rule} during
 * evaluation — the contract predefined (extension-declared) rules are written
 * against; {@code null} leaves {@code rule} unbound.
 */
public record CelConstraint(
        String id, String expression, String message, String celField,
        String rulePath, Object ruleValue) {

    public CelConstraint {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(expression, "expression");
        Objects.requireNonNull(message, "message");
        celField = (celField == null || celField.isBlank()) ? "cel" : celField;
        rulePath = rulePath == null ? "" : rulePath;
    }

    public CelConstraint(String id, String expression, String message, String celField) {
        this(id, expression, message, celField, "", null);
    }

    /** The rule lives on the {@code cel} repeated field by default. */
    public CelConstraint(String id, String expression, String message) {
        this(id, expression, message, "cel", "", null);
    }
}
