package ai.pipestream.proto.validate.model;

import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * Neutral constraints for repeated (non-map) fields. Violation rule ids are the
 * fixed {@code repeated.*} ids. {@code items} rules are applied to every element
 * (the element type selects which rule set inside applies; nested repeated/map
 * rules are ignored). Collection-size rules also apply when the field is empty.
 */
public record RepeatedConstraints(
        OptionalLong minItems,
        OptionalLong maxItems,
        boolean unique,
        Optional<FieldConstraints> items) {

    public RepeatedConstraints {
        Objects.requireNonNull(minItems, "minItems");
        Objects.requireNonNull(maxItems, "maxItems");
        Objects.requireNonNull(items, "items");
    }

    public boolean isEmpty() {
        return minItems.isEmpty() && maxItems.isEmpty() && !unique && items.isEmpty();
    }
}
