package ai.pipestream.proto.validate.model;

import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * Neutral constraints for map fields. Violation rule ids are the fixed
 * {@code map.*} ids. {@code keys}/{@code values} rules are applied per entry;
 * key violations report at {@code path["key"]#key}, value violations at
 * {@code path["key"]}. Pair-count rules also apply when the map is empty.
 */
public record MapConstraints(
        OptionalLong minPairs,
        OptionalLong maxPairs,
        Optional<FieldConstraints> keys,
        Optional<FieldConstraints> values) {

    public MapConstraints {
        Objects.requireNonNull(minPairs, "minPairs");
        Objects.requireNonNull(maxPairs, "maxPairs");
        Objects.requireNonNull(keys, "keys");
        Objects.requireNonNull(values, "values");
    }

    public boolean isEmpty() {
        return minPairs.isEmpty() && maxPairs.isEmpty() && keys.isEmpty() && values.isEmpty();
    }
}
