package ai.pipestream.proto.validate.model;

import java.util.Objects;
import java.util.Optional;

/**
 * Neutral constraints for bool fields. The violation rule id is {@code bool.const}.
 *
 * <p>Note: under proto3 implicit presence a {@code false} value is treated as absent,
 * so {@code const: false} is only enforceable on fields with explicit presence
 * ({@code optional bool}).
 */
public record BoolConstraints(Optional<Boolean> constant) {

    public BoolConstraints {
        Objects.requireNonNull(constant, "constant");
    }

    public boolean isEmpty() {
        return constant.isEmpty();
    }
}
