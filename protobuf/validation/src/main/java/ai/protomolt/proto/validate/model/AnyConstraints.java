package ai.protomolt.proto.validate.model;

import java.util.List;
import java.util.Objects;

/**
 * Neutral constraints for {@code google.protobuf.Any} fields: the type URL must be one of
 * {@link #in} (when non-empty) and none of {@link #notIn}. Violation rule ids are
 * {@code any.in} / {@code any.not_in}.
 */
public record AnyConstraints(List<String> in, List<String> notIn) {

    public AnyConstraints {
        in = List.copyOf(Objects.requireNonNull(in, "in"));
        notIn = List.copyOf(Objects.requireNonNull(notIn, "notIn"));
    }

    public boolean isEmpty() {
        return in.isEmpty() && notIn.isEmpty();
    }
}
