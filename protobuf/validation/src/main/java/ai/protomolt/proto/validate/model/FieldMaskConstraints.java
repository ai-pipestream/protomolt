package ai.protomolt.proto.validate.model;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Neutral constraints for {@code google.protobuf.FieldMask} fields. The mask is compared in its
 * canonical comma-joined path form ({@code "a,b.c"}). Violation rule ids are {@code field_mask.const}
 * / {@code field_mask.in} / {@code field_mask.not_in}.
 */
public record FieldMaskConstraints(
        Optional<String> constant, List<String> in, List<String> notIn) {

    public FieldMaskConstraints {
        Objects.requireNonNull(constant, "constant");
        in = List.copyOf(Objects.requireNonNull(in, "in"));
        notIn = List.copyOf(Objects.requireNonNull(notIn, "notIn"));
    }

    public boolean isEmpty() {
        return constant.isEmpty() && in.isEmpty() && notIn.isEmpty();
    }
}
