package ai.pipestream.proto.validate.model;

import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;

/**
 * Neutral constraints for enum fields, expressed over the enum's numeric values.
 * Violation rule ids are the fixed {@code enum.*} ids ({@code enum.const},
 * {@code enum.defined_only}, …). {@code definedOnly} demands the number is declared
 * in the enum (proto3 enums are open and can carry unknown numbers).
 */
public record EnumConstraints(
        OptionalInt constant,
        boolean definedOnly,
        List<Integer> in,
        List<Integer> notIn) {

    public EnumConstraints {
        Objects.requireNonNull(constant, "constant");
        in = List.copyOf(Objects.requireNonNull(in, "in"));
        notIn = List.copyOf(Objects.requireNonNull(notIn, "notIn"));
    }

    public boolean isEmpty() {
        return constant.isEmpty() && !definedOnly && in.isEmpty() && notIn.isEmpty();
    }
}
