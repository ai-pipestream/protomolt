package ai.protomolt.proto.validate.model;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Neutral constraints for {@code google.protobuf.Duration} fields, expressed in
 * {@code java.time}. Violation rule ids are the fixed {@code duration.*} ids.
 */
public record DurationConstraints(
        Optional<Duration> constant,
        Optional<Duration> gt,
        Optional<Duration> gte,
        Optional<Duration> lt,
        Optional<Duration> lte,
        List<Duration> in,
        List<Duration> notIn) {

    public DurationConstraints {
        Objects.requireNonNull(constant, "constant");
        Objects.requireNonNull(gt, "gt");
        Objects.requireNonNull(gte, "gte");
        Objects.requireNonNull(lt, "lt");
        Objects.requireNonNull(lte, "lte");
        in = List.copyOf(Objects.requireNonNull(in, "in"));
        notIn = List.copyOf(Objects.requireNonNull(notIn, "notIn"));
    }

    public boolean isEmpty() {
        return constant.isEmpty() && gt.isEmpty() && gte.isEmpty() && lt.isEmpty() && lte.isEmpty()
                && in.isEmpty() && notIn.isEmpty();
    }
}
