package ai.pipestream.proto.validate.model;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/**
 * Neutral constraints for {@code google.protobuf.Duration} fields, expressed in
 * {@code java.time}. Violation rule ids are the fixed {@code duration.*} ids.
 */
public record DurationConstraints(
        Optional<Duration> gt,
        Optional<Duration> gte,
        Optional<Duration> lt,
        Optional<Duration> lte) {

    public DurationConstraints {
        Objects.requireNonNull(gt, "gt");
        Objects.requireNonNull(gte, "gte");
        Objects.requireNonNull(lt, "lt");
        Objects.requireNonNull(lte, "lte");
    }

    public boolean isEmpty() {
        return gt.isEmpty() && gte.isEmpty() && lt.isEmpty() && lte.isEmpty();
    }
}
