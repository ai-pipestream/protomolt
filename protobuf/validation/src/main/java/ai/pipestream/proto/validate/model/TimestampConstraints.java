package ai.pipestream.proto.validate.model;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * Neutral constraints for {@code google.protobuf.Timestamp} fields, expressed in
 * {@code java.time}. Violation rule ids are the fixed {@code timestamp.*} ids.
 * {@code ltNow}/{@code gtNow}/{@code within} compare against the clock at
 * validation time.
 */
public record TimestampConstraints(
        Optional<Instant> gt,
        Optional<Instant> gte,
        Optional<Instant> lt,
        Optional<Instant> lte,
        boolean ltNow,
        boolean gtNow,
        Optional<Duration> within) {

    public TimestampConstraints {
        Objects.requireNonNull(gt, "gt");
        Objects.requireNonNull(gte, "gte");
        Objects.requireNonNull(lt, "lt");
        Objects.requireNonNull(lte, "lte");
        Objects.requireNonNull(within, "within");
    }

    public boolean isEmpty() {
        return gt.isEmpty() && gte.isEmpty() && lt.isEmpty() && lte.isEmpty()
                && !ltNow && !gtNow && within.isEmpty();
    }
}
