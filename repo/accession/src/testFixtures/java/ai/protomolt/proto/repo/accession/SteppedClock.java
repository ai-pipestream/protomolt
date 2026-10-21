package ai.protomolt.proto.repo.accession;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Objects;

/**
 * A clock that starts at a fixed instant and moves forward by one step on
 * every read. Two clocks built from the same start and step hand out the same
 * sequence of instants, so a corpus written through it commits at the same
 * times on every load.
 */
public final class SteppedClock extends Clock {
    private final Instant start;
    private final Duration step;
    private final ZoneId zone;
    private long reads;

    /**
     * @param start the instant the first read returns
     * @param step how far each read moves the clock; positive
     */
    public SteppedClock(Instant start, Duration step) {
        this(start, step, ZoneOffset.UTC);
    }

    private SteppedClock(Instant start, Duration step, ZoneId zone) {
        this.start = Objects.requireNonNull(start, "start");
        this.step = Objects.requireNonNull(step, "step");
        this.zone = Objects.requireNonNull(zone, "zone");
        if (step.isNegative() || step.isZero()) {
            throw new IllegalArgumentException("Clock step must be positive");
        }
    }

    @Override
    public ZoneId getZone() {
        return zone;
    }

    @Override
    public Clock withZone(ZoneId zone) {
        return new SteppedClock(start, step, zone);
    }

    /** The next instant in the sequence; every call advances the clock. */
    @Override
    public synchronized Instant instant() {
        var now = start.plus(step.multipliedBy(reads));
        reads++;
        return now;
    }

    /** How many instants this clock has handed out. */
    public synchronized long reads() {
        return reads;
    }
}
