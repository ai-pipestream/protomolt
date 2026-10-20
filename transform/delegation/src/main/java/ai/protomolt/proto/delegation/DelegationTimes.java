package ai.protomolt.proto.delegation;

import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;

import java.time.Instant;

/**
 * Protobuf time conversions shared by the coordinator and its extracted review-runtime and
 * lease-schedule helpers. These are the exact conversions the coordinator has always used;
 * centralizing them keeps every lane on the same millisecond-precision timestamp and
 * nanosecond-precision duration rules.
 */
final class DelegationTimes {

    private DelegationTimes() {
    }

    /** Millisecond-precision protobuf timestamp for one instant. */
    static Timestamp toTimestamp(Instant instant) {
        return Timestamps.fromMillis(instant.toEpochMilli());
    }

    /** Full-precision instant for one protobuf timestamp. */
    static Instant toInstant(Timestamp timestamp) {
        return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
    }

    /** Nanosecond-precision protobuf duration for one {@link java.time.Duration}. */
    static Duration toProtoDuration(java.time.Duration duration) {
        return Durations.fromNanos(duration.toNanos());
    }
}
