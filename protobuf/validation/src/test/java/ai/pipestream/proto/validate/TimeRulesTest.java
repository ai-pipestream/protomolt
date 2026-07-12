package ai.pipestream.proto.validate;

import ai.pipestream.proto.validate.testdata.TimeGauntlet;
import com.google.protobuf.Duration;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/** google.protobuf.Timestamp and google.protobuf.Duration rules. */
class TimeRulesTest {

    private static final ProtoValidator VALIDATOR = ProtoValidator.create();

    private static void assertViolation(Message message, String path, String ruleId) {
        assertThat(VALIDATOR.validate(message).violations())
                .as("expected %s at %s", ruleId, path)
                .anyMatch(v -> v.path().equals(path) && v.ruleId().equals(ruleId));
    }

    private static void assertValid(Message message) {
        assertThat(VALIDATOR.validate(message).valid())
                .as("expected no violations, got %s", VALIDATOR.validate(message).violations())
                .isTrue();
    }

    private static Timestamp ts(Instant instant) {
        return Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }

    private static Duration secs(long seconds) {
        return Duration.newBuilder().setSeconds(seconds).build();
    }

    @Test
    void ltNow() {
        assertValid(TimeGauntlet.newBuilder()
                .setPast(ts(Instant.now().minusSeconds(60))).build());
        assertViolation(TimeGauntlet.newBuilder()
                        .setPast(ts(Instant.now().plusSeconds(86400))).build(),
                "past", "timestamp.lt_now");
    }

    @Test
    void gtNow() {
        assertValid(TimeGauntlet.newBuilder()
                .setFuture(ts(Instant.now().plusSeconds(3600))).build());
        assertViolation(TimeGauntlet.newBuilder()
                        .setFuture(Timestamp.getDefaultInstance()).build(),
                "future", "timestamp.gt_now");
    }

    @Test
    void absoluteBounds() {
        assertValid(TimeGauntlet.newBuilder()
                .setEpochWindow(Timestamp.newBuilder().setSeconds(500)).build());
        assertViolation(TimeGauntlet.newBuilder()
                        .setEpochWindow(Timestamp.newBuilder().setSeconds(-5)).build(),
                "epoch_window", "timestamp.gte");
        assertViolation(TimeGauntlet.newBuilder()
                        .setEpochWindow(Timestamp.newBuilder().setSeconds(2_000_000)).build(),
                "epoch_window", "timestamp.lt");
    }

    @Test
    void withinWindowOfNow() {
        assertValid(TimeGauntlet.newBuilder().setRecent(ts(Instant.now())).build());
        assertValid(TimeGauntlet.newBuilder()
                .setRecent(ts(Instant.now().minusSeconds(600))).build());
        assertViolation(TimeGauntlet.newBuilder()
                        .setRecent(ts(Instant.now().minusSeconds(7200))).build(),
                "recent", "timestamp.within");
        // `within` is symmetric: the far future also fails.
        assertViolation(TimeGauntlet.newBuilder()
                        .setRecent(ts(Instant.now().plusSeconds(7200))).build(),
                "recent", "timestamp.within");
    }

    @Test
    void durationBounds() {
        assertValid(TimeGauntlet.newBuilder().setTimeout(secs(30)).build());
        assertViolation(TimeGauntlet.newBuilder()
                        .setTimeout(Duration.newBuilder().setNanos(500_000_000)).build(),
                "timeout", "duration.gte");
        assertViolation(TimeGauntlet.newBuilder().setTimeout(secs(61)).build(),
                "timeout", "duration.lte");
    }

    @Test
    void strictDurationBounds() {
        assertValid(TimeGauntlet.newBuilder()
                .setStrict(Duration.newBuilder().setNanos(1000)).build());
        assertViolation(TimeGauntlet.newBuilder()
                        .setStrict(Duration.newBuilder().setNanos(100)).build(),
                "strict", "duration.gt");
        // Exactly the lt bound fails a strict comparison.
        assertViolation(TimeGauntlet.newBuilder().setStrict(secs(1)).build(),
                "strict", "duration.lt");
    }
}
