package ai.pipestream.proto.validate;

import ai.pipestream.proto.validate.model.DurationConstraints;
import ai.pipestream.proto.validate.model.FieldConstraints;
import ai.pipestream.proto.validate.model.MessageConstraints;
import ai.pipestream.proto.validate.model.TimestampConstraints;
import ai.pipestream.proto.validate.spi.ValidationRuleSource;
import ai.pipestream.proto.validate.testdata.TimeGauntlet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Time-rule edge cases: out-of-range wire values are evaluation errors (not violations), and the
 * const/in/not_in families the dialect does not express run through custom sources.
 */
class TimeEdgeCasesTest {

    /** A rule source contributing a fixed constraint set to one named field of any message. */
    private record FieldSource(String fieldName, FieldConstraints constraints)
            implements ValidationRuleSource {
        @Override
        public Optional<FieldConstraints> fieldConstraints(FieldDescriptor field) {
            return field.getName().equals(fieldName) ? Optional.of(constraints) : Optional.empty();
        }

        @Override
        public Optional<MessageConstraints> messageConstraints(Descriptor message) {
            return Optional.empty();
        }
    }

    private static ProtoValidator forField(String fieldName, FieldConstraints constraints) {
        return ProtoValidator.create(List.of(new FieldSource(fieldName, constraints)));
    }

    @Test
    void outOfRangeTimestampIsAnEvaluationError() {
        // Long.MAX_VALUE seconds cannot be represented as a java.time.Instant; the failure is a
        // runtime error, not a rule violation.
        TimeGauntlet message = TimeGauntlet.newBuilder()
                .setPast(Timestamp.newBuilder().setSeconds(Long.MAX_VALUE))
                .build();

        assertThatThrownBy(() -> ProtoValidator.create().validate(message))
                .isInstanceOf(RuleEvaluationException.class)
                .hasMessageContaining("out of range");
    }

    @Test
    void outOfRangeDurationIsAnEvaluationError() {
        // seconds at Long.MAX_VALUE plus a nanos overflow exceeds a java.time.Duration.
        TimeGauntlet message = TimeGauntlet.newBuilder()
                .setTimeout(Duration.newBuilder()
                        .setSeconds(Long.MAX_VALUE).setNanos(2_000_000_000))
                .build();

        assertThatThrownBy(() -> ProtoValidator.create().validate(message))
                .isInstanceOf(RuleEvaluationException.class)
                .hasMessageContaining("out of range");
    }

    @Test
    void timestampConst() {
        Instant epoch = Instant.ofEpochSecond(100);
        ProtoValidator validator = forField("past", FieldConstraints.builder()
                .timestamp(new TimestampConstraints(Optional.of(epoch),
                        Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
                        false, false, Optional.empty()))
                .build());

        assertThat(validator.validate(TimeGauntlet.newBuilder()
                        .setPast(Timestamp.newBuilder().setSeconds(100)).build()).valid())
                .isTrue();
        assertThat(validator.validate(TimeGauntlet.newBuilder()
                        .setPast(Timestamp.newBuilder().setSeconds(101)).build()).violations())
                .anyMatch(v -> v.path().equals("past") && v.ruleId().equals("timestamp.const"));
    }

    @Test
    void durationInAndNotIn() {
        java.time.Duration half = java.time.Duration.ofSeconds(30);
        ProtoValidator inValidator = forField("timeout", FieldConstraints.builder()
                .duration(new DurationConstraints(Optional.empty(),
                        Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
                        List.of(half), List.of()))
                .build());

        assertThat(inValidator.validate(TimeGauntlet.newBuilder()
                        .setTimeout(Duration.newBuilder().setSeconds(30)).build()).valid())
                .isTrue();
        assertThat(inValidator.validate(TimeGauntlet.newBuilder()
                        .setTimeout(Duration.newBuilder().setSeconds(31)).build()).violations())
                .anyMatch(v -> v.path().equals("timeout") && v.ruleId().equals("duration.in"));

        ProtoValidator notInValidator = forField("timeout", FieldConstraints.builder()
                .duration(new DurationConstraints(Optional.empty(),
                        Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
                        List.of(), List.of(half)))
                .build());
        assertThat(notInValidator.validate(TimeGauntlet.newBuilder()
                        .setTimeout(Duration.newBuilder().setSeconds(30)).build()).violations())
                .anyMatch(v -> v.path().equals("timeout") && v.ruleId().equals("duration.not_in"));
    }
}
