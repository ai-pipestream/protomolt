package ai.protomolt.proto.validate;

import ai.protomolt.proto.validate.model.FieldConstraints;
import ai.protomolt.proto.validate.model.FloatingConstraints;
import ai.protomolt.proto.validate.model.IntegralConstraints;
import ai.protomolt.proto.validate.model.MessageConstraints;
import ai.protomolt.proto.validate.spi.ValidationRuleSource;
import ai.protomolt.proto.validate.testdata.NumberGauntlet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reversed-bound ranges: when the lower bound exceeds the upper bound the valid region is
 * <em>outside</em> the range, reported as the combined {@code <lower>_<upper>_exclusive} rule.
 * Also the IEEE edge that NaN satisfies no bound at all.
 */
class ExclusiveRangeTest {

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

    @Test
    void reversedIntegralRangeIsExclusive() {
        // gt: 10 with lt: 5 admits only values > 10 or < 5.
        ProtoValidator validator = ProtoValidator.create(List.of(new FieldSource("exact",
                FieldConstraints.builder()
                        .integral(IntegralConstraints.builder("int32").gt(10).lt(5).build())
                        .build())));

        assertThat(validator.validate(NumberGauntlet.newBuilder().setExact(3).build()).valid())
                .isTrue();
        assertThat(validator.validate(NumberGauntlet.newBuilder().setExact(12).build()).valid())
                .isTrue();
        assertThat(validator.validate(NumberGauntlet.newBuilder().setExact(7).build()).violations())
                .anyMatch(v -> v.path().equals("exact")
                        && v.ruleId().equals("int32.gt_lt_exclusive"));
        // The bounds themselves are outside the valid region too.
        assertThat(validator.validate(NumberGauntlet.newBuilder().setExact(5).build()).violations())
                .anyMatch(v -> v.ruleId().equals("int32.gt_lt_exclusive"));
    }

    @Test
    void reversedFloatingRangeIsExclusive() {
        ProtoValidator validator = ProtoValidator.create(List.of(new FieldSource("finite",
                FieldConstraints.builder()
                        .floating(FloatingConstraints.builder("double").gt(10.0).lt(5.0).build())
                        .build())));

        assertThat(validator.validate(NumberGauntlet.newBuilder().setFinite(3.5).build()).valid())
                .isTrue();
        assertThat(validator.validate(NumberGauntlet.newBuilder().setFinite(11.0).build()).valid())
                .isTrue();
        assertThat(validator.validate(NumberGauntlet.newBuilder().setFinite(7.0).build())
                        .violations())
                .anyMatch(v -> v.path().equals("finite")
                        && v.ruleId().equals("double.gt_lt_exclusive"));
    }

    @Test
    void nanSatisfiesNoBound() {
        // Every comparison against NaN is false, so NaN violates any range — a total-order
        // comparator could not express this, hence the IEEE-aware double path.
        ProtoValidator validator = ProtoValidator.create(List.of(new FieldSource("finite",
                FieldConstraints.builder()
                        .floating(FloatingConstraints.builder("double").gt(1.0).build())
                        .build())));

        assertThat(validator.validate(NumberGauntlet.newBuilder().setFinite(Double.NaN).build())
                        .violations())
                .anyMatch(v -> v.path().equals("finite") && v.ruleId().equals("double.gt"));
        assertThat(validator.validate(NumberGauntlet.newBuilder().setFinite(2.0).build()).valid())
                .isTrue();
    }

    @Test
    void strictBoundWinsOverTheInclusiveTwin() {
        // When both gt and gte are set the strict one governs: the boundary value fails.
        ProtoValidator lower = ProtoValidator.create(List.of(new FieldSource("exact",
                FieldConstraints.builder()
                        .integral(IntegralConstraints.builder("int32").gt(10).gte(0).build())
                        .build())));
        assertThat(lower.validate(NumberGauntlet.newBuilder().setExact(10).build()).violations())
                .anyMatch(v -> v.path().equals("exact") && v.ruleId().equals("int32.gt"));
        assertThat(lower.validate(NumberGauntlet.newBuilder().setExact(11).build()).valid())
                .isTrue();

        // Same for lt over lte.
        ProtoValidator upper = ProtoValidator.create(List.of(new FieldSource("exact",
                FieldConstraints.builder()
                        .integral(IntegralConstraints.builder("int32").lt(5).lte(20).build())
                        .build())));
        assertThat(upper.validate(NumberGauntlet.newBuilder().setExact(5).build()).violations())
                .anyMatch(v -> v.path().equals("exact") && v.ruleId().equals("int32.lt"));
        assertThat(upper.validate(NumberGauntlet.newBuilder().setExact(4).build()).valid())
                .isTrue();
    }
}
