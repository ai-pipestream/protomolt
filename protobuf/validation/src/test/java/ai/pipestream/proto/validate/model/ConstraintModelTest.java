package ai.pipestream.proto.validate.model;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The neutral constraint model: {@code isEmpty} on every record, builder collapsing of empty
 * sub-constraints, defensive copies, and {@link CelConstraint}/{@link MessageConstraints}
 * normalization.
 */
class ConstraintModelTest {

    @Test
    void stringConstraintsEmptinessAndCopies() {
        assertThat(StringConstraints.builder().build().isEmpty()).isTrue();
        assertThat(StringConstraints.builder().minLen(1).build().isEmpty()).isFalse();
        assertThat(StringConstraints.builder().format(StringFormat.EMAIL).build().isEmpty())
                .isFalse();

        // The in list is defensively copied; the format set is decoupled from the builder.
        List<String> in = new ArrayList<>(List.of("a"));
        StringConstraints.Builder builder = StringConstraints.builder().in(in)
                .format(StringFormat.EMAIL);
        StringConstraints built = builder.build();
        in.add("b");
        builder.format(StringFormat.UUID);
        assertThat(built.in()).containsExactly("a");
        assertThat(built.formats()).containsExactly(StringFormat.EMAIL);
        assertThatThrownBy(() -> built.formats().add(StringFormat.UUID))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void bytesConstraintsEmptiness() {
        assertThat(BytesConstraints.builder().build().isEmpty()).isTrue();
        assertThat(BytesConstraints.builder().len(1).build().isEmpty()).isFalse();
        assertThat(BytesConstraints.builder().constant(ByteString.EMPTY).build().isEmpty())
                .isFalse();
        assertThat(BytesConstraints.builder().format(BytesFormat.IPV4).build().formats())
                .containsExactly(BytesFormat.IPV4);
    }

    @Test
    void integralConstraintsEmptinessAndUnsignedFlag() {
        assertThat(IntegralConstraints.builder("int32").build().isEmpty()).isTrue();
        assertThat(IntegralConstraints.builder("int32").build().unsigned()).isFalse();
        assertThat(IntegralConstraints.unsignedBuilder("uint64").build().unsigned()).isTrue();
        assertThat(IntegralConstraints.builder("int32").lte(5).build().isEmpty()).isFalse();
        assertThat(IntegralConstraints.builder("int32").build().ruleIdPrefix()).isEqualTo("int32");
    }

    @Test
    void floatingConstraintsEmptiness() {
        assertThat(FloatingConstraints.builder("double").build().isEmpty()).isTrue();
        assertThat(FloatingConstraints.builder("double").finite(true).build().isEmpty()).isFalse();
        assertThat(FloatingConstraints.builder("float").build().ruleIdPrefix()).isEqualTo("float");
    }

    @Test
    void smallRecordsEmptiness() {
        assertThat(new BoolConstraints(Optional.empty()).isEmpty()).isTrue();
        assertThat(new BoolConstraints(Optional.of(true)).isEmpty()).isFalse();

        assertThat(new EnumConstraints(OptionalInt.empty(), false, List.of(), List.of()).isEmpty())
                .isTrue();
        assertThat(new EnumConstraints(OptionalInt.empty(), true, List.of(), List.of()).isEmpty())
                .isFalse();

        assertThat(new RepeatedConstraints(OptionalLong.empty(), OptionalLong.empty(), false,
                Optional.empty()).isEmpty()).isTrue();
        assertThat(new RepeatedConstraints(OptionalLong.empty(), OptionalLong.empty(), true,
                Optional.empty()).isEmpty()).isFalse();

        assertThat(new MapConstraints(OptionalLong.empty(), OptionalLong.empty(),
                Optional.empty(), Optional.empty()).isEmpty()).isTrue();
        assertThat(new MapConstraints(OptionalLong.of(1), OptionalLong.empty(),
                Optional.empty(), Optional.empty()).isEmpty()).isFalse();

        assertThat(new TimestampConstraints(Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty(), false, false, Optional.empty()).isEmpty())
                .isTrue();
        assertThat(new TimestampConstraints(Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty(), true, false, Optional.empty()).isEmpty())
                .isFalse();
        assertThat(new TimestampConstraints(Optional.of(Instant.EPOCH), Optional.empty(),
                Optional.empty(), Optional.empty(), Optional.empty(), false, false,
                Optional.empty()).isEmpty()).isFalse();

        assertThat(new DurationConstraints(Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty(), List.of(), List.of()).isEmpty()).isTrue();
        assertThat(new DurationConstraints(Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty(), List.of(Duration.ZERO), List.of()).isEmpty())
                .isFalse();

        assertThat(new AnyConstraints(List.of(), List.of()).isEmpty()).isTrue();
        assertThat(new AnyConstraints(List.of("type.googleapis.com/x.Y"), List.of()).isEmpty())
                .isFalse();

        assertThat(new FieldMaskConstraints(Optional.empty(), List.of(), List.of()).isEmpty())
                .isTrue();
        assertThat(new FieldMaskConstraints(Optional.of("a"), List.of(), List.of()).isEmpty())
                .isFalse();
    }

    @Test
    void fieldConstraintsBuilderCollapsesEveryEmptySubConstraint() {
        FieldConstraints collapsed = FieldConstraints.builder()
                .string(StringConstraints.builder().build())
                .integral(IntegralConstraints.builder("int32").build())
                .floating(FloatingConstraints.builder("double").build())
                .bool(new BoolConstraints(Optional.empty()))
                .bytes(BytesConstraints.builder().build())
                .enumeration(new EnumConstraints(OptionalInt.empty(), false, List.of(), List.of()))
                .repeated(new RepeatedConstraints(OptionalLong.empty(), OptionalLong.empty(), false,
                        Optional.empty()))
                .map(new MapConstraints(OptionalLong.empty(), OptionalLong.empty(),
                        Optional.empty(), Optional.empty()))
                .timestamp(new TimestampConstraints(Optional.empty(), Optional.empty(),
                        Optional.empty(), Optional.empty(), Optional.empty(), false, false,
                        Optional.empty()))
                .duration(new DurationConstraints(Optional.empty(), Optional.empty(),
                        Optional.empty(), Optional.empty(), Optional.empty(), List.of(), List.of()))
                .any(new AnyConstraints(List.of(), List.of()))
                .fieldMask(new FieldMaskConstraints(Optional.empty(), List.of(), List.of()))
                .build();

        assertThat(collapsed.required()).isFalse();
        assertThat(collapsed.ignore()).isEqualTo(IgnoreMode.UNSPECIFIED);
        assertThat(collapsed.string()).isEmpty();
        assertThat(collapsed.integral()).isEmpty();
        assertThat(collapsed.floating()).isEmpty();
        assertThat(collapsed.bool()).isEmpty();
        assertThat(collapsed.bytes()).isEmpty();
        assertThat(collapsed.enumeration()).isEmpty();
        assertThat(collapsed.repeated()).isEmpty();
        assertThat(collapsed.map()).isEmpty();
        assertThat(collapsed.timestamp()).isEmpty();
        assertThat(collapsed.duration()).isEmpty();
        assertThat(collapsed.any()).isEmpty();
        assertThat(collapsed.fieldMask()).isEmpty();
        assertThat(collapsed.cel()).isEmpty();
    }

    @Test
    void fieldConstraintsBuilderRejectsNulls() {
        FieldConstraints.Builder builder = FieldConstraints.builder();
        assertThatThrownBy(() -> builder.ignore(null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> builder.addCel(null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void celConstraintNormalizesBlanks() {
        CelConstraint plain = new CelConstraint("id", "this > 0", "msg");
        assertThat(plain.celField()).isEqualTo("cel");
        assertThat(plain.rulePath()).isEmpty();
        assertThat(plain.ruleValue()).isNull();

        assertThat(new CelConstraint("id", "e", "m", null).celField()).isEqualTo("cel");
        assertThat(new CelConstraint("id", "e", "m", "  ").celField()).isEqualTo("cel");
        assertThat(new CelConstraint("id", "e", "m", "cel_expression").celField())
                .isEqualTo("cel_expression");
        assertThat(new CelConstraint("id", "e", "m", "cel", null, null).rulePath()).isEmpty();

        assertThatThrownBy(() -> new CelConstraint(null, "e", "m"))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new CelConstraint("id", null, "m"))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new CelConstraint("id", "e", null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void messageConstraintsConstructorsAndEmptiness() {
        assertThat(new MessageConstraints(List.of()).isEmpty()).isTrue();
        assertThat(new MessageConstraints(List.of(), List.of()).isEmpty()).isTrue();
        assertThat(new MessageConstraints(List.of(), List.of(), List.of()).isEmpty()).isTrue();
        assertThat(new MessageConstraints(List.of(), List.of(), List.of()).skipWhen()).isEmpty();

        CelConstraint rule = new CelConstraint("id", "true", "");
        assertThat(new MessageConstraints(List.of(rule)).isEmpty()).isFalse();
        assertThat(new MessageConstraints(List.of(), List.of(), List.of(), "flag").isEmpty())
                .isFalse();

        MessageConstraints.Oneof oneof =
                new MessageConstraints.Oneof(List.of("a", "b"), true);
        assertThat(oneof.fields()).containsExactly("a", "b");
        assertThat(oneof.required()).isTrue();
        assertThatThrownBy(() -> oneof.fields().add("c"))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
