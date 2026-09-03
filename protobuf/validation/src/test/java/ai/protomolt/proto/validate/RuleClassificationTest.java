package ai.protomolt.proto.validate;

import ai.protomolt.proto.validate.model.CelConstraint;
import ai.protomolt.proto.validate.model.FieldConstraints;
import ai.protomolt.proto.validate.model.FloatingConstraints;
import ai.protomolt.proto.validate.model.MessageConstraints;
import ai.protomolt.proto.validate.model.StringConstraints;
import ai.protomolt.proto.validate.spi.ValidationRuleSource;
import ai.protomolt.proto.validate.testdata.Matryoshka;
import ai.protomolt.proto.validate.testdata.NumberGauntlet;
import ai.protomolt.proto.validate.testdata.Person;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Error-classification contract of the validator: malformed rules throw
 * {@link RuleCompilationException} eagerly (independent of the message's values), value-dependent
 * failures throw {@link RuleEvaluationException}, and neither ever surfaces as a plain violation
 * or a bare unchecked exception.
 */
class RuleClassificationTest {

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

    /** A rule source contributing fixed message-level constraints to every message. */
    private record MessageSource(MessageConstraints constraints) implements ValidationRuleSource {
        @Override
        public Optional<FieldConstraints> fieldConstraints(FieldDescriptor field) {
            return Optional.empty();
        }

        @Override
        public Optional<MessageConstraints> messageConstraints(Descriptor message) {
            return Optional.of(constraints);
        }
    }

    @Test
    void invalidPatternIsACompilationErrorEvenWhenTheFieldIsUnset() {
        ProtoValidator validator = ProtoValidator.create(List.of(new FieldSource("name",
                FieldConstraints.builder()
                        .string(StringConstraints.builder().pattern("[unclosed").build())
                        .build())));

        // The name field is unset; the broken pattern must still fail rule compilation.
        assertThatThrownBy(() -> validator.validate(Person.getDefaultInstance()))
                .isInstanceOf(RuleCompilationException.class)
                .hasMessageContaining("invalid regex pattern");
    }

    @Test
    void invalidCelIsACompilationErrorEvenWhenTheFieldIsUnset() {
        ProtoValidator validator = ProtoValidator.create(List.of(new FieldSource("name",
                FieldConstraints.builder()
                        .addCel(new CelConstraint("bad.syntax", "this ==", ""))
                        .build())));

        assertThatThrownBy(() -> validator.validate(Person.getDefaultInstance()))
                .isInstanceOf(RuleCompilationException.class);
    }

    @Test
    void celRuleWithNonBoolResultIsACompilationError() {
        ProtoValidator validator = ProtoValidator.create(List.of(new FieldSource("age",
                FieldConstraints.builder()
                        .addCel(new CelConstraint("bad.type", "1 + 2", ""))
                        .build())));

        assertThatThrownBy(() -> validator.validate(Person.getDefaultInstance()))
                .isInstanceOf(RuleCompilationException.class)
                .hasMessageContaining("bool or string");
    }

    @Test
    void celRuntimeErrorIsATypedEvaluationException() {
        ProtoValidator validator = ProtoValidator.create(List.of(new FieldSource("age",
                FieldConstraints.builder()
                        .addCel(new CelConstraint("div.zero", "this / (this - this) == 1", ""))
                        .build())));

        assertThatThrownBy(() -> validator.validate(Person.newBuilder().setAge(7).build()))
                .isInstanceOf(RuleEvaluationException.class)
                .satisfies(e -> assertThat(((RuleEvaluationException) e).ruleId())
                        .isEqualTo("div.zero"));
    }

    @Test
    void unknownOneofMemberNameIsACompilationError() {
        ProtoValidator validator = ProtoValidator.create(List.of(new MessageSource(
                new MessageConstraints(List.of(),
                        List.of(new MessageConstraints.Oneof(List.of("no_such_field"), false)),
                        List.of()))));

        assertThatThrownBy(() -> validator.validate(Person.getDefaultInstance()))
                .isInstanceOf(RuleCompilationException.class)
                .hasMessageContaining("no_such_field");
    }

    @Test
    void unknownRequiredOneofNameIsACompilationError() {
        ProtoValidator validator = ProtoValidator.create(List.of(new MessageSource(
                new MessageConstraints(List.of(), List.of(), List.of("no_such_oneof")))));

        assertThatThrownBy(() -> validator.validate(Person.getDefaultInstance()))
                .isInstanceOf(RuleCompilationException.class)
                .hasMessageContaining("no_such_oneof");
    }

    @Test
    void runawayNestingFailsWithEvaluationExceptionNotStackOverflow() {
        Matryoshka message = Matryoshka.newBuilder().setLabel("x").build();
        for (int i = 0; i < 600; i++) {
            message = Matryoshka.newBuilder().setLabel("x").setChild(message).build();
        }
        Matryoshka deep = message;

        assertThatThrownBy(() -> ProtoValidator.create().validate(deep))
                .isInstanceOf(RuleEvaluationException.class)
                .hasMessageContaining("nesting");
    }

    @Test
    void numericInMembershipUsesIeeeEquality() {
        // in: [-0.0] admits 0.0 (they compare equal); in: [NaN] admits nothing.
        ProtoValidator negZero = ProtoValidator.create(List.of(new FieldSource("finite",
                FieldConstraints.builder()
                        .floating(FloatingConstraints.builder("double").in(List.of(-0.0)).build())
                        .build())));
        // The optional field is explicitly set, so the in rule runs against 0.0.
        NumberGauntlet zero = NumberGauntlet.newBuilder().setFinite(0.0).build();
        assertThat(negZero.validate(zero).valid()).isTrue();

        ProtoValidator nan = ProtoValidator.create(List.of(new FieldSource("finite",
                FieldConstraints.builder()
                        .floating(FloatingConstraints.builder("double")
                                .in(List.of(Double.NaN)).build())
                        .build())));
        NumberGauntlet nanValue = NumberGauntlet.newBuilder().setFinite(Double.NaN).build();
        assertThat(nan.validate(nanValue).violations())
                .anyMatch(v -> v.ruleId().equals("double.in"));
    }
}
