package ai.protomolt.proto.validate;

import ai.protomolt.proto.cel.CelEnvironmentFactory;
import ai.protomolt.proto.cel.CelEvaluator;
import ai.protomolt.proto.validate.cel.ValidationCelFunctions;
import ai.protomolt.proto.validate.model.CelConstraint;
import ai.protomolt.proto.validate.model.FieldConstraints;
import ai.protomolt.proto.validate.model.MessageConstraints;
import ai.protomolt.proto.validate.spi.ValidationRuleSource;
import ai.protomolt.proto.validate.testdata.Person;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import dev.cel.common.types.SimpleType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Error paths for CEL-backed validation: unknown functions, receiver type mismatches,
 * malformed {@code format()} templates, and the {@code isInf(0)} either-infinity branch.
 */
class CelErrorPathsTest {

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

    private static ProtoValidator validatorFor(String field, CelConstraint rule) {
        return ProtoValidator.create(List.of(
                new FieldSource(field, FieldConstraints.builder().addCel(rule).build())));
    }

    @Test
    void unknownFunctionIsACompilationError() {
        var validator = validatorFor("name",
                new CelConstraint("bad.fn", "this.noSuchFunction() == true", ""));

        assertThatThrownBy(() -> validator.validate(Person.newBuilder().setName("x").build()))
                .isInstanceOf(RuleCompilationException.class);
    }

    @Test
    void unknownFunctionIsACompilationErrorEvenWhenTheFieldIsUnset() {
        var validator = validatorFor("name",
                new CelConstraint("bad.fn", "this.noSuchFunction() == true", ""));

        assertThatThrownBy(() -> validator.validate(Person.getDefaultInstance()))
                .isInstanceOf(RuleCompilationException.class);
    }

    @Test
    void receiverTypeMismatchIsARuntimeEvaluationError() {
        // isNan() is declared on double only, but the member call still compiles against a
        // string field (overload resolution is dynamic); the mismatch fails at evaluation.
        var validator = validatorFor("name",
                new CelConstraint("bad.recv", "this.isNan() == false", ""));

        assertThatThrownBy(() -> validator.validate(Person.newBuilder().setName("x").build()))
                .isInstanceOf(RuleEvaluationException.class)
                .satisfies(e -> assertThat(((RuleEvaluationException) e).ruleId())
                        .isEqualTo("bad.recv"));
    }

    @Test
    void malformedFormatTemplateIsARuntimeEvaluationError() {
        // '%d' with a string argument compiles (format takes a list) but fails at evaluation.
        var validator = validatorFor("age",
                new CelConstraint("bad.fmt", "'%d'.format(['abc']) == 'x'", ""));

        assertThatThrownBy(() -> validator.validate(Person.newBuilder().setAge(7).build()))
                .isInstanceOf(RuleEvaluationException.class)
                .satisfies(e -> assertThat(((RuleEvaluationException) e).ruleId())
                        .isEqualTo("bad.fmt"));
    }

    @Test
    void trailingPercentFormatTemplateIsARuntimeEvaluationError() {
        var validator = validatorFor("age",
                new CelConstraint("bad.fmt", "'oops%'.format([this]) == 'x'", ""));

        assertThatThrownBy(() -> validator.validate(Person.newBuilder().setAge(7).build()))
                .isInstanceOf(RuleEvaluationException.class);
    }

    @Test
    void isInfWithZeroSignMatchesEitherInfinity() {
        var eval = new CelEvaluator(CelEnvironmentFactory.builder()
                .addVar("d", SimpleType.DOUBLE)
                .addFunctions(ValidationCelFunctions.declarations(), ValidationCelFunctions.bindings())
                .build());

        assertThat(eval.evaluateBoolean("d.isInf(0)", Map.of("d", Double.POSITIVE_INFINITY))).isTrue();
        assertThat(eval.evaluateBoolean("d.isInf(0)", Map.of("d", Double.NEGATIVE_INFINITY))).isTrue();
        assertThat(eval.evaluateBoolean("d.isInf(0)", Map.of("d", 1.5))).isFalse();
        assertThat(eval.evaluateBoolean("d.isInf(0)", Map.of("d", Double.NaN))).isFalse();
    }
}
