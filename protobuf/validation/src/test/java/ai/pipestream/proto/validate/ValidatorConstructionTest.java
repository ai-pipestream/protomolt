package ai.pipestream.proto.validate;

import ai.pipestream.proto.cel.CelEnvironmentFactory;
import ai.pipestream.proto.cel.CelEvaluator;
import ai.pipestream.proto.validate.model.CelConstraint;
import ai.pipestream.proto.validate.model.FieldConstraints;
import ai.pipestream.proto.validate.model.MessageConstraints;
import ai.pipestream.proto.validate.spi.ValidationRuleSource;
import ai.pipestream.proto.validate.testdata.Person;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Construction contracts: null rejection, the evaluator-only constructor (whose CEL rules compile
 * through the evaluator rather than a static type-check), and an empty rule-source chain.
 */
class ValidatorConstructionTest {

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

    /** An evaluator-only environment mirroring the one {@link ProtoValidator#create()} builds. */
    private static CelEvaluator fieldEvaluator() {
        return new CelEvaluator(CelEnvironmentFactory.builder()
                .addVar("this").addVar("now").addVar("rule")
                .build());
    }

    @Test
    void nullArgumentsAreRejected() {
        assertThatThrownBy(() -> new ProtoValidator(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new ProtoValidator(fieldEvaluator(), null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> ProtoValidator.create(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> ProtoValidator.forMessageType(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> ProtoValidator.create().validate(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void evaluatorOnlyValidatorAppliesDialectRules() {
        ProtoValidator validator = new ProtoValidator(fieldEvaluator());

        Person valid = Person.newBuilder()
                .setName("Ada").setAge(36).setEmail("ada@example.com").build();
        assertThat(validator.validate(valid).valid()).isTrue();

        // Both a standard rule and the field-level CEL rule must fire without a Cel handle.
        Person broken = Person.newBuilder()
                .setName("A").setAge(36).setEmail("ada@localhost").build();
        assertThat(validator.validate(broken).violations())
                .anyMatch(v -> v.ruleId().equals("string.min_len"))
                .anyMatch(v -> v.ruleId().equals("email.not_localhost"));
    }

    @Test
    void evaluatorOnlyValidatorStillClassifiesErrors() {
        ProtoValidator badSyntax = new ProtoValidator(fieldEvaluator(),
                List.of(new FieldSource("name", FieldConstraints.builder()
                        .addCel(new CelConstraint("bad.syntax", "this ==", ""))
                        .build())));
        assertThatThrownBy(() -> badSyntax.validate(Person.getDefaultInstance()))
                .isInstanceOf(RuleCompilationException.class);

        // A rule that compiles but fails at runtime (unbound-variable errors during eager
        // compilation are ignored; the division fails only once `this` is bound) is an
        // evaluation error, not a violation.
        ProtoValidator runtimeError = new ProtoValidator(fieldEvaluator(),
                List.of(new FieldSource("age", FieldConstraints.builder()
                        .addCel(new CelConstraint("div.zero", "this / (this - this) == 1", ""))
                        .build())));
        assertThatThrownBy(() -> runtimeError.validate(Person.newBuilder().setAge(7).build()))
                .isInstanceOf(RuleEvaluationException.class);
    }

    @Test
    void emptySourceChainValidatesEverything() {
        ProtoValidator validator = ProtoValidator.create(List.of());

        assertThat(validator.validate(Person.getDefaultInstance()).valid()).isTrue();
    }

    @Test
    void exceptionAccessorsNormalizeNulls() {
        assertThat(new RuleEvaluationException("m").ruleId()).isEmpty();
        assertThat(new RuleEvaluationException("m", null).ruleId()).isEmpty();
        assertThat(new RuleEvaluationException(null, "m", null).ruleId()).isEmpty();
        assertThat(new RuleEvaluationException("bytes.pattern", "m", null).ruleId())
                .isEqualTo("bytes.pattern");

        assertThat(new RuleCompilationException("m")).hasMessage("m");
        assertThat(new RuleCompilationException("m", new IllegalStateException("cause")))
                .hasCauseInstanceOf(IllegalStateException.class);
    }
}
