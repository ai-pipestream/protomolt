package ai.pipestream.proto.validate;

import ai.pipestream.proto.validate.model.CelConstraint;
import ai.pipestream.proto.validate.model.FieldConstraints;
import ai.pipestream.proto.validate.model.IntegralConstraints;
import ai.pipestream.proto.validate.model.MessageConstraints;
import ai.pipestream.proto.validate.source.AiPipestreamRuleSource;
import ai.pipestream.proto.validate.spi.ValidationRuleSource;
import ai.pipestream.proto.validate.testdata.Person;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exercises the {@link ValidationRuleSource} extension seam: a second, non-Pipestream
 * source contributes constraints through the neutral model and its violations merge with
 * the built-in reader's — with no change to the validator core. This is the shape an
 * optional {@code buf.validate} module would take.
 */
class RuleSourceSpiTest {

    /** A stand-in "foreign dialect" source that caps any int field named {@code age} at 10. */
    private static final class AgeCapRuleSource implements ValidationRuleSource {
        @Override
        public Optional<FieldConstraints> fieldConstraints(FieldDescriptor field) {
            if (field.getJavaType() == FieldDescriptor.JavaType.INT && field.getName().equals("age")) {
                return Optional.of(FieldConstraints.builder()
                        .integral(IntegralConstraints.builder("agecap").lte(10).build())
                        .build());
            }
            return Optional.empty();
        }

        @Override
        public Optional<MessageConstraints> messageConstraints(Descriptor message) {
            return Optional.of(new MessageConstraints(List.of(
                    new CelConstraint("foreign.msg", "false", "foreign message rule"))));
        }
    }

    @Test
    void foreignSourceMergesWithBuiltIn() {
        List<ValidationRuleSource> chain = List.of(new AiPipestreamRuleSource(), new AgeCapRuleSource());
        ProtoValidator validator = ProtoValidator.forMessageType(Person.getDescriptor(), chain);

        // age 36 is fine for the built-in int32 rules but violates the foreign cap of 10.
        Person person = Person.newBuilder()
                .setName("Ada")
                .setAge(36)
                .setEmail("ada@example.com")
                .build();
        ValidationResult result = validator.validate(person);

        assertThat(result.valid()).isFalse();
        assertThat(result.violations())
                .anyMatch(v -> v.path().equals("age") && v.ruleId().equals("agecap.lte"));
        // The foreign message-level rule fires too, proving message rules merge as well.
        assertThat(result.violations()).anyMatch(v -> v.ruleId().equals("foreign.msg"));
    }

    @Test
    void builtInAloneIgnoresForeignConstraints() {
        ProtoValidator validator = ProtoValidator.forMessageType(
                Person.getDescriptor(), List.of(new AiPipestreamRuleSource()));

        Person person = Person.newBuilder()
                .setName("Ada")
                .setAge(36)
                .setEmail("ada@example.com")
                .build();

        assertThat(validator.validate(person).valid()).isTrue();
    }

    @Test
    void neutralModelBuilderCollapsesEmptyConstraints() {
        FieldConstraints empty = FieldConstraints.builder()
                .string(null)
                .integral(IntegralConstraints.builder("int32").build())
                .build();

        assertThat(empty.required()).isFalse();
        assertThat(empty.string()).isEmpty();
        assertThat(empty.integral()).isEmpty();
        assertThat(empty.floating()).isEmpty();
        assertThat(empty.bool()).isEmpty();
        assertThat(empty.bytes()).isEmpty();
        assertThat(empty.enumeration()).isEmpty();
        assertThat(empty.repeated()).isEmpty();
        assertThat(empty.map()).isEmpty();
        assertThat(empty.timestamp()).isEmpty();
        assertThat(empty.duration()).isEmpty();
        assertThat(empty.cel()).isEmpty();
    }
}
