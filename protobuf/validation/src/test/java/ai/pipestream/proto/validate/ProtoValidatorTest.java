package ai.pipestream.proto.validate;

import ai.pipestream.proto.validate.testdata.Color;
import ai.pipestream.proto.validate.testdata.Container;
import ai.pipestream.proto.validate.testdata.Item;
import ai.pipestream.proto.validate.testdata.Person;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProtoValidatorTest {

    private ProtoValidator validator;

    @BeforeEach
    void setUp() {
        validator = ProtoValidator.forMessageType(Person.getDescriptor());
    }

    @Test
    void acceptsValidPerson() {
        Person person = Person.newBuilder()
                .setName("Ada")
                .setAge(36)
                .setEmail("ada@example.com")
                .build();

        assertThat(validator.validate(person).valid()).isTrue();
    }

    @Test
    void rejectsMissingRequiredName() {
        Person person = Person.newBuilder().setAge(20).setEmail("a@b.co").build();
        ValidationResult result = validator.validate(person);

        assertThat(result.valid()).isFalse();
        assertThat(result.violations())
                .anyMatch(v -> v.path().equals("name") && v.ruleId().equals("required"));
    }

    @Test
    void rejectsShortName() {
        Person person = Person.newBuilder()
                .setName("A")
                .setAge(10)
                .setEmail("a@b.co")
                .build();
        ValidationResult result = validator.validate(person);

        assertThat(result.violations())
                .anyMatch(v -> v.ruleId().equals("string.min_len"));
    }

    @Test
    void rejectsAgeOutOfRange() {
        Person person = Person.newBuilder()
                .setName("Ada")
                .setAge(200)
                .setEmail("ada@example.com")
                .build();
        ValidationResult result = validator.validate(person);

        assertThat(result.violations())
                .anyMatch(v -> v.ruleId().equals("int32.gte_lte"));
    }

    @Test
    void rejectsLocalhostEmailViaCel() {
        Person person = Person.newBuilder()
                .setName("Ada")
                .setAge(36)
                .setEmail("ada@localhost")
                .build();
        ValidationResult result = validator.validate(person);

        assertThat(result.violations())
                .anyMatch(v -> v.ruleId().equals("email.not_localhost"));
    }

    @Test
    void rejectsAdultWithShortNameViaMessageCel() {
        Person person = Person.newBuilder()
                .setName("Al")
                .setAge(21)
                .setEmail("al@example.com")
                .build();
        ValidationResult result = validator.validate(person);

        assertThat(result.violations())
                .anyMatch(v -> v.ruleId().equals("adult.name"));
    }

    @Test
    void handlesMapAndRepeatedMessageFields() {
        ProtoValidator containerValidator = ProtoValidator.forMessageType(Container.getDescriptor());
        Container container = Container.newBuilder()
                .putLabels("env", "prod")
                .addItems(Item.newBuilder().setName("widget"))
                .setColor(Color.COLOR_RED)
                .build();

        assertThat(containerValidator.validate(container).valid()).isTrue();
    }

    @Test
    void rejectsMissingRequiredEnum() {
        ProtoValidator containerValidator = ProtoValidator.forMessageType(Container.getDescriptor());
        Container container = Container.newBuilder()
                .putLabels("env", "prod")
                .addItems(Item.newBuilder().setName("widget"))
                .build();
        ValidationResult result = containerValidator.validate(container);

        assertThat(result.valid()).isFalse();
        assertThat(result.violations())
                .anyMatch(v -> v.path().equals("color") && v.ruleId().equals("required"));
    }

    @Test
    void throwIfInvalidRaises() {
        Person person = Person.newBuilder().setAge(1).setEmail("x@y.z").build();
        assertThatThrownBy(() -> validator.validate(person).throwIfInvalid())
                .isInstanceOf(ValidationResult.ValidationException.class);
    }
}
