package ai.protomolt.proto.validate;

import ai.protomolt.proto.validate.testdata.Person;
import com.google.protobuf.DescriptorProtos.FieldOptions;
import com.google.protobuf.ExtensionRegistry;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The {@link ValidationResult} record, its {@code Violation}, and its exception contract. */
class ValidationResultTest {

    @Test
    void okHasNoViolations() {
        ValidationResult result = ValidationResult.ok();

        assertThat(result.valid()).isTrue();
        assertThat(result.violations()).isEmpty();
    }

    @Test
    void failedCarriesTheViolations() {
        ValidationResult.Violation violation =
                new ValidationResult.Violation("name", "required", "field is required");
        ValidationResult result = ValidationResult.failed(List.of(violation));

        assertThat(result.valid()).isFalse();
        assertThat(result.violations()).containsExactly(violation);
    }

    @Test
    void violationsListIsDefensivelyCopied() {
        List<ValidationResult.Violation> mutable = new ArrayList<>();
        mutable.add(new ValidationResult.Violation("name", "required", "field is required"));
        ValidationResult result = ValidationResult.failed(mutable);
        mutable.add(new ValidationResult.Violation("age", "int32.gte", "must be >= 0"));

        assertThat(result.violations()).hasSize(1);
        assertThatThrownBy(() -> result.violations().add(
                new ValidationResult.Violation("x", "y", "z")))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void nullViolationsListIsRejected() {
        assertThatThrownBy(() -> ValidationResult.failed(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void violationDefaultsBlankRulePath() {
        assertThat(new ValidationResult.Violation("p", "r", "m").rulePath()).isEmpty();
        assertThat(new ValidationResult.Violation("p", "r", "m", null).rulePath()).isEmpty();
        assertThat(new ValidationResult.Violation("p", "r", "m", "cel[1]").rulePath())
                .isEqualTo("cel[1]");
    }

    @Test
    void violationRejectsNullComponents() {
        assertThatThrownBy(() -> new ValidationResult.Violation(null, "r", "m"))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new ValidationResult.Violation("p", null, "m"))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new ValidationResult.Violation("p", "r", null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void throwIfInvalidIsSilentWhenValid() {
        ValidationResult.ok().throwIfInvalid();
    }

    @Test
    void validationExceptionFormatsEveryViolation() {
        ValidationResult result = ValidationResult.failed(List.of(
                new ValidationResult.Violation("name", "required", "field is required"),
                new ValidationResult.Violation("age", "int32.gte_lte", "must be >= 0 and <= 150")));

        assertThatThrownBy(result::throwIfInvalid)
                .isInstanceOf(ValidationResult.ValidationException.class)
                .hasMessageContaining("[name] required: field is required")
                .hasMessageContaining("[age] int32.gte_lte: must be >= 0 and <= 150")
                .satisfies(e -> assertThat(
                        ((ValidationResult.ValidationException) e).result()).isSameAs(result));
    }

    @Test
    void staticValidateUsesTheSharedDefaultValidator() {
        ValidationResult clean = ValidationResult.validate(Person.newBuilder()
                .setName("Ada").setAge(36).setEmail("ada@example.com").build());
        assertThat(clean.valid()).isTrue();

        ValidationResult broken = ValidationResult.validate(Person.getDefaultInstance());
        assertThat(broken.valid()).isFalse();
        assertThat(broken.violations())
                .anyMatch(v -> v.path().equals("name") && v.ruleId().equals("required"));
    }

    @Test
    void registerExtensionsMakesTheFieldOptionReadable() {
        ExtensionRegistry registry = ExtensionRegistry.newInstance();
        ValidationResult.registerExtensions(registry);

        // Person.name carries the field extension; parse its raw options against the registry.
        var options = Person.getDescriptor().findFieldByName("name").getOptions();
        assertThatThrownBy(() -> ValidationResult.registerExtensions(null))
                .isInstanceOf(NullPointerException.class);
        try {
            FieldOptions parsed = FieldOptions.parseFrom(options.toByteString(), registry);
            assertThat(parsed.hasExtension(ValidateProto.field)).isTrue();
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
            throw new AssertionError("options bytes must reparse", e);
        }
    }
}
