package ai.protomolt.proto.validate;

import ai.protomolt.proto.validate.model.FieldConstraints;
import ai.protomolt.proto.validate.model.FloatingConstraints;
import ai.protomolt.proto.validate.model.IgnoreMode;
import ai.protomolt.proto.validate.model.MessageConstraints;
import ai.protomolt.proto.validate.model.RepeatedConstraints;
import ai.protomolt.proto.validate.source.ProtomoltRuleSource;
import ai.protomolt.proto.validate.spi.ValidationRuleSource;
import ai.protomolt.proto.validate.testdata.ImplicitGauntlet;
import ai.protomolt.proto.validate.testdata.RepeatedGauntlet;
import ai.protomolt.proto.validate.testdata.RequiredRepeatedGauntlet;
import ai.protomolt.proto.validate.testdata.Widget;
import ai.protomolt.proto.validate.testdata.ZeroGauntlet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Presence semantics: implicit-presence scalars are validated at their zero value, explicit
 * presence skips unset fields, {@code required} demands population, and the ignore modes
 * (IF_ZERO_VALUE / ALWAYS) suspend rules — including recursion into an ignored item's message.
 */
class PresenceAndIgnoreTest {

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
    void implicitPresenceScalarsAreValidatedAtTheirZeroValue() {
        // Neither field is optional: the default instance has no presence bits to skip on,
        // so the zero values run against the rules and fail them.
        assertViolation(ImplicitGauntlet.getDefaultInstance(), "level", "int32.gte");
        assertViolation(ImplicitGauntlet.getDefaultInstance(), "code", "string.min_len");

        assertValid(ImplicitGauntlet.newBuilder().setLevel(10).setCode("ab").build());
        assertViolation(ImplicitGauntlet.newBuilder().setLevel(10).build(), "code", "string.min_len");
    }

    @Test
    void requiredOnRepeatedFieldDemandsAtLeastOneElement() {
        assertViolation(RequiredRepeatedGauntlet.getDefaultInstance(), "entries", "required");
        assertValid(RequiredRepeatedGauntlet.newBuilder().addEntries("x").build());
    }

    @Test
    void ignoreIfZeroValueSkipsOnlyTheZeroValue() {
        // weight is an implicit-presence double; the custom source alone supplies the rules.
        FieldConstraints constraints = FieldConstraints.builder()
                .ignore(IgnoreMode.IF_ZERO_VALUE)
                .floating(FloatingConstraints.builder("double").gte(1.0).build())
                .build();
        ProtoValidator validator =
                ProtoValidator.create(List.of(new FieldSource("weight", constraints)));

        // 0.0 is the zero value: the gte rule is suspended rather than failed.
        assertThat(validator.validate(ZeroGauntlet.getDefaultInstance()).valid()).isTrue();
        // A populated value below the bound is validated normally.
        assertThat(validator.validate(ZeroGauntlet.newBuilder().setWeight(0.5).build()).violations())
                .anyMatch(v -> v.path().equals("weight") && v.ruleId().equals("double.gte"));
        assertThat(validator.validate(ZeroGauntlet.newBuilder().setWeight(2.0).build()).valid())
                .isTrue();
    }

    @Test
    void ignoreAlwaysNeverValidatesTheField() {
        FieldConstraints constraints = FieldConstraints.builder()
                .ignore(IgnoreMode.ALWAYS)
                .floating(FloatingConstraints.builder("double").gte(1.0).build())
                .build();
        ProtoValidator validator =
                ProtoValidator.create(List.of(new FieldSource("weight", constraints)));

        assertThat(validator.validate(ZeroGauntlet.newBuilder().setWeight(0.5).build()).valid())
                .isTrue();
    }

    @Test
    void theStrongestIgnoreModeAcrossSourcesWins() {
        FieldConstraints ifZero = FieldConstraints.builder()
                .ignore(IgnoreMode.IF_ZERO_VALUE)
                .build();
        FieldConstraints always = FieldConstraints.builder()
                .ignore(IgnoreMode.ALWAYS)
                .floating(FloatingConstraints.builder("double").gte(1.0).build())
                .build();
        // ALWAYS outranks IF_ZERO_VALUE regardless of source order.
        List<ValidationRuleSource> ifZeroFirst =
                List.of(new FieldSource("weight", ifZero), new FieldSource("weight", always));
        List<ValidationRuleSource> alwaysFirst =
                List.of(new FieldSource("weight", always), new FieldSource("weight", ifZero));
        for (List<ValidationRuleSource> chain : List.of(ifZeroFirst, alwaysFirst)) {
            ProtoValidator validator = ProtoValidator.create(chain);
            assertThat(validator.validate(ZeroGauntlet.newBuilder().setWeight(0.5).build()).valid())
                    .as("chain %s must skip the field entirely", chain)
                    .isTrue();
        }
    }

    /**
     * A repeated item ignored by its own rule (IGNORE_ALWAYS) skips embedded validation too:
     * the walk must not recurse into the element's message, mirroring the map-value case in
     * CollectionRulesTest.
     */
    @Test
    void ignoredRepeatedItemsSkipEmbeddedValidation() {
        FieldConstraints items = FieldConstraints.builder().ignore(IgnoreMode.ALWAYS).build();
        ValidationRuleSource ignoreItems = new FieldSource("widgets",
                FieldConstraints.builder()
                        .repeated(new RepeatedConstraints(
                                OptionalLong.empty(), OptionalLong.empty(), false, Optional.of(items)))
                        .build());
        ProtoValidator validator = ProtoValidator.create(
                List.of(new ProtomoltRuleSource(), ignoreItems));

        RepeatedGauntlet message = RepeatedGauntlet.newBuilder()
                .addTags("ab")
                .addWidgets(Widget.getDefaultInstance())
                .build();

        // Without the ignore, widgets[0].name would fail `required`.
        assertThat(validator.validate(message).violations())
                .noneMatch(v -> v.path().startsWith("widgets"));
    }
}
