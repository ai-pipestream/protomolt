package ai.protomolt.proto.validate;

import ai.protomolt.proto.validate.model.FieldConstraints;
import ai.protomolt.proto.validate.model.MessageConstraints;
import ai.protomolt.proto.validate.source.AiPipestreamRuleSource;
import ai.protomolt.proto.validate.spi.ValidationRuleSource;
import ai.protomolt.proto.validate.testdata.Choice;
import ai.protomolt.proto.validate.testdata.SyntheticOneofGauntlet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Message-level oneof rules: a synthetic {@code oneof} over named fields (at most one populated,
 * optionally required) and a real protobuf oneof marked {@code required}. Synthetic-oneof members
 * are presence-tracked: their field-level rules only run while the member is populated.
 */
class MessageOneofRulesTest {

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

    /** The dialect plus a synthetic oneof over {@code alpha}/{@code beta}. */
    private static ProtoValidator syntheticOneofValidator(boolean required) {
        return ProtoValidator.create(List.of(
                new AiPipestreamRuleSource(),
                new MessageSource(new MessageConstraints(List.of(),
                        List.of(new MessageConstraints.Oneof(List.of("alpha", "beta"), required)),
                        List.of()))));
    }

    @Test
    void syntheticOneofMembersAreSkippedWhileUnpopulated() {
        // Without the oneof rule both implicit-presence strings are validated at their zero
        // value and fail min_len; the oneof membership makes them presence-tracked instead.
        assertThat(ProtoValidator.create().validate(SyntheticOneofGauntlet.getDefaultInstance())
                        .violations())
                .anyMatch(v -> v.path().equals("alpha") && v.ruleId().equals("string.min_len"))
                .anyMatch(v -> v.path().equals("beta") && v.ruleId().equals("string.min_len"));

        assertThat(syntheticOneofValidator(false)
                .validate(SyntheticOneofGauntlet.getDefaultInstance()).valid())
                .isTrue();
    }

    @Test
    void syntheticOneofAtMostOneMember() {
        ProtoValidator validator = syntheticOneofValidator(false);

        assertThat(validator.validate(SyntheticOneofGauntlet.newBuilder()
                        .setAlpha("ab").build()).valid())
                .isTrue();
        assertThat(validator.validate(SyntheticOneofGauntlet.newBuilder()
                        .setAlpha("ab").setBeta("cd").build()).violations())
                .anyMatch(v -> v.path().isEmpty() && v.ruleId().equals("message.oneof")
                        && v.message().equals("only one of alpha, beta can be set"));
    }

    @Test
    void syntheticOneofMemberRulesRunWhilePopulated() {
        ProtoValidator validator = syntheticOneofValidator(false);

        assertThat(validator.validate(SyntheticOneofGauntlet.newBuilder()
                        .setAlpha("a").build()).violations())
                .anyMatch(v -> v.path().equals("alpha") && v.ruleId().equals("string.min_len"));
    }

    @Test
    void syntheticOneofRequiredDemandsOneMember() {
        ProtoValidator validator = syntheticOneofValidator(true);

        assertThat(validator.validate(SyntheticOneofGauntlet.getDefaultInstance()).violations())
                .anyMatch(v -> v.path().isEmpty() && v.ruleId().equals("message.oneof")
                        && v.message().equals("one of alpha, beta must be set"));
        assertThat(validator.validate(SyntheticOneofGauntlet.newBuilder()
                        .setBeta("cd").build()).valid())
                .isTrue();
    }

    @Test
    void requiredRealOneofDemandsASetMember() {
        ProtoValidator validator = ProtoValidator.create(List.of(new MessageSource(
                new MessageConstraints(List.of(), List.of(), List.of("pick")))));

        assertThat(validator.validate(Choice.getDefaultInstance()).violations())
                .anyMatch(v -> v.path().equals("pick") && v.ruleId().equals("required")
                        && v.message().equals("exactly one field is required in oneof"));
        assertThat(validator.validate(Choice.newBuilder().setText("x").build()).valid()).isTrue();
        assertThat(validator.validate(Choice.newBuilder().setNumber(1).build()).valid()).isTrue();
    }

    @Test
    void requiredOneofNamingAPlainFieldIsACompilationError() {
        // alpha exists on SyntheticOneofGauntlet but is not a real oneof.
        ProtoValidator validator = ProtoValidator.create(List.of(new MessageSource(
                new MessageConstraints(List.of(), List.of(), List.of("alpha")))));

        assertThatThrownBy(() -> validator.validate(SyntheticOneofGauntlet.getDefaultInstance()))
                .isInstanceOf(RuleCompilationException.class)
                .hasMessageContaining("alpha");
    }
}
