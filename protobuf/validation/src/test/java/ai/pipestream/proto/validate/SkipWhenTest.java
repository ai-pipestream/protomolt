package ai.pipestream.proto.validate;

import ai.pipestream.proto.validate.model.FieldConstraints;
import ai.pipestream.proto.validate.model.MessageConstraints;
import ai.pipestream.proto.validate.spi.ValidationRuleSource;
import ai.pipestream.proto.validate.testdata.DraftChild;
import ai.pipestream.proto.validate.testdata.DraftForm;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The skip-when escape channel: a message whose declared boolean flag is true skips every
 * field-level rule (including recursion into nested messages) while message-level CEL rules
 * keep evaluating, and a skip_when target that is missing or not a singular boolean fails
 * rule compilation rather than being silently ignored.
 */
class SkipWhenTest {

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
    void declaredIncompleteSkipsFieldRules() {
        DraftForm form = DraftForm.newBuilder()
                .setIncomplete(true)
                .setIncompleteReason("source document too damaged to read")
                .build();

        // name is required and topics needs min_items 1; both are suspended by the declaration.
        assertThat(ProtoValidator.create().validate(form).valid()).isTrue();
    }

    @Test
    void messageCelStillFiresWhileFieldRulesAreSkipped() {
        // Declared incomplete without the required reason: the message CEL rule must fire,
        // and no field-level violation may accompany it.
        DraftForm form = DraftForm.newBuilder().setIncomplete(true).build();

        assertThat(ProtoValidator.create().validate(form).violations())
                .singleElement()
                .satisfies(v -> assertThat(v.ruleId()).isEqualTo("incomplete.needs_reason"));
    }

    @Test
    void undeclaredFormGetsNormalFieldRules() {
        // incomplete is false, so the required name and the min_items rule apply as usual.
        assertThat(ProtoValidator.create().validate(DraftForm.getDefaultInstance()).violations())
                .anyMatch(v -> v.ruleId().equals("required") && v.path().equals("name"))
                .anyMatch(v -> v.ruleId().equals("repeated.min_items"));
    }

    @Test
    void skipSuspendsRecursionIntoNestedMessages() {
        // The child's required field and its always-failing message CEL are unreachable while
        // the parent declares incompleteness.
        DraftForm skipped = DraftForm.newBuilder()
                .setIncomplete(true)
                .setIncompleteReason("child not applicable")
                .setChild(DraftChild.getDefaultInstance())
                .build();
        assertThat(ProtoValidator.create().validate(skipped).valid()).isTrue();

        // Without the declaration the walk reaches the child and both of its rules fire.
        DraftForm walked = DraftForm.newBuilder()
                .setName("ab")
                .addTopics("x")
                .setChild(DraftChild.getDefaultInstance())
                .build();
        assertThat(ProtoValidator.create().validate(walked).violations())
                .anyMatch(v -> v.ruleId().equals("required") && v.path().equals("child.value"))
                .anyMatch(v -> v.ruleId().equals("child.always_fails"));
    }

    @Test
    void unknownSkipWhenFieldIsACompilationError() {
        ProtoValidator validator = ProtoValidator.create(List.of(new MessageSource(
                new MessageConstraints(List.of(), List.of(), List.of(), "no_such_field"))));

        assertThatThrownBy(() -> validator.validate(DraftForm.getDefaultInstance()))
                .isInstanceOf(RuleCompilationException.class)
                .hasMessageContaining("no_such_field");
    }

    @Test
    void nonBooleanSkipWhenFieldIsACompilationError() {
        // `name` exists but is a string; `topics` exists but is repeated. Both are schema errors.
        for (String target : List.of("name", "topics")) {
            ProtoValidator validator = ProtoValidator.create(List.of(new MessageSource(
                    new MessageConstraints(List.of(), List.of(), List.of(), target))));

            assertThatThrownBy(() -> validator.validate(DraftForm.getDefaultInstance()))
                    .isInstanceOf(RuleCompilationException.class)
                    .hasMessageContaining("singular boolean");
        }
    }
}
