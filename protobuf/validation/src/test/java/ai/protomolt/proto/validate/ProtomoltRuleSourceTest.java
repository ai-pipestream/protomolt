package ai.protomolt.proto.validate;

import ai.protomolt.proto.validate.model.FieldConstraints;
import ai.protomolt.proto.validate.model.MessageConstraints;
import ai.protomolt.proto.validate.source.ProtomoltRuleSource;
import ai.protomolt.proto.validate.testdata.DraftForm;
import ai.protomolt.proto.validate.testdata.Item;
import ai.protomolt.proto.validate.testdata.MultiNumericGauntlet;
import ai.protomolt.proto.validate.testdata.Person;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The built-in rule source read directly: fields/messages without annotations yield empty, the
 * translated model matches the declared options, and a field carrying several numeric rule sets
 * is reduced to one by the documented precedence (wider signed, then signed over unsigned).
 */
class ProtomoltRuleSourceTest {

    private static final ProtomoltRuleSource SOURCE = new ProtomoltRuleSource();
    private static final ProtoValidator VALIDATOR = ProtoValidator.create();

    @Test
    void unannotatedFieldAndMessageYieldEmpty() {
        assertThat(SOURCE.fieldConstraints(Item.getDescriptor().findFieldByName("name")))
                .isEmpty();
        assertThat(SOURCE.messageConstraints(Item.getDescriptor())).isEmpty();
    }

    @Test
    void fieldRulesTranslateToTheNeutralModel() {
        FieldConstraints name = SOURCE
                .fieldConstraints(Person.getDescriptor().findFieldByName("name"))
                .orElseThrow();

        assertThat(name.required()).isTrue();
        assertThat(name.string()).hasValueSatisfying(s -> {
            assertThat(s.minLen()).hasValue(2);
            assertThat(s.maxLen()).hasValue(40);
        });

        FieldConstraints email = SOURCE
                .fieldConstraints(Person.getDescriptor().findFieldByName("email"))
                .orElseThrow();
        assertThat(email.cel()).singleElement().satisfies(rule -> {
            assertThat(rule.id()).isEqualTo("email.not_localhost");
            assertThat(rule.expression()).isEqualTo("!this.endsWith('@localhost')");
            assertThat(rule.message()).isEqualTo("localhost emails are forbidden");
        });
    }

    @Test
    void messageRulesTranslateToTheNeutralModel() {
        MessageConstraints person = SOURCE.messageConstraints(Person.getDescriptor()).orElseThrow();
        assertThat(person.cel()).singleElement()
                .satisfies(rule -> assertThat(rule.id()).isEqualTo("adult.name"));
        assertThat(person.skipWhen()).isEmpty();

        MessageConstraints draft = SOURCE.messageConstraints(DraftForm.getDescriptor()).orElseThrow();
        assertThat(draft.skipWhen()).isEqualTo("incomplete");
    }

    @Test
    void sourceIdDefaultsToTheClassName() {
        assertThat(SOURCE.sourceId()).isEqualTo("ProtomoltRuleSource");
    }

    @Test
    void int64RulesWinOverInt32() {
        // both carries int32 { gt: 100 } and int64 { lt: 5 }; the wider signed set applies.
        assertThat(VALIDATOR.validate(MultiNumericGauntlet.newBuilder().setBoth(3).build()).valid())
                .as("int32.gt must be ignored; int64.lt passes for 3")
                .isTrue();
        assertThat(VALIDATOR.validate(MultiNumericGauntlet.newBuilder().setBoth(50).build())
                        .violations())
                .anyMatch(v -> v.path().equals("both") && v.ruleId().equals("int64.lt"))
                .noneMatch(v -> v.ruleId().startsWith("int32."));
    }

    @Test
    void signedRulesWinOverUnsigned() {
        // mixed carries int32 { lt: 5 } and uint64 { gt: 100 }; the signed set applies.
        assertThat(VALIDATOR.validate(MultiNumericGauntlet.newBuilder().setMixed(3).build()).valid())
                .isTrue();
        assertThat(VALIDATOR.validate(MultiNumericGauntlet.newBuilder().setMixed(50).build())
                        .violations())
                .anyMatch(v -> v.path().equals("mixed") && v.ruleId().equals("int32.lt"));
    }

    @Test
    void doubleRulesWinOverFloat() {
        // fp carries float { gt: 100 } and double { lt: 5 }; double applies.
        assertThat(VALIDATOR.validate(MultiNumericGauntlet.newBuilder().setFp(3.0).build()).valid())
                .isTrue();
        assertThat(VALIDATOR.validate(MultiNumericGauntlet.newBuilder().setFp(50.0).build())
                        .violations())
                .anyMatch(v -> v.path().equals("fp") && v.ruleId().equals("double.lt"))
                .noneMatch(v -> v.ruleId().startsWith("float."));
    }
}
