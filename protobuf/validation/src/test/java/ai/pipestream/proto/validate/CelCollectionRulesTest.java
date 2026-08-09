package ai.pipestream.proto.validate;

import ai.pipestream.proto.validate.testdata.CollectionCelGauntlet;
import ai.pipestream.proto.validate.testdata.ItemCelGauntlet;
import ai.pipestream.proto.validate.testdata.NestedCelChild;
import ai.pipestream.proto.validate.testdata.NestedCelParent;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * CEL rules on collections bind the whole list/map, item CEL rules run per element with the
 * element path, and a nested message's own message rules are compiled in the nested scope.
 */
class CelCollectionRulesTest {

    private static final ProtoValidator VALIDATOR = ProtoValidator.create();

    private static void assertValid(Message message) {
        assertThat(VALIDATOR.validate(message).valid())
                .as("expected no violations, got %s", VALIDATOR.validate(message).violations())
                .isTrue();
    }

    @Test
    void repeatedFieldCelBindsTheWholeList() {
        assertValid(CollectionCelGauntlet.getDefaultInstance());
        assertValid(CollectionCelGauntlet.newBuilder().addNames("a").addNames("b").build());
        assertThat(VALIDATOR.validate(CollectionCelGauntlet.newBuilder()
                        .addNames("a").addNames("").build()).violations())
                .singleElement()
                .satisfies(v -> {
                    assertThat(v.path()).isEqualTo("names");
                    assertThat(v.ruleId()).isEqualTo("names.non_empty");
                });
    }

    @Test
    void mapFieldCelBindsTheWholeMap() {
        assertValid(CollectionCelGauntlet.newBuilder().putQuotas("a", 1).build());
        assertThat(VALIDATOR.validate(CollectionCelGauntlet.newBuilder()
                        .putQuotas("", 1).build()).violations())
                .singleElement()
                .satisfies(v -> {
                    assertThat(v.path()).isEqualTo("quotas");
                    assertThat(v.ruleId()).isEqualTo("quotas.keyed");
                });
    }

    @Test
    void itemCelRunsPerElementWithSubscriptPaths() {
        assertValid(ItemCelGauntlet.newBuilder().addOdds(1).addOdds(3).build());
        assertThat(VALIDATOR.validate(ItemCelGauntlet.newBuilder()
                        .addOdds(1).addOdds(4).addOdds(6).build()).violations())
                .anyMatch(v -> v.path().equals("odds[1]") && v.ruleId().equals("odds.odd"))
                .anyMatch(v -> v.path().equals("odds[2]") && v.ruleId().equals("odds.odd"));
    }

    @Test
    void nestedMessageCelSeesItsOwnFields() {
        // No child: the child field is absent, so its message rule never runs.
        assertValid(NestedCelParent.getDefaultInstance());
        assertValid(NestedCelParent.newBuilder()
                .setChild(NestedCelChild.newBuilder().setScore(10)).build());

        // The child defaults score to 0, failing its own message rule at the child's path.
        assertThat(VALIDATOR.validate(NestedCelParent.newBuilder()
                        .setChild(NestedCelChild.getDefaultInstance()).build()).violations())
                .singleElement()
                .satisfies(v -> {
                    assertThat(v.path()).isEqualTo("child");
                    assertThat(v.ruleId()).isEqualTo("child.score");
                });
        assertThat(VALIDATOR.validate(NestedCelParent.newBuilder()
                        .setChild(NestedCelChild.newBuilder().setScore(5)).build()).violations())
                .anyMatch(v -> v.path().equals("child") && v.ruleId().equals("child.score"));
    }
}
