package ai.pipestream.proto.validate;

import ai.pipestream.proto.validate.testdata.MapGauntlet;
import ai.pipestream.proto.validate.testdata.RepeatedGauntlet;
import ai.pipestream.proto.validate.testdata.Widget;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Repeated and map rules, including per-element item rules and nested recursion. */
class CollectionRulesTest {

    private static final ProtoValidator VALIDATOR = ProtoValidator.create();

    private static void assertViolation(Message message, String path, String ruleId) {
        assertThat(VALIDATOR.validate(message).violations())
                .as("expected %s at %s", ruleId, path)
                .anyMatch(v -> v.path().equals(path) && v.ruleId().equals(ruleId));
    }

    private static void assertNoViolation(Message message, String ruleId) {
        assertThat(VALIDATOR.validate(message).violations())
                .noneMatch(v -> v.ruleId().equals(ruleId));
    }

    private static RepeatedGauntlet.Builder withValidTags() {
        return RepeatedGauntlet.newBuilder().addTags("ab");
    }

    @Test
    void minItemsAppliesToEmptyCollections() {
        assertViolation(RepeatedGauntlet.getDefaultInstance(), "tags", "repeated.min_items");
    }

    @Test
    void maxItems() {
        assertNoViolation(withValidTags().build(), "repeated.max_items");
        assertViolation(RepeatedGauntlet.newBuilder()
                        .addAllTags(List.of("aa", "bb", "cc", "dd")).build(),
                "tags", "repeated.max_items");
    }

    @Test
    void uniqueFlagsDuplicateElements() {
        assertViolation(RepeatedGauntlet.newBuilder().addTags("ab").addTags("ab").build(),
                "tags[1]", "repeated.unique");
        assertNoViolation(RepeatedGauntlet.newBuilder().addTags("ab").addTags("cd").build(),
                "repeated.unique");
    }

    @Test
    void itemRulesRunPerElement() {
        assertViolation(RepeatedGauntlet.newBuilder().addTags("ab").addTags("x").build(),
                "tags[1]", "string.min_len");
        assertViolation(withValidTags().addPorts(8080).addPorts(80).build(),
                "ports[1]", "int32.gte_lte");
        assertViolation(withValidTags().addPorts(70000).build(),
                "ports[0]", "int32.gte_lte");
        assertNoViolation(withValidTags().addPorts(8080).build(), "int32.gte_lte");
    }

    @Test
    void repeatedMessageElementsAreRecursed() {
        assertViolation(withValidTags().addWidgets(Widget.getDefaultInstance()).build(),
                "widgets[0].name", "required");
        assertNoViolation(withValidTags()
                        .addWidgets(Widget.newBuilder().setName("gear")).build(),
                "required");
    }

    @Test
    void minPairsAppliesToEmptyMaps() {
        assertViolation(MapGauntlet.getDefaultInstance(), "scores", "map.min_pairs");
    }

    @Test
    void maxPairs() {
        assertViolation(MapGauntlet.newBuilder()
                        .putScores("aa", 1).putScores("bb", 2).putScores("cc", 3).build(),
                "scores", "map.max_pairs");
    }

    @Test
    void keyRulesReportWithKeySuffix() {
        assertViolation(MapGauntlet.newBuilder().putScores("a", 1).build(),
                "scores[\"a\"]#key", "string.min_len");
    }

    @Test
    void valueRulesReportAtEntryPath() {
        assertViolation(MapGauntlet.newBuilder().putScores("ab", -1).build(),
                "scores[\"ab\"]", "int32.gte");
        assertNoViolation(MapGauntlet.newBuilder().putScores("ab", 1).build(), "int32.gte");
    }

    @Test
    void mapMessageValuesAreRecursed() {
        assertViolation(MapGauntlet.newBuilder()
                        .putScores("ab", 1)
                        .putParts("ab", Widget.getDefaultInstance()).build(),
                "parts[\"ab\"].name", "required");
    }
}
