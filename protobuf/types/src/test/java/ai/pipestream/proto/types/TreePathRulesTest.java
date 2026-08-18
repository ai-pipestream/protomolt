package ai.pipestream.proto.types;

import static org.assertj.core.api.Assertions.assertThat;

import ai.pipestream.proto.validate.ProtoValidator;
import ai.pipestream.proto.validate.ValidationResult;
import com.google.protobuf.Message;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The taxonomy-path invariants ride the schema: at least one segment, each
 * non-empty and free of the "/" render delimiter — the delimiter rule is what
 * keeps ["a/b"] and ["a", "b"] from rendering identically. Segments may
 * repeat: "a/b/a" is a legal path, so there is deliberately no uniqueness
 * rule.
 */
class TreePathRulesTest {

    private static final ProtoValidator VALIDATOR = ProtoValidator.create();

    private static List<String> ruleIds(Message message) {
        return VALIDATOR.validate(message).violations().stream()
                .map(ValidationResult.Violation::ruleId)
                .toList();
    }

    @Test
    void wellFormedPathsPass() {
        assertThat(ruleIds(TreePath.newBuilder()
                .addSegments("electronics").build())).isEmpty();
        assertThat(ruleIds(TreePath.newBuilder()
                .addSegments("electronics").addSegments("audio").addSegments("headphones")
                .build())).isEmpty();
    }

    @Test
    void repeatedSegmentsAreLegalPaths() {
        assertThat(ruleIds(TreePath.newBuilder()
                .addSegments("a").addSegments("b").addSegments("a").build())).isEmpty();
    }

    @Test
    void anEmptyPathIsRefused() {
        assertThat(ruleIds(TreePath.getDefaultInstance()))
                .containsExactly("repeated.min_items");
    }

    @Test
    void anEmptySegmentIsRefused() {
        assertThat(ruleIds(TreePath.newBuilder()
                .addSegments("electronics").addSegments("").build()))
                .containsExactly("string.min_len");
    }

    @Test
    void aSegmentContainingTheRenderDelimiterIsRefused() {
        assertThat(ruleIds(TreePath.newBuilder()
                .addSegments("electronics/audio").build()))
                .containsExactly("string.not_contains");
    }
}
