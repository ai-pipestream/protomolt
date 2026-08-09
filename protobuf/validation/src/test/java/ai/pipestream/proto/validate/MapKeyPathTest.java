package ai.pipestream.proto.validate;

import ai.pipestream.proto.validate.testdata.IntMapGauntlet;
import ai.pipestream.proto.validate.testdata.MapGauntlet;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Violation paths for map entries: string keys are quoted with backslash/quote escaping so the
 * path round-trips unambiguously, and non-string keys render as bare subscripts.
 */
class MapKeyPathTest {

    private static final ProtoValidator VALIDATOR = ProtoValidator.create();

    private static void assertViolation(Message message, String path, String ruleId) {
        assertThat(VALIDATOR.validate(message).violations())
                .as("expected %s at %s", ruleId, path)
                .anyMatch(v -> v.path().equals(path) && v.ruleId().equals(ruleId));
    }

    @Test
    void stringKeysWithQuotesAreEscaped() {
        // The one-character key `"` fails the min_len key rule; its path must escape the quote.
        assertViolation(MapGauntlet.newBuilder().putScores("\"", 1).build(),
                "scores[\"\\\"\"]#key", "string.min_len");
    }

    @Test
    void stringKeysWithBackslashesAreEscaped() {
        assertViolation(MapGauntlet.newBuilder().putScores("\\", 1).build(),
                "scores[\"\\\\\"]#key", "string.min_len");
    }

    @Test
    void intKeysRenderAsBareSubscripts() {
        assertViolation(IntMapGauntlet.newBuilder().putPrefs(3, "ab").build(),
                "prefs[3]#key", "int32.gte");
        assertViolation(IntMapGauntlet.newBuilder().putPrefs(10, "a").build(),
                "prefs[10]", "string.min_len");
        assertThat(VALIDATOR.validate(IntMapGauntlet.newBuilder().putPrefs(10, "ab").build())
                        .valid())
                .isTrue();
    }
}
