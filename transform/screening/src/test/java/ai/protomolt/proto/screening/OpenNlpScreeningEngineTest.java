package ai.protomolt.proto.screening;

import opennlp.tools.dictionary.Dictionary;
import opennlp.tools.namefind.DictionaryNameFinder;
import opennlp.tools.util.StringList;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The OpenNLP engine over a dictionary token-name finder: a real
 * {@code TokenNameFinder} that needs no model artifact, which is the point —
 * model artifacts are data, never bundled in-tree, so the token-to-character
 * offset mapping is proven here and the thin model-loading path stays a
 * mount-time concern.
 */
class OpenNlpScreeningEngineTest {

    private static OpenNlpScreeningEngine engine() {
        Dictionary names = new Dictionary();
        names.put(new StringList("Ada", "Lovelace"));
        names.put(new StringList("Babbage"));
        return new OpenNlpScreeningEngine(
                new DictionaryNameFinder(names, "person"), "dictionary-fixture-1");
    }

    @Test
    void detectionsMapTokenSpansBackToCharacterOffsets() {
        String text = "Report prepared by Ada Lovelace for review";

        var detections = engine().detect(text);

        assertThat(detections).singleElement().satisfies(d -> {
            assertThat(d.type()).isEqualTo("person");
            assertThat(text.substring(d.begin(), d.end())).isEqualTo("Ada Lovelace");
        });
    }

    @Test
    void multipleDetectionsArriveInOffsetOrder() {
        String text = "Babbage wrote to Ada Lovelace";

        var detections = engine().detect(text);

        assertThat(detections).hasSize(2);
        assertThat(text.substring(detections.get(0).begin(), detections.get(0).end()))
                .isEqualTo("Babbage");
        assertThat(text.substring(detections.get(1).begin(), detections.get(1).end()))
                .isEqualTo("Ada Lovelace");
    }

    @Test
    void blankTextAndUnmatchedTextDetectNothing() {
        assertThat(engine().detect("")).isEmpty();
        assertThat(engine().detect("   ")).isEmpty();
        assertThat(engine().detect("no names in this text")).isEmpty();
    }

    @Test
    void anEngineWithoutAModelVersionRefuses() {
        Dictionary names = new Dictionary();
        assertThatThrownBy(() -> new OpenNlpScreeningEngine(
                new DictionaryNameFinder(names, "person"), " "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not auditable");
    }
}
