package ai.protomolt.proto.asset.bridge.parse;

import ai.protomolt.proto.asset.v1.QualityDimension;
import ai.protomolt.proto.asset.v1.QualityScore;
import ai.protomolt.proto.asset.v1.RecoveredText;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Measuring recovered text, and the score the schema's own dimensions give
 * it. The point of the split is that what "good OCR" means lives on the
 * descriptor, so these tests pin the counts and let the declaration decide
 * what they are worth.
 */
class RecoveredTextMeasuresTest {

    @Test
    void cleanProseCountsAsCleanProse() {
        String clean = """
                The quick brown fox jumps over the lazy dog every morning.
                Meanwhile the patient archivist records every single page.
                Nothing in this paragraph looks like a failed recognition.
                """;

        RecoveredText measured = RecoveredTextMeasures.measure(clean,
                RecoveredTextMeasures.NO_ENGINE_CONFIDENCE);

        assertThat(measured.getLineCount()).isEqualTo(3);
        assertThat(measured.getStubLineCount()).isZero();
        assertThat(measured.getTextCharacters()).isEqualTo(measured.getCharacterCount());
        assertThat(measured.getWellFormedWords())
                .isGreaterThan(measured.getWordCount() * 8 / 10);
        assertThat(RecoveredTextMeasures.score(measured).getScore())
                .isGreaterThan(0.9);
    }

    @Test
    void aFailedPassScoresBadlyBecauseOfWhatItLeavesBehind() {
        // The shapes a failed recognition produces: replacement characters,
        // one-character confetti, and fragment lines.
        String mess = "�� 1l|\n. ~ 3\n�@ x\n|| ,,\n";

        RecoveredText measured = RecoveredTextMeasures.measure(mess,
                RecoveredTextMeasures.NO_ENGINE_CONFIDENCE);

        assertThat(measured.getTextCharacters()).isLessThan(measured.getCharacterCount());
        assertThat(measured.getWellFormedWords()).isZero();
        assertThat(measured.getStubLineCount()).isEqualTo(measured.getLineCount());
        // The two dimensions that say whether a document came back are both
        // zero, so whatever legibility survives cannot carry the composite.
        QualityScore score = RecoveredTextMeasures.score(measured);
        assertThat(dimension(score, "word-shape")).isZero();
        assertThat(dimension(score, "layout-coherence")).isZero();
        assertThat(score.getScore()).isLessThan(0.25);
    }

    @Test
    void theScoreCarriesEveryDeclaredDimensionWithItsWeight() {
        QualityScore score = RecoveredTextMeasures.score(RecoveredTextMeasures.measure(
                "A perfectly ordinary sentence of recovered prose here.\n",
                RecoveredTextMeasures.NO_ENGINE_CONFIDENCE));

        assertThat(score.getDimensionsList()).extracting(QualityDimension::getName)
                .containsExactlyInAnyOrder(
                        "legible-characters", "word-shape", "layout-coherence");
        assertThat(score.getDimensionsList()).allSatisfy(dimension -> {
            assertThat(dimension.getScore()).isBetween(0.0, 1.0);
            assertThat(dimension.getWeight()).isBetween(0.0, 1.0);
        });
        // The weights come from the same declaration the scorer read.
        assertThat(weight(score, "legible-characters")).isEqualTo(0.3, within(1e-9));
        assertThat(weight(score, "word-shape")).isEqualTo(0.5, within(1e-9));
        assertThat(weight(score, "layout-coherence")).isEqualTo(0.2, within(1e-9));
    }

    @Test
    void emptyTextScoresZeroWithoutDividingByNothing() {
        RecoveredText measured = RecoveredTextMeasures.measure("",
                RecoveredTextMeasures.NO_ENGINE_CONFIDENCE);

        assertThat(measured.getCharacterCount()).isZero();
        assertThat(RecoveredTextMeasures.score(measured).getScore()).isZero();
    }

    @Test
    void anEnginesOwnConfidenceIsKeptAsProvenanceAndNotScored() {
        RecoveredText measured = RecoveredTextMeasures.measure("Legible enough prose.", 0.93);

        assertThat(measured.getEngineConfidence()).isEqualTo(0.93);
        // Most engines report nothing, and a dimension that scored silence
        // as zero would punish the output for the engine's reticence.
        assertThat(RecoveredTextMeasures.score(measured).getDimensionsList())
                .extracting(QualityDimension::getName)
                .doesNotContain("engine-confidence");
    }

    @Test
    void tokenAndLineSplittingKeepTheRegexSemantics() {
        // Every regex whitespace form separates tokens: space, tab, newline,
        // vertical tab, form feed, carriage return. Runs collapse, and
        // leading whitespace yields no token.
        RecoveredText vertical = RecoveredTextMeasures.measure(
                "alpha\u000Bbravo\fcharlie\rdelta echo",
                RecoveredTextMeasures.NO_ENGINE_CONFIDENCE);
        assertThat(vertical.getWordCount()).isEqualTo(5);
        assertThat(vertical.getWellFormedWords()).isEqualTo(5);

        // A trailing newline leaves a blank tail line that is not counted,
        // and a CR is ordinary line content (stripped for the stub check).
        RecoveredText lines = RecoveredTextMeasures.measure(
                "one line of plain recovered prose here\nshort\r\n",
                RecoveredTextMeasures.NO_ENGINE_CONFIDENCE);
        assertThat(lines.getLineCount()).isEqualTo(2);
        assertThat(lines.getStubLineCount()).isEqualTo(1);
    }

    @Test
    void edgePunctuationStrippingUsesTheAsciiPunctuationSet() {
        // Punctuation hanging off either end does not cost a word its shape;
        // a token of nothing but edge punctuation has no word inside it, and
        // non-ASCII punctuation is not in the set, so it stays and counts.
        RecoveredText measured = RecoveredTextMeasures.measure(
                "\"hello,\" [world] --dash-- ... \u2014ab\u2014",
                RecoveredTextMeasures.NO_ENGINE_CONFIDENCE);
        assertThat(measured.getWordCount()).isEqualTo(5);
        assertThat(measured.getWellFormedWords()).isEqualTo(3);
    }

    private static double dimension(QualityScore score, String name) {
        return score.getDimensionsList().stream()
                .filter(item -> item.getName().equals(name))
                .mapToDouble(QualityDimension::getScore)
                .findFirst().orElseThrow();
    }

    private static double weight(QualityScore score, String name) {
        return score.getDimensionsList().stream()
                .filter(dimension -> dimension.getName().equals(name))
                .mapToDouble(QualityDimension::getWeight)
                .findFirst().orElseThrow();
    }
}
