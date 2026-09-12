package ai.protomolt.proto.asset.bridge.parse;

import ai.protomolt.proto.asset.v1.QualityDimension;
import ai.protomolt.proto.asset.v1.QualityScore;
import ai.protomolt.proto.asset.v1.RecoveredText;
import ai.protomolt.proto.quality.QualityProto;
import ai.protomolt.proto.quality.QualityReport;
import ai.protomolt.proto.quality.QualityScorer;

import java.util.Map;

/**
 * Measuring recovered text, and scoring what was measured.
 *
 * <p>The split is deliberate. This class only counts: characters,
 * text-bearing characters, tokens, word-shaped tokens, lines, stub lines.
 * What those counts are worth is declared on {@link RecoveredText} as CEL
 * dimensions
 * and computed by the quality family's scorer, so the definition of "good
 * OCR" is reviewable schema rather than a number some code decided.
 */
public final class RecoveredTextMeasures {

    /** Shortest token that can be shaped like a word. */
    private static final int MIN_WORD_LENGTH = 2;

    /** Longest token that is plausibly one word rather than run-together noise. */
    private static final int MAX_WORD_LENGTH = 30;

    /** Share of a token's characters that must be letters for it to read as a word. */
    private static final double WORD_LETTER_SHARE = 0.6;

    /** Shortest line that reads as prose rather than a fragment. */
    private static final int MIN_PROSE_LINE = 20;

    /** The punctuation prose is made of, as opposed to symbol confetti. */
    private static final String SENTENCE_PUNCTUATION = ".,;:!?'\"()-\u2019\u201c\u201d\u2014";

    /** The engine reported nothing. */
    public static final double NO_ENGINE_CONFIDENCE = -1.0;

    private static final QualityScorer SCORER = QualityScorer.create();

    private RecoveredTextMeasures() {
    }

    /**
     * Counts what one OCR pass left behind.
     *
     * @param text the recovered text
     * @param engineConfidence the engine's own confidence, or
     *        {@link #NO_ENGINE_CONFIDENCE} when it reports none
     * @return the measurements
     */
    public static RecoveredText measure(String text, double engineConfidence) {
        long textBearing = text.codePoints().filter(RecoveredTextMeasures::textBearing).count();
        long lines = 0;
        long stubLines = 0;
        for (String line : text.split("\n", -1)) {
            if (line.isBlank()) {
                continue;
            }
            lines++;
            if (line.strip().length() < MIN_PROSE_LINE) {
                stubLines++;
            }
        }
        long words = 0;
        long wellFormed = 0;
        for (String token : text.split("\\s+")) {
            if (token.isEmpty()) {
                continue;
            }
            words++;
            if (wordShaped(token)) {
                wellFormed++;
            }
        }
        return RecoveredText.newBuilder()
                .setCharacterCount(text.length())
                .setTextCharacters(textBearing)
                .setWordCount(words)
                .setWellFormedWords(wellFormed)
                .setLineCount(lines)
                .setStubLineCount(stubLines)
                .setEngineConfidence(engineConfidence)
                .build();
    }

    /**
     * Scores measurements through the dimensions the message declares.
     *
     * @param measured the measurements
     * @return the score, in the asset contract's shape
     */
    public static QualityScore score(RecoveredText measured) {
        QualityReport report = SCORER.score(measured);
        QualityScore.Builder score = QualityScore.newBuilder().setScore(report.composite());
        for (Map.Entry<String, Double> dimension : report.dimensions().entrySet()) {
            score.addDimensions(QualityDimension.newBuilder()
                    .setName(dimension.getKey())
                    .setScore(dimension.getValue())
                    // The declared weights live on the descriptor; the report
                    // carries the scores, and the asset contract wants both,
                    // so the weight is read back from the same declaration.
                    .setWeight(declaredWeight(dimension.getKey())));
        }
        return score.build();
    }

    /** The weight the schema declares for one dimension. */
    private static double declaredWeight(String id) {
        return RecoveredText.getDescriptor().getOptions()
                .getExtension(QualityProto.quality)
                .getDimensionList().stream()
                .filter(dimension -> dimension.getId().equals(id))
                .mapToDouble(dimension -> dimension.getWeight() == 0
                        ? 1.0 : dimension.getWeight())
                .findFirst()
                .orElse(1.0);
    }

    /**
     * Text-bearing: a letter, a digit, ordinary spacing, or the punctuation
     * sentences are made of. Deliberately narrower than "decodable" — the
     * replacement character decodes fine and means a byte nobody read, and a
     * page of loose pipes and tildes is what a failed pass emits, so neither
     * counts as text.
     */
    private static boolean textBearing(int codePoint) {
        if (codePoint == ' ' || codePoint == '\n' || codePoint == '\t') {
            return true;
        }
        if (Character.isLetterOrDigit(codePoint)) {
            return true;
        }
        return SENTENCE_PUNCTUATION.indexOf(codePoint) >= 0;
    }

    /** Word-shaped: plausible length, mostly letters. */
    private static boolean wordShaped(String token) {
        String trimmed = token.replaceAll("^[\\p{Punct}]+|[\\p{Punct}]+$", "");
        if (trimmed.length() < MIN_WORD_LENGTH || trimmed.length() > MAX_WORD_LENGTH) {
            return false;
        }
        long letters = trimmed.codePoints().filter(Character::isLetter).count();
        return (double) letters / trimmed.length() >= WORD_LETTER_SHARE;
    }
}
