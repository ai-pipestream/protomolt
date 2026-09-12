package ai.protomolt.proto.asset.bridge.parse;

import ai.protomolt.proto.asset.bridge.Bridge;

import java.util.List;

/**
 * What a bridge needs from a parser: bytes in, text out, with the parser's
 * own account of how it went.
 *
 * <p>A seam rather than a direct dependency on one client, because "text
 * extraction" and "OCR" are the same call to two different parsers, and a
 * host wires which parser answers for which. The shipped implementation
 * ({@link PluginExtraction}) speaks the {@code ParserPluginService}
 * contract; a host with an extractor that is not a gRPC parser implements
 * this instead.
 */
public interface ParserExtraction {

    /**
     * Extracts text from one asset.
     *
     * @param original the asset's bytes
     * @param context what the classification established about the asset
     * @return the result — a failure is a result, never an exception
     */
    Result extract(byte[] original, Bridge.Context context);

    /**
     * One extraction.
     *
     * @param text the recovered text; blank when the parser found none,
     *        which is a finding rather than a failure
     * @param parserName the parser's identity, for provenance
     * @param parserVersion the parser's build, for reparse auditability
     * @param engineConfidence the engine's own confidence, or
     *        {@link RecoveredTextMeasures#NO_ENGINE_CONFIDENCE}
     * @param warnings the parser's degradation notes, verbatim
     * @param detail why the extraction failed; blank when it did not
     */
    record Result(String text, String parserName, String parserVersion,
                  double engineConfidence, List<String> warnings, String detail) {

        /** Validates and copies. */
        public Result {
            warnings = List.copyOf(warnings);
        }

        /** A successful extraction. */
        public static Result of(String text, String parserName, String parserVersion,
                                List<String> warnings) {
            return new Result(text, parserName, parserVersion,
                    RecoveredTextMeasures.NO_ENGINE_CONFIDENCE, warnings, "");
        }

        /** A failed extraction, in the parser's own words. */
        public static Result failure(String detail) {
            return new Result("", "", "", RecoveredTextMeasures.NO_ENGINE_CONFIDENCE,
                    List.of(), detail);
        }

        /** Whether the parser failed to produce anything at all. */
        public boolean failed() {
            return !detail.isEmpty();
        }
    }
}
