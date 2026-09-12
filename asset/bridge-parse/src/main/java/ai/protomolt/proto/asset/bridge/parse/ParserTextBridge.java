package ai.protomolt.proto.asset.bridge.parse;

import ai.protomolt.proto.asset.bridge.Bridge;
import ai.protomolt.proto.asset.v1.BridgeKind;
import ai.protomolt.proto.asset.v1.ContentClass;
import ai.protomolt.proto.asset.v1.ContentProfile;
import ai.protomolt.proto.asset.v1.FormatFact;
import ai.protomolt.proto.asset.v1.RecoveredText;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

/**
 * The bridges that ride a parser: prose out of a document, and text
 * recovered from a scan.
 *
 * <p>Both run the same wire idiom — the {@code ParserPluginService}
 * contract, driven by the coordinator's own client — and differ in what
 * they claim about the result. Prose extraction claims informational text
 * and measures nothing; OCR claims recovered text and must measure its
 * quality, because the content contract refuses an OCR profile without a
 * score.
 *
 * <p>Neither bridge invents an extraction. A parser that fails, or that
 * completes without emitting a document, fails the bridge with the
 * parser's own words; a parser that emits a document with no text produces
 * an empty extraction that says so, which is what lets a scanned PDF be
 * recognized as scanned rather than assumed to be.
 */
public final class ParserTextBridge implements Bridge {

    /**
     * The finding a scanned document produces: the caller escalates to OCR
     * on this, so it is a named constant rather than prose in a log.
     */
    public static final String NO_PROSE = "the parser recovered no prose from this document";

    private final BridgeKind kind;
    private final Set<FormatFact.FormatCase> formats;
    private final ParserExtraction extraction;

    private ParserTextBridge(BridgeKind kind, Set<FormatFact.FormatCase> formats,
                             ParserExtraction extraction) {
        this.kind = kind;
        this.formats = formats;
        this.extraction = extraction;
    }

    /**
     * The prose bridge: PDF, word-processing, and HTML documents to
     * {@code text}.
     *
     * @param extraction the parser this bridge calls
     * @return the bridge
     */
    public static ParserTextBridge prose(ParserExtraction extraction) {
        return new ParserTextBridge(BridgeKind.BRIDGE_KIND_DOCUMENT_TEXT,
                Set.of(FormatFact.FormatCase.PDF, FormatFact.FormatCase.WORD,
                        FormatFact.FormatCase.HTML),
                extraction);
    }

    /**
     * The OCR bridge: raster images, and scanned PDFs the prose bridge
     * found nothing in, to {@code ocr-text}.
     *
     * @param extraction the parser this bridge calls
     * @return the bridge
     */
    public static ParserTextBridge ocr(ParserExtraction extraction) {
        return new ParserTextBridge(BridgeKind.BRIDGE_KIND_OCR_TEXT,
                Set.of(FormatFact.FormatCase.IMAGE, FormatFact.FormatCase.PDF),
                extraction);
    }

    @Override
    public BridgeKind kind() {
        return kind;
    }

    @Override
    public boolean handles(FormatFact format) {
        return formats.contains(format.getFormatCase());
    }

    @Override
    public Derivation derive(InputStream original, Context context) throws IOException {
        ParserExtraction.Result result = extraction.extract(original.readAllBytes(), context);
        if (result.failed()) {
            throw new IOException(result.detail());
        }
        return kind == BridgeKind.BRIDGE_KIND_DOCUMENT_TEXT
                ? prose(result) : recovered(result);
    }

    /** Prose: a class, no score, and a loud note when nothing came out. */
    private static Derivation prose(ParserExtraction.Result result) {
        ContentProfile profile = ContentProfile.newBuilder()
                .setContentClass(ContentClass.CONTENT_CLASS_INFORMATIONAL_TEXT)
                .build();
        // A document with no prose is the finding a scan produces. Naming it
        // is what lets the caller escalate to OCR instead of guessing.
        return Derivation.ofText(result.text(), profile,
                result.text().isBlank()
                        ? join(result.warnings(), NO_PROSE)
                        : result.warnings());
    }

    /** Recovered text: measured, then scored through the declared dimensions. */
    private static Derivation recovered(ParserExtraction.Result result) {
        RecoveredText measured = RecoveredTextMeasures.measure(
                result.text(), result.engineConfidence());
        ContentProfile profile = ContentProfile.newBuilder()
                .setContentClass(ContentClass.CONTENT_CLASS_OCR_TEXT)
                .setQuality(RecoveredTextMeasures.score(measured))
                .build();
        return Derivation.ofText(result.text(), profile,
                result.text().isBlank()
                        ? join(result.warnings(), "the parser recovered no text from this scan")
                        : result.warnings());
    }

    private static List<String> join(List<String> warnings, String added) {
        return Stream.concat(warnings.stream(), Stream.of(added)).toList();
    }
}
