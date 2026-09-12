package ai.protomolt.proto.asset.bridge.parse;

import ai.protomolt.proto.asset.bridge.Bridge;
import ai.protomolt.proto.parse.document.DoclingProjection;
import ai.protomolt.proto.parse.plugin.v1.DocumentClaims;
import ai.protomolt.proto.parse.plugin.v1.ParseOptions;
import ai.protomolt.proto.parse.service.ParserClient;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;

import java.time.Duration;
import java.util.List;

/**
 * Extraction over the {@code ParserPluginService} contract, driven by the
 * coordinator's own client — one client for the contract, so a bridge and a
 * parse job speak to a parser identically.
 *
 * <p>Where the text comes from, in order: the parser's {@code body} claim
 * (the doc-level field every parser in the fleet fills), then the page
 * events it streamed. A parser that emits neither recovered nothing, and
 * that is reported as a finding rather than patched over.
 */
public final class PluginExtraction implements ParserExtraction {

    /** The doc-level claim carrying a parsed document's prose. */
    private static final String BODY = DoclingProjection.FIELD_BODY;

    private final ParserClient parser;
    private final Duration deadline;

    /**
     * Extraction through one parser.
     *
     * @param parser the parser client; the channel is the caller's to close
     * @param deadline per-extraction deadline
     */
    public PluginExtraction(ParserClient parser, Duration deadline) {
        if (parser == null) {
            throw new IllegalArgumentException("parser must not be null");
        }
        if (deadline == null || deadline.isZero() || deadline.isNegative()) {
            throw new IllegalArgumentException("deadline must be positive");
        }
        this.parser = parser;
        this.deadline = deadline;
    }

    @Override
    public Result extract(byte[] original, Bridge.Context context) {
        ParseOptions.Builder options = ParseOptions.newBuilder().setEmitPages(true);
        if (context.filename() != null) {
            options.setFilename(context.filename());
        }
        ParserClient.ParseOutcome outcome = parser.parse(options.build(), original, deadline);
        if (outcome.failed()) {
            return Result.failure(outcome.error());
        }
        String name = "";
        String version = "";
        try {
            name = parser.info().getParserName();
            version = parser.info().getParserVersion();
        } catch (RuntimeException unreachable) {
            // Identity is provenance, not correctness: a parser that answered
            // the parse but not GetParserInfo still produced text.
        }
        return Result.of(textOf(outcome), name, version,
                outcome.output().getWarningsList());
    }

    /** The body claim when the parser made one; the page texts otherwise. */
    private static String textOf(ParserClient.ParseOutcome outcome) {
        String claimed = claimedBody(outcome);
        if (!claimed.isBlank()) {
            return claimed;
        }
        Struct fields = outcome.output().getDocument().getExtractedFields();
        Value body = fields.getFieldsMap().get(BODY);
        if (body != null && !body.getStringValue().isBlank()) {
            return body.getStringValue();
        }
        return String.join("\n", outcome.pageTexts());
    }

    /** Later claims replace earlier ones per key, per the plugin contract. */
    private static String claimedBody(ParserClient.ParseOutcome outcome) {
        String body = "";
        List<DocumentClaims> claims = outcome.claims();
        for (DocumentClaims claim : claims) {
            Value value = claim.getClaims().getFieldsMap().get(BODY);
            if (value != null && !value.getStringValue().isBlank()) {
                body = value.getStringValue();
            }
        }
        return body;
    }
}
