package ai.protomolt.proto.parse.grparse;

import java.time.Duration;

/**
 * Configuration of one {@link GrparseParserAdapter}.
 *
 * @param parserVersion the parser build version {@code GetParserInfo}
 *        advertises; auditability for reparse and migration
 * @param maxDocumentBytes per-document byte cap; larger payloads are
 *        RESOURCE_EXHAUSTED before anything reaches gRParse
 * @param deadline per-parse deadline on the gRParse stream; a slow engine
 *        fails the plugin stream instead of hanging it
 * @param emitsPreviews whether {@code GetParserInfo} advertises previews.
 *        A deployment fact, not a request: the streaming wire carries no
 *        processing options, so page images arrive exactly when the
 *        gRParse fleet is built to render them into
 *        {@code PageData.page_meta.image}; set this true for such a fleet
 */
public record GrparseAdapterOptions(
        String parserVersion, long maxDocumentBytes, Duration deadline, boolean emitsPreviews) {

    /** The default advertised parser version. */
    public static final String DEFAULT_PARSER_VERSION = "1.0.0";

    /** The default per-document byte cap: 256 MiB. */
    public static final long DEFAULT_MAX_DOCUMENT_BYTES = 256L * 1024 * 1024;

    /** The default per-parse deadline on the gRParse stream. */
    public static final Duration DEFAULT_DEADLINE = Duration.ofMinutes(10);

    /**
     * The historical three-field form: previews not advertised.
     *
     * @param parserVersion the advertised parser build version
     * @param maxDocumentBytes per-document byte cap
     * @param deadline per-parse deadline on the gRParse stream
     */
    public GrparseAdapterOptions(String parserVersion, long maxDocumentBytes, Duration deadline) {
        this(parserVersion, maxDocumentBytes, deadline, false);
    }

    /**
     * Validates the configuration; every field is required.
     */
    public GrparseAdapterOptions {
        if (parserVersion == null || parserVersion.isBlank()) {
            throw new IllegalArgumentException("parserVersion must not be blank");
        }
        if (maxDocumentBytes <= 0) {
            throw new IllegalArgumentException("maxDocumentBytes must be positive");
        }
        if (deadline == null || deadline.isZero() || deadline.isNegative()) {
            throw new IllegalArgumentException("deadline must be positive");
        }
    }

    /** The default configuration. */
    public static GrparseAdapterOptions defaults() {
        return new GrparseAdapterOptions(
                DEFAULT_PARSER_VERSION, DEFAULT_MAX_DOCUMENT_BYTES, DEFAULT_DEADLINE);
    }
}
