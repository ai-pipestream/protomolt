package ai.pipestream.proto.parse.grparse;

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
 */
public record GrparseAdapterOptions(String parserVersion, long maxDocumentBytes, Duration deadline) {

    /** The default advertised parser version. */
    public static final String DEFAULT_PARSER_VERSION = "1.0.0";

    /** The default per-document byte cap: 256 MiB. */
    public static final long DEFAULT_MAX_DOCUMENT_BYTES = 256L * 1024 * 1024;

    /** The default per-parse deadline on the gRParse stream. */
    public static final Duration DEFAULT_DEADLINE = Duration.ofMinutes(10);

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
