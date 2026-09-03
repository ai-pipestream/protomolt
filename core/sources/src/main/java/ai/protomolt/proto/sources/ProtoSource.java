package ai.protomolt.proto.sources;

import java.util.Objects;

/**
 * One {@code .proto} file as text, keyed by its import path.
 *
 * @param path    import path of the file, e.g. {@code common/v1/core.proto}; this is the path
 *                other files use in their {@code import} statements
 * @param content full {@code .proto} source text
 * @param origin  human-readable provenance, e.g. {@code git:https://…@main},
 *                {@code jar:common-protos-1.2.jar}, {@code file:/schemas/core.proto};
 *                used in diagnostics only
 */
public record ProtoSource(String path, String content, String origin) {

    public ProtoSource {
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(content, "content");
        Objects.requireNonNull(origin, "origin");
        if (path.isBlank()) {
            throw new IllegalArgumentException("path must not be blank");
        }
    }

    /** Creates a source with an unspecified origin. */
    public static ProtoSource of(String path, String content) {
        return new ProtoSource(path, content, "unspecified");
    }
}
