package ai.pipestream.proto.index.spi;

import com.google.protobuf.Message;

/**
 * Gate offered every message unpacked from a {@code google.protobuf.Any} during write-time
 * mapping expansion, before the payload's fields are mapped and indexed.
 *
 * <p>{@link AnyIndexing} discovers implementations via {@link java.util.ServiceLoader} and
 * runs them in discovery order. An implementation signals a violation by throwing its
 * standard's (unchecked) exception, which aborts the document — an invalid payload is never
 * partially indexed. The {@code protomolt-protobuf-indexing} module registers the
 * declared-rules validation standard ({@code ai.pipestream.proto.validate.v1} and, when its
 * optional reader is present, {@code buf.validate}), which validates clean at no cost for
 * payload types that declare no rules and honors the standard's own escape hatches
 * ({@code skip_when}, per-field {@code ignore}).
 */
@FunctionalInterface
public interface AnyPayloadValidator {

    /**
     * Checks one unpacked payload.
     *
     * @param unpacked the message unpacked from the Any
     * @param path dotted proto-name path of the Any field from the root message
     */
    void validate(Message unpacked, String path);
}
