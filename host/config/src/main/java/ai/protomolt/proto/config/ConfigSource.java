package ai.protomolt.proto.config;

import java.util.Optional;

/**
 * Where config documents come from: one subject resolves to at most one
 * versioned payload. A source is a reader, deliberately nothing more —
 * no watches, no sessions, no coordination protocol. The registry plug
 * reads the git-backed registry, the Kafka plug reads a compacted topic
 * through the house serde, and a test hands out maps; the consumer
 * ({@link DistributedConfig}) owns parsing, validation, and the atomic
 * swap, identically over every source.
 */
public interface ConfigSource extends AutoCloseable {

    /**
     * The subject's current document, or empty when the source has none.
     *
     * @param subject the config subject
     * @return the versioned payload, or empty
     * @throws Exception when the source cannot answer; the consumer keeps
     *         serving its current config and surfaces the failure
     */
    Optional<Fetched> fetch(String subject) throws Exception;

    /**
     * One fetched document.
     *
     * @param version the source's version of this document (a git commit,
     *        a topic offset); the consumer skips re-applying an unchanged
     *        version and reports the applied one as evidence
     * @param payload the serialized protobuf of the subject's declared type
     */
    record Fetched(String version, byte[] payload) {

        public Fetched {
            if (version == null || version.isBlank()) {
                throw new IllegalArgumentException("version must not be blank");
            }
            if (payload == null) {
                throw new IllegalArgumentException("payload must not be null");
            }
            payload = payload.clone();
        }
    }

    @Override
    default void close() {
    }
}
